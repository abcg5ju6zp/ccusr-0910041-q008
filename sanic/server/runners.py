from __future__ import annotations

from ssl import SSLContext
from typing import TYPE_CHECKING

from sanic.config import Config
from sanic.exceptions import ServerError
from sanic.http.constants import HTTP
from sanic.http.tls import get_ssl_context


if TYPE_CHECKING:
    from sanic.app import Sanic

import asyncio
import os
import socket

from functools import partial
from signal import SIG_IGN, SIGINT, SIGTERM
from signal import signal as signal_func
from typing import Callable

from sanic.application.ext import setup_ext
from sanic.compat import OS_IS_WINDOWS, ctrlc_workaround_for_windows
from sanic.http.http3 import SessionTicketStore, get_config
from sanic.log import error_logger, server_logger
from sanic.logging.setup import setup_logging
from sanic.models.server_types import Signal
from sanic.server.async_server import AsyncioServer
from sanic.server.protocols.http_protocol import Http3Protocol, HttpProtocol
from sanic.server.socket import bind_unix_socket, remove_unix_socket


try:
    from aioquic.asyncio import serve as quic_serve

    HTTP3_AVAILABLE = True
except ModuleNotFoundError:  # no cov
    HTTP3_AVAILABLE = False


def serve(
    host,
    port,
    app: Sanic,
    ssl: SSLContext | None = None,
    sock: socket.socket | None = None,
    unix: str | None = None,
    reuse_port: bool = False,
    loop=None,
    protocol: type[asyncio.Protocol] = HttpProtocol,
    backlog: int = 100,
    register_sys_signals: bool = True,
    run_multiple: bool = False,
    run_async: bool = False,
    connections=None,
    signal=Signal(),
    state=None,
    asyncio_server_kwargs=None,
    version=HTTP.VERSION_1,
):
    """项目内部接口说明。"""
    if not run_async and not loop:
        # create new event_loop after fork
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    setup_logging(app.debug, app.config.NO_COLOR, app.config.LOG_EXTRA)

    if app.debug:
        loop.set_debug(app.debug)

    app.asgi = False

    if version is HTTP.VERSION_3:
        return _serve_http_3(host, port, app, loop, ssl)
    return _serve_http_1(
        host,
        port,
        app,
        ssl,
        sock,
        unix,
        reuse_port,
        loop,
        protocol,
        backlog,
        register_sys_signals,
        run_multiple,
        run_async,
        connections,
        signal,
        state,
        asyncio_server_kwargs,
    )


def _setup_system_signals(
    app: Sanic,
    run_multiple: bool,
    register_sys_signals: bool,
    loop: asyncio.AbstractEventLoop,
) -> None:  # no cov
    signal_func(SIGINT, SIG_IGN)
    signal_func(SIGTERM, SIG_IGN)
    os.environ["SANIC_WORKER_PROCESS"] = "true"
    # Register signals for graceful termination
    if register_sys_signals:
        if OS_IS_WINDOWS:
            ctrlc_workaround_for_windows(app)
        else:
            for _signal in [SIGINT, SIGTERM]:
                loop.add_signal_handler(
                    _signal, partial(app.request_stop, signal=_signal)
                )


def _register_escalation_signals(
    app: Sanic, register_sys_signals: bool, loop
) -> Callable[[], None] | None:
    """排空进行中再次收到停止信号时升级为强制取消。

    返回一个清理回调。与正常停机信号不同，这里不能调用
    ``loop.stop()``（会打断正在驱动排空的 run_until_complete），
    只需通知协调器中断等待。``register_sys_signals=False`` 时
    不注册任何处理器。
    """
    if not register_sys_signals:
        return None

    registered = []

    def _escalate(signum):
        app.drain_coordinator.begin_drain(
            "signal", hard=True
        )
        server_logger.warning(
            "Second stop signal received (%s); escalating drain", signum
        )

    for _signal in (SIGINT, SIGTERM):
        try:
            loop.add_signal_handler(_signal, partial(_escalate, _signal))
            registered.append(_signal)
        except (NotImplementedError, RuntimeError):
            pass

    def _cleanup_signals():
        for _signal in registered:
            try:
                loop.remove_signal_handler(_signal)
            except (NotImplementedError, OSError):
                pass

    return _cleanup_signals


def _run_shutdown_coro(loop, coro):
    """项目内部接口说明。"""
    # Clear asyncio's stopped state if accessible
    if hasattr(loop, "_stopping"):
        loop._stopping = False

    try:
        loop.run_until_complete(coro())
    except (RuntimeError, KeyboardInterrupt):
        # RuntimeError: loop was stopped (uvloop behavior)
        # KeyboardInterrupt: signal arrived during select (asyncio behavior)
        # Try once more - this handles uvloop's behavior where the first
        # run_until_complete after stop() fails but subsequent calls succeed.
        if hasattr(loop, "_stopping"):
            loop._stopping = False
        try:
            loop.run_until_complete(coro())
        except (RuntimeError, KeyboardInterrupt):
            # If it still fails, the loop is truly unusable
            pass


def _run_server_forever(
    loop, before_stop, after_stop, cleanup, unix, pid, escalate=None
):
    escalation_cleanup = None
    try:
        server_logger.info("Worker ready [%s]", pid)
        loop.run_forever()
    finally:
        server_logger.info("Stopping worker [%s]", pid)

        for _signal in [SIGINT, SIGTERM]:
            try:
                loop.remove_signal_handler(_signal)
            except (NotImplementedError, OSError):
                pass

        # 排空期间再次收到停止信号 -> 升级为强制取消，而不是
        # 让信号被忽略（旧行为）或打断正在驱动排空的协程。
        if escalate is not None:
            escalation_cleanup = escalate(loop)

        _run_shutdown_coro(loop, before_stop)

        if cleanup:
            cleanup()

        if escalation_cleanup is not None:
            escalation_cleanup()

        _run_shutdown_coro(loop, after_stop)

        remove_unix_socket(unix)
        loop.close()
        server_logger.info("Worker complete [%s]", pid)


def _setup_drain_publisher(app: Sanic) -> None:
    """把排空快照发布到多进程共享的 worker_state，供 Inspector 查询。

    仅在 worker 进程（存在 multiplexer）中生效；写入已由协调器按
    时间间隔节流，避免高频 IPC。
    """
    multiplexer = getattr(app, "multiplexer", None)
    if multiplexer is None:
        return

    def _publish(snapshot: dict) -> None:
        try:
            multiplexer.state["drain"] = snapshot
        except (
            BrokenPipeError,
            ConnectionRefusedError,
            ConnectionResetError,
            EOFError,
        ):
            pass

    app.drain_coordinator.add_publisher(_publish)


def _serve_http_1(
    host,
    port,
    app,
    ssl,
    sock,
    unix,
    reuse_port,
    loop,
    protocol,
    backlog,
    register_sys_signals,
    run_multiple,
    run_async,
    connections,
    signal,
    state,
    asyncio_server_kwargs,
):
    connections = connections if connections is not None else set()
    protocol_kwargs = _build_protocol_kwargs(protocol, app.config)
    server = partial(
        protocol,
        loop=loop,
        connections=connections,
        signal=signal,
        app=app,
        state=state,
        unix=unix,
        **protocol_kwargs,
    )
    asyncio_server_kwargs = (
        asyncio_server_kwargs if asyncio_server_kwargs else {}
    )
    if OS_IS_WINDOWS and sock:
        pid = os.getpid()
        sock = sock.share(pid)
        sock = socket.fromshare(sock)
    # UNIX sockets are always bound by us (to preserve semantics between modes)
    elif unix:
        sock = bind_unix_socket(unix, backlog=backlog)
    server_coroutine = loop.create_server(
        server,
        None if sock else host,
        None if sock else port,
        ssl=ssl,
        reuse_port=reuse_port,
        sock=sock,
        backlog=backlog,
        **asyncio_server_kwargs,
    )

    setup_ext(app)
    if run_async:
        return AsyncioServer(
            app=app,
            loop=loop,
            serve_coro=server_coroutine,
            connections=connections,
        )

    pid = os.getpid()
    server_logger.info("Starting worker [%s]", pid)
    # 允许同一 app 多次启动（典型：测试）：把上一轮的排空状态清零。
    app.drain_coordinator.reset()
    loop.run_until_complete(app._startup())
    loop.run_until_complete(app._server_event("init", "before"))
    app.ack()

    try:
        http_server = loop.run_until_complete(server_coroutine)
    except BaseException:
        error_logger.exception("Unable to start server", exc_info=True)
        return

    def _cleanup():
        # 排空由协调器统一裁决，而不是盲目轮询 connections：
        #   1. 关闭 listening socket，停止接收新连接；
        #   2. 关闭空闲连接，标记 signal.stopped；
        #   3. 等待租约（长请求/流式响应/后台任务）按截止时间
        #      完成、取消或延期；
        #   4. 对无视取消的工作强制执行（abort 传输层）。
        http_server.close()
        loop.run_until_complete(http_server.wait_closed())

        signal.stopped = True
        for connection in connections:
            connection.close_if_idle()

        coordinator = app.drain_coordinator
        result = loop.run_until_complete(
            coordinator.drain(reason="signal")
        )

        # 兜底：强制关闭仍残留的连接（正常情况下租约的 force
        # 钩子已经 abort 了它们）。
        for conn in connections:
            if hasattr(conn, "websocket") and conn.websocket:
                conn.websocket.fail_connection(code=1001)
            else:
                conn.abort()

        # 只在确有租约终局或监听器失败时输出排空详情；空排空保持安静。
        if result.records or result.listener_failures:
            server_logger.info("Drain result: %s", result.summary())
            for record in result.records:
                server_logger.info("  drained lease: %s", record.to_dict())
            for failure in result.listener_failures:
                server_logger.warning("  drain listener failure: %s", failure)

        try:
            app.set_serving(False)
        except (BrokenPipeError, ConnectionResetError, EOFError):
            pass

    _setup_system_signals(app, run_multiple, register_sys_signals, loop)
    _setup_drain_publisher(app)
    loop.run_until_complete(app._server_event("init", "after"))
    app.set_serving(True)
    _run_server_forever(
        loop,
        partial(app._server_event_safe, "shutdown", "before"),
        partial(app._server_event_safe, "shutdown", "after"),
        _cleanup,
        unix,
        pid,
        escalate=partial(
            _register_escalation_signals, app, register_sys_signals
        ),
    )


def _serve_http_3(
    host,
    port,
    app,
    loop,
    ssl,
    register_sys_signals: bool = True,
    run_multiple: bool = False,
):
    if not HTTP3_AVAILABLE:
        raise ServerError(
            "Cannot run HTTP/3 server without aioquic installed. "
        )
    pid = os.getpid()
    server_logger.info("Starting worker [%s]", pid)
    protocol = partial(Http3Protocol, app=app)
    ticket_store = SessionTicketStore()
    ssl_context = get_ssl_context(app, ssl)
    config = get_config(app, ssl_context)
    coro = quic_serve(
        host,
        port,
        configuration=config,
        create_protocol=protocol,
        session_ticket_fetcher=ticket_store.pop,
        session_ticket_handler=ticket_store.add,
    )
    server = AsyncioServer(app, loop, coro, [])
    loop.run_until_complete(server.startup())
    loop.run_until_complete(server.before_start())
    app.ack()
    loop.run_until_complete(server)
    _setup_system_signals(app, run_multiple, register_sys_signals, loop)
    loop.run_until_complete(server.after_start())

    # TODO: Create connection cleanup and graceful shutdown
    cleanup = None
    _run_server_forever(
        loop, server.before_stop, server.after_stop, cleanup, None, pid
    )


def _build_protocol_kwargs(
    protocol: type[asyncio.Protocol], config: Config
) -> dict[str, int | float]:
    if hasattr(protocol, "websocket_handshake"):
        return {
            "websocket_max_size": config.WEBSOCKET_MAX_SIZE,
            "websocket_ping_timeout": config.WEBSOCKET_PING_TIMEOUT,
            "websocket_ping_interval": config.WEBSOCKET_PING_INTERVAL,
        }
    return {}
