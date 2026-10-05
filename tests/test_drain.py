import asyncio

from typing import Any
from unittest.mock import AsyncMock

from sanic import Sanic
from sanic.asgi import Lifespan
from sanic.drain import (
    DrainCoordinator,
    DrainStage,
    LeaseOutcome,
    StopReason,
    WorkKind,
)
from sanic.response import empty, text
from sanic.worker.inspector import Inspector


# ---------------------------------------------------------------------- #
# Coordinator unit semantics
# ---------------------------------------------------------------------- #


async def test_empty_drain_is_immediate_and_idempotent():
    coordinator = DrainCoordinator(
        graceful_timeout=0.2, cancel_timeout=0.1, tick=0.02
    )

    result = await coordinator.drain(reason=StopReason.SIGNAL)

    assert coordinator.stage is DrainStage.DRAINED
    assert result.summary() == {
        "completed": 0,
        "cancelled": 0,
        "deferred": 0,
        "forced": 0,
    }
    # 重复停止合并为同一次排空，返回同一个结果。
    assert await coordinator.drain() is result


async def test_lease_completes_within_grace():
    coordinator = DrainCoordinator(
        graceful_timeout=1.0, cancel_timeout=0.1, tick=0.02
    )
    lease = coordinator.admit(WorkKind.REQUEST, name="fast")

    async def worker():
        await asyncio.sleep(0.03)
        lease.release()

    asyncio.create_task(worker())
    result = await coordinator.drain()

    assert len(result.completed) == 1
    assert result.completed[0].name == "fast"


async def test_lease_is_cancelled_at_soft_deadline():
    coordinator = DrainCoordinator(
        graceful_timeout=0.1, cancel_timeout=0.5, tick=0.02
    )
    task = asyncio.create_task(asyncio.sleep(10))
    coordinator.admit(WorkKind.REQUEST, name="slow", task=task)

    result = await coordinator.drain()

    assert task.cancelled()
    assert len(result.cancelled) == 1
    assert not result.forced
    assert result.cancelled[0].detail == "soft deadline reached"


async def test_uncooperative_work_is_forced_at_hard_deadline():
    async def stubborn():
        while True:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                # 故意吞掉取消，模拟无视取消的工作。
                continue

    coordinator = DrainCoordinator(
        graceful_timeout=0.05, cancel_timeout=0.05, settle_timeout=0.05,
        tick=0.02,
    )
    task = asyncio.create_task(stubborn())
    forced = asyncio.Event()
    coordinator.admit(
        WorkKind.TASK,
        name="stubborn",
        task=task,
        on_force=forced.set,
    )

    result = await coordinator.drain()
    task.cancel()

    assert len(result.forced) == 1
    assert forced.is_set()


async def test_definable_work_is_handed_off():
    coordinator = DrainCoordinator(
        graceful_timeout=0.1, cancel_timeout=0.1, tick=0.02
    )
    handed_off = []
    lease = coordinator.admit(
        WorkKind.STREAM,
        name="stream",
        extendable=True,
        deadline=1_000_000.0,
        on_defer=lambda: handed_off.append("handoff"),
    )

    result = await coordinator.drain()

    assert handed_off == ["handoff"]
    assert len(result.deferred) == 1
    assert lease.outcome is LeaseOutcome.DEFERRED


async def test_deferred_lease_waiting_is_cancelled_after_its_deadline():
    coordinator = DrainCoordinator(
        graceful_timeout=0.05, cancel_timeout=0.4, tick=0.02
    )
    # 可延期但没有移交钩子：允许继续等待到被裁剪后的截止时间，
    # 这里把自有截止时间压到很近，随后应被取消而非无限等待。
    from time import monotonic

    task = asyncio.create_task(asyncio.sleep(10))
    lease = coordinator.admit(
        WorkKind.STREAM,
        extendable=True,
        deadline=monotonic() + 0.1,
        task=task,
    )
    result = await coordinator.drain()

    assert task.cancelled()
    assert len(result.cancelled) == 1
    # 软截止 0.05，延期截止约 0.1，应在硬截止（0.45）之前被取消。
    assert result.duration < 0.45
    assert lease.outcome is LeaseOutcome.CANCELLED


async def test_multiple_stop_sources_merge_and_hard_escalates():
    coordinator = DrainCoordinator(
        graceful_timeout=5.0, cancel_timeout=1.0, tick=0.02
    )
    task = asyncio.create_task(asyncio.sleep(10))
    coordinator.admit(WorkKind.REQUEST, task=task)

    asyncio.create_task(coordinator.drain(reason=StopReason.SIGNAL))
    await asyncio.sleep(0.03)

    # 重复信号源不产生第二次排空；hard 立即升级为强制。
    coordinator.begin_drain(StopReason.SIGNAL)
    coordinator.begin_drain(StopReason.LOCAL_API, hard=True)

    result = await coordinator.drain()
    assert set(result.reasons) == {"signal", "local_api"}
    assert result.duration < 1.0
    assert task.cancelled()


async def test_newcomer_lease_is_cancelled_not_extended():
    coordinator = DrainCoordinator(
        graceful_timeout=0.3, cancel_timeout=0.3, tick=0.02
    )
    # 先有存量租约，使排空停留在 DRAINING。
    coordinator.admit(
        WorkKind.REQUEST, task=asyncio.create_task(asyncio.sleep(10))
    )
    asyncio.create_task(coordinator.drain())
    await asyncio.sleep(0.03)
    assert coordinator.stage is DrainStage.DRAINING

    newcomer = coordinator.admit(
        WorkKind.TASK,
        extendable=True,
        task=asyncio.create_task(asyncio.sleep(10)),
    )

    assert newcomer is not None
    assert newcomer.newcomer is True
    # 排空期间新生的租约没有资格延期。
    assert newcomer.extendable is False

    while coordinator.stage is not DrainStage.DRAINED:
        await asyncio.sleep(0.02)

    # 终态拒绝一切新租约。
    assert coordinator.admit(WorkKind.TASK) is None


async def test_cease_hook_failure_does_not_break_lifecycle():
    coordinator = DrainCoordinator(
        graceful_timeout=0.05, cancel_timeout=0.05, tick=0.02
    )

    def boom():
        raise RuntimeError("listener exploded")

    coordinator.cease_on_drain(boom)
    coordinator.cease_on_drain(lambda: None)
    result = await coordinator.drain()

    assert coordinator.stage is DrainStage.DRAINED
    assert len(result.listener_failures) == 1
    assert "listener exploded" in result.listener_failures[0]


async def test_per_kind_timeout_short_circuits_wait():
    coordinator = DrainCoordinator(
        graceful_timeout=5.0,
        cancel_timeout=0.2,
        tick=0.02,
        kind_timeouts={WorkKind.REQUEST: 0.05},
    )
    task = asyncio.create_task(asyncio.sleep(10))
    coordinator.admit(WorkKind.REQUEST, task=task)

    result = await coordinator.drain()

    assert result.duration < 0.6
    assert task.cancelled()


async def test_short_kind_timeout_does_not_cancel_other_kinds_early():
    # REQUEST 类别 0.1s 到期，STREAM 类别应享受到全局软截止 0.6s：
    # 流式租约在约 0.35s 自然完成，必须被等待，而不是在 0.1s 被取消。
    coordinator = DrainCoordinator(
        graceful_timeout=0.6,
        cancel_timeout=0.2,
        tick=0.02,
        kind_timeouts={
            WorkKind.REQUEST: 0.1,
            WorkKind.STREAM: 5.0,
        },
    )
    request_task = asyncio.create_task(asyncio.sleep(10))
    stream_lease = coordinator.admit(
        WorkKind.STREAM, name="stream"
    )
    coordinator.admit(WorkKind.REQUEST, task=request_task)

    async def finish_stream():
        await asyncio.sleep(0.35)
        stream_lease.release()

    asyncio.create_task(finish_stream())
    result = await coordinator.drain()

    assert request_task.cancelled()
    assert len(result.completed) == 1
    assert result.completed[0].kind is WorkKind.STREAM
    # 被取消的只有短宽限的 REQUEST，STREAM 绝不能被提前取消。
    assert [r.kind for r in result.cancelled] == [WorkKind.REQUEST]


async def test_status_snapshot_shape_and_counts():
    coordinator = DrainCoordinator()
    coordinator.admit(WorkKind.REQUEST, name="r")
    coordinator.admit(WorkKind.STREAM, name="s")
    coordinator.admit(WorkKind.STREAM, name="s2")

    snapshot = coordinator.status()

    assert snapshot["stage"] == "idle"
    assert snapshot["active_total"] == 3
    assert snapshot["active_by_kind"] == {
        "request": 1,
        "stream": 2,
        "task": 0,
    }
    assert {item["name"] for item in snapshot["leases"]} == {"r", "s", "s2"}


async def test_publisher_receives_stage_transitions():
    coordinator = DrainCoordinator(
        graceful_timeout=0.05, cancel_timeout=0.05, tick=0.02,
        publish_interval=0.0,
    )
    seen = []
    coordinator.add_publisher(lambda snapshot: seen.append(snapshot["stage"]))

    await coordinator.drain()

    assert "draining" in seen
    assert seen[-1] == "drained"


async def test_reset_restores_idle_for_next_run():
    coordinator = DrainCoordinator(
        graceful_timeout=0.05, cancel_timeout=0.05, tick=0.02
    )
    seen = []
    coordinator.add_publisher(lambda snapshot: seen.append(snapshot))
    await coordinator.drain()
    assert coordinator.stage is DrainStage.DRAINED

    coordinator.reset()

    assert coordinator.stage is DrainStage.IDLE
    assert coordinator.active() == []
    # 发布者在 reset 后仍然保留。
    assert coordinator._publishers


async def test_reclassify_switches_kind_and_extendability():
    coordinator = DrainCoordinator()
    lease = coordinator.admit(WorkKind.REQUEST)

    coordinator.reclassify(lease, WorkKind.STREAM, extendable=True)

    assert lease.kind is WorkKind.STREAM
    assert lease.extendable is True
    assert coordinator.active(WorkKind.STREAM) == [lease]
    lease.release()


# ---------------------------------------------------------------------- #
# Framework integration: real rolling shutdown
# ---------------------------------------------------------------------- #


def test_long_request_is_waited_for_and_recorded(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 2
    app.config.DRAIN_CANCEL_TIMEOUT = 1

    @app.get("/")
    async def handler(request):
        await asyncio.sleep(0.3)
        return text("done")

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        # 让请求先进入处理器再触发停机。
        await asyncio.sleep(0.05)
        sanic.stop()

    app.run(single_process=True, port=port)

    result = app.drain_coordinator._result
    assert result is not None
    assert result.summary()["completed"] >= 1


def test_request_past_deadline_is_aborted(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0.3
    app.config.DRAIN_CANCEL_TIMEOUT = 0.3

    @app.get("/")
    async def handler(request):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            # 处理器合作清理：返回一个响应也可能来不及，直接重抛。
            raise
        return text("unreachable")

    disconnected = asyncio.Event()

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")

        async def watch():
            await reader.read()
            disconnected.set()

        asyncio.create_task(watch())
        await asyncio.sleep(0.05)
        sanic.stop()

    app.run(single_process=True, port=port)

    assert disconnected.is_set()
    result = app.drain_coordinator._result
    assert result is not None
    assert result.summary()["cancelled"] >= 1
    assert result.duration < 2.5


def test_background_task_is_drained_not_waited_forever(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0.3
    app.config.DRAIN_CANCEL_TIMEOUT = 0.3

    @app.listener("after_server_start")
    async def _spawn(sanic, loop):
        sanic.add_task(asyncio.sleep(10), name="long.background")
        await asyncio.sleep(0.05)
        sanic.stop()

    @app.get("/")
    async def handler(request):
        return empty()

    app.run(single_process=True, port=port)

    result = app.drain_coordinator._result
    assert result is not None
    assert result.duration < 2.5


def test_streaming_response_is_drained_as_stream_kind(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 2
    app.config.DRAIN_CANCEL_TIMEOUT = 1

    @app.get("/stream")
    async def stream_handler(request):
        response = await request.respond(content_type="text/plain")
        for piece in ("a", "b", "c"):
            await asyncio.sleep(0.15)
            await response.send(piece)
        await response.eof()

    body_parts = []

    @app.listener("after_server_start")
    async def _drive(sanic, loop):
        async def client():
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", port
            )
            writer.write(
                b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n"
            )
            data = await reader.read()
            body_parts.append(data)
            writer.close()

        asyncio.create_task(client())
        # 让首个分块先发出（租约此时已改类为 STREAM），再停机。
        await asyncio.sleep(0.2)
        sanic.stop()

    app.run(single_process=True, port=port)

    joined = b"".join(body_parts)
    assert b"200" in joined
    # chunked 编码下三个分块与终止块都必须完整送达，
    # 证明排空等待了流式响应（而非提前切断）。
    assert b"1\r\na\r\n" in joined
    assert b"1\r\nb\r\n" in joined
    assert b"1\r\nc\r\n" in joined
    assert joined.rstrip().endswith(b"0\r\n\r\n".rstrip())
    result = app.drain_coordinator._result
    assert result is not None
    kinds = {r.kind for r in result.completed}
    assert WorkKind.STREAM in kinds


def test_drain_status_visible_while_serving(app, port):
    snapshots: list[dict[str, Any]] = []

    @app.listener("after_server_start")
    async def _snapshot(sanic, loop):
        snapshots.append(sanic.drain_status())
        sanic.stop()

    @app.get("/")
    async def handler(request):
        return empty()

    app.run(single_process=True, port=port)

    assert snapshots
    assert snapshots[0]["stage"] == "idle"
    assert "active_by_kind" in snapshots[0]


# ---------------------------------------------------------------------- #
# Inspector local management action
# ---------------------------------------------------------------------- #


def test_inspector_drain_action_aggregates_worker_state():
    from unittest.mock import MagicMock

    publisher = MagicMock()
    worker_state = {
        "Sanic-Server-0": {
            "server": "Server",
            "drain": {"stage": "draining", "active_total": 2},
        },
        "Sanic-Reloader-0": {"server": False},
    }
    inspector = Inspector(
        publisher=publisher,
        app_info={},
        worker_state=worker_state,
        host="localhost",
        port=6457,
        api_key="",
        tls_key=None,
        tls_cert=None,
    )

    # 纯查询不触发停止消息。
    output = inspector.drain()
    assert output["count"] == 1
    assert output["workers"]["Sanic-Server-0"]["stage"] == "draining"
    publisher.send.assert_not_called()

    # trigger=True 发送终止消息，作为又一个停止源合并进排空。
    inspector.drain(trigger=True)
    publisher.send.assert_called_once_with("__TERMINATE__")


# ---------------------------------------------------------------------- #
# ASGI lifespan drain
# ---------------------------------------------------------------------- #


async def test_asgi_lifespan_drains(app: Sanic):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0.1
    app.config.DRAIN_CANCEL_TIMEOUT = 0.1

    recv = AsyncMock(
        side_effect=[
            {"type": "lifespan.startup"},
            {"type": "lifespan.shutdown"},
        ]
    )
    send = AsyncMock()
    app.asgi = True
    await app._startup()

    lifespan = Lifespan(app, {"type": "lifespan"}, recv, send)
    await lifespan()

    send.assert_any_call({"type": "lifespan.startup.complete"})
    send.assert_any_call({"type": "lifespan.shutdown.complete"})
    assert app.drain_coordinator.stage is DrainStage.DRAINED
