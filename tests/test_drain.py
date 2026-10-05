import asyncio
import logging

import pytest

from sanic.response import text
from sanic.server.drain import (
    DrainClosedError,
    DrainCoordinator,
    DrainExtensionDenied,
    DrainOutcome,
    DrainPhase,
    DrainPolicy,
    LeaseKind,
)


# -------------------------------------------------------------------- #
# Lease tracking
# -------------------------------------------------------------------- #


def test_lease_acquire_and_release():
    coordinator = DrainCoordinator()
    lease = coordinator.acquire(LeaseKind.REQUEST, name="GET /")

    assert lease.lease_id == 1
    assert lease.kind is LeaseKind.REQUEST
    assert lease.late is False

    status = coordinator.status()
    assert status["phase"] == "idle"
    assert status["leases"]["active"] == 1
    assert status["leases"]["by_kind"] == {"request": 1}

    assert coordinator.release(lease.lease_id) is True
    status = coordinator.status()
    assert status["leases"]["active"] == 0
    assert status["leases"]["completed"] == 1


def test_lease_release_is_idempotent():
    coordinator = DrainCoordinator()
    lease = coordinator.acquire(LeaseKind.TASK, name="job")

    assert coordinator.release(lease.lease_id) is True
    assert coordinator.release(lease.lease_id) is False
    assert coordinator.release(9999) is False
    assert coordinator.status()["leases"]["completed"] == 1


def test_lease_upgrade_changes_kind():
    coordinator = DrainCoordinator()
    lease = coordinator.acquire(LeaseKind.REQUEST, name="GET /stream")

    assert coordinator.upgrade(lease.lease_id, LeaseKind.STREAM) is True
    assert coordinator.upgrade(9999, LeaseKind.STREAM) is False

    status = coordinator.status()
    assert status["leases"]["by_kind"] == {"stream": 1}
    assert status["leases"]["items"][0]["kind"] == "stream"


def test_acquire_after_done_raises():
    coordinator = DrainCoordinator(DrainPolicy(timeout=0.01))
    coordinator.begin(source="test")
    coordinator._finish(DrainOutcome.COMPLETED)

    with pytest.raises(DrainClosedError):
        coordinator.acquire(LeaseKind.TASK, name="too-late")


# -------------------------------------------------------------------- #
# Drain lifecycle
# -------------------------------------------------------------------- #


async def test_drain_completes_when_leases_release():
    coordinator = DrainCoordinator(DrainPolicy(timeout=1.0))
    lease = coordinator.acquire(LeaseKind.REQUEST, name="GET /slow")

    async def release_soon():
        await asyncio.sleep(0.05)
        coordinator.release(lease.lease_id)

    asyncio.create_task(release_soon())
    report = await coordinator.drain(source="test")

    assert report.outcome is DrainOutcome.COMPLETED
    assert report.completed == 1
    assert report.cancelled == ()
    assert coordinator.done


async def test_drain_completes_immediately_without_leases():
    coordinator = DrainCoordinator(DrainPolicy(timeout=5.0))
    report = await coordinator.drain(source="test")

    assert report.outcome is DrainOutcome.COMPLETED
    assert report.elapsed < 1.0


async def test_drain_cancels_remaining_leases_at_deadline():
    coordinator = DrainCoordinator(DrainPolicy(timeout=0.1, cancel_grace=0.05))
    cancelled = []
    coordinator.acquire(
        LeaseKind.TASK, name="stuck", cancel=lambda: cancelled.append("stuck")
    )
    coordinator.acquire(
        LeaseKind.STREAM,
        name="stream",
        cancel=lambda: cancelled.append("stream"),
    )

    report = await coordinator.drain(source="test")

    assert report.outcome is DrainOutcome.CANCELLED
    assert sorted(cancelled) == ["stream", "stuck"]
    assert len(report.cancelled) == 2
    status = coordinator.status()
    assert status["leases"]["cancelled"] == 2
    assert status["leases"]["active"] == 0


async def test_drain_cancel_grace_allows_unwind():
    coordinator = DrainCoordinator(DrainPolicy(timeout=0.05, cancel_grace=0.5))
    loop = asyncio.get_running_loop()

    def cancel():
        # Simulate a task that unwinds shortly after being cancelled
        loop.call_later(0.05, lambda: coordinator.release(lease.lease_id))

    lease = coordinator.acquire(LeaseKind.TASK, name="bg", cancel=cancel)
    report = await coordinator.drain(source="test")

    assert report.outcome is DrainOutcome.CANCELLED
    assert report.cancelled == (lease.lease_id,)
    # The lease was released during the grace window
    assert coordinator.status()["leases"]["active"] == 0


async def test_late_lease_is_tracked_and_awaited():
    coordinator = DrainCoordinator(DrainPolicy(timeout=1.0))
    # An in-flight lease keeps the drain open while the late one arrives
    inflight = coordinator.acquire(LeaseKind.REQUEST, name="GET /")
    coordinator.begin(source="test")

    async def spawn_late():
        await asyncio.sleep(0.02)
        lease = coordinator.acquire(LeaseKind.TASK, name="born-late")
        assert lease.late is True
        await asyncio.sleep(0.02)
        coordinator.release(lease.lease_id)
        coordinator.release(inflight.lease_id)

    asyncio.create_task(spawn_late())
    report = await coordinator.drain(source="test")

    assert report.outcome is DrainOutcome.COMPLETED
    assert report.late == 1


async def test_waiter_cancellation_does_not_kill_drain():
    coordinator = DrainCoordinator(DrainPolicy(timeout=1.0))
    lease = coordinator.acquire(LeaseKind.TASK, name="bg")
    coordinator.begin(source="test")

    async def release_soon():
        await asyncio.sleep(0.05)
        coordinator.release(lease.lease_id)

    asyncio.create_task(release_soon())

    waiter = asyncio.create_task(coordinator.wait())
    await asyncio.sleep(0.01)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    # The drain itself survives and still resolves for other waiters
    report = await coordinator.wait()
    assert report.outcome is DrainOutcome.COMPLETED


# -------------------------------------------------------------------- #
# Stop source merging
# -------------------------------------------------------------------- #


async def test_multiple_stop_sources_merge_into_one_drain():
    coordinator = DrainCoordinator(DrainPolicy(timeout=0.5))

    first, second = await asyncio.gather(
        coordinator.drain(source="sigterm"),
        coordinator.drain(source="app.stop"),
    )

    assert first is second
    assert set(first.sources) == {"sigterm", "app.stop"}
    events = [event for _, event, _ in coordinator._events]
    assert events.count("drain.begin") == 1
    assert events.count("drain.done") == 1


async def test_repeated_drain_returns_same_report():
    coordinator = DrainCoordinator(DrainPolicy(timeout=0.05))

    first = await coordinator.drain(source="one")
    second = await coordinator.drain(source="two")

    assert first is second


def test_begin_is_idempotent_and_reports_initiator():
    coordinator = DrainCoordinator(DrainPolicy(timeout=1.0))

    assert coordinator.begin(source="first") is True
    assert coordinator.begin(source="second") is False
    assert coordinator.begin(source="second") is False

    status = coordinator.status()
    assert status["phase"] == "draining"
    assert status["sources"] == ["first", "second"]


def test_note_source_records_without_starting_drain():
    coordinator = DrainCoordinator()

    coordinator.note_source("app.stop")
    coordinator.note_source("app.stop")
    coordinator.note_source("sigterm")

    assert coordinator.phase is DrainPhase.IDLE
    assert coordinator.status()["sources"] == ["app.stop", "sigterm"]

    coordinator.begin(source="server.cleanup", timeout=0.01)
    assert coordinator.status()["sources"] == [
        "app.stop",
        "sigterm",
        "server.cleanup",
    ]


# -------------------------------------------------------------------- #
# Deadline extensions
# -------------------------------------------------------------------- #


async def test_extension_allows_completion_past_original_deadline():
    coordinator = DrainCoordinator(
        DrainPolicy(timeout=0.1, max_extensions=1, extension_step=0.2)
    )
    lease = coordinator.acquire(LeaseKind.STREAM, name="GET /feed")
    coordinator.begin(source="test")
    coordinator.extend(reason="client still reading")

    async def release_soon():
        await asyncio.sleep(0.2)
        coordinator.release(lease.lease_id)

    asyncio.create_task(release_soon())
    report = await coordinator.wait()

    assert report.outcome is DrainOutcome.EXTENDED
    assert report.extensions == 1
    assert report.elapsed > 0.1


def test_extension_denied_when_not_draining():
    coordinator = DrainCoordinator()

    with pytest.raises(DrainExtensionDenied):
        coordinator.extend()


def test_extension_denied_beyond_max_extensions():
    coordinator = DrainCoordinator(
        DrainPolicy(timeout=1.0, max_extensions=1, extension_step=0.1)
    )
    coordinator.begin(source="test")

    coordinator.extend()
    with pytest.raises(DrainExtensionDenied):
        coordinator.extend()


def test_extension_denied_beyond_cap():
    coordinator = DrainCoordinator(
        DrainPolicy(
            timeout=1.0,
            max_extensions=5,
            extension_step=2.0,
            extension_cap=3.0,
        )
    )
    coordinator.begin(source="test")

    coordinator.extend(2.0)
    with pytest.raises(DrainExtensionDenied):
        coordinator.extend(2.0)


def test_extension_moves_deadline():
    coordinator = DrainCoordinator(
        DrainPolicy(timeout=1.0, max_extensions=1, extension_step=0.5)
    )
    coordinator.begin(source="test")
    before = coordinator.status()["deadline"]

    new_deadline = coordinator.extend()

    assert new_deadline == before + 0.5
    status = coordinator.status()
    assert status["extensions"]["used"] == 1
    assert status["extensions"]["total"] == 0.5


# -------------------------------------------------------------------- #
# Status / introspection
# -------------------------------------------------------------------- #


async def test_status_snapshot_through_lifecycle():
    coordinator = DrainCoordinator(DrainPolicy(timeout=1.0))
    request = coordinator.acquire(LeaseKind.REQUEST, name="GET /a")
    coordinator.acquire(LeaseKind.TASK, name="job")

    coordinator.begin(source="test")
    status = coordinator.status()
    assert status["phase"] == "draining"
    assert status["outcome"] is None
    assert status["leases"]["active"] == 2
    assert status["leases"]["by_kind"] == {"request": 1, "task": 1}
    assert status["remaining"] > 0
    assert {item["name"] for item in status["leases"]["items"]} == {
        "GET /a",
        "job",
    }

    coordinator.release(request.lease_id)
    coordinator.release(2)
    report = await coordinator.wait()

    status = coordinator.status()
    assert status["phase"] == "done"
    assert status["outcome"] == "completed"
    assert status["remaining"] == 0.0
    assert status["leases"]["completed"] == 2
    assert report.outcome is DrainOutcome.COMPLETED
    # Lifecycle transitions are recorded in order
    events = [event["event"] for event in status["events"]]
    assert events == ["drain.begin", "drain.done"]


def test_transition_callback_receives_status():
    transitions = []
    coordinator = DrainCoordinator(
        DrainPolicy(timeout=1.0), on_transition=transitions.append
    )

    coordinator.begin(source="test")
    coordinator._finish(DrainOutcome.COMPLETED)

    assert [t["phase"] for t in transitions] == ["draining", "done"]


def test_transition_callback_failure_is_contained():
    def broken(_):
        raise RuntimeError("boom")

    coordinator = DrainCoordinator(
        DrainPolicy(timeout=1.0), on_transition=broken
    )
    coordinator.begin(source="test")
    report = coordinator._finish(DrainOutcome.COMPLETED)

    assert report.outcome is DrainOutcome.COMPLETED
    assert coordinator.done


# -------------------------------------------------------------------- #
# Application integration
# -------------------------------------------------------------------- #


def test_app_drain_status_starts_idle(app):
    status = app.drain_status

    assert status["phase"] == "idle"
    assert status["outcome"] is None
    assert status["leases"]["active"] == 0


def test_app_publishes_drain_status_to_multiplexer(app):
    published = []

    class FakeMultiplexer:
        def set_drain_status(self, status):
            published.append(status)

    app.multiplexer = FakeMultiplexer()
    app.drain_coordinator.begin(source="test")

    assert len(published) == 1
    assert published[0]["phase"] == "draining"


async def test_app_drain_merges_stop_sources(app):
    app.drain_coordinator.note_source("app.stop")
    report = await app.drain(timeout=0.5, source="server.cleanup")

    assert report.outcome is DrainOutcome.COMPLETED
    assert set(report.sources) == {"app.stop", "server.cleanup"}


def test_graceful_drain_completes_in_flight_request(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 5

    @app.get("/")
    async def handler(request):
        await asyncio.sleep(0.3)
        return text("ok")

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        app.stop()

    app.run(single_process=True, port=port)

    status = app.drain_status
    assert status["phase"] == "done"
    assert status["outcome"] == "completed"
    assert status["leases"]["completed"] == 1
    assert "app.stop" in status["sources"]


def test_drain_deadline_cancels_long_request(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0.5
    app.config.DRAIN_CANCEL_GRACE = 0.2

    @app.get("/")
    async def handler(request):
        await asyncio.sleep(30)

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        app.stop()

    app.run(single_process=True, port=port)

    status = app.drain_status
    assert status["outcome"] == "cancelled"
    assert status["leases"]["cancelled"] == 1


def test_listener_failure_preserves_lifecycle_order(app, port, caplog):
    events = []

    @app.listener("before_server_stop")
    async def bad_listener(sanic, loop):
        raise ValueError("listener exploded")

    @app.listener("after_server_stop")
    async def good_listener(sanic, loop):
        events.append("after_server_stop")

    @app.listener("after_server_start")
    async def stopper(sanic, loop):
        app.stop()

    with caplog.at_level(logging.ERROR):
        app.run(single_process=True, port=port)

    # The failing before_server_stop listener did not skip the drain
    # or the after_server_stop listeners
    assert events == ["after_server_stop"]
    assert app.drain_status["phase"] == "done"
    assert "listener exploded" in caplog.text


def test_repeated_stop_merges_into_single_drain(app, port):
    stopped = []

    @app.listener("after_server_stop")
    async def good_listener(sanic, loop):
        stopped.append("after_server_stop")

    @app.listener("after_server_start")
    async def stopper(sanic, loop):
        app.stop()
        app.stop()
        app.stop()

    app.run(single_process=True, port=port)

    assert stopped == ["after_server_stop"]
    status = app.drain_status
    assert status["phase"] == "done"
    assert status["sources"].count("app.stop") == 1


def test_task_born_during_shutdown_is_awaited(app, port):
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 5
    completed = []

    async def background():
        await asyncio.sleep(0.3)
        completed.append("done")

    @app.listener("before_server_stop")
    async def spawn(sanic, loop):
        app.add_task(background(), name="late_task")

    @app.listener("after_server_start")
    async def stopper(sanic, loop):
        app.stop()

    app.run(single_process=True, port=port)

    # The task registered after the stop signal was still tracked by a
    # lease and allowed to finish instead of being cancelled on sight
    assert completed == ["done"]
    assert app.drain_status["outcome"] == "completed"


async def _read_response(reader):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = await reader.read(1024)
        if not chunk:
            return data, b""
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    headers = {}
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        headers[name.strip().lower()] = value.strip().lower()
    if headers.get("transfer-encoding") == b"chunked":
        while not body.endswith(b"0\r\n\r\n"):
            chunk = await reader.read(1024)
            if not chunk:
                break
            body += chunk
    else:
        length = int(headers.get("content-length", 0))
        while len(body) < length:
            chunk = await reader.read(1024)
            if not chunk:
                break
            body += chunk
    return head, body


def test_draining_server_rejects_new_requests(app, port):
    responses = []

    @app.get("/begin")
    async def begin(request):
        request.app.drain_coordinator.begin(source="handler")
        return text("draining")

    @app.get("/next")
    async def next_(request):
        return text("unreachable")

    @app.listener("after_server_start")
    async def _request(sanic, loop):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /begin HTTP/1.1\r\nHost: localhost\r\n\r\n")
        head, _ = await _read_response(reader)
        responses.append(head.split(b"\r\n")[0])

        # Same keep-alive connection, but the drain has begun
        writer.write(b"GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n")
        head, _ = await _read_response(reader)
        responses.append(head.split(b"\r\n")[0])
        app.stop()

    app.run(single_process=True, port=port)

    assert responses[0].startswith(b"HTTP/1.1 200")
    assert responses[1].startswith(b"HTTP/1.1 503")
