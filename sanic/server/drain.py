from __future__ import annotations

import asyncio

from contextlib import suppress
from dataclasses import dataclass
from itertools import count
from time import monotonic
from typing import Any, Callable

from sanic.application.constants import StrEnum
from sanic.exceptions import SanicException
from sanic.log import error_logger


class LeaseKind(StrEnum):
    """项目内部接口说明。"""

    REQUEST = "request"
    STREAM = "stream"
    TASK = "task"
    CUSTOM = "custom"


class DrainPhase(StrEnum):
    """项目内部接口说明。"""

    IDLE = "idle"
    DRAINING = "draining"
    DONE = "done"


class DrainOutcome(StrEnum):
    """项目内部接口说明。"""

    COMPLETED = "completed"
    EXTENDED = "extended"
    CANCELLED = "cancelled"


class DrainError(SanicException):
    """项目内部接口说明。"""


class DrainClosedError(DrainError):
    """项目内部接口说明。"""


class DrainExtensionDenied(DrainError):
    """项目内部接口说明。"""


@dataclass
class DrainPolicy:
    """项目内部接口说明。"""

    timeout: float = 15.0
    max_extensions: int = 0
    extension_step: float = 5.0
    extension_cap: float = 30.0
    cancel_grace: float = 0.5


@dataclass
class Lease:
    """项目内部接口说明。"""

    lease_id: int
    kind: LeaseKind
    name: str
    acquired_at: float
    late: bool = False
    cancel: Callable[[], Any] | None = None
    state: str = "active"


@dataclass(frozen=True)
class DrainReport:
    """项目内部接口说明。"""

    outcome: DrainOutcome
    started_at: float
    finished_at: float
    deadline: float
    extensions: int
    sources: tuple[str, ...]
    completed: int
    cancelled: tuple[int, ...]
    late: int

    @property
    def elapsed(self) -> float:
        return self.finished_at - self.started_at


class DrainCoordinator:
    """项目内部接口说明。"""

    MAX_EVENTS = 256
    MAX_LEASE_ITEMS = 100

    def __init__(
        self,
        policy: DrainPolicy | None = None,
        on_transition: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self._policy = policy or DrainPolicy()
        self._on_transition = on_transition
        self._phase = DrainPhase.IDLE
        self._leases: dict[int, Lease] = {}
        self._ids = count(1)
        self._sources: list[str] = []
        self._started_at: float | None = None
        self._deadline: float | None = None
        self._extensions_used = 0
        self._extended_total = 0.0
        self._changed: asyncio.Event | None = None
        self._run_task: asyncio.Task[DrainReport] | None = None
        self._report: DrainReport | None = None
        self._events: list[tuple[float, str, str]] = []
        self._completed = 0
        self._cancelled: list[int] = []
        self._late = 0

    # ------------------------------------------------------------------ #
    # Lease tracking
    # ------------------------------------------------------------------ #
    def acquire(
        self,
        kind: LeaseKind,
        name: str = "",
        cancel: Callable[[], Any] | None = None,
    ) -> Lease:
        """项目内部接口说明。"""
        if self._phase is DrainPhase.DONE:
            raise DrainClosedError(
                f"Cannot acquire {kind} lease {name!r}: "
                "the drain coordinator is done"
            )
        late = self._phase is DrainPhase.DRAINING
        lease = Lease(
            lease_id=next(self._ids),
            kind=LeaseKind(kind),
            name=name,
            acquired_at=monotonic(),
            late=late,
            cancel=cancel,
        )
        self._leases[lease.lease_id] = lease
        if late:
            self._late += 1
            self._note("lease.late", f"{lease.kind}:{name}")
        return lease

    def release(self, lease_id: int) -> bool:
        """项目内部接口说明。"""
        lease = self._leases.pop(lease_id, None)
        if lease is None:
            return False
        if lease.state != "cancelled":
            lease.state = "released"
            self._completed += 1
        self._wake()
        return True

    def upgrade(
        self, lease_id: int, kind: LeaseKind, name: str | None = None
    ) -> bool:
        """项目内部接口说明。"""
        lease = self._leases.get(lease_id)
        if lease is None:
            return False
        lease.kind = LeaseKind(kind)
        if name:
            lease.name = name
        return True

    # ------------------------------------------------------------------ #
    # Drain lifecycle
    # ------------------------------------------------------------------ #
    def note_source(self, source: str) -> None:
        """项目内部接口说明。"""
        if self._phase is DrainPhase.DONE:
            return
        if source and source not in self._sources:
            self._sources.append(source)

    def begin(
        self, source: str = "unknown", timeout: float | None = None
    ) -> bool:
        """项目内部接口说明。"""
        self.note_source(source)
        if self._phase is not DrainPhase.IDLE:
            self._note("drain.merged", source)
            return False
        self._phase = DrainPhase.DRAINING
        self._started_at = monotonic()
        effective = self._policy.timeout if timeout is None else timeout
        self._deadline = self._started_at + effective
        self._note("drain.begin", f"source={source} timeout={effective:.3f}")
        self._transition()
        return True

    def extend(self, seconds: float | None = None, reason: str = "") -> float:
        """项目内部接口说明。"""
        if self._phase is not DrainPhase.DRAINING:
            raise DrainExtensionDenied(
                "Cannot extend a drain that is not in progress"
            )
        step = self._policy.extension_step if seconds is None else seconds
        if self._extensions_used >= self._policy.max_extensions:
            raise DrainExtensionDenied(
                f"Drain extension limit reached "
                f"({self._policy.max_extensions})"
            )
        if self._extended_total + step > self._policy.extension_cap:
            raise DrainExtensionDenied(
                f"Drain extension cap exceeded ({self._policy.extension_cap}s)"
            )
        self._extensions_used += 1
        self._extended_total += step
        assert self._deadline is not None
        self._deadline += step
        self._note("drain.extend", f"+{step:.3f}s {reason}".strip())
        self._wake()
        self._transition()
        return self._deadline

    async def drain(
        self, source: str = "manual", timeout: float | None = None
    ) -> DrainReport:
        """项目内部接口说明。"""
        self.begin(source=source, timeout=timeout)
        return await self.wait()

    async def wait(self) -> DrainReport:
        """项目内部接口说明。"""
        if self._phase is DrainPhase.IDLE:
            self.begin(source="wait")
        if self._run_task is None:
            self._run_task = asyncio.get_running_loop().create_task(
                self._run(), name="sanic.drain"
            )
        # Shielded so that a cancelled waiter cannot interrupt the one
        # drain that every stop source has merged into.
        return await asyncio.shield(self._run_task)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    @property
    def phase(self) -> DrainPhase:
        return self._phase

    @property
    def draining(self) -> bool:
        return self._phase is DrainPhase.DRAINING

    @property
    def done(self) -> bool:
        return self._phase is DrainPhase.DONE

    @property
    def report(self) -> DrainReport | None:
        return self._report

    def status(self) -> dict[str, Any]:
        """项目内部接口说明。"""
        now = monotonic()
        leases = sorted(
            self._leases.values(), key=lambda lease: lease.acquired_at
        )
        by_kind: dict[str, int] = {}
        for lease in leases:
            by_kind[lease.kind.value] = by_kind.get(lease.kind.value, 0) + 1
        return {
            "phase": self._phase.value,
            "outcome": self._report.outcome.value if self._report else None,
            "sources": list(self._sources),
            "started_at": self._started_at,
            "deadline": self._deadline,
            "remaining": (
                max(0.0, self._deadline - now)
                if self._phase is DrainPhase.DRAINING
                and self._deadline is not None
                else 0.0
            ),
            "extensions": {
                "used": self._extensions_used,
                "max": self._policy.max_extensions,
                "total": self._extended_total,
                "cap": self._policy.extension_cap,
            },
            "leases": {
                "active": len(leases),
                "completed": self._completed,
                "cancelled": len(self._cancelled),
                "late": self._late,
                "by_kind": by_kind,
                "items": [
                    {
                        "id": lease.lease_id,
                        "kind": lease.kind.value,
                        "name": lease.name,
                        "age": now - lease.acquired_at,
                        "late": lease.late,
                        "state": lease.state,
                    }
                    for lease in leases[: self.MAX_LEASE_ITEMS]
                ],
            },
            "events": [
                {"at": at, "event": event, "detail": detail}
                for at, event, detail in self._events[-self.MAX_EVENTS :]
            ],
        }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    async def _run(self) -> DrainReport:
        try:
            self._changed = asyncio.Event()
            while True:
                if not self._leases:
                    outcome = (
                        DrainOutcome.EXTENDED
                        if self._extensions_used
                        else DrainOutcome.COMPLETED
                    )
                    return self._finish(outcome)
                assert self._deadline is not None
                remaining = self._deadline - monotonic()
                if remaining <= 0:
                    await self._cancel_remaining()
                    return self._finish(DrainOutcome.CANCELLED)
                self._changed.clear()
                if not self._leases:
                    continue
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._changed.wait(), remaining)
        finally:
            if self._phase is not DrainPhase.DONE:
                # The drain task itself was interrupted; settle
                # deterministically instead of leaking the lifecycle.
                self._cancel_all_sync()
                self._finish(DrainOutcome.CANCELLED)

    async def _cancel_remaining(self) -> None:
        self._cancel_all_sync()
        grace = self._policy.cancel_grace
        if grace > 0 and self._leases:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._wait_released(), grace)
        for lease_id in self._cancelled:
            self._leases.pop(lease_id, None)

    def _cancel_all_sync(self) -> None:
        for lease in list(self._leases.values()):
            if lease.state == "cancelled":
                continue
            lease.state = "cancelled"
            self._cancelled.append(lease.lease_id)
            self._note("lease.cancel", f"{lease.kind}:{lease.name}")
            if lease.cancel is not None:
                try:
                    lease.cancel()
                except Exception:
                    error_logger.exception(
                        "Failed cancelling lease %s:%s",
                        lease.kind,
                        lease.name,
                    )

    async def _wait_released(self) -> None:
        while self._leases:
            assert self._changed is not None
            self._changed.clear()
            if not self._leases:
                return
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._changed.wait(), 0.05)

    def _finish(self, outcome: DrainOutcome) -> DrainReport:
        if self._report is not None:
            return self._report
        self._phase = DrainPhase.DONE
        finished = monotonic()
        assert self._started_at is not None and self._deadline is not None
        self._report = DrainReport(
            outcome=outcome,
            started_at=self._started_at,
            finished_at=finished,
            deadline=self._deadline,
            extensions=self._extensions_used,
            sources=tuple(self._sources),
            completed=self._completed,
            cancelled=tuple(self._cancelled),
            late=self._late,
        )
        self._note("drain.done", outcome.value)
        self._transition()
        return self._report

    def _wake(self) -> None:
        if self._changed is not None:
            self._changed.set()

    def _note(self, event: str, detail: str = "") -> None:
        self._events.append((monotonic(), event, detail))
        if len(self._events) > self.MAX_EVENTS:
            del self._events[: len(self._events) - self.MAX_EVENTS]

    def _transition(self) -> None:
        if self._on_transition is None:
            return
        try:
            self._on_transition(self.status())
        except Exception:
            error_logger.exception("Drain transition callback failed")
