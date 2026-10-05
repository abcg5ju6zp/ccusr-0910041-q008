from __future__ import annotations

import asyncio

from contextlib import suppress
from dataclasses import dataclass, field
from functools import partial
from time import monotonic
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from sanic.drain.constants import (
    DrainStage,
    LeaseOutcome,
    LeaseVerdict,
    StopReason,
    WorkKind,
)
from sanic.drain.lease import Lease
from sanic.log import error_logger, logger


if TYPE_CHECKING:
    from asyncio import Task

# 停止接收新业务 / 进入新阶段时执行的钩子。
LifecycleHook = Callable[[], Awaitable[None] | None]

# 各类工作的软截止宽限：None 表示沿用整体宽限。
KindTimeouts = dict[WorkKind, float | None]


@dataclass
class LeaseRecord:
    """租约终局记录，用于排空报告与管理接口。"""

    id: int
    kind: WorkKind
    name: str
    verdict: LeaseVerdict
    outcome: LeaseOutcome
    age: float
    forced: bool = False
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        """项目内部接口说明。"""
        return {
            "id": self.id,
            "kind": self.kind.value,
            "name": self.name,
            "verdict": self.verdict.value,
            "outcome": self.outcome.value,
            "age": round(self.age, 4),
            "forced": self.forced,
            "detail": self.detail,
        }


@dataclass
class DrainResult:
    """一次排空的确定结果。"""

    reasons: list[str] = field(default_factory=list)
    started_at: float = 0.0
    ended_at: float = 0.0
    soft_deadline: float = 0.0
    hard_deadline: float = 0.0
    records: list[LeaseRecord] = field(default_factory=list)
    listener_failures: list[str] = field(default_factory=list)

    @property
    def duration(self) -> float:
        """项目内部接口说明。"""
        return max(0.0, self.ended_at - self.started_at)

    @property
    def completed(self) -> list[LeaseRecord]:
        """项目内部接口说明。"""
        return [r for r in self.records if r.outcome is LeaseOutcome.COMPLETED]

    @property
    def cancelled(self) -> list[LeaseRecord]:
        """项目内部接口说明。"""
        return [r for r in self.records if r.outcome is LeaseOutcome.CANCELLED]

    @property
    def deferred(self) -> list[LeaseRecord]:
        """项目内部接口说明。"""
        return [r for r in self.records if r.outcome is LeaseOutcome.DEFERRED]

    @property
    def forced(self) -> list[LeaseRecord]:
        """项目内部接口说明。"""
        return [r for r in self.records if r.forced]

    def summary(self) -> dict[str, int]:
        """项目内部接口说明。"""
        return {
            "completed": len(self.completed),
            "cancelled": len(self.cancelled),
            "deferred": len(self.deferred),
            "forced": len(self.forced),
        }

    def to_dict(self) -> dict[str, Any]:
        """项目内部接口说明。"""
        return {
            "reasons": list(self.reasons),
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration": round(self.duration, 4),
            "soft_deadline": self.soft_deadline,
            "hard_deadline": self.hard_deadline,
            "summary": self.summary(),
            "leases": [record.to_dict() for record in self.records],
            "listener_failures": list(self.listener_failures),
        }


class DrainCoordinator:
    """进程内排空协调器。

    职责：

    1. 合并多个停止来源（信号、本地管理接口、子进程失败等）为一次排空；
    2. 按类别（长请求、流式响应、框架后台任务）追踪在途工作的租约；
    3. 在软截止点对每个租约给出确定裁决：完成、取消或延期，
       在硬截止点对无视取消的工作强制执行；
    4. 隔离生命周期钩子（监听器）失败，保证状态机单向推进；
    5. 通过 :meth:`status` 向本地管理接口提供可观测快照。

    状态机严格单向::

        IDLE -> DRAINING -> CANCELLING -> DRAINED

    协调器本身不依赖 Sanic 应用对象，便于在任意事件循环中单独测试；
    与框架的集成通过钩子和 ``publish`` 回调完成。
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = monotonic,
        graceful_timeout: float = 15.0,
        cancel_timeout: float = 5.0,
        settle_timeout: float = 0.25,
        tick: float = 0.1,
        publish_interval: float = 0.5,
        kind_timeouts: KindTimeouts | None = None,
        allow_defer: frozenset[WorkKind] = frozenset(
            {WorkKind.STREAM, WorkKind.TASK}
        ),
    ) -> None:
        self._clock = clock
        self._graceful_timeout = graceful_timeout
        self._cancel_timeout = cancel_timeout
        # 硬截止后的“沉降窗口”：合作响应取消的工作可能只差几个事件循环
        # 迭代就能离场；在此窗口内离场不计为强制。无视取消的工作不会
        # 离场，窗口耗尽后仍被强制执行，保证总等待有界。
        self._settle_timeout = settle_timeout
        self._tick = tick
        self._publish_interval = publish_interval
        self._kind_timeouts: KindTimeouts = dict(kind_timeouts or {})
        self._allow_defer = set(allow_defer)

        self.stage: DrainStage = DrainStage.IDLE
        self._leases: dict[int, Lease] = {}
        self._records: list[LeaseRecord] = []
        self._recorded_ids: set[int] = set()
        self._next_id = 1

        self._reasons: list[str] = []
        self._hard_requested = False
        self._started_at: float | None = None
        self._ended_at: float | None = None
        self._soft_deadline: float | None = None
        self._hard_deadline: float | None = None
        self._cease_hooks: list[LifecycleHook] = []
        self._listener_failures: list[str] = []
        self._listener_exceptions: list[BaseException] = []
        self._publishers: list[Callable[[dict[str, Any]], None]] = []
        self._last_publish = 0.0
        self._published_stage: DrainStage | None = None

        self._idle = asyncio.Event()
        self._idle.set()
        self._escalated = asyncio.Event()
        self._drain_done = asyncio.Event()
        self._drain_running = False
        self._result: DrainResult | None = None

    # ------------------------------------------------------------------ #
    # 租约管理
    # ------------------------------------------------------------------ #

    def admit(
        self,
        kind: WorkKind,
        *,
        name: str = "",
        deadline: float | None = None,
        extendable: bool = False,
        task: Task | None = None,
        on_cancel: Callable[[], Any] | None = None,
        on_force: Callable[[], Any] | None = None,
        on_defer: Callable[[], Any] | None = None,
    ) -> Lease | None:
        """登记一份在途工作，返回租约。

        新生竞争规则：

        - ``IDLE``：正常颁发；
        - ``DRAINING`` / ``CANCELLING``：仍允许已入场的工作登记
          （listening socket 关闭瞬间已 accept 的请求、请求处理中
          新生的后台任务等），租约被标记为 ``newcomer``，不可延期，
          会在下一个裁决点被取消，且硬截止保证等待有界；
        - ``DRAINED``：终态拒绝新租约，返回 ``None``，
          调用方必须自行处理（如立即结束新生工作）。

        以此保证生命周期顺序不会被“晚到一步”的工作破坏。
        """
        if self.stage is DrainStage.DRAINED:
            return None

        ident = self._next_id
        self._next_id += 1
        lease = Lease(
            ident=ident,
            kind=kind,
            created=self._clock(),
            coordinator=self,
            name=name,
            deadline=deadline,
            # 排空开始后才入场的租约不允许延期。
            extendable=extendable and self.stage is DrainStage.IDLE,
            task=task,
            on_cancel=on_cancel,
            on_force=on_force,
            on_defer=on_defer,
        )
        lease.newcomer = self.stage is not DrainStage.IDLE
        self._leases[ident] = lease
        # 绑定任务的租约随任务终局自动归还：正常完成 -> COMPLETED，
        # 被取消 -> CANCELLED。这是框架后台任务/请求任务的主要追踪方式。
        if task is not None:
            task.add_done_callback(partial(self._task_finished, lease=lease))
        self._idle.clear()
        self._publish()
        return lease

    @staticmethod
    def _task_finished(task: Task, *, lease: Lease) -> None:
        """绑定任务终局时自动归还租约（幂等）。"""
        if not lease.active:
            return
        if task.cancelled():
            lease.mark_cancelled()
        else:
            lease.release()

    def _release(self, lease: Lease) -> None:
        """由 :meth:`Lease.release` 调用，必须幂等。"""
        existing = self._leases.pop(lease.id, None)
        if existing is None:
            # 重复释放或已在终态被强制收尾：忽略，不改变任何状态。
            return
        if lease.outcome is None:
            lease.outcome = LeaseOutcome.COMPLETED
        if self.stage is not DrainStage.IDLE:
            self._record(lease)
        if not self._leases:
            self._idle.set()
        self._publish()

    def active(self, kind: WorkKind | None = None) -> list[Lease]:
        """项目内部接口说明。"""
        if kind is None:
            return list(self._leases.values())
        return [lease for lease in self._leases.values() if lease.kind is kind]

    def reclassify(
        self,
        lease: Lease,
        kind: WorkKind,
        *,
        extendable: bool | None = None,
        deadline: float | None = None,
        replace_deadline: bool = False,
    ) -> Lease:
        """变更在途租约类别（典型：普通请求进入流式响应阶段）。

        只能在排空开始前把租约标记为可延期；``deadline`` 默认取
        “更早者”，``replace_deadline=True`` 时直接覆盖。
        """
        lease.kind = kind
        if extendable is not None:
            lease.extendable = bool(
                extendable and self.stage is DrainStage.IDLE
            )
        if deadline is not None:
            lease.deadline = (
                deadline
                if replace_deadline or lease.deadline is None
                else min(lease.deadline, deadline)
            )
        self._publish()
        return lease

    def record_listener_failure(
        self, scope: str, name: str, exc: BaseException
    ) -> None:
        """记录生命周期监听器失败，但不中断状态推进。"""
        entry = f"{scope}:{name}:{exc!r}"
        if entry not in self._listener_failures:
            self._listener_failures.append(entry)
            self._listener_exceptions.append(exc)
            error_logger.exception(
                "Drain listener failed (%s:%s)", scope, name
            )
            self._publish()

    @property
    def listener_exception(self) -> BaseException | None:
        """第一个监听器失败的原始异常（如有）。"""
        if not self._listener_exceptions:
            return None
        return self._listener_exceptions[0]

    # ------------------------------------------------------------------ #
    # 停止源合并
    # ------------------------------------------------------------------ #

    def reset(self) -> None:
        """把协调器恢复到 IDLE 初始状态。

        服务器（或测试中同一 app 的再次 ``run``）重新启动时调用。
        仅保留状态发布者与超时配置，清空全部租约、记录、原因与结果。
        若仍有在途排空则拒绝重置，避免覆盖进行中的生命周期。
        """
        if self.stage in (DrainStage.DRAINING, DrainStage.CANCELLING):
            raise RuntimeError("Cannot reset a coordinator that is draining")

        publishers = list(self._publishers)
        kind_timeouts = dict(self._kind_timeouts)
        allow_defer = set(self._allow_defer)

        self.__init__(  # type: ignore[misc]
            clock=self._clock,
            graceful_timeout=self._graceful_timeout,
            cancel_timeout=self._cancel_timeout,
            settle_timeout=self._settle_timeout,
            tick=self._tick,
            publish_interval=self._publish_interval,
            kind_timeouts=kind_timeouts,
            allow_defer=frozenset(allow_defer),
        )
        self._publishers = publishers

    @property
    def draining(self) -> bool:
        """项目内部接口说明。"""
        return self.stage is not DrainStage.IDLE

    def begin_drain(
        self,
        reason: str | StopReason = StopReason.UNKNOWN,
        *,
        hard: bool = False,
    ) -> bool:
        """请求开始排空（可在信号处理器等同步上下文中调用）。

        多个停止来源合并为同一次排空：只有首次调用推进状态，
        后续调用仅记录原因；``hard=True``（例如第二发 SIGTERM/SIGINT）
        会立即升级到取消/强制阶段，即使排空协程正在等待也会被唤醒。

        返回是否由本次调用首次触发排空。
        """
        reason_value = (
            reason.value if isinstance(reason, StopReason) else reason
        )
        first = False
        if self.stage is DrainStage.IDLE:
            first = True
            self.stage = DrainStage.DRAINING
            self._started_at = self._clock()
            logger.debug("Drain started (reason=%s)", reason_value)
        if reason_value not in self._reasons:
            self._reasons.append(reason_value)
        if hard:
            self._hard_requested = True
            # 中断 drain() 的等待：软等待立即结束，取消阶段直接进入强制。
            self._escalated.set()
        self._publish()
        return first

    def cease_on_drain(self, hook: LifecycleHook) -> None:
        """注册“停止接收新业务”动作（如关闭 listening socket）。"""
        self._cease_hooks.append(hook)

    def add_publisher(self, hook: Callable[[dict[str, Any]], None]) -> None:
        """注册状态快照发布者（如写入多进程共享的 worker_state）。"""
        self._publishers.append(hook)

    # ------------------------------------------------------------------ #
    # 排空主流程
    # ------------------------------------------------------------------ #

    async def drain(
        self,
        graceful_timeout: float | None = None,
        cancel_timeout: float | None = None,
        reason: str | StopReason = StopReason.UNKNOWN,
    ) -> DrainResult:
        """执行排空，返回确定结果。

        停止源可能已经通过 :meth:`begin_drain` 提前开启了排空
        （典型：信号处理器只能同步调用）。第一个到达的 :meth:`drain`
        负责执行，后续调用（无论来自哪个停止源）都合并等待同一个结果，
        排空只进行一次。
        """
        if self._result is not None:
            return self._result
        if self.stage is DrainStage.IDLE:
            self.begin_drain(reason)

        if self._drain_running:
            await self._drain_done.wait()
            assert self._result is not None
            return self._result

        self._drain_running = True
        try:
            self._result = await self._run_drain(
                self._graceful_timeout
                if graceful_timeout is None
                else graceful_timeout,
                self._cancel_timeout
                if cancel_timeout is None
                else cancel_timeout,
            )
        finally:
            self._drain_running = False
            self._drain_done.set()
        return self._result

    async def _run_drain(
        self, graceful_timeout: float, cancel_timeout: float
    ) -> DrainResult:
        assert self._started_at is not None
        started = self._started_at

        # 硬请求（第二发信号）把软截止压缩到当下。
        if self._hard_requested:
            soft_at = started
        else:
            soft_at = started + graceful_timeout
        hard_at = soft_at + cancel_timeout
        self._soft_deadline = soft_at
        self._hard_deadline = hard_at

        # 阶段一：停止接收新业务。随后进入宽限循环——租约要么自然
        # 完成，要么在各自（类别/自身）的决策点被裁决；一个类别的
        # 短宽限不应提前取消仍享有长宽限的其他类别。
        await self._run_hooks(self._cease_hooks, "cease")
        await self._grace_phase(soft_at, hard_at)

        # 全局软截止：裁决所有仍未决策的租约（延期或取消）。
        self._adjudicate_remaining(hard_at)

        if self._leases:
            self.stage = DrainStage.CANCELLING
            self._publish()
            # 阶段二：等待取消生效。新生租约即时取消；延期租约守各自
            # 截止；升级（第二发信号）直接跳到强制执行。
            await self._settle_phase(hard_at)

        # 硬截止：对仍未离开的租约强制执行并收尾。
        await self._force_remaining(hard_at)

        self.stage = DrainStage.DRAINED
        self._ended_at = self._clock()
        self._soft_deadline = soft_at
        self._hard_deadline = hard_at
        self._idle.set()

        result = DrainResult(
            reasons=list(self._reasons),
            started_at=started,
            ended_at=self._ended_at,
            soft_deadline=soft_at,
            hard_deadline=hard_at,
            records=list(self._records),
            listener_failures=list(self._listener_failures),
        )
        self._result = result
        # 发布终态的完整状态快照（其中嵌入排空结果）。
        self._publish(force=True)
        logger.debug("Drain complete: %s", result.summary())
        return result

    def _undecided(self) -> list[Lease]:
        """尚未得到裁决（仍在宽限等待）的租约。"""
        return [
            lease
            for lease in self._leases.values()
            if lease.verdict is None
        ]

    def _lease_decision_at(
        self, lease: Lease, soft_at: float
    ) -> float:
        """单个租约（在全局软截止之前）最早的决策时刻。"""
        moments = [soft_at]
        kind_timeout = self._kind_timeouts.get(lease.kind)
        if kind_timeout is not None:
            moments.append(lease.created + kind_timeout)
        if lease.deadline is not None:
            moments.append(lease.deadline)
        return min(moments)

    async def _wait_wake(self, delay: float) -> None:
        """等待租约离场或升级信号，``delay`` 秒后必定返回。

        显式取消两个等待任务：``asyncio.wait`` 超时不会替我们取消，
        否则每次 tick 都会泄漏 Event.wait() 任务。
        """
        idle_task = asyncio.ensure_future(self._idle.wait())
        escalate_task = asyncio.ensure_future(self._escalated.wait())
        try:
            await asyncio.wait(
                {idle_task, escalate_task},
                timeout=delay,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (idle_task, escalate_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                idle_task, escalate_task, return_exceptions=True
            )

    async def _grace_phase(
        self, soft_at: float, hard_at: float
    ) -> None:
        """宽限阶段：在每个决策点只裁决已到期的未决策租约。

        已裁决（CANCEL/DEFER）的租约交由取消/强制阶段收尾，不参与
        后续决策点计算。这样一个类别的短宽限（如 REQUEST=5s）不会
        提前取消仍享有长宽限的其他类别（如 STREAM=15s）。
        """
        while not self._hard_requested:
            now = self._clock()
            undecided = self._undecided()
            if not undecided or now >= soft_at:
                return

            deadline = min(
                self._lease_decision_at(lease, soft_at)
                for lease in undecided
            )
            wait = min(
                self._tick,
                max(0.0, deadline - now),
                soft_at - now,
            )
            # 始终真正让出事件循环，使取消投递/任务终局回调得以落地。
            if wait > 0:
                await self._wait_wake(wait)
            else:
                await asyncio.sleep(0)
            if self._escalated.is_set():
                return

            self._adjudicate_due(
                self._clock(), soft_at=soft_at, hard_cap=hard_at
            )

    def _adjudicate_due(
        self,
        now: float,
        *,
        soft_at: float,
        hard_cap: float,
    ) -> None:
        """只裁决已到达自身决策点的租约；其余继续等待。

        到达全局软截止（``now >= soft_at``）时对所有租约决策。
        """
        for lease in list(self._leases.values()):
            if lease.verdict is not None:
                continue
            due = (
                lease.newcomer
                or now >= soft_at
                or now >= self._lease_decision_at(lease, soft_at)
            )
            if not due:
                continue
            self._decide_lease(lease, now, hard_cap=hard_cap)

    def _adjudicate_remaining(self, hard_cap: float) -> None:
        """全局软截止点：裁决所有仍未决策的租约。"""
        if not self._leases:
            return
        logger.info(
            "Drain soft deadline reached with %d lease(s) active",
            len(self._leases),
        )
        self._adjudicate_due(
            self._clock(), soft_at=self._clock(), hard_cap=hard_cap
        )

    async def _settle_phase(self, hard_deadline: float) -> None:
        """取消阶段：等待离场；新生租约即时取消；延期租约到期取消。"""
        while self._leases:
            now = self._clock()
            if now >= hard_deadline or self._hard_requested:
                return
            self._cancel_due_deferred()
            self._cancel_newcomers()
            if not self._leases:
                return
            try:
                await asyncio.wait_for(
                    self._idle.wait(),
                    timeout=min(self._tick, hard_deadline - now),
                )
                return
            except asyncio.TimeoutError:
                continue

    def _cancel_newcomers(self) -> None:
        """排空期间新生的租约没有资格继续占用进程，立即取消。"""
        for lease in list(self._leases.values()):
            if lease.newcomer and lease.verdict is None:
                logger.info(
                    "Cancelling newcomer lease during drain: %s", lease
                )
                self._verdict_cancel(lease, "new work after drain began")

    def _cancel_due_deferred(self) -> None:
        """取消已到达延期截止时间的租约。"""
        now = self._clock()
        for lease in list(self._leases.values()):
            if (
                lease.verdict is LeaseVerdict.DEFER
                and lease.deadline is not None
                and now >= lease.deadline
            ):
                self._verdict_cancel(lease, "deferred deadline reached")

    def _decide_lease(
        self, lease: Lease, now: float, *, hard_cap: float
    ) -> None:
        """对一个已到期租约给出确定裁决：移交 / 延期等待 / 取消。"""
        if lease.newcomer:
            self._verdict_cancel(lease, "new work after drain began")
            return

        kind_timeout = self._kind_timeouts.get(lease.kind)
        kind_deadline = (
            lease.created + kind_timeout
            if kind_timeout is not None
            else None
        )
        deadlines = [
            d for d in (lease.deadline, kind_deadline) if d is not None
        ]
        effective_deadline = min(deadlines) if deadlines else None

        if (
            lease.extendable
            and lease.kind in self._allow_defer
            and effective_deadline is not None
            and effective_deadline > now
            and lease._on_defer is not None
        ):
            # 可移交：立即脱离进程，不阻塞排空。
            try:
                maybe = lease._on_defer()
                if maybe is not None and hasattr(maybe, "__await__"):
                    # 移交钩子约定为同步；误传协程时调度但不等待，
                    # 避免让已移交的工作继续阻塞进程退出。
                    asyncio.ensure_future(maybe)
            except Exception as e:  # noqa: BLE001
                error_logger.exception("Defer hook failed")
                self._verdict_cancel(lease, f"defer hook failed: {e}")
                return
            lease.verdict = LeaseVerdict.DEFER
            lease.release(LeaseOutcome.DEFERRED)
            self._annotate_last(lease.id, "handed off before deadline")
            return

        if (
            lease.extendable
            and lease.kind in self._allow_defer
            and effective_deadline is not None
            and effective_deadline > now
        ):
            # 延期等待：不得越过硬截止。
            lease.verdict = LeaseVerdict.DEFER
            lease.deadline = min(effective_deadline, hard_cap)
            logger.info(
                "Lease deferred until %.2f: %s", lease.deadline, lease
            )
            return

        reason = "soft deadline reached"
        if effective_deadline is not None and effective_deadline <= now:
            reason = "lease deadline reached"
        self._verdict_cancel(lease, reason)

    def _verdict_cancel(self, lease: Lease, detail: str) -> None:
        """项目内部接口说明。"""
        lease.verdict = LeaseVerdict.CANCEL
        lease.cancel_detail = detail
        try:
            lease.cancel()
        except Exception:  # noqa: BLE001
            error_logger.exception("Cancel hook failed for %s", lease)

    async def _force_remaining(self, hard_deadline: float) -> None:
        """硬截止点：对仍未离开的租约强制执行并记录终局。"""
        if not self._leases:
            return

        # 进入强制阶段前先给已取消工作一次在硬截止内离场的机会。
        if not self._hard_requested:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._idle.wait(),
                    timeout=max(0.0, hard_deadline - self._clock()),
                )
        if not self._leases:
            return

        # 硬截止已到，但合作响应取消的工作可能只差几个事件循环迭代
        # 就能离场（取消投递 -> 工作恢复清理 -> 任务终局回调归还租约）。
        # 给一个有界沉降窗口轮询离场；真正无视取消的工作不会离场，
        # 窗口耗尽后仍被强制执行。硬升级（第二发信号）跳过沉降。
        if not self._hard_requested:
            settle_until = self._clock() + self._settle_timeout
            while self._leases and self._clock() < settle_until:
                await asyncio.sleep(min(self._tick, 0.02))
        if not self._leases:
            return

        logger.warning(
            "Drain hard deadline reached; forcing %d lease(s)",
            len(self._leases),
        )
        for lease in list(self._leases.values()):
            try:
                maybe = lease.force()
                if maybe is not None:
                    await maybe
            except Exception:  # noqa: BLE001
                error_logger.exception("Force hook failed for %s", lease)
            detail = lease.cancel_detail or "hard deadline reached"
            self._record(
                lease,
                verdict=LeaseVerdict.CANCEL,
                outcome=LeaseOutcome.CANCELLED,
                forced=True,
                detail=detail,
            )
            lease._released = True
            lease.outcome = LeaseOutcome.CANCELLED
        self._leases.clear()
        self._idle.set()

    # ------------------------------------------------------------------ #
    # 钩子隔离 / 记录 / 观测
    # ------------------------------------------------------------------ #

    async def _run_hooks(
        self, hooks: list[LifecycleHook], label: str
    ) -> None:
        """逐个执行生命周期钩子，单个失败不破坏状态推进。"""
        while hooks:
            hook = hooks.pop(0)
            name = getattr(hook, "__qualname__", repr(hook))
            try:
                maybe = hook()
                if maybe is not None:
                    await maybe
            except Exception as e:  # noqa: BLE001
                error_logger.exception(
                    "Drain %s hook failed (%s); continuing", label, name
                )
                entry = f"{label}:{name}:{e!r}"
                if entry not in self._listener_failures:
                    self._listener_failures.append(entry)
                    self._listener_exceptions.append(e)
                self._publish()

    def _record(
        self,
        lease: Lease,
        *,
        verdict: LeaseVerdict | None = None,
        outcome: LeaseOutcome | None = None,
        forced: bool = False,
        detail: str = "",
    ) -> None:
        """项目内部接口说明。"""
        if lease.id in self._recorded_ids:
            if detail:
                for record in self._records:
                    if record.id == lease.id and not record.detail:
                        record.detail = detail
            return
        final_verdict = (
            verdict
            or getattr(lease, "verdict", None)
            or LeaseVerdict.COMPLETE
        )
        final_outcome = outcome or lease.outcome or LeaseOutcome.COMPLETED
        final_detail = detail or lease.cancel_detail
        self._recorded_ids.add(lease.id)
        self._records.append(
            LeaseRecord(
                id=lease.id,
                kind=lease.kind,
                name=lease.name,
                verdict=final_verdict,
                outcome=final_outcome,
                age=lease.age(self._clock()),
                forced=forced,
                detail=final_detail,
            )
        )

    def _annotate_last(self, lease_id: int, detail: str) -> None:
        """项目内部接口说明。"""
        for record in reversed(self._records):
            if record.id == lease_id:
                record.detail = detail
                return

    def _publish(
        self,
        *,
        result: DrainResult | None = None,
        force: bool = False,
    ) -> None:
        """发布状态快照。

        阶段变化、失败与终态总是立即发布；租约进出造成的高频更新按
        ``publish_interval`` 节流，避免压垮跨进程的共享状态通道。
        """
        now = self._clock()
        stage_changed = self._published_stage != self.stage
        if (
            not force
            and not stage_changed
            and not self._listener_failures
            and result is None
            and now - self._last_publish < self._publish_interval
        ):
            return
        self._last_publish = now
        self._published_stage = self.stage
        snapshot = self.status()
        for publisher in self._publishers:
            try:
                publisher(snapshot)
            except Exception:  # noqa: BLE001
                error_logger.exception("Drain status publisher failed")

    def status(self) -> dict[str, Any]:
        """供本地管理接口查询的当前状态快照（全部为原生可序列化类型）。"""
        now = self._clock()
        counts = {kind.value: 0 for kind in WorkKind}
        newcomers = 0
        leases: list[dict[str, Any]] = []
        for lease in self._leases.values():
            counts[lease.kind.value] += 1
            if lease.newcomer:
                newcomers += 1
            item = lease.to_dict(now)
            item["newcomer"] = lease.newcomer
            leases.append(item)

        return {
            "stage": self.stage.value,
            "reasons": list(self._reasons),
            "hard_requested": self._hard_requested,
            "started_at": self._started_at,
            "elapsed": round(now - self._started_at, 4)
            if self._started_at is not None
            else 0.0,
            "soft_deadline": self._soft_deadline,
            "hard_deadline": self._hard_deadline,
            "active_total": len(self._leases),
            "active_by_kind": counts,
            "newcomers": newcomers,
            "leases": leases,
            "listener_failures": list(self._listener_failures),
            "result": self._result.to_dict() if self._result else None,
        }
