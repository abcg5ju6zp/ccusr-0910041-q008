from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable

from sanic.drain.constants import LeaseOutcome, LeaseVerdict, WorkKind


if TYPE_CHECKING:
    from asyncio import Task

    from sanic.drain.coordinator import DrainCoordinator

# 取消钩子：普通取消（软）与强制中止（硬）。
# 软取消应让工作看到 CancelledError 并自行清理；
# 硬中止用于工作无视取消时（例如直接 abort 传输层）。
CancelHook = Callable[[], Awaitable[None] | None]
ForceHook = Callable[[], Awaitable[None] | None]
DeferHook = Callable[[], Any]


class Lease:
    """一份在途工作的租约。

    租约由 :meth:`DrainCoordinator.admit` 颁发，工作结束时必须通过
    :meth:`release` 归还（也支持 ``async with``）。协调器在截止时间点
    根据租约类别、自带截止时间与可延期性做出确定裁决，再通过
    :meth:`cancel` / :meth:`force` 把裁决落到实际工作上。
    """

    __slots__ = (
        "id",
        "kind",
        "name",
        "created",
        "deadline",
        "extendable",
        "task",
        "outcome",
        "verdict",
        "newcomer",
        "cancel_detail",
        "_coordinator",
        "_on_cancel",
        "_on_force",
        "_on_defer",
        "_released",
    )

    def __init__(
        self,
        *,
        ident: int,
        kind: WorkKind,
        created: float,
        coordinator: DrainCoordinator,
        name: str = "",
        deadline: float | None = None,
        extendable: bool = False,
        task: Task | None = None,
        on_cancel: CancelHook | None = None,
        on_force: ForceHook | None = None,
        on_defer: DeferHook | None = None,
    ) -> None:
        self.id = ident
        self.kind = kind
        self.name = name
        self.created = created
        # 租约自己声明的绝对截止时间。超过排空宽限仍未完成、
        # 且该截止时间晚于宽限点时，可被裁决为“延期”。
        self.deadline = deadline
        # 是否允许在软截止点被延期（移交/稍后接续）而非取消。
        self.extendable = extendable
        self.task = task
        self.outcome: LeaseOutcome | None = None
        # 协调器在截止点给出的裁决；newcomer 标记排空后新生的租约。
        self.verdict: LeaseVerdict | None = None
        self.newcomer = False
        self.cancel_detail = ""
        self._coordinator = coordinator
        self._on_cancel = on_cancel
        self._on_force = on_force
        self._on_defer = on_defer
        self._released = False

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    @property
    def active(self) -> bool:
        """项目内部接口说明。"""
        return not self._released

    def release(self, outcome: LeaseOutcome = LeaseOutcome.COMPLETED) -> None:
        """归还租约。重复释放是安全的幂等操作。"""
        if self._released:
            return
        self._released = True
        self.outcome = outcome
        self._coordinator._release(self)

    async def __aenter__(self) -> Lease:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.outcome is not None:
            self.release(self.outcome)
        else:
            self.release()

    # ------------------------------------------------------------------ #
    # 协调器裁决后的落地动作
    # ------------------------------------------------------------------ #

    def cancel(self) -> None:
        """软取消：优先执行自定义钩子，否则取消绑定的任务。"""
        if self._on_cancel is not None:
            self._on_cancel()
        elif self.task is not None and not self.task.done():
            self.task.cancel()

    async def force(self) -> None:
        """强制中止：硬截止点仍未离开的工作。"""
        if self._on_force is not None:
            maybe = self._on_force()
            if maybe is not None:
                await maybe
        if self.task is not None and not self.task.done():
            self.task.cancel()

    def mark_deferred(self) -> None:
        """标记为延期：不再阻塞排空，但结果被确定记录。"""
        self.release(LeaseOutcome.DEFERRED)

    def mark_cancelled(self) -> None:
        """项目内部接口说明。"""
        self.release(LeaseOutcome.CANCELLED)

    # ------------------------------------------------------------------ #

    def age(self, now: float) -> float:
        """项目内部接口说明。"""
        return max(0.0, now - self.created)

    def to_dict(self, now: float | None = None) -> dict:
        """项目内部接口说明。"""
        data = {
            "id": self.id,
            "kind": self.kind.value,
            "name": self.name,
            "created": self.created,
            "deadline": self.deadline,
            "extendable": self.extendable,
            "outcome": self.outcome.value if self.outcome else None,
        }
        if now is not None:
            data["age"] = round(self.age(now), 4)
        return data

    def __repr__(self) -> str:
        return (
            f"Lease(id={self.id}, kind={self.kind.value}, "
            f"name={self.name!r}, active={self.active})"
        )
