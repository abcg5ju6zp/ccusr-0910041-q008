from __future__ import annotations

from enum import Enum


class WorkKind(str, Enum):
    """租约所追踪工作的类别。

    不同类别在排空时拥有独立的租约集合与截止时间，
    因此长请求、流式响应和框架后台任务可以被分别裁决。
    """

    REQUEST = "request"
    STREAM = "stream"
    TASK = "task"


class DrainStage(str, Enum):
    """协调器生命周期状态。

    生命周期严格单向推进::

        IDLE -> DRAINING -> CANCELLING -> DRAINED

    - ``IDLE``: 正常服务，允许新租约
    - ``DRAINING``: 已停止接收新业务，等待租约自然完成；
      允许在排空开始前已经获得入场资格的租约正常登记
    - ``CANCELLING``: 已过软截止时间，未完成租约收到取消信号，
      不再接受任何新租约
    - ``DRAINED``: 硬截止到达或租约全部离场，终态
    """

    IDLE = "idle"
    DRAINING = "draining"
    CANCELLING = "cancelling"
    DRAINED = "drained"


class LeaseOutcome(str, Enum):
    """单个租约在排空结束时的确定结果。"""

    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DEFERRED = "deferred"


class LeaseVerdict(str, Enum):
    """截止时间点对一个未完成租约的裁决。"""

    COMPLETE = "complete"
    CANCEL = "cancel"
    DEFER = "defer"


class StopReason(str, Enum):
    """排空的触发来源类别，用于合并多个停止源。"""

    SIGNAL = "signal"
    LOCAL_API = "local_api"
    CHILD_FAILURE = "child_failure"
    UNKNOWN = "unknown"
