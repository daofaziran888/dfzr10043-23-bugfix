"""排程计划服务：周期性排程、时区管理与任务投递。

维护流程的周期性排程、时区设定和生效区间，触发时向任务
队列投递排队任务。支持迟到数据防护和并发上限控制。
"""
from __future__ import annotations

from typing import Any

from app.dedup import is_late_arrival
from app.services.base import BaseModuleService


class SchedulePlanService(BaseModuleService):
    MODULE = "schedule_plan"
    ENTITY = "排程计划"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "计划编号", "关联模板", "触发周期", "时区设定", "生效起日",
        "生效止日", "并发上限", "下次触发",
    ]
    REQUIRED_FIELDS = ["计划编号", "关联模板", "触发周期"]
    STATUS_ORDER = ["待生效", "生效中", "已暂停", "已过期"]
    ACTION_RULES = {"启效排程": "生效中", "暂停排程": "已暂停", "终止排程": "已过期"}
    NEGATIVE_ACTIONS = []

    def find_by_plan_code(self, plan_code: str) -> dict[str, Any] | None:
        """按计划编号查找排程，用于重复提交判重与拒绝原因追踪。"""
        if not plan_code:
            return None
        items, _ = self.list_entries(size=10000)
        for it in items:
            if it.get("计划编号") == plan_code:
                return it
        return None

    def reject_expired_plan(self, entry: dict[str, Any], end_date: str | None = None) -> str:
        """把过期排程终止为「已过期」并记录拒绝原因（可按计划编号回查）。

        被拒绝的排程不会残留为生效状态，也不再占用合法计划的可派范围。
        """
        end = end_date or str(entry.get("生效止日", ""))
        reason = f"生效止日{end}已过期，迟到数据拒绝"
        entry["拒绝原因"] = reason
        if entry.get("status") != "已过期":
            self.run_action(int(entry["id"]), "终止排程")
        return reason

    def reconcile_collection_window(self) -> list[dict[str, Any]]:
        """采集窗口核对：生效止日已过的「生效中」排程立即终止并移出生效清单。

        返回本次被核对终止的排程；历史遗留的错误生效记录在此被纠正，
        不影响其余有效排程继续处理真实缺采日。
        """
        items, _ = self.list_entries(size=10000)
        terminated = []
        for it in items:
            if it.get("status") == "生效中" and is_late_arrival(str(it.get("生效止日", ""))):
                self.reject_expired_plan(it)
                terminated.append(it)
        return terminated

    def check_late_arrival(self, plan_id: int) -> dict[str, Any]:
        """检查排程是否发生迟到数据（生效止日已过），过期即终止并记录拒绝原因。"""
        entry = self.get_entry(plan_id)
        if entry is None:
            return {"ok": False, "reason": "排程计划不存在"}
        end_date = str(entry.get("生效止日", ""))
        if not end_date:
            return {"ok": True, "late": False}
        if is_late_arrival(end_date):
            reason = self.reject_expired_plan(entry, end_date)
            return {"ok": True, "late": True, "reason": reason}
        return {"ok": True, "late": False}

    def get_active_plans(self) -> list[dict[str, Any]]:
        """获取生效中的排程计划列表（生效清单）。

        返回前先做采集窗口核对：过期排程被终止并记录拒绝原因，
        不再出现在生效清单，也不占用合法计划的可派范围。
        """
        self.reconcile_collection_window()
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("status") == "生效中"]

    def is_plan_dispatchable(self, plan_code: str) -> bool:
        """判断来源计划当前是否可派（与生效清单共用采集窗口核对结果）。"""
        if not plan_code:
            return True
        plan = self.find_by_plan_code(plan_code)
        if plan is None:
            return False
        if plan.get("status") != "生效中":
            return False
        if is_late_arrival(str(plan.get("生效止日", ""))):
            self.reject_expired_plan(plan)
            return False
        return True
