"""下载任务管理：跟踪运行中任务的取消标志（撤权时按记录批量取消）。"""
import threading


class DownloadManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._tasks = {}  # task_id -> {"event": Event, "record_id": str}

    def register(self, task_id: str, record_id: str) -> threading.Event:
        ev = threading.Event()
        with self._lock:
            self._tasks[task_id] = {"event": ev, "record_id": record_id}
        return ev

    def unregister(self, task_id: str):
        with self._lock:
            self._tasks.pop(task_id, None)

    def is_cancelled(self, task_id: str) -> bool:
        with self._lock:
            t = self._tasks.get(task_id)
            return bool(t and t["event"].is_set())

    def cancel_record(self, record_id: str) -> int:
        """撤权时调用：给该记录所有运行中任务置取消标志。返回取消数量。"""
        n = 0
        with self._lock:
            for t in self._tasks.values():
                if t["record_id"] == record_id and not t["event"].is_set():
                    t["event"].set()
                    n += 1
        return n
