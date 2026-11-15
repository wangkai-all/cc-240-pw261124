"""内容寻址存储（CAS）：data/blobs/<sha256> 一份内容只存一次。

- 下载先写 data/tmp/<task>.part，校验哈希后原子 rename 进 CAS；
- 多个附件（甚至多条记录）可引用同一 blob —— 共享内容省空间；
- 删除只由 GC 发起，且仅删除“无任何附件引用”的 blob，绝不误删其他记录仍在用的内容。
"""
import os


class BlobStore:
    def __init__(self, data_dir: str):
        self.blob_dir = os.path.join(data_dir, "blobs")
        self.tmp_dir = os.path.join(data_dir, "tmp")
        os.makedirs(self.blob_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)

    def blob_path(self, h: str) -> str:
        return os.path.join(self.blob_dir, h)

    def tmp_path(self, task_id: str) -> str:
        return os.path.join(self.tmp_dir, task_id + ".part")

    def has(self, h: str) -> bool:
        return bool(h) and os.path.exists(self.blob_path(h))

    def size_of(self, h: str) -> int:
        return os.path.getsize(self.blob_path(h))

    def commit(self, tmp_path: str, h: str) -> int:
        """把校验通过的临时文件纳入 CAS；若内容已存在（去重命中）则丢弃临时文件。"""
        dst = self.blob_path(h)
        if os.path.exists(dst):
            os.remove(tmp_path)
        else:
            os.replace(tmp_path, dst)
        return os.path.getsize(dst)

    def discard(self, tmp_path: str):
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass

    def delete(self, h: str):
        try:
            os.remove(self.blob_path(h))
        except FileNotFoundError:
            pass

    def leftover_tmps(self):
        return [f for f in os.listdir(self.tmp_dir) if f.endswith(".part")]
