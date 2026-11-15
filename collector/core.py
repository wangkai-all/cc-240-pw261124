"""核心业务逻辑：授权同步、下载、缓存引用、撤权清理、重新确认、GC。

关键语义：
1. 撤权（来源侧撤回，采集端同步感知）：
   - auth_epoch +1 —— 旧预览/下载链接（携带旧 epoch）立即 410；
   - 运行中的下载任务被取消，临时分块文件删除，只留必要审计（任务状态 + 事件）；
   - 该记录附件到 blob 的显式引用全部解除（refcount 下降），附件状态 -> revoked；
   - 共享 blob 若仍被其他记录引用，则保留，GC 不会误删。
2. 重新授权：附件进入 unconfirmed，绝不直接发布旧缓存；
   必须经“重新确认”——拉取来源最新元数据，哈希一致才允许复用 CAS（cache_reuse_confirmed），
   否则重新下载并校验。
3. 内容替换：同步发现来源哈希变化 -> 附件 stale，重新下载后换引用，旧 blob 等待 GC。
"""
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid

from .db import connect
from .downloads import DownloadManager
from .store import BlobStore

CHUNK = 16384


class AccessDenied(Exception):
    """预览/下载入口的访问控制结果。"""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class Core:
    def __init__(self, data_dir: str, source_url: str, sync_interval: float = 1.0):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.source_url = source_url.rstrip("/")
        self.lock = threading.RLock()
        self.db = connect(os.path.join(data_dir, "collector.db"))
        self.store = BlobStore(data_dir)
        self.manager = DownloadManager()
        self._stop = threading.Event()
        self._sync_thread = None
        if sync_interval and sync_interval > 0:
            self._sync_thread = threading.Thread(target=self._sync_loop, args=(sync_interval,), daemon=True)
            self._sync_thread.start()

    def close(self):
        self._stop.set()

    # ---------- 来源 HTTP 客户端 ----------
    def _get_json(self, path: str, timeout: float = 10.0):
        with urllib.request.urlopen(self.source_url + path, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    # ---------- 审计 ----------
    def log(self, kind: str, record_id=None, detail: str = ""):
        with self.lock:
            self.db.execute(
                "INSERT INTO events(ts,kind,record_id,detail) VALUES(?,?,?,?)",
                (time.time(), kind, record_id, detail),
            )

    # ---------- 行读取 ----------
    def _record(self, rid):
        return self.db.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()

    def _att(self, att_id):
        return self.db.execute("SELECT * FROM attachments WHERE id=?", (att_id,)).fetchone()

    # ---------- 引用计数 ----------
    def _incref(self, h):
        self.db.execute("UPDATE blobs SET refcount=refcount+1 WHERE hash=?", (h,))

    def _decref(self, h):
        self.db.execute("UPDATE blobs SET refcount=refcount-1 WHERE hash=?", (h,))

    # ---------- 同步 ----------
    def _sync_loop(self, interval):
        while not self._stop.wait(interval):
            try:
                self.sync()
            except Exception as e:  # 来源短暂不可用不致命
                self.log("sync_error", detail=str(e))

    def sync(self):
        try:
            state = self._get_json("/api/state")
        except Exception as e:
            self.log("source_unreachable", detail=str(e))
            return {"ok": False, "error": str(e)}
        records = state.get("records", {})
        with self.lock:
            for rid, rmeta in records.items():
                rec = self._record(rid)
                if rec is None:
                    self._ingest(rid, rmeta)
                else:
                    self._sync_existing(rec, rmeta)
        return {"ok": True, "records": len(records)}

    def _ingest(self, rid, rmeta):
        now = time.time()
        authorized = 1 if rmeta["authorized"] else 0
        self.db.execute(
            "INSERT INTO records(id,title,authorized,auth_epoch,created_at,updated_at) VALUES(?,?,?,?,?,?)",
            (rid, rmeta["title"], authorized, 0 if authorized else 1, now, now),
        )
        for a in rmeta["attachments"]:
            self.db.execute(
                "INSERT INTO attachments(id,record_id,name,version,size,sha256,status,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (f"{rid}:{a['name']}", rid, a["name"], a["version"], a["size"], a["sha256"],
                 "pending" if authorized else "revoked", now),
            )
        self.log("record_ingested", rid,
                 f"采集记录元数据与附件索引（{len(rmeta['attachments'])} 个附件，"
                 f"{'已授权' if authorized else '未授权'}）")

    def _sync_existing(self, rec, rmeta):
        rid = rec["id"]
        if rec["authorized"] and not rmeta["authorized"]:
            self._apply_revoke(rid)
        elif not rec["authorized"] and rmeta["authorized"]:
            self._apply_grant(rid)
        if rmeta["authorized"]:
            self._refresh_index(rid, rmeta)

    def _refresh_index(self, rid, rmeta):
        """对齐来源附件索引：新增附件、发现内容替换（哈希变化）。"""
        now = time.time()
        for a in rmeta["attachments"]:
            att_id = f"{rid}:{a['name']}"
            row = self._att(att_id)
            if row is None:
                self.db.execute(
                    "INSERT INTO attachments(id,record_id,name,version,size,sha256,status,updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (att_id, rid, a["name"], a["version"], a["size"], a["sha256"], "pending", now),
                )
                self.log("attachment_added", rid, f"新附件 {a['name']}")
                continue
            if row["sha256"] != a["sha256"]:
                if row["status"] == "downloading":
                    self.manager.cancel_record(rid)  # 内容在下载途中被替换，取消旧任务
                new_status = "stale" if row["status"] in ("available", "downloading") else row["status"]
                self.db.execute(
                    "UPDATE attachments SET version=?, size=?, sha256=?, status=?, updated_at=? WHERE id=?",
                    (a["version"], a["size"], a["sha256"], new_status, now, att_id),
                )
                if new_status == "stale":
                    self.log("content_replaced", rid,
                             f"附件 {a['name']} 内容已被来源替换（v{a['version']}），旧缓存标记为待更新")
            else:
                self.db.execute(
                    "UPDATE attachments SET version=?, size=?, updated_at=? WHERE id=?",
                    (a["version"], a["size"], now, att_id),
                )

    # ---------- 撤权 / 重新授权 ----------
    def _apply_revoke(self, rid):
        now = time.time()
        self.db.execute(
            "UPDATE records SET authorized=0, auth_epoch=auth_epoch+1, updated_at=? WHERE id=?",
            (now, rid),
        )
        self.manager.cancel_record(rid)  # 下载途中撤权：通知工作线程停止
        n_tasks = self.db.execute(
            "UPDATE tasks SET status='aborted_revoked', finished_at=? WHERE record_id=? AND status='running'",
            (now, rid),
        ).rowcount
        linked = self.db.execute(
            "SELECT blob_hash FROM attachments WHERE record_id=? AND blob_hash IS NOT NULL", (rid,)
        ).fetchall()
        for row in linked:
            self._decref(row["blob_hash"])
        self.db.execute(
            "UPDATE attachments SET blob_hash=NULL, status='revoked', updated_at=? WHERE record_id=?",
            (now, rid),
        )
        self.log("record_revoked", rid,
                 f"授权撤回：解除 {len(linked)} 个附件内容引用，中止 {n_tasks} 个在途下载，"
                 f"临时分块已清除，仅保留必要审计")

    def _apply_grant(self, rid):
        now = time.time()
        self.db.execute("UPDATE records SET authorized=1, updated_at=? WHERE id=?", (now, rid))
        n = self.db.execute(
            "UPDATE attachments SET status='unconfirmed', updated_at=? WHERE record_id=? AND status='revoked'",
            (now, rid),
        ).rowcount
        self.log("record_reauthorized", rid,
                 f"重新授权：{n} 个附件转为待确认，旧缓存不直接发布，需重新确认或下载")

    # ---------- 下载 ----------
    def start_download(self, att_id: str):
        """单个附件下载/确认。返回结果 dict。三个阶段：校验 -> 拉来源元数据 -> 复用或起任务。"""
        with self.lock:
            att = self._att(att_id)
            if att is None:
                return {"ok": False, "error": "no_such_attachment"}
            rec = self._record(att["record_id"])
            if not rec["authorized"]:
                return {"ok": False, "error": "record_unauthorized"}
            if att["status"] == "available":
                return {"ok": True, "already": "available"}
            if att["status"] == "downloading":
                return {"ok": True, "already": "downloading"}
            rid = att["record_id"]
            name = att["name"]

        try:
            meta = self._get_json(f"/api/records/{rid}")
        except urllib.error.HTTPError as e:
            if e.code == 403:
                self.sync()  # 来源已撤权，尽快同步本地状态
                return {"ok": False, "error": "source_unauthorized"}
            return {"ok": False, "error": f"source_error:{e}"}
        except Exception as e:
            return {"ok": False, "error": f"source_unreachable:{e}"}
        att_meta = next((a for a in meta["attachments"] if a["name"] == name), None)
        if att_meta is None:
            return {"ok": False, "error": "attachment_gone_at_source"}

        with self.lock:
            att = self._att(att_id)
            rec = self._record(rid)
            if not rec["authorized"]:
                return {"ok": False, "error": "record_unauthorized"}
            if att["status"] == "downloading":
                return {"ok": True, "already": "downloading"}
            now = time.time()
            # 用来源最新元数据刷新期望哈希（这是“确认”的依据）
            self.db.execute(
                "UPDATE attachments SET version=?, size=?, sha256=?, updated_at=? WHERE id=?",
                (att_meta["version"], att_meta["size"], att_meta["sha256"], now, att_id),
            )
            expected = att_meta["sha256"]
            # 经来源元数据确认后，CAS 已有该内容 => 允许直接引用（不是盲目发布旧缓存）
            if self.store.has(expected):
                old = att["blob_hash"]
                if old and old != expected:
                    self._decref(old)
                self.db.execute(
                    "INSERT OR IGNORE INTO blobs(hash,size,refcount,created_at) VALUES(?,?,0,?)",
                    (expected, self.store.size_of(expected), now),
                )
                self.db.execute(
                    "UPDATE attachments SET blob_hash=?, status='available', updated_at=? WHERE id=?",
                    (expected, now, att_id),
                )
                self._incref(expected)
                self.log("cache_reuse_confirmed", rid,
                         f"附件 {name} 经来源元数据确认（sha256={expected[:12]}…），复用已有内容，无需重新传输")
                return {"ok": True, "reused": True}
            task_id = "task-" + uuid.uuid4().hex[:12]
            self.db.execute(
                "INSERT INTO tasks(id,record_id,attachment_id,status,total,created_at)"
                " VALUES(?,?,?,'running',?,?)",
                (task_id, rid, att_id, att_meta["size"], now),
            )
            self.db.execute(
                "UPDATE attachments SET status='downloading', updated_at=? WHERE id=?", (now, att_id)
            )
        url = self.source_url + att_meta["url"]
        self.manager.register(task_id, rid)
        threading.Thread(
            target=self._download_worker,
            args=(task_id, rid, att_id, url, expected),
            daemon=True,
        ).start()
        self.log("download_started", rid, f"附件 {name} 开始下载（任务 {task_id}）")
        return {"ok": True, "task": task_id}

    def start_record_downloads(self, rid: str):
        with self.lock:
            rows = self.db.execute(
                "SELECT id FROM attachments WHERE record_id=? AND status IN ('pending','stale','unconfirmed')",
                (rid,),
            ).fetchall()
        results = [self.start_download(r["id"]) for r in rows]
        return {"ok": True, "started": len(results), "results": results}

    def revalidate(self, rid: str):
        """重新授权后的恢复入口：对所有待确认附件执行“确认或重下”。"""
        with self.lock:
            rec = self._record(rid)
            if rec is None:
                return {"ok": False, "error": "no_such_record"}
            if not rec["authorized"]:
                return {"ok": False, "error": "record_unauthorized"}
            rows = self.db.execute(
                "SELECT id FROM attachments WHERE record_id=? AND status='unconfirmed'", (rid,)
            ).fetchall()
        results = [self.start_download(r["id"]) for r in rows]
        reused = sum(1 for r in results if r.get("reused"))
        return {"ok": True, "confirmed": len(results), "reused_from_cache": reused}

    class _Cancelled(Exception):
        pass

    def _download_worker(self, task_id, rid, att_id, url, expected):
        tmp = self.store.tmp_path(task_id)
        h = hashlib.sha256()
        n = 0
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=15) as resp, open(tmp, "wb") as f:
                while True:
                    if self.manager.is_cancelled(task_id):
                        raise self._Cancelled()
                    chunk = resp.read(CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
                    h.update(chunk)
                    n += len(chunk)
                    with self.lock:
                        self.db.execute(
                            "UPDATE tasks SET bytes_done=? WHERE id=? AND status='running'", (n, task_id)
                        )
            digest = h.hexdigest()
            if digest != expected:
                self.store.discard(tmp)
                with self.lock:
                    self.db.execute(
                        "UPDATE tasks SET status='failed', error='hash_mismatch', finished_at=?"
                        " WHERE id=? AND status='running'",
                        (time.time(), task_id),
                    )
                    self.db.execute(
                        "UPDATE attachments SET status='stale', updated_at=? WHERE id=? AND status='downloading'",
                        (time.time(), att_id),
                    )
                self.log("download_failed", rid,
                         f"任务 {task_id} 哈希校验失败（下载途中内容被替换？），已丢弃临时数据")
                return
            self._commit_download(task_id, rid, att_id, tmp, digest, n)
        except self._Cancelled:
            self.store.discard(tmp)
            with self.lock:
                self.db.execute(
                    "UPDATE tasks SET status='aborted_revoked', finished_at=?"
                    " WHERE id=? AND status='running'",
                    (time.time(), task_id),
                )
            self.log("download_aborted", rid, f"任务 {task_id} 因撤权中止，临时数据已删除，仅保留此审计")
        except Exception as e:
            self.store.discard(tmp)
            with self.lock:
                self.db.execute(
                    "UPDATE tasks SET status='failed', error=?, finished_at=? WHERE id=? AND status='running'",
                    (str(e)[:200], time.time(), task_id),
                )
                self.db.execute(
                    "UPDATE attachments SET status='pending', updated_at=? WHERE id=? AND status='downloading'",
                    (time.time(), att_id),
                )
            self.log("download_failed", rid, f"任务 {task_id} 失败：{e}")
        finally:
            self.manager.unregister(task_id)

    def _commit_download(self, task_id, rid, att_id, tmp, digest, nbytes):
        now = time.time()
        with self.lock:
            task = self.db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            rec = self._record(rid)
            if (task is None or task["status"] != "running" or not rec["authorized"]
                    or self.manager.is_cancelled(task_id)):
                # 与撤权竞争失败：内容不入库
                self.store.discard(tmp)
                if task is not None:
                    self.db.execute(
                        "UPDATE tasks SET status='aborted_revoked', finished_at=? WHERE id=? AND status='running'",
                        (now, task_id),
                    )
                self.log("download_aborted", rid, f"任务 {task_id} 完成前记录已撤权，内容已丢弃")
                return
            is_new = not self.store.has(digest)
            size = self.store.commit(tmp, digest)
            self.db.execute(
                "INSERT OR IGNORE INTO blobs(hash,size,refcount,created_at) VALUES(?,?,0,?)",
                (digest, size, now),
            )
            att = self._att(att_id)
            old = att["blob_hash"]
            if old and old != digest:
                self._decref(old)
            self.db.execute(
                "UPDATE attachments SET blob_hash=?, sha256=?, size=?, status='available', updated_at=?"
                " WHERE id=?",
                (digest, digest, nbytes, now, att_id),
            )
            self._incref(digest)
            self.db.execute(
                "UPDATE tasks SET status='done', bytes_done=?, finished_at=? WHERE id=?",
                (nbytes, now, task_id),
            )
            self.log("download_done", rid,
                     f"附件 {att['name']} 下载完成（{nbytes} B），"
                     f"{'新存内容' if is_new else '去重命中已有内容'} {digest[:12]}…")

    # ---------- 预览 / 下载入口的访问控制 ----------
    def check_access(self, att_id: str, epoch):
        """撤权后预览与旧下载入口都不可取正文：epoch 不符或未授权或内容未发布一律拒绝。"""
        with self.lock:
            att = self._att(att_id)
            if att is None:
                raise AccessDenied(404, "附件不存在")
            rec = self._record(att["record_id"])
            if epoch is None or int(epoch) != rec["auth_epoch"]:
                raise AccessDenied(410, "链接已失效：授权纪元已变更（撤权会使旧入口全部作废）")
            if not rec["authorized"]:
                raise AccessDenied(410, "授权已撤回，正文不可再获取")
            if att["status"] != "available" or not att["blob_hash"]:
                raise AccessDenied(409, f"内容未发布（当前状态 {att['status']}，需先确认/下载）")
            if not self.store.has(att["blob_hash"]):
                raise AccessDenied(410, "内容已被回收")
            return att, rec, self.store.blob_path(att["blob_hash"])

    # ---------- GC ----------
    def gc(self):
        """按显式引用关系回收：只删 refcount 重算后仍为 0 的 blob，绝不误删共享内容。"""
        now_deleted = []
        with self.lock:
            refs = {}
            for row in self.db.execute(
                "SELECT blob_hash, COUNT(*) c FROM attachments WHERE blob_hash IS NOT NULL GROUP BY blob_hash"
            ):
                refs[row["blob_hash"]] = row["c"]
            for b in self.db.execute("SELECT hash, size, refcount FROM blobs").fetchall():
                real = refs.get(b["hash"], 0)
                if real == 0:
                    self.store.delete(b["hash"])
                    self.db.execute("DELETE FROM blobs WHERE hash=?", (b["hash"],))
                    now_deleted.append(b["hash"])
                    self.log("gc_blob_deleted", None,
                             f"回收无引用内容 {b['hash'][:12]}…（{b['size']} B）")
                elif real != b["refcount"]:
                    self.db.execute("UPDATE blobs SET refcount=? WHERE hash=?", (real, b["hash"]))
                    self.log("gc_refcount_corrected", None,
                             f"校正 {b['hash'][:12]}… 引用计数 {b['refcount']} -> {real}")
            kept = {h: c for h, c in refs.items()}
            self.log("gc_run", None,
                     f"GC 完成：删除 {len(now_deleted)} 个无引用内容，保留 {len(kept)} 个仍被引用的内容")
        return {"ok": True, "deleted": now_deleted, "kept": kept}

    # ---------- 状态 ----------
    def state(self):
        with self.lock:
            records = []
            for rec in self.db.execute("SELECT * FROM records ORDER BY created_at"):
                atts = [dict(a) for a in self.db.execute(
                    "SELECT * FROM attachments WHERE record_id=? ORDER BY name", (rec["id"],))]
                records.append({**dict(rec), "attachments": atts})
            tasks = [dict(t) for t in self.db.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT 50")]
            blobs = [dict(b) for b in self.db.execute("SELECT * FROM blobs ORDER BY created_at")]
            events = [dict(e) for e in self.db.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT 200")]
        return {
            "source_url": self.source_url,
            "records": records,
            "tasks": tasks,
            "blobs": blobs,
            "events": events,
            "tmp_leftovers": self.store.leftover_tmps(),
        }
