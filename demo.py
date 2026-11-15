#!/usr/bin/env python3
"""端到端复现与验证：两个记录共享同一内容，仅其中一个被撤权。

场景：
  1. 启动慢速来源服务 + 采集服务（真实 HTTP 进程）
  2. 两条记录并发下载全部附件 -> 共享内容只存一份（refcount=2）
  3. 替换 rec-a 独有附件内容 -> 重新下载途中，来源撤权 rec-a
     -> 在途任务中止、临时数据清除、旧预览/下载入口 410、共享内容因 rec-b 引用而保留
  4. GC -> 只回收无引用的 rec-a 旧内容，共享内容不误删
  5. 重新授权 rec-a -> 旧缓存不直接发布（409），需重新确认
     -> 共享内容经来源元数据确认后复用，独有附件重新下载新版本
全部断言通过则退出码 0。
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
CHUNK_DELAY = 0.03

PASS = []


def ok(name, cond, extra=""):
    status = "PASS" if cond else "FAIL"
    PASS.append(cond)
    print(f"  [{status}] {name}" + (f"  ({extra})" if extra else ""))
    if not cond:
        raise AssertionError(name)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def get(url, expect=200):
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        if expect is not None and e.code == expect:
            return e.code, e.read()
        raise


def get_json(url):
    code, body = get(url)
    return json.loads(body.decode())


def post_json(url):
    req = urllib.request.Request(url, data=b"{}", method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def wait_for(desc, cond, timeout=30.0, interval=0.2):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = cond()
        except Exception:
            v = None
        if v:
            return v
        time.sleep(interval)
    raise TimeoutError(f"等待超时: {desc}")


def main():
    src_port, col_port = free_port(), free_port()
    SRC = f"http://127.0.0.1:{src_port}"
    COL = f"http://127.0.0.1:{col_port}"
    data_dir = tempfile.mkdtemp(prefix="collector-demo-")
    procs = []
    try:
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "source_server.py"),
             "--port", str(src_port), "--chunk-delay", str(CHUNK_DELAY)],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        procs.append(subprocess.Popen(
            [sys.executable, "-m", "collector.app",
             "--port", str(col_port), "--source", SRC,
             "--data", data_dir, "--sync-interval", "0.3"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait_for("来源服务就绪", lambda: get_json(SRC + "/api/state"))
        wait_for("采集服务就绪", lambda: get_json(COL + "/api/state") is not None)

        print("\n== 1. 同步来源：采集元数据与附件索引 ==")
        post_json(COL + "/api/sync")
        st = get_json(COL + "/api/state")
        ok("两条记录已采集", {r["id"] for r in st["records"]} == {"rec-a", "rec-b"})
        ok("附件索引已建立", all(len(r["attachments"]) == 2 for r in st["records"]))

        print("\n== 2. 并发下载两条记录的全部附件 ==")
        post_json(COL + "/api/records/rec-a/download")
        post_json(COL + "/api/records/rec-b/download")
        wait_for("全部附件可用", lambda: all(
            a["status"] == "available"
            for r in get_json(COL + "/api/state")["records"] for a in r["attachments"]), 60)
        st = get_json(COL + "/api/state")
        ok("下载任务全部完成", all(t["status"] == "done" for t in st["tasks"]))
        ok("共享内容只存一份（3 个 blob）", len(st["blobs"]) == 3,
           f"blobs={len(st['blobs'])}")
        shared = [b for b in st["blobs"] if b["refcount"] == 2]
        ok("共享 blob 引用计数=2", len(shared) == 1)
        shared_hash = shared[0]["hash"]
        rec_a = next(r for r in st["records"] if r["id"] == "rec-a")
        a_shared = next(a for a in rec_a["attachments"] if a["name"] == "shared-dataset.bin")
        old_url = f"{COL}/download/{a_shared['id']}?e={rec_a['auth_epoch']}"
        code, _ = get(old_url)
        ok("撤权前旧下载入口可用", code == 200)

        print("\n== 3. 来源替换 rec-a 独有附件内容 -> 标记 stale ==")
        post_json(SRC + "/admin/replace/rec-a/a-field-notes.bin")
        post_json(COL + "/api/sync")
        st = get_json(COL + "/api/state")
        rec_a = next(r for r in st["records"] if r["id"] == "rec-a")
        a_notes = next(a for a in rec_a["attachments"] if a["name"] == "a-field-notes.bin")
        ok("替换被识别为 stale", a_notes["status"] == "stale")
        old_notes_blob = a_notes["blob_hash"]

        print("\n== 4. 下载途中撤权 rec-a（慢速来源，下载约需 2 秒） ==")
        post_json(f"{COL}/api/attachments/{urllib.parse.quote(a_notes['id'])}")
        st = get_json(COL + "/api/state")
        ok("任务进入 running", any(t["status"] == "running" for t in st["tasks"]))
        time.sleep(0.6)  # 下载进行中
        post_json(SRC + "/admin/revoke/rec-a")
        wait_for("采集端感知撤权", lambda: not next(
            r for r in get_json(COL + "/api/state")["records"] if r["id"] == "rec-a")["authorized"], 10)
        time.sleep(0.5)  # 等工作线程清理
        st = get_json(COL + "/api/state")
        rec_a = next(r for r in st["records"] if r["id"] == "rec-a")
        rec_b = next(r for r in st["records"] if r["id"] == "rec-b")
        ok("rec-a 已撤权且授权纪元+1", rec_a["auth_epoch"] == 1)
        ok("在途任务被中止(aborted_revoked)",
           any(t["status"] == "aborted_revoked" for t in st["tasks"]))
        ok("临时分块已清除（只留必要审计）", st["tmp_leftovers"] == [])
        ok("rec-a 附件全部 revoked 且无内容引用",
           all(a["status"] == "revoked" and a["blob_hash"] is None
               for a in rec_a["attachments"]))
        shared_blob = next(b for b in st["blobs"] if b["hash"] == shared_hash)
        ok("共享 blob 引用计数降为 1（rec-b 仍引用）", shared_blob["refcount"] == 1)
        ok("rec-b 不受影响，附件仍可用",
           all(a["status"] == "available" for a in rec_b["attachments"]))

        print("\n== 5. 撤权后预览与旧下载入口不可取正文 ==")
        try:
            code, _ = get(old_url)
            code = 999
        except urllib.error.HTTPError as e:
            code = e.code
        ok("旧下载入口(旧epoch) -> 410", code == 410, f"got {code}")
        a_shared = next(a for a in rec_a["attachments"] if a["name"] == "shared-dataset.bin")
        for label, url in [
            ("预览(新epoch)", f"{COL}/preview/{urllib.parse.quote(a_shared['id'])}?e=1"),
            ("下载(新epoch)", f"{COL}/download/{urllib.parse.quote(a_shared['id'])}?e=1"),
        ]:
            try:
                get(url)
                code = 999
            except urllib.error.HTTPError as e:
                code = e.code
            ok(f"撤权后{label} -> 410", code == 410, f"got {code}")
        b_shared = next(a for a in rec_b["attachments"] if a["name"] == "shared-dataset.bin")
        code, body = get(f"{COL}/download/{urllib.parse.quote(b_shared['id'])}?e={rec_b['auth_epoch']}")
        ok("rec-b 的共享附件仍可下载", code == 200 and len(body) == 2_000_000)

        print("\n== 6. GC：不误删其他记录仍在用的内容 ==")
        r = post_json(COL + "/api/gc")
        ok("rec-a 旧独有内容被回收", old_notes_blob in r["deleted"])
        ok("共享内容未被误删", shared_hash not in r["deleted"]
           and os.path.exists(os.path.join(data_dir, "blobs", shared_hash)))
        st = get_json(COL + "/api/state")
        ok("剩余 blob 均被引用", all(b["refcount"] >= 1 for b in st["blobs"]))

        print("\n== 7. 重新授权：未确认缓存不得直接发布 ==")
        post_json(SRC + "/admin/grant/rec-a")
        wait_for("采集端感知重新授权", lambda: next(
            r for r in get_json(COL + "/api/state")["records"] if r["id"] == "rec-a")["authorized"], 10)
        st = get_json(COL + "/api/state")
        rec_a = next(r for r in st["records"] if r["id"] == "rec-a")
        ok("附件进入待确认(unconfirmed)，未直接发布",
           all(a["status"] == "unconfirmed" and a["blob_hash"] is None
               for a in rec_a["attachments"]))
        a_shared = next(a for a in rec_a["attachments"] if a["name"] == "shared-dataset.bin")
        try:
            get(f"{COL}/preview/{urllib.parse.quote(a_shared['id'])}?e={rec_a['auth_epoch']}")
            code = 999
        except urllib.error.HTTPError as e:
            code = e.code
        ok("待确认状态预览 -> 409（缓存未确认不发布）", code == 409, f"got {code}")
        try:
            get(old_url)
            code = 999
        except urllib.error.HTTPError as e:
            code = e.code
        ok("撤权前的旧下载入口仍 -> 410", code == 410, f"got {code}")

        print("\n== 8. 重新确认恢复：共享内容确认后复用，替换内容重新下载 ==")
        post_json(COL + "/api/records/rec-a/revalidate")
        wait_for("rec-a 附件恢复可用", lambda: all(
            a["status"] == "available"
            for a in next(r for r in get_json(COL + "/api/state")["records"]
                          if r["id"] == "rec-a")["attachments"]), 60)
        st = get_json(COL + "/api/state")
        rec_a = next(r for r in st["records"] if r["id"] == "rec-a")
        shared_blob = next(b for b in st["blobs"] if b["hash"] == shared_hash)
        ok("共享 blob 引用计数恢复为 2", shared_blob["refcount"] == 2)
        ok("共享内容经确认后复用（有 cache_reuse_confirmed 事件）",
           any(e["kind"] == "cache_reuse_confirmed" and e["record_id"] == "rec-a"
               for e in st["events"]))
        a_notes = next(a for a in rec_a["attachments"] if a["name"] == "a-field-notes.bin")
        ok("被替换的附件拿到新版本", a_notes["version"] == 2
           and a_notes["blob_hash"] != old_notes_blob)
        code, _ = get(f"{COL}/preview/{urllib.parse.quote(a_shared['id'])}?e={rec_a['auth_epoch']}")
        ok("恢复后预览可用", code == 200)

        print("\n== 9. 历史/审计完整性 ==")
        kinds = {e["kind"] for e in st["events"]}
        for k in ["record_ingested", "download_done", "content_replaced", "record_revoked",
                  "download_aborted", "gc_blob_deleted", "record_reauthorized",
                  "cache_reuse_confirmed"]:
            ok(f"事件存在: {k}", k in kinds)

        print("\n" + ("=" * 50))
        print(f"全部 {len(PASS)} 项断言通过 ✔")
        return 0
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, TimeoutError) as e:
        print(f"\n演示失败: {e}")
        sys.exit(1)
