#!/usr/bin/env python3
"""慢速外部来源服务（模拟“外部记录系统”）。

特点：
- 所有附件内容均为生成的样例：sha256 计数器流，确定性、可校验、无需真实外部数据。
- 每条记录有授权开关；撤权后旧 token 立即失效，来源侧旧下载地址同样取不到正文。
- 附件内容可被替换（版本号 +1，同一 URL 返回新内容、新哈希）。
- 下载刻意缓慢（分块 + 延时），便于复现“下载途中撤权”。

管理接口（模拟外部世界对来源的操作）：
  POST /admin/revoke/<rid>            撤回某条记录的授权
  POST /admin/grant/<rid>             重新授权
  POST /admin/replace/<rid>/<name>    替换某附件内容（版本号递增）

只读接口：
  GET  /api/state                     目录状态（含授权位、附件哈希，不需要 token）
  GET  /api/records/<rid>             记录元数据 + 带 token 的附件下载地址（未授权时 403）
  GET  /attachments/<rid>/<name>?token=...   慢速流式下载正文（token 失效即 403）
"""
import argparse
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

CHUNK = 16384  # 16KB 一块，块间sleep，模拟慢速来源

# 两条记录共享同一个逻辑附件 shared-dataset.bin（相同 seed => 相同内容 => 采集端去重为同一 blob）
RECORDS = {
    "rec-a": {
        "title": "记录 A · 现场勘查报告",
        "attachments": [
            {"name": "shared-dataset.bin", "seed": "shared-dataset", "size": 2_000_000},
            {"name": "a-field-notes.bin", "seed": "a-field-notes", "size": 1_200_000},
        ],
    },
    "rec-b": {
        "title": "记录 B · 复核报告",
        "attachments": [
            {"name": "shared-dataset.bin", "seed": "shared-dataset", "size": 2_000_000},
            {"name": "b-photo-set.bin", "seed": "b-photo-set", "size": 900_000},
        ],
    },
}


def iter_content(seed: str, size: int, chunk: int = CHUNK):
    """确定性伪随机字节流：sha256(seed:counter) 拼接。"""
    remaining = size
    counter = 0
    while remaining > 0:
        want = min(chunk, remaining)
        block = b""
        while len(block) < want:
            block += hashlib.sha256(f"{seed}:{counter}".encode()).digest()
            counter += 1
        part = block[:want]
        remaining -= len(part)
        yield part


def content_hash(seed: str, size: int) -> str:
    h = hashlib.sha256()
    for part in iter_content(seed, size):
        h.update(part)
    return h.hexdigest()


class SourceState:
    """来源侧可变状态：授权位、授权版本（决定 token）、附件内容版本。"""

    def __init__(self):
        self.lock = threading.RLock()
        self.authorized = {rid: True for rid in RECORDS}
        self.auth_version = {rid: 1 for rid in RECORDS}
        self.att_version = {
            rid: {a["name"]: 1 for a in meta["attachments"]} for rid, meta in RECORDS.items()
        }
        self._hash_cache = {}

    def token(self, rid):
        return f"tok-{rid}-{self.auth_version[rid]}"

    def token_valid(self, rid, token):
        with self.lock:
            return self.authorized[rid] and token == self.token(rid)

    def att_meta(self, rid, name):
        base = next(a for a in RECORDS[rid]["attachments"] if a["name"] == name)
        version = self.att_version[rid][name]
        seed = f"{base['seed']}-v{version}"
        key = (seed, base["size"])
        if key not in self._hash_cache:
            self._hash_cache[key] = content_hash(seed, base["size"])
        return {
            "name": name,
            "size": base["size"],
            "version": version,
            "seed": seed,
            "sha256": self._hash_cache[key],
        }

    def revoke(self, rid):
        with self.lock:
            self.authorized[rid] = False
            self.auth_version[rid] += 1  # 旧 token 立即失效
            return self.auth_version[rid]

    def grant(self, rid):
        with self.lock:
            self.authorized[rid] = True
            self.auth_version[rid] += 1  # 签发新 token
            return self.auth_version[rid]

    def replace(self, rid, name):
        with self.lock:
            self.att_version[rid][name] += 1
            return self.att_version[rid][name]

    def directory(self):
        with self.lock:
            out = {}
            for rid, meta in RECORDS.items():
                out[rid] = {
                    "title": meta["title"],
                    "authorized": self.authorized[rid],
                    "auth_version": self.auth_version[rid],
                    "attachments": [
                        {k: v for k, v in self.att_meta(rid, a["name"]).items() if k != "seed"}
                        for a in meta["attachments"]
                    ],
                }
            return out


STATE = SourceState()
CHUNK_DELAY = 0.03


class Handler(BaseHTTPRequestHandler):
    server_version = "SlowSource/1.0"

    def log_message(self, fmt, *args):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/":
            return self._json(200, {
                "service": "slow-source",
                "records": list(RECORDS),
                "hint": "GET /api/state, GET /api/records/<rid>, GET /attachments/<rid>/<name>?token=",
            })
        if path == "/api/state":
            return self._json(200, {"records": STATE.directory()})
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "records":
            rid = parts[2]
            if rid not in RECORDS:
                return self._json(404, {"error": "no_such_record"})
            with STATE.lock:
                if not STATE.authorized[rid]:
                    return self._json(403, {"error": "unauthorized", "record": rid})
                atts = []
                for a in RECORDS[rid]["attachments"]:
                    m = STATE.att_meta(rid, a["name"])
                    atts.append({
                        "name": m["name"], "size": m["size"], "version": m["version"],
                        "sha256": m["sha256"],
                        "url": f"/attachments/{rid}/{a['name']}?token={STATE.token(rid)}",
                    })
                return self._json(200, {
                    "id": rid, "title": RECORDS[rid]["title"], "token": STATE.token(rid),
                    "attachments": atts,
                })
        if len(parts) == 3 and parts[0] == "attachments":
            rid, name = parts[1], parts[2]
            token = parse_qs(parsed.query).get("token", [""])[0]
            if rid not in RECORDS or name not in [a["name"] for a in RECORDS.get(rid, {"attachments": []})["attachments"]]:
                return self._json(404, {"error": "no_such_attachment"})
            if not STATE.token_valid(rid, token):
                return self._json(403, {"error": "token_invalid_or_revoked"})
            m = STATE.att_meta(rid, name)
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(m["size"]))
            self.send_header("X-Content-Sha256", m["sha256"])
            self.end_headers()
            try:
                for part in iter_content(m["seed"], m["size"]):
                    self.wfile.write(part)
                    self.wfile.flush()
                    time.sleep(CHUNK_DELAY)  # 慢速来源
            except (BrokenPipeError, ConnectionResetError):
                pass  # 采集端撤权中止，主动断开
            return
        return self._json(404, {"error": "not_found"})

    def do_POST(self):
        parts = [p for p in urlparse(self.path).path.split("/") if p]
        if len(parts) >= 3 and parts[0] == "admin":
            action, rid = parts[1], parts[2]
            if rid not in RECORDS:
                return self._json(404, {"error": "no_such_record"})
            if action == "revoke":
                return self._json(200, {"record": rid, "authorized": False, "auth_version": STATE.revoke(rid)})
            if action == "grant":
                return self._json(200, {"record": rid, "authorized": True, "auth_version": STATE.grant(rid)})
            if action == "replace" and len(parts) == 4:
                name = parts[3]
                if name not in [a["name"] for a in RECORDS[rid]["attachments"]]:
                    return self._json(404, {"error": "no_such_attachment"})
                v = STATE.replace(rid, name)
                m = STATE.att_meta(rid, name)
                return self._json(200, {"record": rid, "attachment": name, "version": v, "sha256": m["sha256"]})
        return self._json(404, {"error": "not_found"})


def main():
    global CHUNK_DELAY
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8951)
    ap.add_argument("--chunk-delay", type=float, default=0.03, help="每 16KB 块的延时秒数")
    args = ap.parse_args()
    CHUNK_DELAY = args.chunk_delay
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    srv.daemon_threads = True
    print(f"[source] 慢速来源服务: http://127.0.0.1:{args.port} (chunk_delay={CHUNK_DELAY}s)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
