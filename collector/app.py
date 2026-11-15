"""采集端 HTTP 服务：采集页 + JSON API + 预览/下载入口（带授权纪元校验）。

运行：python3 -m collector.app --port 8950 --source http://127.0.0.1:8951 --data ./data
"""
import argparse
import json
import os
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .core import AccessDenied, Core

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")


def make_handler(core: Core):
    class Handler(BaseHTTPRequestHandler):
        server_version = "Collector/1.0"

        def log_message(self, fmt, *args):
            pass

        def _json(self, code, obj):
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _text(self, code, msg):
            body = msg.encode()
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        # ---------- GET ----------
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query)
            if path == "/":
                with open(os.path.join(WEB_DIR, "index.html"), "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/api/state":
                return self._json(200, core.state())
            if path.startswith("/preview/") or path.startswith("/download/"):
                att_id = urllib.parse.unquote(path.split("/", 2)[2])
                epoch = qs.get("e", [None])[0]
                try:
                    att, rec, blob_path = core.check_access(att_id, epoch)
                except AccessDenied as d:
                    return self._text(d.status, f"{d.status} {d.message}")
                if path.startswith("/preview/"):
                    with open(blob_path, "rb") as f:
                        head = f.read(512)
                    hexs = head.hex(" ", 1)
                    asc = "".join(chr(b) if 32 <= b < 127 else "." for b in head)
                    body = (
                        f"记录: {rec['id']} {rec['title']}\n附件: {att['name']}  "
                        f"大小: {att['size']} B  sha256: {att['blob_hash']}\n"
                        f"授权纪元: {rec['auth_epoch']}\n\n前 512 字节 (hex):\n{hexs}\n\nASCII:\n{asc}\n"
                    )
                    return self._text(200, body)
                # 正式下载：流式输出完整内容
                size = os.path.getsize(blob_path)
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(size))
                self.send_header("Content-Disposition",
                                 f"attachment; filename=\"{att['name']}\"")
                self.send_header("X-Content-Sha256", att["blob_hash"])
                self.end_headers()
                with open(blob_path, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                return
            return self._json(404, {"error": "not_found"})

        # ---------- POST ----------
        def do_POST(self):
            parts = [urllib.parse.unquote(p) for p in urllib.parse.urlparse(self.path).path.split("/") if p]
            try:
                if parts == ["api", "sync"]:
                    return self._json(200, core.sync())
                if parts == ["api", "gc"]:
                    return self._json(200, core.gc())
                if len(parts) == 4 and parts[:2] == ["api", "records"] and parts[3] == "download":
                    return self._json(200, core.start_record_downloads(parts[2]))
                if len(parts) == 4 and parts[:2] == ["api", "records"] and parts[3] == "revalidate":
                    return self._json(200, core.revalidate(parts[2]))
                if len(parts) == 3 and parts[:2] == ["api", "attachments"]:
                    att_id = parts[2]
                    r = core.start_download(att_id)
                    return self._json(200 if r.get("ok") else 409, r)
                # 模拟外部来源的管理操作（真实场景中由来源方自己操作），代理后立刻同步
                if len(parts) >= 4 and parts[:2] == ["api", "source"]:
                    action, rid = parts[2], parts[3]
                    tail = "/" + parts[4] if len(parts) == 5 else ""
                    if action not in ("revoke", "grant", "replace"):
                        return self._json(400, {"error": "bad_action"})
                    url = f"{core.source_url}/admin/{action}/{rid}{tail}"
                    try:
                        req = urllib.request.Request(url, data=b"{}", method="POST")
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            src = json.loads(resp.read().decode())
                    except Exception as e:
                        return self._json(502, {"error": f"source_admin_failed: {e}"})
                    core.sync()
                    return self._json(200, {"source": src, "synced": True})
                return self._json(404, {"error": "not_found"})
            except Exception as e:
                return self._json(500, {"error": str(e)})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8950)
    ap.add_argument("--source", default="http://127.0.0.1:8951")
    ap.add_argument("--data", default="./data")
    ap.add_argument("--sync-interval", type=float, default=1.0)
    args = ap.parse_args()
    core = Core(args.data, args.source, sync_interval=args.sync_interval)
    core.sync()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(core))
    srv.daemon_threads = True
    print(f"[collector] 采集页: http://127.0.0.1:{args.port}  (来源: {args.source})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        core.close()


if __name__ == "__main__":
    main()
