"""SQLite 元数据层：记录、附件索引、下载任务、内容(blob)引用、审计事件。

关联关系（采集页把三者关联起来）：
  records 1─n attachments（附件索引，含指向 blobs 的显式引用 blob_hash）
  records 1─n tasks（下载任务）
  blobs   1─n attachments（同一份内容可被多条记录的附件共享，省空间）
  events  审计/历史
"""
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS records(
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  authorized INTEGER NOT NULL DEFAULT 1,
  auth_epoch INTEGER NOT NULL DEFAULT 0,   -- 撤权时 +1：使旧预览/下载链接（携带旧 epoch）失效
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS attachments(
  id TEXT PRIMARY KEY,                     -- "<record_id>:<name>"
  record_id TEXT NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 0,      -- 来源侧内容版本
  size INTEGER,
  sha256 TEXT,                             -- 来源元数据声明的期望哈希（重新确认的依据）
  status TEXT NOT NULL DEFAULT 'pending',  -- pending|downloading|available|stale|unconfirmed|revoked
  blob_hash TEXT,                          -- 显式引用：指向 blobs.hash；撤权时置空
  updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_att_record ON attachments(record_id);
CREATE INDEX IF NOT EXISTS idx_att_blob ON attachments(blob_hash);
CREATE TABLE IF NOT EXISTS blobs(
  hash TEXT PRIMARY KEY,                   -- 内容寻址：sha256
  size INTEGER NOT NULL,
  refcount INTEGER NOT NULL DEFAULT 0,     -- 引用计数（GC 前会按 attachments 重算校正）
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks(
  id TEXT PRIMARY KEY,
  record_id TEXT NOT NULL,
  attachment_id TEXT NOT NULL,
  status TEXT NOT NULL,                    -- running|done|aborted_revoked|failed
  bytes_done INTEGER NOT NULL DEFAULT 0,
  total INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at REAL NOT NULL,
  finished_at REAL
);
CREATE INDEX IF NOT EXISTS idx_tasks_record ON tasks(record_id);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  record_id TEXT,
  detail TEXT
);
"""


def connect(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)  # autocommit
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(SCHEMA)
    return db
