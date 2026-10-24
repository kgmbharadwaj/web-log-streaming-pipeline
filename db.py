"""Shared SQLite helpers."""
import sqlite3

DB_PATH = "weblogs.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS web_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT UNIQUE NOT NULL,   -- UUID, makes inserts idempotent
    ts          TEXT NOT NULL,          -- 'YYYY-MM-DD HH:MM:SS' (UTC)
    ip          TEXT NOT NULL,
    method      TEXT NOT NULL,
    path        TEXT NOT NULL,
    status      INTEGER NOT NULL,
    response_ms INTEGER NOT NULL,
    user_agent  TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_ts     ON web_logs(ts);
CREATE INDEX IF NOT EXISTS idx_logs_status ON web_logs(status);
"""


def init_db(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn
