#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List, Optional
from urllib import parse, request


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.write_lock = threading.Lock()
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS applied_ops (
                    op_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def get(self, key: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT key, value, updated_at FROM kv WHERE key = ?",
                (key,),
            ).fetchone()
            return dict(row) if row else None

    def all_items(self):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key, value, updated_at FROM kv ORDER BY key"
            ).fetchall()
            return [dict(r) for r in rows]

    def apply_op(
        self,
        op_id: str,
        action: str,
        key: str,
        value: Optional[str] = None,
    ) -> bool:
        with self.write_lock:
            with self._connect() as conn:
                already = conn.execute(
                    "SELECT 1 FROM applied_ops WHERE op_id = ?",
                    (op_id,),
                ).fetchone()
                if already:
                    return False

                now = utc_now()

                if action == "put":
                    conn.execute(
                        """
                        INSERT INTO kv (key, value, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value = excluded.value,
                            updated_at = excluded.updated_at
                        """,
                        (key, value, now),
                    )
                elif action == "delete":
                    conn.execute("DELETE FROM kv WHERE key = ?", (key,))
                else:
                    raise ValueError(f"unsupported action: {action}")

                conn.execute(
                    "INSERT INTO applied_ops (op_id, action, applied_at) VALUES (?, ?, ?)",
                    (op_id, action, now),
                )
                conn.commit()
                return True


class ReplicatedNode:
    def __init__(
        self,
        node_id: str,
        role: str,
        port: int,
        db_path: str,
        peers: List[str],
    ):
        self.node_id = node_id
        self.role = role
        self.port = port
        self.store = SQLiteStore(db_path)
        self.peers = [p.rstrip("/") for p in peers if p.strip()]

    @property
    def is_leader(self) -> bool:
        return self.role == "leader"

    def replicate_to_followers(self, payload: dict):
        failures = []
        for peer in self.peers:
            url = f"{peer}/internal/replicate"
            req = request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with request.urlopen(req, timeout=2) as resp:
                    if resp.status >= 300:
                        failures.append({"peer": peer, "status": resp.status})
            except Exception as exc:
                failures.append({"peer": peer, "error": str(exc)})
        return failures


class Handler(BaseHTTPRequestHandler):
    server_version = "DistSQLite/0.1"

    @property
    def node(self) -> ReplicatedNode:
        return self.server.node  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):
        print(f"[{self.node.node_id}] {self.address_string()} - {fmt % args}")

    def _send_json(self, status: int, payload):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8") or "{}")

    def do_GET(self):
        path = parse.urlparse(self.path).path

        if path == "/health":
            return self._send_json(
                200,
                {
                    "ok": True,
                    "node_id": self.node.node_id,
                    "role": self.node.role,
                    "port": self.node.port,
                    "peers": self.node.peers,
                },
            )

        if path == "/kv":
            return self._send_json(200, {"items": self.node.store.all_items()})

        if path.startswith("/kv/"):
            key = path[len("/kv/") :]
            item = self.node.store.get(key)
            if item is None:
                return self._send_json(404, {"error": "key not found", "key": key})
            return self._send_json(200, item)

        return self._send_json(404, {"error": "not found"})

    def do_PUT(self):
        path = parse.urlparse(self.path).path

        if not path.startswith("/kv/"):
            return self._send_json(404, {"error": "not found"})

        if not self.node.is_leader:
            return self._send_json(403, {"error": "writes must go to the leader"})

        key = path[len("/kv/") :]
        data = self._read_json()
        if "value" not in data:
            return self._send_json(400, {"error": "JSON body must contain 'value'"})

        op_id = str(uuid.uuid4())
        payload = {
            "op_id": op_id,
            "action": "put",
            "key": key,
            "value": str(data["value"]),
            "source": self.node.node_id,
            "ts": utc_now(),
        }

        self.node.store.apply_op(
            op_id=op_id,
            action="put",
            key=key,
            value=payload["value"],
        )

        failures = self.node.replicate_to_followers(payload)

        return self._send_json(
            200,
            {
                "ok": True,
                "leader": self.node.node_id,
                "replication_failures": failures,
                "item": self.node.store.get(key),
            },
        )

    def do_DELETE(self):
        path = parse.urlparse(self.path).path

        if not path.startswith("/kv/"):
            return self._send_json(404, {"error": "not found"})

        if not self.node.is_leader:
            return self._send_json(403, {"error": "writes must go to the leader"})

        key = path[len("/kv/") :]
        op_id = str(uuid.uuid4())
        payload = {
            "op_id": op_id,
            "action": "delete",
            "key": key,
            "value": None,
            "source": self.node.node_id,
            "ts": utc_now(),
        }

        self.node.store.apply_op(op_id=op_id, action="delete", key=key)
        failures = self.node.replicate_to_followers(payload)

        return self._send_json(
            200,
            {
                "ok": True,
                "leader": self.node.node_id,
                "deleted": key,
                "replication_failures": failures,
            },
        )

    def do_POST(self):
        path = parse.urlparse(self.path).path

        if path != "/internal/replicate":
            return self._send_json(404, {"error": "not found"})

        data = self._read_json()
        missing = [k for k in ("op_id", "action", "key") if k not in data]
        if missing:
            return self._send_json(
                400, {"error": f"missing fields: {', '.join(missing)}"}
            )

        try:
            applied = self.node.store.apply_op(
                op_id=data["op_id"],
                action=data["action"],
                key=data["key"],
                value=data.get("value"),
            )
        except ValueError as exc:
            return self._send_json(400, {"error": str(exc)})

        return self._send_json(
            200,
            {
                "ok": True,
                "node_id": self.node.node_id,
                "applied": applied,
            },
        )


def main():
    parser = argparse.ArgumentParser(
        description="Tiny distributed SQLite key-value store"
    )
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--role", choices=["leader", "follower"], required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--db", required=True, help="Path to sqlite db file")
    parser.add_argument(
        "--peers",
        default="",
        help="Comma-separated follower URLs. Example: http://127.0.0.1:8002,http://127.0.0.1:8003",
    )
    args = parser.parse_args()

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    node = ReplicatedNode(
        node_id=args.node_id,
        role=args.role,
        port=args.port,
        db_path=args.db,
        peers=peers,
    )

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.node = node  # type: ignore[attr-defined]

    print(
        json.dumps(
            {
                "node_id": node.node_id,
                "role": node.role,
                "port": node.port,
                "db": args.db,
                "peers": node.peers,
            },
            indent=2,
        )
    )
    print(f"Serving on http://127.0.0.1:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
