"""
Local state database for the endpoint client.

The client keeps track of which packages are installed and at what
version in a small SQLite database.  This information is used to make
idempotent decisions when computing actions from the server plan.  If
a package is already at the desired version, the client will report
success without attempting installation again.

This module is pure Python and has no external dependencies.
"""

from __future__ import annotations

import sqlite3
import datetime
from typing import Optional, Dict, Any, List


class ClientDB:
    """Simple wrapper around a SQLite database storing installed packages."""

    def __init__(self, path: str = "client.db") -> None:
        self.path = path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS installed_packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_id INTEGER,
                name TEXT,
                version TEXT,
                status TEXT,
                last_action TEXT,
                last_rc INTEGER
            )
            """
        )
        self.conn.commit()

    def get_installed(self, package_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM installed_packages WHERE package_id = ?",
            (package_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def list_installed(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM installed_packages ORDER BY name"
        )
        return [dict(row) for row in cur.fetchall()]

    def update_installed(
        self, package_id: int, name: str, version: str, status: str, rc: int
    ) -> None:
        now = datetime.datetime.utcnow().isoformat()
        with self.conn:
            cur = self.conn.execute(
                "SELECT id FROM installed_packages WHERE package_id = ?",
                (package_id,),
            )
            row = cur.fetchone()
            if row:
                self.conn.execute(
                    "UPDATE installed_packages SET version = ?, status = ?, last_action = ?, last_rc = ? WHERE package_id = ?",
                    (version, status, now, rc, package_id),
                )
            else:
                self.conn.execute(
                    "INSERT INTO installed_packages (package_id, name, version, status, last_action, last_rc) VALUES (?, ?, ?, ?, ?, ?)",
                    (package_id, name, version, status, now, rc),
                )