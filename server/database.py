"""Database access layer for the endpoint manager server.

This module defines a ``Database`` class that wraps a SQLite connection and
provides high level CRUD operations for computers, packages, assignments and
jobs.  All methods operate on plain Python types (dicts, lists) to keep
application logic simple and free of ORM dependencies.  The schema is
created automatically on first use.

The default database path is controlled by the ``SERVER_DB_PATH``
environment variable.  If unset, the database file ``server.db`` in
the current working directory will be used.
"""

from __future__ import annotations

import json
import os
import sqlite3
import datetime
from typing import Any, Dict, List, Optional


class Database:
    """Encapsulates all database operations for the server."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        # Determine the path to the SQLite file.  Allow overriding via
        # environment variable for easy deployment.
        if db_path is None:
            db_path = os.getenv("SERVER_DB_PATH", "server.db")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def _create_tables(self) -> None:
        """Create all tables if they do not already exist."""
        cur = self.conn.cursor()
        # Computers table: one row per enrolled device
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS computers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hostname TEXT UNIQUE NOT NULL,
                os TEXT,
                arch TEXT,
                tags TEXT,
                last_check_in TEXT,
                client_version TEXT,
                token TEXT UNIQUE,
                token_revoked INTEGER DEFAULT 0
            )
            """
        )
        # Packages table: catalogue of software packages
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                platform TEXT NOT NULL,
                download_url TEXT,
                sha256 TEXT,
                install_cmd TEXT,
                uninstall_cmd TEXT,
                silent_args TEXT,
                precheck_cmd TEXT,
                postcheck_cmd TEXT,
                expected_exit_codes TEXT
            )
            """
        )
        # Assignments table: desired state for each (computer, package)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                computer_id INTEGER NOT NULL,
                package_id INTEGER NOT NULL,
                desired_state TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(computer_id, package_id)
            )
            """
        )
        # Jobs table: individual installation/uninstallation tasks
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                computer_id INTEGER NOT NULL,
                package_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                rc INTEGER,
                stdout_tail TEXT,
                stderr_tail TEXT,
                attempts INTEGER DEFAULT 0
            )
            """
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        """Convert a SQLite row to a plain dictionary."""
        return {k: row[k] for k in row.keys()}

    # ------------------------------------------------------------------
    # Computer operations
    # ------------------------------------------------------------------
    def create_computer(self, hostname: str, os_name: str, arch: str, tags: List[str]) -> int:
        """Insert a new computer and return its ID."""
        tags_json = json.dumps(tags or [])
        cur = self.conn.execute(
            """
            INSERT INTO computers (hostname, os, arch, tags, last_check_in, client_version, token)
            VALUES (?, ?, ?, ?, NULL, NULL, NULL)
            """,
            (hostname, os_name, arch, tags_json),
        )
        self.conn.commit()
        return cur.lastrowid

    def list_computers(self) -> List[Dict[str, Any]]:
        """Return all computers ordered by ID descending."""
        cur = self.conn.execute("SELECT * FROM computers ORDER BY id DESC")
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def get_computer(self, cid: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM computers WHERE id = ?", (cid,))
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def get_computer_by_hostname(self, hostname: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM computers WHERE hostname = ?", (hostname,))
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def get_computer_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Return the computer matching the given bearer token (if not revoked)."""
        cur = self.conn.execute(
            "SELECT * FROM computers WHERE token = ? AND token_revoked = 0", (token,)
        )
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def update_computer(self, cid: int, *, hostname: Optional[str] = None, tags: Optional[List[str]] = None) -> None:
        """Update the hostname and/or tags for a computer."""
        if hostname is not None:
            self.conn.execute(
                "UPDATE computers SET hostname = ? WHERE id = ?", (hostname, cid)
            )
        if tags is not None:
            tags_json = json.dumps(tags)
            self.conn.execute(
                "UPDATE computers SET tags = ? WHERE id = ?", (tags_json, cid)
            )
        self.conn.commit()

    def set_token_and_meta(self, cid: int, token: str, os_name: str, arch: str, client_version: Optional[str]) -> None:
        """Assign a new bearer token and update system metadata."""
        self.conn.execute(
            """
            UPDATE computers
            SET token = ?, os = COALESCE(?, os), arch = COALESCE(?, arch), client_version = COALESCE(?, client_version)
            WHERE id = ?
            """,
            (token, os_name, arch, client_version, cid),
        )
        self.conn.commit()

    def update_last_check_in(self, cid: int) -> None:
        now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        self.conn.execute(
            "UPDATE computers SET last_check_in = ? WHERE id = ?", (now, cid)
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Package operations
    # ------------------------------------------------------------------
    def create_package(
        self,
        name: str,
        version: str,
        platform: str,
        download_url: str,
        sha256: str,
        install_cmd: str,
        uninstall_cmd: str,
        silent_args: str,
        precheck_cmd: str,
        postcheck_cmd: str,
        expected_exit_codes: List[int],
    ) -> int:
        """Insert a new package and return its ID."""
        codes_json = json.dumps(expected_exit_codes or [])
        cur = self.conn.execute(
            """
            INSERT INTO packages (name, version, platform, download_url, sha256, install_cmd, uninstall_cmd,
                                  silent_args, precheck_cmd, postcheck_cmd, expected_exit_codes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                version,
                platform,
                download_url,
                sha256,
                install_cmd,
                uninstall_cmd,
                silent_args,
                precheck_cmd,
                postcheck_cmd,
                codes_json,
            ),
        )
        self.conn.commit()
        return cur.lastrowid

    def list_packages(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM packages ORDER BY id DESC")
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def get_package(self, pid: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM packages WHERE id = ?", (pid,))
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    # ------------------------------------------------------------------
    # Assignment operations
    # ------------------------------------------------------------------
    def set_assignment(self, computer_id: int, package_id: int, desired_state: str) -> int:
        """Create or update an assignment and schedule a job if needed.

        The desired state should be one of ``install``, ``uninstall`` or ``hold``.
        A new job will only be scheduled if the state is ``install`` or
        ``uninstall`` and the assignment is new or changed.
        Returns the assignment ID.
        """
        now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        cur = self.conn.execute(
            "SELECT id, desired_state FROM assignments WHERE computer_id = ? AND package_id = ?",
            (computer_id, package_id),
        )
        row = cur.fetchone()
        if row:
            assignment_id = row["id"]
            if row["desired_state"] != desired_state:
                self.conn.execute(
                    "UPDATE assignments SET desired_state = ?, updated_at = ? WHERE id = ?",
                    (desired_state, now, assignment_id),
                )
                if desired_state in ("install", "uninstall"):
                    self.schedule_job(computer_id, package_id, desired_state)
        else:
            cur2 = self.conn.execute(
                "INSERT INTO assignments (computer_id, package_id, desired_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (computer_id, package_id, desired_state, now, now),
            )
            assignment_id = cur2.lastrowid
            if desired_state in ("install", "uninstall"):
                self.schedule_job(computer_id, package_id, desired_state)
        self.conn.commit()
        return assignment_id

    def list_assignments(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM assignments ORDER BY id DESC")
        return [self._row_to_dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Job operations
    # ------------------------------------------------------------------
    def schedule_job(self, computer_id: int, package_id: int, action: str) -> int:
        """Insert a new pending job and return its ID."""
        cur = self.conn.execute(
            "INSERT INTO jobs (computer_id, package_id, action, status, attempts) VALUES (?, ?, ?, 'Pending', 0)",
            (computer_id, package_id, action),
        )
        self.conn.commit()
        return cur.lastrowid

    def list_jobs(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            """
            SELECT jobs.*, computers.hostname AS hostname, packages.name AS package_name
            FROM jobs
            LEFT JOIN computers ON jobs.computer_id = computers.id
            LEFT JOIN packages ON jobs.package_id = packages.id
            ORDER BY jobs.id DESC
            """
        )
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def update_job_status(
        self,
        job_id: int,
        phase: str,
        status: Optional[str] = None,
        rc: Optional[int] = None,
        stdout_tail: Optional[str] = None,
        stderr_tail: Optional[str] = None,
    ) -> None:
        """Update job status based on an event from the client.

        Phases:
            - ``start``: marks the job as ``InProgress`` and sets ``started_at``.
            - ``completed``: finalises the job with a status (default "Succeeded"), return code and output tails.
        """
        now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
        if phase == "start":
            self.conn.execute(
                "UPDATE jobs SET status = 'InProgress', started_at = ?, attempts = attempts + 1 WHERE id = ?",
                (now, job_id),
            )
        elif phase == "completed":
            final_status = status or "Succeeded"
            self.conn.execute(
                """
                UPDATE jobs
                SET status = ?, finished_at = ?, rc = ?, stdout_tail = ?, stderr_tail = ?
                WHERE id = ?
                """,
                (final_status, now, rc, stdout_tail, stderr_tail, job_id),
            )
        self.conn.commit()

    # ------------------------------------------------------------------
    # Plan computation
    # ------------------------------------------------------------------
    def get_plan_for_computer(self, cid: int) -> List[Dict[str, Any]]:
        """Compute the plan for a client: list of assignments with actions and package details."""
        sql = """
        SELECT a.id AS assignment_id, a.package_id, a.desired_state,
               j.id AS job_id, j.status AS job_status,
               p.name, p.version, p.platform, p.download_url, p.sha256,
               p.install_cmd, p.uninstall_cmd, p.silent_args, p.precheck_cmd, p.postcheck_cmd, p.expected_exit_codes
        FROM assignments a
        JOIN packages p ON p.id = a.package_id
        LEFT JOIN jobs j ON j.computer_id = a.computer_id AND j.package_id = a.package_id AND j.status IN ('Pending','InProgress')
        WHERE a.computer_id = ?
        """
        out: List[Dict[str, Any]] = []
        for row in self.conn.execute(sql, (cid,)):
            desired = row["desired_state"]
            if desired not in ("install", "uninstall"):
                continue
            # parse expected exit codes JSON
            try:
                codes = json.loads(row["expected_exit_codes"]) if row["expected_exit_codes"] else []
            except Exception:
                codes = []
            out.append(
                {
                    "job_id": row["job_id"],
                    "action": desired,
                    "package": {
                        "id": row["package_id"],
                        "name": row["name"],
                        "version": row["version"],
                        "platform": row["platform"],
                        "download_url": row["download_url"] or "",
                        "sha256": row["sha256"] or "",
                        "install_cmd": row["install_cmd"] or "",
                        "uninstall_cmd": row["uninstall_cmd"] or "",
                        "silent_args": row["silent_args"] or "",
                        "precheck_cmd": row["precheck_cmd"] or "",
                        "postcheck_cmd": row["postcheck_cmd"] or "",
                        "expected_exit_codes": codes,
                    },
                }
            )
        return out

    def get_summary(self) -> Dict[str, int]:
        """Return counts for dashboard metrics."""
        cur = self.conn.cursor()
        summary: Dict[str, int] = {}
        cur.execute("SELECT COUNT(*) FROM computers")
        summary["computers"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM packages")
        summary["packages"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM assignments")
        summary["assignments"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM jobs")
        summary["jobs"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Failed'")
        summary["failed_jobs"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'Succeeded'")
        summary["succeeded_jobs"] = cur.fetchone()[0]
        return summary

    # ------------------------------------------------------------------
    # Helper for UI: list packages with desired state for a computer
    # ------------------------------------------------------------------
    def list_packages_for_computer(self, cid: int, platform: Optional[str] = None, q: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return all packages along with the desired state for a given computer.

        The returned list contains dictionaries with keys:

        - ``id``
        - ``name``
        - ``version``
        - ``platform``
        - ``desired_state`` (``install``, ``uninstall`` or ``hold``)
        - ``installed_version`` (currently always ``None`` as the server does not track this)

        Optional ``platform`` and ``q`` parameters filter by platform and
        substring match on name or version.
        """
        params: List[Any] = [cid]
        where_clause = []
        if platform:
            where_clause.append("p.platform = ?")
            params.append(platform)
        if q:
            where_clause.append("(p.name LIKE ? OR p.version LIKE ?)")
            params.append(f"%{q}%")
            params.append(f"%{q}%")
        where_sql = "WHERE " + " AND ".join(where_clause) if where_clause else ""
        sql = f"""
            SELECT p.id, p.name, p.version, p.platform,
                   COALESCE(a.desired_state, 'hold') AS desired_state
            FROM packages p
            LEFT JOIN assignments a ON a.package_id = p.id AND a.computer_id = ?
            {where_sql}
            ORDER BY p.name COLLATE NOCASE
        """
        cur = self.conn.execute(sql, params)
        rows = cur.fetchall()
        results: List[Dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "version": row["version"],
                    "platform": row["platform"],
                    "desired_state": row["desired_state"],
                    "installed_version": None,
                }
            )
        return results