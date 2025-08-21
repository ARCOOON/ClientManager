"""
Entry point for the endpoint client service.

This script orchestrates the polling loop for the endpoint client.  It
reads a TOML configuration file, enrols the client with the management
server if necessary, fetches the current assignment plan and executes
install or uninstall operations accordingly.  The client persists
local state in a SQLite database to ensure idempotency.

The implementation avoids third‑party dependencies and uses the
dataclasses defined in ``shared.models`` for data exchange with the
server.
"""

from __future__ import annotations

import argparse
import time
import os
import sys
import tomllib  # type: ignore
from typing import Dict, Any, List

from shared.models import JobEvent

from . import network
from . import db as client_db
from . import installer


def load_config(path: str) -> network.Config:
    """Load a configuration TOML file and return a Config instance."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file {path} not found")
    with open(path, "rb") as f:
        data = tomllib.load(f)
    cfg = network.Config(
        server_url=data.get("server_url"),
        client_id=data.get("client_id"),
        token=data.get("token"),
        poll_interval=data.get("poll_interval", 600),
        tags=data.get("tags"),
    )
    return cfg


def save_config(cfg: network.Config, path: str) -> None:
    """Write a minimal TOML configuration file for the client.

    Only defined values are written.  Lists and strings are formatted
    using Python ``repr`` which yields valid TOML for simple types.
    """
    data: Dict[str, Any] = {
        "server_url": cfg.server_url,
        "client_id": cfg.client_id,
        "token": cfg.token,
        "poll_interval": cfg.poll_interval,
        "tags": cfg.tags,
    }
    lines: List[str] = []
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        else:
            lines.append(f"{key} = {value!r}")
    content = "\n".join(lines) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Endpoint client service")
    parser.add_argument(
        "--config",
        default="client_config.toml",
        help="Path to configuration TOML file",
    )
    args = parser.parse_args(argv)

    # load config and enrol if necessary
    cfg = load_config(args.config)
    cfg = network.enroll_if_needed(cfg)
    save_config(cfg, args.config)

    # local database for installed packages
    db = client_db.ClientDB()

    print(f"Client enrolled with ID {cfg.client_id}")

    while True:
        try:
            assignments = network.fetch_plan(cfg)
        except Exception as e:
            print(f"Error fetching plan: {e}")
            time.sleep(cfg.poll_interval)
            continue
        # process each assignment sequentially
        for assignment in assignments:
            action = assignment.action
            job_id = assignment.job_id
            pkg = assignment.package
            # look up local state
            local_pkg = db.get_installed(pkg.id)
            if action == "install":
                if local_pkg and local_pkg["version"] == pkg.version and local_pkg["status"] == "installed":
                    # already installed; send success event
                    event = JobEvent(
                        phase="completed",
                        status="Succeeded",
                        rc=0,
                        stdout_tail="already installed",
                        stderr_tail="",
                    )
                    if job_id is not None:
                        network.post_job_event(cfg, job_id, event)
                    continue
            elif action == "uninstall":
                if not local_pkg or local_pkg["status"] != "installed":
                    event = JobEvent(
                        phase="completed",
                        status="Succeeded",
                        rc=0,
                        stdout_tail="already uninstalled",
                        stderr_tail="",
                    )
                    if job_id is not None:
                        network.post_job_event(cfg, job_id, event)
                    continue

            # send start event
            if job_id is not None:
                start_event = JobEvent(phase="start")
                network.post_job_event(cfg, job_id, start_event)

            # execute action
            try:
                rc, out, err = installer.perform_action(assignment, action)
            except Exception as e:
                rc, out, err = 1, "", str(e)
            ok_codes = pkg.expected_exit_codes or [0]
            status = "Succeeded" if rc in ok_codes else "Failed"
            # update local DB
            if action == "install" and status == "Succeeded":
                db.update_installed(pkg.id, pkg.name, pkg.version, "installed", rc)
            elif action == "uninstall" and status == "Succeeded":
                db.update_installed(pkg.id, pkg.name, pkg.version, "uninstalled", rc)
            # send completion event
            completed_event = JobEvent(
                phase="completed",
                status=status,
                rc=rc,
                stdout_tail=out,
                stderr_tail=err,
            )
            if job_id is not None:
                network.post_job_event(cfg, job_id, completed_event)
        # wait before next poll
        time.sleep(cfg.poll_interval)


if __name__ == "__main__":
    main()