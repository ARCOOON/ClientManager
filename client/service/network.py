"""
Simple HTTP client for the endpoint manager service.

This module uses only Python's standard library to communicate with the
management server.  It avoids third‑party dependencies such as httpx
and pydantic.  A ``Config`` dataclass stores runtime configuration
including the server URL, client identifier, bearer token, polling
interval and optional tags.  Convenience functions enrol the client
with the server, fetch the plan for a machine, post job status
updates and resolve artifact URLs.

All functions raise ``RuntimeError`` on network or protocol errors.
"""

from __future__ import annotations

import json
import socket
import platform
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import List, Optional

from shared.models import (
    PackageSpec,
    PlanAssignment,
    JobEvent,
)


@dataclass
class Config:
    """Runtime configuration for the client service.

    :param server_url: Base URL of the management server (e.g. ``http://localhost:8000``).
    :param client_id: Assigned integer ID for this machine; ``None`` until enrolment succeeds.
    :param token: Bearer token issued by the server; ``None`` until enrolment succeeds.
    :param poll_interval: Delay between plan fetches in seconds (default 600).
    :param tags: Optional list of tag strings to send on enrolment.
    """

    server_url: str
    client_id: Optional[int] = None
    token: Optional[str] = None
    poll_interval: int = 600
    tags: Optional[List[str]] = None

    @property
    def base_url(self) -> str:
        """Return the server URL without a trailing slash."""
        return self.server_url.rstrip("/")


def enroll_if_needed(cfg: Config) -> Config:
    """Enroll the client if necessary.

    If ``cfg.client_id`` and ``cfg.token`` are already set, this function
    returns ``cfg`` unchanged.  Otherwise it collects basic system
    information (hostname, operating system and architecture), posts
    that data to the server's enrolment endpoint and stores the
    returned ``id`` and ``token`` back on the config.

    :param cfg: Configuration instance to update.
    :returns: The updated configuration with ``client_id`` and ``token`` set.
    :raises RuntimeError: If the enrolment request fails.
    """
    if cfg.client_id is not None and cfg.token:
        return cfg

    # gather system information
    hostname = socket.gethostname()
    os_name = platform.system().lower()
    arch = platform.machine()
    payload = {
        "hostname": hostname,
        "os": os_name,
        "arch": arch,
        "client_version": None,
    }

    url = f"{cfg.base_url}/api/v1/clients/enroll"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_data = json.load(resp)
    except urllib.error.HTTPError as e:
        try:
            err = json.load(e)
        except Exception:
            raise RuntimeError(f"Failed to enroll client: {e.reason}")
        raise RuntimeError(f"Failed to enroll client: {err}")

    cfg.client_id = resp_data.get("id")
    cfg.token = resp_data.get("token")

    return cfg


def fetch_plan(cfg: Config) -> List[PlanAssignment]:
    """Retrieve the current plan assignments for this client from the server.

    :param cfg: Client configuration (must have ``client_id`` and ``token``).
    :returns: A list of ``PlanAssignment`` instances.
    :raises RuntimeError: If the request fails or the client is not enrolled.
    """
    if cfg.client_id is None or not cfg.token:
        raise RuntimeError("Client not enrolled")

    url = f"{cfg.base_url}/api/v1/clients/{cfg.client_id}/plan"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {cfg.token}"},
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as e:
        try:
            err_data = json.load(e)
        except Exception:
            raise RuntimeError(f"Failed to fetch plan: {e.reason}")
        raise RuntimeError(f"Failed to fetch plan: {err_data}")

    assignments_raw = data.get("assignments", [])
    assignments: List[PlanAssignment] = []

    for item in assignments_raw:
        pkg_data = item.get("package", {})
        pkg = PackageSpec(
            id=pkg_data.get("id"),
            name=pkg_data.get("name"),
            version=pkg_data.get("version"),
            platform=pkg_data.get("platform"),
            download_url=pkg_data.get("download_url") or "",
            sha256=pkg_data.get("sha256") or "",
            install_cmd=pkg_data.get("install_cmd") or "",
            uninstall_cmd=pkg_data.get("uninstall_cmd") or "",
            silent_args=pkg_data.get("silent_args") or "",
            precheck_cmd=pkg_data.get("precheck_cmd") or "",
            postcheck_cmd=pkg_data.get("postcheck_cmd") or "",
            expected_exit_codes=pkg_data.get("expected_exit_codes") or [],
        )
        assignments.append(
            PlanAssignment(
                job_id=item.get("job_id"),
                action=item.get("action"),
                package=pkg,
            )
        )

    return assignments


def post_job_event(cfg: Config, job_id: int, event: JobEvent) -> None:
    """Send a job status update to the server.

    Network errors are swallowed silently; the status will be retried on the
    next polling cycle.  If the client is not enrolled, the call is a no‑op.

    :param cfg: Client configuration
    :param job_id: The job identifier assigned by the server
    :param event: Event payload
    """
    if cfg.client_id is None or not cfg.token:
        return

    url = f"{cfg.base_url}/api/v1/jobs/{job_id}/events"
    data = json.dumps(event.__dict__).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.token}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
    except urllib.error.HTTPError:
        # swallow errors; they will be retried
        pass


def resolve_artifact(cfg: Config, package_id: int) -> str:
    """Resolve the download URL for a specific package.

    Some server implementations may return a signed, time‑limited URL for a
    package.  This helper encapsulates the call.  Returns the URL as
    provided by the server; returns an empty string if no URL is
    provided.

    :param cfg: Client configuration
    :param package_id: ID of the package to resolve
    :returns: The resolved URL string
    :raises RuntimeError: On HTTP errors
    """
    if not cfg.token:
        raise RuntimeError("Client not enrolled")

    url = f"{cfg.base_url}/api/v1/artifacts/resolve"
    payload = json.dumps({"package_id": package_id}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {cfg.token}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
            return data.get("url", "")
    except urllib.error.HTTPError as e:
        try:
            err_data = json.load(e)
        except Exception:
            raise RuntimeError(f"Failed to resolve artifact: {e.reason}")
        raise RuntimeError(f"Failed to resolve artifact: {err_data}")
