"""Shared data structures for the endpoint manager.

This module defines a handful of simple dataclasses used by both the
server and client.  They do not depend on Pydantic and therefore run
without requiring `pydantic_core` to be installed.  These classes
provide lightweight type annotations and convenient containers for
request and response payloads exchanged over the API.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class EnrollRequest:
    hostname: str
    os: str
    arch: str
    client_version: Optional[str] = None


@dataclass
class EnrollResponse:
    id: int
    token: str


@dataclass
class PackageSpec:
    id: int
    name: str
    version: str
    platform: str
    download_url: str
    sha256: str
    install_cmd: str
    uninstall_cmd: str
    silent_args: str
    precheck_cmd: str
    postcheck_cmd: str
    expected_exit_codes: List[int] = field(default_factory=list)


@dataclass
class PlanAssignment:
    job_id: Optional[int]
    action: str
    package: PackageSpec


@dataclass
class PlanResponse:
    assignments: List[PlanAssignment]


@dataclass
class JobEvent:
    phase: str
    status: Optional[str] = None
    rc: Optional[int] = None
    stdout_tail: Optional[str] = None
    stderr_tail: Optional[str] = None