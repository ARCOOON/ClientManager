"""
Installation and uninstallation routines for the endpoint client.

This module abstracts downloading artifacts and executing platform‑specific
installers.  It is designed to work without third‑party libraries.  It
expects assignments provided by the server to contain a ``package``
definition describing the install and uninstall commands, silent
arguments and optional pre‑ and post‑checks.  Artifacts are downloaded
using ``urllib.request`` and stored in a local cache directory.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Optional, Tuple
import urllib.request
import urllib.error

from shared.models import PlanAssignment


def download_file(url: str, dest: str) -> None:
    """Download a file from a URL to a destination path."""
    with urllib.request.urlopen(url, timeout=60) as resp:
        with open(dest, "wb") as f:
            f.write(resp.read())


def verify_sha256(path: str, expected_hex: Optional[str]) -> bool:
    """Verify that the SHA‑256 checksum of a file matches ``expected_hex``.

    If ``expected_hex`` is empty or ``None``, verification always
    succeeds.  The file is read in chunks to avoid loading large files
    entirely into memory.
    """
    if not expected_hex:
        return True
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest().lower() == expected_hex.lower()


def run_subprocess(command: str) -> Tuple[int, str, str]:
    """Run a shell command and capture its exit code and output tails.

    Returns a tuple ``(returncode, stdout_tail, stderr_tail)``.  Only the
    last 4 kB of output streams are kept to prevent sending overly large
    payloads back to the server.
    """
    try:
        proc = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=None,
        )
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        stdout_tail = stdout[-4096:]
        stderr_tail = stderr[-4096:]
        return proc.returncode, stdout_tail, stderr_tail
    except Exception as e:
        return 1, "", str(e)


def perform_action(assignment: PlanAssignment, action: str) -> Tuple[int, str, str]:
    """Execute an install or uninstall action for a plan assignment.

    The function downloads the package artifact if necessary, verifies
    its checksum, runs pre‑ and post‑check commands if defined and
    executes the install or uninstall command with optional silent
    arguments.  The ``action`` parameter must be either ``"install"``
    or ``"uninstall"``.  It returns a tuple of return code and output
    tails.
    """
    pkg = assignment.package
    cache_dir = Path("artifact_cache")
    cache_dir.mkdir(exist_ok=True)
    artifact_path = cache_dir / f"pkg_{pkg.id}_{pkg.version}"
    # download artifact if provided
    if pkg.download_url:
        try:
            if not artifact_path.exists():
                download_file(pkg.download_url, str(artifact_path))
        except Exception as e:
            return 1, "", f"Download failed: {e}"
        # verify checksum if available
        if not verify_sha256(str(artifact_path), pkg.sha256):
            return 1, "", "Checksum mismatch"
    # run precheck if defined
    if pkg.precheck_cmd:
        rc, out, err = run_subprocess(pkg.precheck_cmd)
        if rc != 0:
            return rc, out, err
    # choose command based on action
    if action == "install":
        base_cmd = pkg.install_cmd or ""
    else:
        base_cmd = pkg.uninstall_cmd or ""
    if not base_cmd:
        return 1, "", "No command defined"
    # substitute {file} placeholder with artifact path if present
    cmd = base_cmd.format(file=str(artifact_path))
    if pkg.silent_args:
        cmd = f"{cmd} {pkg.silent_args}"
    rc, stdout_tail, stderr_tail = run_subprocess(cmd)
    # run postcheck if defined
    if pkg.postcheck_cmd:
        rc2, out2, err2 = run_subprocess(pkg.postcheck_cmd)
        if rc2 != 0:
            return rc2, out2, err2
    return rc, stdout_tail, stderr_tail