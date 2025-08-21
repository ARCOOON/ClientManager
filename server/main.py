"""WSGI server for the endpoint manager.

This module implements the management server entirely with Python's
standard library.  It exposes a simple web UI rendered via Jinja2
templates and a JSON API consumed by the client service.  There are no
dependencies on FastAPI or Pydantic, making the server runnable on
minimal installations where those packages are unavailable.

Endpoints:

* **Web UI**
    - ``/`` – dashboard with summary counts
    - ``/computers`` – list of computers and add form
    - ``/computers/<cid>`` – computer detail & package plan editor
    - ``/packages`` – list packages and create form
    - ``/jobs`` – list jobs

* **API**
    - ``POST /api/v1/clients/enroll`` – enrol a client, returns id and token
    - ``GET /api/v1/clients/<cid>/plan`` – fetch plan for a client (bearer token)
    - ``POST /api/v1/jobs/<job_id>/events`` – report job status events (bearer token)

To run the server, execute ``python -m server.main`` from the project
root.  The server listens on ``127.0.0.1:8000`` by default.  You can
override the bind address and port via the ``HOST`` and ``PORT``
environment variables.
"""

from __future__ import annotations

import json
import os
import secrets
import urllib.parse
from typing import Callable, Dict, List, Tuple, Optional, Iterable

from wsgiref.simple_server import make_server
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .database import Database


def load_templates() -> Environment:
    """Set up the Jinja2 environment pointing at the templates directory."""
    tmpl_dir = os.path.join(os.path.dirname(__file__), "templates")
    return Environment(
        loader=FileSystemLoader(tmpl_dir), autoescape=select_autoescape(["html", "xml"])
    )


class EndpointManagerServer:
    """A simple WSGI application implementing the management server."""

    def __init__(self) -> None:
        self.db = Database()
        self.templates = load_templates()

    # --------------------------------------------------------------
    # Request handling
    # --------------------------------------------------------------
    def __call__(self, environ: Dict[str, object], start_response: Callable) -> Iterable[bytes]:
        """WSGI entrypoint.  Dispatches the request based on path and method."""
        path = environ.get("PATH_INFO", "/")
        method = environ.get("REQUEST_METHOD", "GET").upper()
        try:
            if path.startswith("/api/"):
                status, headers, body = self.handle_api(path, method, environ)
            else:
                status, headers, body = self.handle_web(path, method, environ)
        except Exception as exc:
            # In debug builds we could log the exception.  For now return 500.
            status = "500 Internal Server Error"
            body = f"Internal server error: {exc}".encode("utf-8")
            headers = [("Content-Type", "text/plain; charset=utf-8"), ("Content-Length", str(len(body)))]
        start_response(status, headers)
        return [body]

    # --------------------------------------------------------------
    # Helpers
    # --------------------------------------------------------------
    def _render(self, template_name: str, **context: object) -> bytes:
        """Render a Jinja2 template to bytes."""
        template = self.templates.get_template(template_name)
        html = template.render(**context)
        return html.encode("utf-8")

    def _parse_form(self, environ: Dict[str, object]) -> Dict[str, str]:
        """Parse URL‑encoded form data from the request body."""
        try:
            length = int(environ.get("CONTENT_LENGTH", 0))
        except (TypeError, ValueError):
            length = 0
        body = environ.get("wsgi.input")
        data = body.read(length) if body else b""
        fields = urllib.parse.parse_qs(data.decode("utf-8"))
        # flatten lists to first value
        return {k: v[0] if v else "" for k, v in fields.items()}

    def _parse_json(self, environ: Dict[str, object]) -> Dict[str, object]:
        """Parse JSON payload from the request body."""
        try:
            length = int(environ.get("CONTENT_LENGTH", 0))
        except (TypeError, ValueError):
            length = 0
        body = environ.get("wsgi.input")
        data = body.read(length) if body else b""
        if not data:
            return {}
        try:
            return json.loads(data.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _get_bearer_token(self, environ: Dict[str, object]) -> Optional[str]:
        """Extract bearer token from the Authorization header."""
        auth = environ.get("HTTP_AUTHORIZATION")
        if not isinstance(auth, str):
            return None
        parts = auth.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1]
        return None

    # --------------------------------------------------------------
    # Web handlers
    # --------------------------------------------------------------
    def handle_web(self, path: str, method: str, environ: Dict[str, object]) -> Tuple[str, List[Tuple[str, str]], bytes]:
        """Handle all non‑API requests.  Returns (status, headers, body)."""
        # Dashboard
        if path in ("", "/") and method == "GET":
            summary = self.db.get_summary()
            body = self._render("dashboard.html", active="dashboard", summary=summary)
            headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
            return "200 OK", headers, body
        # List computers
        if path == "/computers" and method == "GET":
            comps = self.db.list_computers()
            body = self._render("computers.html", active="computers", computers=comps)
            headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
            return "200 OK", headers, body
        # Add computer
        if path == "/computers" and method == "POST":
            form = self._parse_form(environ)
            hostname = form.get("hostname", "").strip()
            os_name = form.get("os", "").strip()
            arch = form.get("arch", "").strip()
            tags_str = form.get("tags", "").strip()
            if hostname:
                existing = self.db.get_computer_by_hostname(hostname)
                if not existing:
                    tags = [t.strip() for t in tags_str.split(",") if t.strip()]
                    self.db.create_computer(hostname, os_name, arch, tags)
            # redirect back to list
            headers = [("Location", "/computers")]
            return "303 See Other", headers, b""
        # Computer detail
        if path.startswith("/computers/"):
            parts = path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] == "computers":
                # parse ID
                try:
                    cid = int(parts[1])
                except ValueError:
                    return self._not_found()
                comp = self.db.get_computer(cid)
                if not comp:
                    return self._not_found()
                if method == "GET":
                    # list packages for this computer
                    packages = self.db.list_packages_for_computer(cid)
                    body = self._render("computer_detail.html", computer=comp, packages=packages)
                    headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
                    return "200 OK", headers, body
                # update
                if method == "POST" and len(parts) == 3 and parts[2] == "update":
                    form = self._parse_form(environ)
                    hostname = form.get("hostname", None)
                    tags_str = form.get("tags", "")
                    tags = [t.strip() for t in tags_str.split(",") if t.strip()]
                    self.db.update_computer(cid, hostname=hostname.strip() if hostname else None, tags=tags)
                    comp = self.db.get_computer(cid)
                    packages = self.db.list_packages_for_computer(cid)
                    body = self._render("computer_detail.html", computer=comp, packages=packages)
                    headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
                    return "200 OK", headers, body
                # apply assignments
                if method == "POST" and len(parts) == 3 and parts[2] == "apply":
                    form = self._parse_form(environ)
                    desires: Dict[int, str] = {}
                    for key, value in form.items():
                        if key.startswith("pkg_"):
                            try:
                                pid = int(key.split("_", 1)[1])
                                desires[pid] = value
                            except ValueError:
                                continue
                    for pid, desired in desires.items():
                        self.db.set_assignment(cid, pid, desired)
                    comp = self.db.get_computer(cid)
                    packages = self.db.list_packages_for_computer(cid)
                    body = self._render("computer_detail.html", computer=comp, packages=packages)
                    headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
                    return "200 OK", headers, body
        # List packages
        if path == "/packages" and method == "GET":
            pkgs = self.db.list_packages()
            body = self._render("packages.html", active="packages", packages=pkgs)
            headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
            return "200 OK", headers, body
        # Create package
        if path == "/packages" and method == "POST":
            form = self._parse_form(environ)
            name = form.get("name", "").strip()
            version = form.get("version", "").strip()
            platform = form.get("platform", "").strip()
            download_url = form.get("download_url", "").strip()
            sha256 = form.get("sha256", "").strip()
            install_cmd = form.get("install_cmd", "").strip()
            uninstall_cmd = form.get("uninstall_cmd", "").strip()
            silent_args = form.get("silent_args", "").strip()
            precheck_cmd = form.get("precheck_cmd", "").strip()
            postcheck_cmd = form.get("postcheck_cmd", "").strip()
            codes_str = form.get("expected_exit_codes", "").strip()
            codes: List[int] = []
            if codes_str:
                for part in codes_str.split(","):
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        codes.append(int(part))
                    except ValueError:
                        continue
            if name and version and platform:
                self.db.create_package(
                    name=name,
                    version=version,
                    platform=platform,
                    download_url=download_url,
                    sha256=sha256,
                    install_cmd=install_cmd,
                    uninstall_cmd=uninstall_cmd,
                    silent_args=silent_args,
                    precheck_cmd=precheck_cmd,
                    postcheck_cmd=postcheck_cmd,
                    expected_exit_codes=codes,
                )
            headers = [("Location", "/packages")]
            return "303 See Other", headers, b""
        # List jobs
        if path == "/jobs" and method == "GET":
            jobs = self.db.list_jobs()
            body = self._render("jobs.html", active="jobs", jobs=jobs)
            headers = [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))]
            return "200 OK", headers, body
        # Fallback 404
        return self._not_found()

    def _not_found(self) -> Tuple[str, List[Tuple[str, str]], bytes]:
        body = b"Not found"
        headers = [("Content-Type", "text/plain; charset=utf-8"), ("Content-Length", str(len(body)))]
        return "404 Not Found", headers, body

    # --------------------------------------------------------------
    # API handlers
    # --------------------------------------------------------------
    def handle_api(self, path: str, method: str, environ: Dict[str, object]) -> Tuple[str, List[Tuple[str, str]], bytes]:
        """Handle API endpoints under /api/v1."""
        # Enrol: POST /api/v1/clients/enroll
        if path == "/api/v1/clients/enroll" and method == "POST":
            payload = self._parse_json(environ)
            hostname = (payload.get("hostname") or "").strip()
            os_name = (payload.get("os") or "").strip()
            arch = (payload.get("arch") or "").strip()
            client_version = payload.get("client_version")
            if not hostname:
                return self._json_response(400, {"detail": "hostname is required"})
            comp = self.db.get_computer_by_hostname(hostname)
            if comp:
                cid = comp["id"]
            else:
                cid = self.db.create_computer(hostname, os_name, arch, [])
            token = secrets.token_hex(16)
            self.db.set_token_and_meta(cid, token, os_name, arch, client_version)
            self.db.update_last_check_in(cid)
            return self._json_response(200, {"id": cid, "token": token})
        # Plan: GET /api/v1/clients/<cid>/plan
        if path.startswith("/api/v1/clients/") and path.endswith("/plan") and method == "GET":
            parts = path.split("/")
            try:
                cid = int(parts[4])  # ['', 'api','v1','clients','<cid>','plan']
            except (IndexError, ValueError):
                return self._json_response(404, {"detail": "not found"})
            token = self._get_bearer_token(environ)
            if not token:
                return self._json_response(401, {"detail": "Missing bearer token"})
            comp = self.db.get_computer(cid)
            if not comp or comp.get("token_revoked"):
                return self._json_response(401, {"detail": "Unauthorized"})
            if comp.get("token") != token:
                return self._json_response(401, {"detail": "Invalid token"})
            self.db.update_last_check_in(cid)
            plan = self.db.get_plan_for_computer(cid)
            return self._json_response(200, {"assignments": plan})
        # Job events: POST /api/v1/jobs/<job_id>/events
        if path.startswith("/api/v1/jobs/") and path.endswith("/events") and method == "POST":
            parts = path.split("/")
            try:
                job_id = int(parts[4])  # ['', 'api','v1','jobs','<job_id>','events']
            except (IndexError, ValueError):
                return self._json_response(404, {"detail": "not found"})
            token = self._get_bearer_token(environ)
            if not token:
                return self._json_response(401, {"detail": "Missing bearer token"})
            job = self.db.get_job(job_id)
            if not job:
                return self._json_response(404, {"detail": "Job not found"})
            comp = self.db.get_computer(job["computer_id"])
            if not comp or comp.get("token") != token or comp.get("token_revoked"):
                return self._json_response(401, {"detail": "Unauthorized"})
            event = self._parse_json(environ)
            phase = event.get("phase")
            status_val = event.get("status")
            rc = event.get("rc")
            stdout_tail = event.get("stdout_tail")
            stderr_tail = event.get("stderr_tail")
            self.db.update_job_status(job_id, phase, status_val, rc, stdout_tail, stderr_tail)
            return self._json_response(200, {"detail": "ok"})
        # Unknown API path
        return self._json_response(404, {"detail": "not found"})

    def _json_response(self, status_code: int, data: Dict[str, object]) -> Tuple[str, List[Tuple[str, str]], bytes]:
        """Serialize a dictionary to JSON and build a response."""
        body_str = json.dumps(data)
        body = body_str.encode("utf-8")
        status_text = {200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 500: "Internal Server Error", 303: "See Other"}.get(status_code, "")
        status = f"{status_code} {status_text}".strip()
        headers = [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(body)))]
        return status, headers, body


def run_server() -> None:
    """Run the management server on the configured host and port."""
    host = os.getenv("HOST", "127.0.0.1")
    try:
        port = int(os.getenv("PORT", "8000"))
    except ValueError:
        port = 8000
    app = EndpointManagerServer()
    with make_server(host, port, app) as httpd:
        print(f"Serving on http://{host}:{port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("Server stopped")


if __name__ == "__main__":
    run_server()