# Client Manager

This project provides a simple, cross‑platform endpoint management system
implemented without third‑party frameworks such as FastAPI or pydantic.  It
features a minimal management server that exposes a web interface and a
token‑secured API, as well as a headless client service that polls the
server, executes installation/uninstallation tasks and reports status.  The
codebase is intentionally light on dependencies to improve portability in
environments where installing complex Python packages is undesirable or
impossible.

## Project Structure

```
ClientManager/
├── README.md              # this file
├── server/                # management server package
│   ├── main.py            # WSGI server entrypoint
│   ├── database.py        # SQLite database layer
│   └── templates/         # Jinja2 templates for the web UI
├── client/               # client package
│   ├── config.sample.toml # example configuration
│   └── service/           # headless service
│       ├── main.py        # polling loop
│       ├── network.py     # HTTP client using urllib
│       ├── db.py          # local SQLite state
│       └── installer.py   # command execution and downloading
└── shared/                # shared data structures
    └── models.py          # dataclasses for API messages
```

## Running the Server

The server uses Python’s built‑in WSGI server and Jinja2 for rendering HTML.
To start the server, install Jinja2 (it’s the only external dependency) and
run the module:

```bash
pip install Jinja2

cd ClientManager
python -m server.main
```

By default the server listens on `127.0.0.1:8000`.  You can change the
address and port using environment variables:

```bash
HOST=0.0.0.0 PORT=9000 python -m server.main
```

Point your browser at the host and port to access the web UI.

## Running the Client

The client is a headless service that polls the server for package
assignments, performs installs/uninstalls and reports results back.  It
stores its configuration in a TOML file; a sample config is provided in
`client/config.sample.toml`.

To run the client:

```bash
cd ClientManager
python -m client.service.main --config client/config.sample.toml
```

On the first run the client will enrol with the server and store the
assigned `client_id` and `token` in the configuration file.  The polling
interval defaults to 10 minutes but can be adjusted in the config.

## Notes

* This rewrite removes dependencies on FastAPI and pydantic.  It uses
  only Python’s standard library and Jinja2 for templates.
* The server is intentionally simple and may not implement every feature of
  the original system.  It can serve as a starting point for further
  enhancements.
* The client currently does not include a GUI; it runs in a console and
  logs actions to stdout.  A GUI could be added by interfacing with the
  local database or by exposing a small HTTP interface for display.
