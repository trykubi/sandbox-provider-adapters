# SPDX-FileCopyrightText: Kubi
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Kubi as an SPS 1.1 provider: a sandbox is a disposable Kubi screen, a Linux desktop in its own VM.

Every call goes to Kubi's public API (`https://api.trykubi.com/v1`, docs.trykubi.com) with the
account's API key. The adapter keeps nothing between calls:

- A sandbox is a disposable screen named `sps-<digest of the Idempotency-Key>`. The adapter reads,
  lists and deletes only screens with that prefix, so a key that also holds an account's own
  screens never lets a consumer touch them.
- Create sends the consumer's `Idempotency-Key` to Kubi, which keeps it 24 hours for screens, and
  long-polls the create operation until the screen runs. The lifetime is the screen's end time.
- Labels are Kubi labels. Create-time `env` goes into a file in the screen that each command
  sources before the command's own env; a Kubi label marks the screens that have one.
- `run` and the processes are Kubi exec: argv with no shell, a time limit that ends the command's
  process group, output read by byte offset. `cost` reads the screen's spend record, which Kubi
  keeps after the screen is deleted.
"""

from __future__ import annotations

import hashlib
import json
import math
import shlex
import time
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
from sandbox_provider_spec.v1 import (
    Cost,
    CostLine,
    CreateRequest,
    DestroyResult,
    Discovery,
    FileInfo,
    FileStream,
    OutputChunk,
    OutputPage,
    Process,
    ProcessStartRequest,
    Provider,
    RunRequest,
    RunResult,
    Sandbox,
    SandboxPage,
    SpsError,
)

from sps_provider_kubi.facts import (
    CLASS,
    ENV_LABEL,
    ENV_PATH,
    HOME,
    IMAGE,
    LIMITS,
    LIST_LIMIT,
    MAX_LIFETIME_S,
    MAX_WAIT_S,
    MIN_LIFETIME_S,
    NAME_PREFIX,
    PAGE_BYTES,
    REGION,
    SIZE,
    build_discovery,
)

__all__ = ["KubiProvider"]

DEFAULT_ENDPOINT = "https://api.trykubi.com"
CREATE_WAIT_S = 300  # a blank screen runs in about 20 s; a busy host can take longer

# Kubi's error codes by HTTP status, to SPS codes. Kubi's `outcome` says whether anything ran.
_STATUS = {
    400: "invalid_request",
    401: "unauthenticated",
    402: "quota_exceeded",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
    413: "too_large",
    415: "invalid_request",
    422: "unsupported",
    429: "rate_limited",
    500: "provider_error",
    502: "unavailable",
    503: "unavailable",
    504: "timeout",
}
_CODES = {
    "NO_CAPACITY": "unavailable",
    "CONTROL_HELD": "unavailable",
    "SCREEN_UNAVAILABLE": "unavailable",
    "SCREEN_NOT_RUNNING": "conflict",
    "OUTCOME_UNKNOWN": "timeout",
    "PAYLOAD_TOO_LARGE": "too_large",
    "UNSUPPORTED_CAPABILITY": "unsupported",
    "URL_NOT_ALLOWED": "invalid_request",
}
_RUNNING = {"provisioning", "starting", "running", "restarting"}
_STOPPED = {"stopping", "stopped"}
_ENDED = {"exited", "killed", "timed_out"}
_SIGNALS = {"HUP": 1, "INT": 2, "QUIT": 3, "KILL": 9, "SEGV": 11, "PIPE": 13, "TERM": 15}


def _stamp(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text) if text else None


def _name(idempotency_key: str, body_digest: str) -> str:
    """`sps-<key digest>-<body digest>`: the name tells a replay with another body apart."""
    return f"{NAME_PREFIX}{hashlib.sha256(idempotency_key.encode()).hexdigest()[:24]}-{body_digest}"


def _exit_code(e: Mapping[str, Any]) -> int | None:
    if e.get("exit_code") is not None:
        return int(e["exit_code"])
    sig = e.get("signal")
    if sig:
        sig = str(sig).upper().removeprefix("SIG")
        return 128 + (int(sig) if sig.isdigit() else _SIGNALS.get(sig, 15))
    return None


class KubiProvider(Provider):
    """SPS 1.1 over Kubi's public screens API."""

    def __init__(
        self,
        credentials: Mapping[str, str],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        super().__init__(credentials)
        endpoint = (self.credentials.get("endpoint") or DEFAULT_ENDPOINT).rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=f"{endpoint}/v1",
            headers={
                "Authorization": f"Bearer {self.credentials['api_key']}",
                "User-Agent": "sps-provider-kubi",
            },
            transport=transport,
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

    # -- transport ------------------------------------------------------------

    @staticmethod
    def _error(resp: httpx.Response) -> SpsError:
        try:
            body = resp.json()["error"]
            kubi, message = str(body.get("code", "")), str(body.get("message", ""))
            retry = body.get("retry_after")
            outcome = body.get("outcome")
        except (ValueError, KeyError, TypeError, AttributeError):
            kubi, message, retry, outcome = "", f"Kubi answered HTTP {resp.status_code}", None, None
        code = _CODES.get(kubi) or _STATUS.get(resp.status_code, "provider_error")
        if kubi == "FILE_NOT_FOUND" and "is a folder" in message:
            code = "invalid_request"
        if kubi in ("IDEMPOTENCY_KEY_REUSED", "IDEMPOTENCY_CONFLICT") or "Idempotency" in message:
            code = "conflict"
        provider_fault = code in ("unavailable", "timeout", "provider_error")
        if outcome == "unknown":
            out = "unknown"
        elif outcome == "not_applied" or not provider_fault:
            out = "failed"
        else:
            out = "unknown"
        if retry is None and resp.headers.get("retry-after", "").isdigit():
            retry = int(resp.headers["retry-after"])
        text = f"{kubi}: {message}" if kubi else message
        return SpsError(
            code,
            text[:1000],
            outcome=out,
            retry_after_seconds=int(retry) if isinstance(retry, (int, float)) else None,
        )

    async def _req(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            resp = await self._http.request(method, path, **kwargs)
        except httpx.TimeoutException:
            raise SpsError("timeout", "Kubi did not answer in time", outcome="unknown") from None
        except httpx.HTTPError:
            raise SpsError("unavailable", "Kubi is unreachable", outcome="unknown") from None
        if resp.status_code >= 400:
            raise self._error(resp)
        return resp

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        return (await self._req(method, path, **kwargs)).json()

    # -- screens ----------------------------------------------------------------

    async def _screen(self, sandbox_id: str) -> dict[str, Any]:
        """A screen the adapter made; any other screen is unknown to it."""
        screen = await self._json("GET", f"/screens/{sandbox_id}")
        if (
            not str(screen.get("name", "")).startswith(NAME_PREFIX)
            or screen.get("kind") != "disposable"
        ):
            raise SpsError("not_found", f"No sandbox {sandbox_id}")
        return screen

    @staticmethod
    def _to_sandbox(screen: Mapping[str, Any]) -> Sandbox:
        state = screen.get("state")
        mapped = "running" if state in _RUNNING else "stopped" if state in _STOPPED else "destroyed"
        labels = {k: v for k, v in dict(screen.get("labels") or {}).items() if k != ENV_LABEL}
        ends = _stamp(screen.get("ends_at"))
        return Sandbox(
            id=screen["id"],
            sandbox_class=CLASS,
            state=mapped,
            size=SIZE,
            region=REGION,
            image=IMAGE,
            labels=labels,
            created_at=_stamp(screen["created_at"]) or datetime.now(UTC),
            ends_at=ends,
            at_end="destroyed" if ends else None,
        )

    def _plan(self, request: CreateRequest) -> tuple[dict[str, Any], dict[str, str]]:
        if request.snapshot is not None:
            raise SpsError("unsupported", "Kubi restores no snapshots through SPS")
        if request.sandbox_class != CLASS:
            raise SpsError("unsupported", f"Kubi has no class {request.sandbox_class!r}")
        if request.at_end != "destroyed":
            raise SpsError("unsupported", "A Kubi sandbox ends destroyed")
        if request.secrets:
            raise SpsError("invalid_request", "Kubi does not claim hidden_secrets")
        for gate in dict(request.requirements):
            raise SpsError("unsupported", f"Kubi does not claim the section {gate!r}")
        if request.image not in (None, IMAGE):
            raise SpsError("unsupported", f"Kubi offers only the {IMAGE!r} image")
        for key, value in dict(request.resources).items():
            if value is None:
                continue
            ok = {
                "cpu_min": lambda v: float(v) <= SIZE.cpu,
                "memory_min_mib": lambda v: float(v) <= SIZE.memory_mib,
                "disk_min_mib": lambda v: float(v) <= 47683,
                "region": lambda v: v in (REGION, "FR", "EU", "eu", "europe"),
                "kernel": lambda v: "dedicated" in (v if isinstance(v, list) else [v]),
                "arch": lambda v: "amd64" in (v if isinstance(v, list) else [v]),
                "root": lambda v: "sudo" in (v if isinstance(v, list) else [v]),
                "long_running": lambda v: True,
            }.get(key)
            if ok is None or not ok(value):
                raise SpsError("unsupported", f"Kubi cannot meet resources.{key}={value!r}")
        body: dict[str, Any] = {"kind": "disposable", "labels": dict(request.labels)}
        lifetime = request.lifetime_seconds
        if lifetime is not None:
            if not isinstance(lifetime, int) or lifetime < 1:
                raise SpsError("invalid_request", "lifetime_seconds must be an integer >= 1")
            if lifetime > MAX_LIFETIME_S:
                raise SpsError("unsupported", f"A Kubi sandbox lives at most {MAX_LIFETIME_S} s")
            if lifetime < MIN_LIFETIME_S:
                raise SpsError("unsupported", f"A Kubi sandbox lives at least {MIN_LIFETIME_S} s")
            body["ends_in_s"] = lifetime
        env = dict(request.env)
        if env:
            body["labels"][ENV_LABEL] = "1"
        return body, env

    async def _wait_operation(self, op_id: str) -> None:
        deadline = time.monotonic() + CREATE_WAIT_S
        while True:
            op = await self._json("GET", f"/operations/{op_id}", params={"wait_s": MAX_WAIT_S})
            status = op.get("status")
            if status in ("succeeded", "done", "completed"):
                return
            if status in ("failed", "canceled", "cancelled"):
                err = op.get("error") or {}
                code = err.get("code", "")
                raise SpsError(
                    _CODES.get(
                        code,
                        "quota_exceeded"
                        if code in ("PAYMENT_REQUIRED", "CREDIT_EXHAUSTED", "QUOTA_EXCEEDED")
                        else "provider_error",
                    ),
                    f"Kubi could not create the screen: {code} {err.get('message', '')}"[:1000],
                    outcome="failed",
                )
            if time.monotonic() > deadline:
                raise SpsError("timeout", "The screen did not start in time", outcome="unknown")

    # -- operations -------------------------------------------------------------

    async def discovery(self) -> Discovery:
        await self._json("GET", "/me")  # proves the key
        return build_discovery(provider_id=self.provider_id, provider_name=self.name)

    async def create(self, request: CreateRequest, *, idempotency_key: str) -> Sandbox:
        body, env = self._plan(request)
        # The body's digest is part of the screen's name, so a replay with another body is
        # refused here: Kubi answers a replayed key with the first screen whatever the body.
        digest = hashlib.sha256(
            json.dumps(
                {
                    "r": dict(request.resources),
                    "q": dict(request.requirements),
                    "i": request.image,
                    "l": request.lifetime_seconds,
                    "e": env,
                    "b": dict(request.labels),
                    "c": request.sandbox_class,
                },
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()[:16]
        body["name"] = _name(idempotency_key, digest)
        resp = await self._req(
            "POST", "/screens", json=body, headers={"Idempotency-Key": idempotency_key}
        )
        op = resp.json()
        screen_id = op.get("screen_id")
        if not screen_id:
            raise SpsError("provider_error", "Kubi's create named no screen", outcome="unknown")
        screen = await self._json("GET", f"/screens/{screen_id}")
        if screen.get("name") != body["name"]:
            raise SpsError("conflict", "Idempotency-Key was used with a different body")
        if screen.get("state") not in ("running",):
            if screen.get("state") in ("deleting", "deleted", "failed"):
                raise SpsError("provider_error", "The screen failed to start", outcome="failed")
            await self._wait_operation(op["id"])
        if env:
            text = "".join(f"export {k}={shlex.quote(v)}\n" for k, v in env.items())
            await self._req(
                "PUT",
                f"/screens/{screen_id}/files",
                params={"path": ENV_PATH, "mode": "0600"},
                content=text.encode(),
                headers={"Content-Type": "application/octet-stream"},
            )
        screen = await self._json("GET", f"/screens/{screen_id}")
        return self._to_sandbox(screen)

    async def get(self, sandbox_id: str) -> Sandbox:
        return self._to_sandbox(await self._screen(sandbox_id))

    async def list(
        self,
        *,
        labels: Mapping[str, str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SandboxPage:
        if not 1 <= limit <= LIST_LIMIT:
            raise SpsError("invalid_request", f"limit must be 1 to {LIST_LIMIT}")
        params: list[tuple[str, str]] = [("kind", "disposable"), ("limit", str(limit))]
        params += [("label", f"{k}:{v}") for k, v in dict(labels or {}).items()]
        if cursor:
            params.append(("after", cursor))
        page = await self._json("GET", "/screens", params=params)
        boxes = []
        for screen in page.get("data", []):
            if not str(screen.get("name", "")).startswith(NAME_PREFIX):
                continue
            box = self._to_sandbox(screen)
            want = dict(labels or {})
            if box.state != "destroyed" and all(box.labels.get(k) == v for k, v in want.items()):
                boxes.append(box)
        return SandboxPage(sandboxes=boxes, next_cursor=page.get("next"))

    async def destroy(self, sandbox_id: str) -> DestroyResult:
        try:
            await self._screen(sandbox_id)
        except SpsError as error:
            if error.code != "not_found":
                raise
            # An ended sandbox is no longer readable; Kubi's spend record says whether it was ours.
            spend = await self._spend(sandbox_id)
            if spend is None:
                raise
            return DestroyResult(sandbox_id=sandbox_id, state="destroyed")
        try:
            await self._req(
                "DELETE",
                f"/screens/{sandbox_id}",
                headers={"Idempotency-Key": f"sps-destroy-{sandbox_id}"},
            )
        except SpsError as error:
            if error.code not in ("not_found", "conflict"):
                raise
        return DestroyResult(sandbox_id=sandbox_id, state="destroyed")

    # -- commands -----------------------------------------------------------------

    async def _argv(
        self, sandbox_id: str, argv: list[str], env: Mapping[str, str]
    ) -> tuple[list[str], dict[str, str]]:
        """argv as Kubi runs it: unchanged, or after sourcing the create-time env file, with the
        call's own env applied after it so the call wins."""
        screen = await self._screen(sandbox_id)
        if screen.get("state") not in _RUNNING:
            raise SpsError("conflict", f"Sandbox {sandbox_id} is {screen.get('state')}")
        if ENV_LABEL not in dict(screen.get("labels") or {}):
            return argv, dict(env)
        pairs = [f"{k}={v}" for k, v in env.items()]
        wrapped = ["/bin/sh", "-c", f'. {ENV_PATH}; exec env "$@"', "sps", *pairs, "--", *argv]
        return wrapped, {}

    async def _start(
        self,
        sandbox_id: str,
        argv: list[str],
        cwd: str | None,
        env: Mapping[str, str],
        timeout_s: int,
        wait_s: int,
    ) -> dict[str, Any]:
        if not argv:
            raise SpsError("invalid_request", "argv is empty")
        full, call_env = await self._argv(sandbox_id, argv, env)
        body: dict[str, Any] = {
            "argv": full,
            "cwd": cwd or HOME,
            "timeout_s": timeout_s,
            "wait_s": wait_s,
            "max_bytes": PAGE_BYTES,
        }
        if call_env:
            body["env"] = call_env
        if cwd is not None:
            probe = await self._json(
                "POST",
                f"/screens/{sandbox_id}/exec",
                json={"argv": ["test", "-d", cwd], "cwd": "/", "timeout_s": 30, "wait_s": 25},
            )
            if probe.get("exit_code") != 0:
                raise SpsError("not_found", f"No such directory: {cwd}")
        return await self._json(
            "POST",
            f"/screens/{sandbox_id}/exec",
            json=body,
            timeout=httpx.Timeout(MAX_WAIT_S + 35, connect=10.0),
        )

    async def _poll(self, sandbox_id: str, exec_id: str, **params: Any) -> dict[str, Any]:
        return await self._json(
            "GET",
            f"/screens/{sandbox_id}/exec/{exec_id}",
            params={"max_bytes": PAGE_BYTES, **params},
            timeout=httpx.Timeout(MAX_WAIT_S + 35, connect=10.0),
        )

    async def _read_all(
        self, sandbox_id: str, e: dict[str, Any], stream: str, limit: int
    ) -> tuple[str, bool, bool]:
        """One stream's output from offset 0 up to limit bytes: text, truncated, head lost."""
        total = int(e.get(f"{stream}_bytes") or 0)
        text, offset, lost = (
            e.get(stream, ""),
            int(e.get(f"{stream}_next") or 0),
            bool(e.get("output_dropped")),
        )
        if lost:
            text, offset = "", 0
        while offset < min(total, limit):
            other = "stderr" if stream == "stdout" else "stdout"
            page = await self._poll(
                sandbox_id,
                e["id"],
                wait_s=0,
                **{f"{stream}_from": offset, f"{other}_from": 1 << 40},
                max_bytes=min(PAGE_BYTES, limit - offset) if limit > offset else 1,
            )
            if (
                page.get("output_dropped")
                and offset == 0
                and page.get(f"{stream}_next", 0) > 0
                and not page.get(stream)
            ):
                lost = True
            nxt = int(page.get(f"{stream}_next") or offset)
            if nxt <= offset:
                break
            text += page.get(stream, "")
            offset = nxt
        encoded = text.encode()
        truncated = total > limit or len(encoded) > limit
        if len(encoded) > limit:
            text = encoded[:limit].decode("utf-8", "ignore")
        return text, truncated, lost

    async def run(self, sandbox_id: str, request: RunRequest) -> RunResult:
        if request.user not in (None, "user"):
            raise SpsError("unsupported", "Kubi does not claim user_selection")
        if request.output != "text":
            raise SpsError("unsupported", "Kubi does not claim run.base64")
        limit = request.output_limit_bytes
        if not 0 <= limit <= LIMITS.output_bytes:
            raise SpsError("invalid_request", f"output_limit_bytes over {LIMITS.output_bytes}")
        if not 1 <= request.timeout_ms <= LIMITS.run_timeout_ms:
            raise SpsError("invalid_request", f"timeout_ms is 1 to {LIMITS.run_timeout_ms}")
        timeout_s = max(1, math.ceil(request.timeout_ms / 1000))
        e = await self._start(
            sandbox_id, list(request.argv), request.cwd, request.env, timeout_s, MAX_WAIT_S
        )
        deadline = time.monotonic() + timeout_s + 120
        while e.get("status") == "running":
            if time.monotonic() > deadline:
                raise SpsError("timeout", "The command did not end in time", outcome="unknown")
            e = await self._poll(
                sandbox_id,
                e["id"],
                wait_s=MAX_WAIT_S,
                until="exit",
                stdout_from=e.get("stdout_next", 0),
                stderr_from=e.get("stderr_next", 0),
            )
            e["stdout"], e["stderr"] = "", ""
            e["stdout_next"], e["stderr_next"] = 0, 0
        if e.get("status") == "lost":
            raise SpsError(
                "provider_error", "The screen restarted during the command", outcome="unknown"
            )
        if "stdout_next" not in e or e.get("stdout_next") == 0:
            e["stdout"], e["stderr"] = "", ""
        out, out_cut, out_lost = await self._read_all(sandbox_id, e, "stdout", limit)
        err, err_cut, err_lost = await self._read_all(sandbox_id, e, "stderr", limit)
        if out_lost or err_lost:
            raise SpsError(
                "too_large",
                "The command printed more than Kubi keeps; send its output to a file and read the file",
            )
        timed_out = e.get("status") == "timed_out"
        return RunResult(
            exit_code=None if timed_out else _exit_code(e),
            stdout=out,
            stderr=err,
            stdout_truncated=out_cut,
            stderr_truncated=err_cut,
            timed_out=timed_out,
        )

    async def process_start(self, sandbox_id: str, request: ProcessStartRequest) -> Process:
        if request.stdin:
            raise SpsError("unsupported", "Kubi processes take no stdin")
        if request.user not in (None, "user"):
            raise SpsError("unsupported", "Kubi does not claim user_selection")
        e = await self._start(sandbox_id, list(request.argv), request.cwd, request.env, 0, 0)
        return Process(
            id=e["id"],
            sandbox_id=sandbox_id,
            started_at=_stamp(e.get("started_at")) or datetime.now(UTC),
        )

    async def process_output(
        self, sandbox_id: str, process_id: str, *, cursor: str | None = None, wait_ms: int = 0
    ) -> OutputPage:
        out_from, err_from = 0, 0
        if cursor:
            try:
                a, b = cursor.split(".")
                out_from, err_from = int(a), int(b)
            except ValueError:
                raise SpsError("invalid_request", "Not a cursor of this provider") from None
        if not 0 <= wait_ms <= LIMITS.wait_ms:
            raise SpsError("invalid_request", f"wait_ms is 0 to {LIMITS.wait_ms}")
        await self._screen(sandbox_id)
        deadline = time.monotonic() + wait_ms / 1000
        while True:
            left = max(0, deadline - time.monotonic())
            e = await self._poll(
                sandbox_id,
                process_id,
                wait_s=min(MAX_WAIT_S, int(left)),
                until="output",
                stdout_from=out_from,
                stderr_from=err_from,
            )
            done = e.get("status") in _ENDED or e.get("status") == "lost"
            if e.get("stdout") or e.get("stderr") or done or left < 1:
                break
        if e.get("status") == "lost":
            raise SpsError(
                "provider_error", "The screen restarted during the process", outcome="unknown"
            )
        chunks = [OutputChunk(s, e[s]) for s in ("stdout", "stderr") if e.get(s)]
        nxt_out = int(e.get("stdout_next") or out_from)
        nxt_err = int(e.get("stderr_next") or err_from)
        # Unread output left: the process has not finished being read, even if it exited.
        more = nxt_out < int(e.get("stdout_bytes") or 0) or nxt_err < int(
            e.get("stderr_bytes") or 0
        )
        exited = done and not more
        return OutputPage(
            chunks=chunks,
            next_cursor=f"{nxt_out}.{nxt_err}",
            gap=bool(e.get("output_dropped")),
            stdout_closed=exited,
            stderr_closed=exited,
            state="exited" if exited else "running",
            exit_code=_exit_code(e) if exited else None,
        )

    # -- files ----------------------------------------------------------------------

    async def files_read(self, sandbox_id: str, path: str) -> FileStream:
        await self._screen(sandbox_id)
        ctx = self._http.stream(
            "GET",
            f"/screens/{sandbox_id}/files",
            params={"path": path},
            timeout=httpx.Timeout(None, connect=10.0, read=120.0),
        )
        try:
            resp = await ctx.__aenter__()
        except httpx.HTTPError:
            raise SpsError("unavailable", "Kubi is unreachable", outcome="unknown") from None
        if resp.status_code >= 400:
            await resp.aread()
            await ctx.__aexit__(None, None, None)
            raise self._error(resp)
        length = resp.headers.get("content-length")
        size = int(length) if length else None

        async def chunks() -> AsyncIterator[bytes]:
            n = 0
            try:
                async for part in resp.aiter_bytes(1 << 16):
                    n += len(part)
                    yield part
            except httpx.HTTPError:
                raise SpsError(
                    "provider_error", "The download broke off", outcome="unknown"
                ) from None
            finally:
                await ctx.__aexit__(None, None, None)
            if size is not None and n != size:
                raise SpsError("provider_error", "The download broke off", outcome="unknown")

        return FileStream(size=size, chunks=chunks())

    async def files_write(
        self,
        sandbox_id: str,
        path: str,
        data: AsyncIterable[bytes],
        *,
        size: int,
        user: str | None = None,
    ) -> FileInfo:
        if user not in (None, "user"):
            raise SpsError("unsupported", "Kubi does not claim user_selection")
        if size > LIMITS.file_bytes:
            raise SpsError("too_large", f"Files are up to {LIMITS.file_bytes} bytes")
        await self._screen(sandbox_id)
        resp = await self._req(
            "PUT",
            f"/screens/{sandbox_id}/files",
            params={"path": path},
            content=data,
            headers={"Content-Length": str(size), "Content-Type": "application/octet-stream"},
            timeout=httpx.Timeout(None, connect=10.0, read=300.0),
        )
        body = resp.json()
        return FileInfo(path=body.get("path", path), size=int(body.get("size_bytes", size)))

    # -- cost -------------------------------------------------------------------------

    async def _spend(self, sandbox_id: str) -> dict[str, Any] | None:
        try:
            return await self._json("GET", f"/screens/{sandbox_id}/spend")
        except SpsError as error:
            if error.code == "not_found":
                return None
            raise

    async def cost(self, sandbox_id: str, *, created_at: datetime | None = None) -> Cost:
        spend = await self._spend(sandbox_id)
        if spend is None or not str(spend.get("name", "")).startswith(NAME_PREFIX):
            raise SpsError("not_found", f"No sandbox {sandbox_id}")
        lines = []
        for h in spend.get("hours", []):
            start = _stamp(h["hour"])
            assert start is not None
            lines.append(
                CostLine(
                    "machine",
                    Decimal(str(h["charged_usd"])),
                    Decimal(str(h["running_s"])),
                    "machine_second",
                    start,
                    start + timedelta(hours=1),
                )
            )
        total = sum((line.amount_usd or Decimal(0) for line in lines), Decimal(0))
        final = bool(spend.get("final"))
        if not lines:
            lines = [CostLine("machine", Decimal(0), Decimal(0), "machine_second")] if final else []
        return Cost(
            sandbox_id=sandbox_id,
            status="final" if final else "accruing",
            total_usd=total if lines else None,
            source="reported",
            lines=tuple(lines),
            as_of=_stamp(spend.get("as_of")) or datetime.now(UTC),
        )

    async def aclose(self) -> None:
        await self._http.aclose()
