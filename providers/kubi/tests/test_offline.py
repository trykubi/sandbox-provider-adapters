# SPDX-FileCopyrightText: Kubi
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""Offline tests: Kubi's API answered by httpx.MockTransport."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from sandbox_provider_spec.v1 import CreateRequest, RunRequest, SpsError

from sps_provider_kubi import KubiProvider

SCREEN = {
    "id": "scr_aaaaaaaaaaaaaaaaaaaa",
    "kind": "disposable",
    "state": "running",
    "labels": {"team": "a"},
    "ends_at": "2026-11-28T01:00:00Z",
    "created_at": "2026-11-28T00:00:00Z",
}


def provider(handler) -> KubiProvider:
    return KubiProvider({"api_key": "k"}, transport=httpx.MockTransport(handler))


def test_create_sends_the_key_lifetime_and_labels_and_refuses_another_body() -> None:
    seen: list[httpx.Request] = []
    name: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "POST":
            body = json.loads(request.content)
            name.setdefault("first", body["name"])
            return httpx.Response(202, json={"id": "op_1", "screen_id": SCREEN["id"]})
        return httpx.Response(200, json={**SCREEN, "name": name["first"]})

    p = provider(handler)
    box = asyncio.run(
        p.create(CreateRequest(lifetime_seconds=90, labels={"team": "a"}), idempotency_key="k1")
    )
    post = json.loads(seen[0].content)
    assert seen[0].headers["idempotency-key"] == "k1"
    assert (
        post["kind"] == "disposable" and post["ends_in_s"] == 90 and post["labels"] == {"team": "a"}
    )
    assert box.state == "running" and box.labels == {"team": "a"}
    with pytest.raises(SpsError) as err:
        asyncio.run(p.create(CreateRequest(lifetime_seconds=91), idempotency_key="k1"))
    assert err.value.code == "conflict"


def test_other_screens_of_the_account_are_unknown() -> None:
    p = provider(lambda r: httpx.Response(200, json={**SCREEN, "name": "my-own-screen"}))
    with pytest.raises(SpsError) as err:
        asyncio.run(p.get(SCREEN["id"]))
    assert err.value.code == "not_found"


def test_kubi_errors_map_to_sps_codes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = {
            "error": {
                "code": "NO_CAPACITY",
                "message": "no room",
                "retry_after": 30,
                "outcome": "not_applied",
            }
        }
        return httpx.Response(409, json=body)

    with pytest.raises(SpsError) as err:
        asyncio.run(provider(handler).create(CreateRequest(), idempotency_key="k"))
    assert err.value.code == "unavailable" and err.value.retry_after_seconds == 30


def test_lifetime_under_a_minute_is_unsupported() -> None:
    with pytest.raises(SpsError) as err:
        asyncio.run(
            provider(lambda r: httpx.Response(500)).create(
                CreateRequest(lifetime_seconds=30), idempotency_key="k"
            )
        )
    assert err.value.code == "unsupported"


def test_run_returns_exit_code_and_output() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/exec"):
            return httpx.Response(
                200,
                json={
                    "id": "ex_1",
                    "status": "exited",
                    "exit_code": 3,
                    "stdout": "hi\n",
                    "stderr": "",
                    "stdout_bytes": 3,
                    "stderr_bytes": 0,
                    "stdout_next": 3,
                    "stderr_next": 0,
                },
            )
        return httpx.Response(200, json={**SCREEN, "name": "sps-x"})

    res = asyncio.run(
        provider(handler).run(SCREEN["id"], RunRequest(argv=["sh", "-c", "echo hi; exit 3"]))
    )
    assert res.exit_code == 3 and res.stdout == "hi\n" and not res.timed_out
