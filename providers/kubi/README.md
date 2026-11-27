<!-- SPDX-FileCopyrightText: Kubi -->
<!-- SPDX-License-Identifier: PolyForm-Shield-1.0.0 -->

# Kubi: an SPS 1.1 provider adapter

[Kubi](https://trykubi.com) rents always-on Linux desktops ("screens") that AI agents use through an API. This
adapter makes a **disposable** Kubi screen an SPS sandbox: a Linux desktop (Debian 13, 4 vCPU, 8 GiB, 50 GB) in
its own VM (`kernel: dedicated`), in France (`kubi:eu-1`). It calls only Kubi's public API at `api.trykubi.com`.

## What maps to what

| SPS | Kubi |
| --- | --- |
| `create` | `POST /v1/screens` (disposable, `ends_in_s` = `lifetime_seconds`, the consumer's `Idempotency-Key`), then the create operation until it runs |
| `get`, `list`, `destroy` | `GET /v1/screens/{id}`, `GET /v1/screens?kind=disposable&label=`, `DELETE` |
| `run`, `process.*` | `POST /v1/screens/{id}/exec` with `argv` (no shell), long-polled; output read by byte offset |
| `files.read`, `files.write` | `GET`/`PUT /v1/screens/{id}/files`, streamed, up to 1 GiB |
| `cost` | `GET /v1/screens/{id}/spend`, one `machine` line per UTC hour, `reported`, `final` once deleted |

- The adapter only reads, lists and deletes screens named `sps-...`, the ones it made, so a key that also holds an
  account's own screens never lets a consumer touch them.
- The screen's name carries digests of the `Idempotency-Key` and the request, so a replay with another body is `conflict`.
- Create-time `env` goes into `/home/user/.config/sps/env.sh` (mode 0600), sourced before each command; the
  command's own `env` wins.
- Lifetimes run from 60 s to 30 days; Kubi deletes the screen at its end time, checked every 10 s.
- Commands run as `user` in `/home/user`, with passwordless `sudo` (`root: sudo`).

## Run the tester

The key needs `read`, `exec` and `power`. Disposable screens draw on the account's usage credit (USD 0.035 an
hour, 60 s steps).

```console
$ export KUBI_API_KEY=...
$ uv run --group tools sandbox-provider-spec test kubi --credential api_key=KUBI_API_KEY
```

## Not claimed (yet)

Kubi has these natively, outside this adapter: the desktop (screenshots, input, accessibility tree, live view),
snapshots and templates, ports (3 per screen), stop and start. They map to SPS's `desktop`, `snapshots`,
`previews` and `pause_resume` sections and come in later versions.
