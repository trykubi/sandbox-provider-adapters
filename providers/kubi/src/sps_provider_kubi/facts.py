# SPDX-FileCopyrightText: Kubi
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""What Kubi offers: the facts behind discovery, from Kubi's public docs (docs.trykubi.com)."""

from __future__ import annotations

from decimal import Decimal

from sandbox_provider_spec.v1 import (
    Discovery,
    Image,
    Limits,
    Price,
    ProviderInfo,
    SandboxClass,
    Size,
)

CLASS = "default"
REGION = "kubi:eu-1"
IMAGE = "kubi-desktop"
SIZE = Size(cpu=4, memory_mib=8192)  # Kubi's one size, `standard`, with a 50 GB disk
DISK_MIB = 50 * 1000 * 1000 * 1000 // (1 << 20)
MAX_LIFETIME_S = 2_592_000  # a disposable screen's longest end time, 30 days
MIN_LIFETIME_S = 60  # the shortest end time Kubi takes
MAX_WAIT_S = 25  # Kubi's long poll on exec and operations
PAGE_BYTES = 1 << 20  # the most output of one stream that one exec read returns
LIST_LIMIT = 100
NAME_PREFIX = "sps-"  # every screen the adapter makes; it never touches another screen
ENV_PATH = "/home/user/.config/sps/env.sh"  # create-time env, sourced before each command
ENV_LABEL = "sps.env"  # Kubi-side marker that a screen has ENV_PATH; never shown as a label
HOME = "/home/user"
COST_FINAL_WITHIN_S = 300  # a deleted screen's charges are all made before it is deleted
LIMITS = Limits(
    file_bytes=1 << 30,
    run_timeout_ms=86_400_000,
    output_bytes=16 << 20,
    wait_ms=60_000,
    process_output_bytes=16 << 20,
    process_retention_seconds=86_400,
    list_limit=LIST_LIMIT,
    cost_final_within_seconds=COST_FINAL_WITHIN_S,
)


def build_discovery(provider_id: str, provider_name: str) -> Discovery:
    klass = SandboxClass(
        id=CLASS,
        name="Linux desktop in its own VM",
        ready=True,
        resources={
            "regions": [REGION],
            "located_in": ["FR"],
            "sizes": [{"cpu": SIZE.cpu, "memory_mib": SIZE.memory_mib, "disk_mib": DISK_MIB}],
            "default_size": {"cpu": SIZE.cpu, "memory_mib": SIZE.memory_mib, "disk_mib": DISK_MIB},
            "arch": ["amd64"],
            "kernel": "dedicated",
            "max_lifetime_seconds": MAX_LIFETIME_S,
            "default_cwd": HOME,
            "default_user": "user",
            "root": ["sudo"],
            "outbound_traffic": ["http", "any_tcp", "udp"],
            "long_running": True,
        },
        lifetime_timer={
            "enforced_by": "provider",
            "at_end": ["destroyed"],
            "counts_paused_time": False,
        },
        images=(Image(IMAGE, True, "Debian 13 with a desktop, Chromium, bash and python3"),),
        prices=(
            Price(
                "machine",
                Decimal("0.035"),
                "machine_hour",
                "allocated",
                increment_seconds=60,
                minimum_seconds=60,
            ),
        ),
    )
    return Discovery(
        spec_version="1.1",
        provider=ProviderInfo(provider_id, provider_name, "https://docs.trykubi.com"),
        limits=LIMITS,
        classes=(klass,),
    )
