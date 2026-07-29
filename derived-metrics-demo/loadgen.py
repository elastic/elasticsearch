#!/usr/bin/env python3
"""Continuously index documents into the demo data stream at a deliberately varying rate.

The point of the varying rate is to make the derived metrics visibly track the write load while the
number of derived documents stays flat: no matter which phase the load is in, each interval produces
one document per series, so `ingest.docs.rate` moves and the derived document count does not.

Only the standard library is used, so this runs against any Python 3.9+ without a virtualenv.
"""

import argparse
import base64
import json
import math
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque

SERVICES = ["checkout", "search", "cart", "payments", "recommendations"]
REGIONS = ["eu-west-1", "us-east-1", "ap-south-1"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
HOSTS = [f"host-{i:02d}" for i in range(1, 7)]

# (label, seconds, docs/sec at start, docs/sec at end, ratio of 5xx responses)
CYCLE = [
    ("calm", 60, 100, 100, 0.01),
    ("ramp up", 90, 100, 1500, 0.02),
    ("spike", 30, 4000, 4000, 0.25),
    ("recovery", 45, 4000, 200, 0.10),
    ("steady", 90, 400, 400, 0.02),
    ("quiet", 30, 20, 20, 0.00),
]

PROFILES = {
    "cycle": CYCLE,
    "flat": [("flat", 3600, 500, 500, 0.02)],
    "spiky": [("low", 20, 50, 50, 0.0), ("spike", 10, 5000, 5000, 0.3)],
}


class Indexer:
    def __init__(self, url, user, password, data_stream):
        self.bulk_url = f"{url}/{data_stream}/_bulk"
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"Basic {token}",
        }
        self.indexed = 0
        self.failed = 0
        self.lock = threading.Lock()

    def document(self, now_ms, error_ratio):
        service = random.choice(SERVICES)
        if random.random() < error_ratio:
            status = random.choice([500, 502, 503])
        elif random.random() < 0.05:
            status = random.choice([400, 404, 429])
        else:
            status = 200
        # Latency and queue depth track the error ratio, so a spike is visible in the gauges too.
        latency_base = 40_000_000 if status < 500 else 900_000_000
        return {
            "@timestamp": now_ms,
            "message": f"{service} handled a request",
            "service": {"name": service},
            "cloud": {"region": random.choice(REGIONS)},
            "host": {"name": random.choice(HOSTS)},
            "http": {
                "request": {"method": random.choice(METHODS)},
                "response": {
                    "status_code": status,
                    "body": {"bytes": random.randint(200, 20_000)},
                },
            },
            "event": {
                "duration": int(random.expovariate(1 / latency_base)),
                "outcome": "failure" if status >= 500 else "success",
            },
            "queue": {"depth": max(0, int(random.gauss(20 + 200 * error_ratio, 8)))},
        }

    def send(self, count, error_ratio):
        if count <= 0:
            return
        now_ms = int(time.time() * 1000)
        lines = []
        for _ in range(count):
            # Data streams only accept create.
            lines.append('{"create":{}}')
            lines.append(json.dumps(self.document(now_ms, error_ratio)))
        body = ("\n".join(lines) + "\n").encode()
        request = urllib.request.Request(self.bulk_url, data=body, headers=self.headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
            errors = sum(1 for item in payload.get("items", []) if item.get("create", {}).get("error"))
            with self.lock:
                self.indexed += count - errors
                self.failed += errors
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            with self.lock:
                self.failed += count
            print(f"  bulk request failed: {e}", file=sys.stderr)


def rate_at(phase, elapsed):
    _, duration, start_rate, end_rate, _ = phase
    if duration <= 0:
        return end_rate
    progress = min(1.0, elapsed / duration)
    return start_rate + (end_rate - start_rate) * progress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--user", default="elastic-admin")
    parser.add_argument("--password", default="elastic-password")
    parser.add_argument("--data-stream", default="logs-derived-demo-default")
    parser.add_argument("--profile", default="cycle", choices=sorted(PROFILES))
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    phases = PROFILES[args.profile]
    indexer = Indexer(args.url, args.user, args.password, args.data_stream)
    # A small pool keeps the higher rates achievable without letting requests pile up unboundedly.
    workers = []
    pending = deque()
    pending_lock = threading.Lock()
    stop = threading.Event()

    def worker():
        while stop.is_set() is False:
            with pending_lock:
                batch = pending.popleft() if pending else None
            if batch is None:
                time.sleep(0.01)
                continue
            indexer.send(*batch)

    for _ in range(4):
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        workers.append(thread)

    print(f"Indexing into {args.data_stream} with the '{args.profile}' profile. Ctrl-C to stop.")
    tick = 0.1  # seconds
    try:
        while True:
            for phase in phases:
                label, duration, _, _, error_ratio = phase
                print(f"\n== phase: {label} ({duration}s)")
                started = time.monotonic()
                last_report = started
                while True:
                    elapsed = time.monotonic() - started
                    if elapsed >= duration:
                        break
                    rate = rate_at(phase, elapsed)
                    # A gentle wobble on top of the phase shape, so no interval looks identical.
                    rate *= 1 + 0.15 * math.sin(elapsed * 1.7)
                    batch = int(round(rate * tick))
                    with pending_lock:
                        if len(pending) < 40:
                            pending.append((batch, error_ratio))
                    time.sleep(tick)
                    if time.monotonic() - last_report >= 5:
                        with indexer.lock:
                            total, failed = indexer.indexed, indexer.failed
                        print(
                            f"   ~{int(rate):>5} docs/s   total indexed: {total:>9}"
                            f"   failed: {failed}   5xx ratio: {error_ratio:.0%}"
                        )
                        last_report = time.monotonic()
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        stop.set()
        for thread in workers:
            thread.join(timeout=2)
        with indexer.lock:
            print(f"Indexed {indexer.indexed} documents, {indexer.failed} failed.")


if __name__ == "__main__":
    main()
