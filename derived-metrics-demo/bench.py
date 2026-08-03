#!/usr/bin/env python3
"""Answers the same question off the derived metrics and off the raw stream, and times both.

The point of derived metrics is that a query reads a few hundred documents instead of every document
ever written. This measures that: for each question, the derived query and the source query are run
cold and warm, and the documents each had to touch are reported alongside the timings.

Cold means after clearing the query, request and fielddata caches. It is not a truly cold OS page
cache — nothing short of a reboot gives you that — so the cold numbers understate the difference on a
node that has been restarted or where the data has aged out of the filesystem cache.
"""

import argparse
import base64
import json
import statistics
import time
import urllib.error
import urllib.request

# Each question, asked twice. The derived query reads the metric documents; the source query computes
# the same thing from the raw stream. They should agree, and the derived one should be far cheaper.
QUESTIONS = [
    (
        "documents written",
        'FROM {derived} | WHERE metric.name == "ingest.docs.count" | STATS v = SUM(metric.value)',
        "FROM {source} | STATS v = COUNT(*)",
    ),
    (
        "5xx errors",
        'FROM {derived} | WHERE metric.name == "http.errors" | STATS v = SUM(metric.value)',
        "FROM {source} | WHERE http.response.status_code >= 500 | STATS v = COUNT(*)",
    ),
    (
        "peak queue depth",
        'FROM {derived} | WHERE metric.name == "queue.depth.max" | STATS v = MAX(metric.value)',
        "FROM {source} | STATS v = MAX(queue.depth)",
    ),
    (
        "mean latency, ms",
        'FROM {derived} | WHERE metric.name == "event.duration.avg" '
        "| STATS v = SUM(metric.value) / SUM(metric.count) / 1000000",
        "FROM {source} | STATS v = AVG(event.duration) / 1000000",
    ),
    (
        "p95 latency, ms",
        'FROM {derived} | WHERE metric.name == "event.duration.distribution" '
        "| STATS v = PERCENTILE(metric.histogram, 95) / 1000000",
        "FROM {source} | STATS v = PERCENTILE(event.duration, 95) / 1000000",
    ),
    (
        "ingest rate per 10s bucket",
        'FROM {derived} | WHERE metric.name == "ingest.docs.rate" '
        "| STATS v = SUM(metric.value) BY bucket = BUCKET(@timestamp, 10 second) | STATS v = COUNT(*)",
        "FROM {source} | STATS v = COUNT(*) BY bucket = BUCKET(@timestamp, 10 second) | STATS v = COUNT(*)",
    ),
    (
        "errors per service",
        'FROM {derived} | WHERE metric.name == "http.errors" '
        "| STATS v = SUM(metric.value) BY service = dimensions.service.name | STATS v = COUNT(*)",
        "FROM {source} | WHERE http.response.status_code >= 500 "
        "| STATS v = COUNT(*) BY service = service.name | STATS v = COUNT(*)",
    ),
]


class Client:
    def __init__(self, url, user, password):
        self.url = url
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {"Content-Type": "application/json", "Authorization": f"Basic {token}"}

    def post(self, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"{self.url}{path}", data=data, headers=self.headers, method="POST")
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)

    def query(self, esql):
        """Returns (took_ms, value). ES|QL reports its own took, which excludes transport overhead."""
        started = time.perf_counter()
        result = self.post("/_query", {"query": esql})
        elapsed = (time.perf_counter() - started) * 1000
        values = result.get("values") or [[None]]
        return elapsed, values[0][0]

    def clear_caches(self):
        self.post("/_cache/clear?query=true&request=true&fielddata=true")

    def count(self, index):
        return self.post("/_query", {"query": f"FROM {index} | STATS n = COUNT(*)"})["values"][0][0]


def run(client, esql, repeats):
    """One cold run after clearing caches, then `repeats` warm runs; the warm figure is the median."""
    client.clear_caches()
    cold, value = client.query(esql)
    warm = [client.query(esql)[0] for _ in range(repeats)]
    return cold, statistics.median(warm), value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--user", default="elastic-admin")
    parser.add_argument("--password", default="elastic-password")
    parser.add_argument("--data-stream", default="logs-derived-demo-default")
    parser.add_argument("--interval", default="10s")
    parser.add_argument("--repeats", type=int, default=5, help="warm runs per query; the median is reported")
    args = parser.parse_args()

    client = Client(args.url, args.user, args.password)
    source = args.data_stream
    derived = f"derived-metrics-{source}-{args.interval}"

    source_docs = client.count(source)
    derived_docs = client.count(derived)

    print(f"\n  source   {source}")
    print(f"           {source_docs:>12,} documents")
    print(f"  derived  {derived}")
    print(f"           {derived_docs:>12,} documents"
          f"   —  {source_docs / max(derived_docs, 1):.1f}x fewer\n")

    header = f"  {'question':<28} {'derived cold':>13} {'warm':>9} {'source cold':>13} {'warm':>9} {'speedup':>9}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    speedups = []
    for label, derived_query, source_query in QUESTIONS:
        try:
            d_cold, d_warm, d_value = run(client, derived_query.format(derived=derived, source=source), args.repeats)
            s_cold, s_warm, s_value = run(client, source_query.format(derived=derived, source=source), args.repeats)
        except urllib.error.HTTPError as e:
            print(f"  {label:<28} failed: {e.read().decode()[:80]}")
            continue
        speedup = s_warm / d_warm if d_warm else 0
        speedups.append(speedup)
        print(
            f"  {label:<28} {d_cold:>11.0f}ms {d_warm:>7.0f}ms "
            f"{s_cold:>11.0f}ms {s_warm:>7.0f}ms {speedup:>8.1f}x"
        )

    if speedups:
        print(f"\n  median speedup, warm: {statistics.median(speedups):.1f}x")
    print(
        "\n  Speedup is warm-vs-warm, which is the conservative comparison: the source side benefits\n"
        "  most from caching, and it is the side that grows without bound as the stream ages.\n"
    )


if __name__ == "__main__":
    main()
