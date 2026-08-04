#!/usr/bin/env python3
"""Prints the thing the feature is actually about: the source write rate swings, the derived
document rate does not, and the derived metrics still track the load."""

import argparse
import base64
import json
import urllib.request

BLOCKS = "▁▂▃▄▅▆▇█"


def sparkline(counts):
    """Renders the series against its own peak, so a flat series stays visibly flat."""
    if not counts:
        return ""
    peak = max(counts)
    if peak == 0:
        return BLOCKS[0] * len(counts)
    return "".join(BLOCKS[min(len(BLOCKS) - 1, round(c / peak * (len(BLOCKS) - 1)))] for c in counts)


def summarize(counts):
    if not counts:
        return "no data"
    return f"min {min(counts):>7,}  max {max(counts):>7,}"


def spread(counts):
    """Peak-to-trough over intervals that produced anything.

    Intervals with no writes emit no documents at all, which is deliberate. Counting those zeros as
    the trough would report a flat series as infinitely variable.
    """
    active = [c for c in counts if c > 0]
    return max(active) / min(active) if active else 1.0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--user", default="elastic-admin")
    parser.add_argument("--password", default="elastic-password")
    parser.add_argument("--data-stream", default="logs-derived-demo-default")
    parser.add_argument("--interval", default="10s")
    parser.add_argument("--compare-with", default=None, help="a second source stream to compare derived volume against")
    parser.add_argument("--window", default="6m", help="how far back the rate comparison looks")
    args = parser.parse_args()

    token = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()

    def search(index, body):
        request = urllib.request.Request(
            f"{args.url}/{index}/_search?size=0",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Basic {token}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    interval = args.interval
    source = args.data_stream
    derived = f"derived-metrics-{args.data_stream}-{args.interval}"
    # Wide enough to span a full load cycle, so the comparison covers more than one phase.
    window = {"range": {"@timestamp": {"gte": f"now-{args.window}"}}}

    def per_bucket(index, extra_filter=None):
        filters = [window] + ([extra_filter] if extra_filter else [])
        response = search(index, {
            "query": {"bool": {"filter": filters}},
            "aggs": {"per_bucket": {"date_histogram": {"field": "@timestamp", "fixed_interval": interval}}},
        })
        buckets = response["aggregations"]["per_bucket"]["buckets"]
        # The first and last buckets are partial, so they are not comparable to the rest.
        counts = [b["doc_count"] for b in buckets]
        return counts[1:-1] or counts

    source_counts = per_bucket(source)
    # The demo runs a single interval, so everything in the destination is comparable as-is. The
    # filter still belongs here in case more intervals get configured in setup.sh.
    derived_counts = per_bucket(derived, {"term": {"derived_metrics.interval": interval}})

    print()
    print(f"  Documents per {interval} bucket over the last {args.window}")
    print(f"    source   {sparkline(source_counts)}  {summarize(source_counts)}")
    print(f"    derived  {sparkline(derived_counts)}  {summarize(derived_counts)}")
    if source_counts and derived_counts:
        print()
        print(f"    source rate varied by  {spread(source_counts):>6.1f}x")
        print(f"    derived rate varied by {spread(derived_counts):>6.1f}x   <- this is the point")
        idle = sum(1 for c in derived_counts if c == 0)
        if idle:
            print(f"    {idle} interval(s) emitted nothing at all, because nothing was written to them")

    metrics = search(derived, {
        "query": {"bool": {"filter": [
            {"term": {"derived_metrics.interval": interval}},
            window,
        ]}},
        "aggs": {"per_metric": {
            "terms": {"field": "metric.name", "size": 30},
            "aggs": {
                # A counter lands in metric.counter and a gauge in metric.value, because a field
                # carries exactly one time_series_metric type. Whichever one this metric does not
                # use is absent, and sums to zero, so the larger of the two is the metric's value.
                "per_bucket": {
                    "date_histogram": {"field": "@timestamp", "fixed_interval": interval, "min_doc_count": 1},
                    "aggs": {
                        "value": {"sum": {"field": "metric.value"}},
                        "counter": {"sum": {"field": "metric.counter"}},
                    },
                },
                "latest": {"max_bucket": {"buckets_path": "per_bucket>value"}},
                "latest_counter": {"max_bucket": {"buckets_path": "per_bucket>counter"}},
                # A histogram metric carries a distribution rather than a value, so metric.value is
                # absent and summing it yields nothing. Its p99 is the interesting number anyway.
                "p99": {"percentiles": {"field": "metric.histogram", "percents": [99]}},
            },
        }},
    })

    if args.compare_with:
        print()
        print("  Derived documents per bucket for the same input, by configuration")
        for label, stream in (("this stream", source), ("compared with", args.compare_with)):
            counts = per_bucket(f"derived-metrics-{stream}-{interval}")
            average = sum(counts) / len(counts) if counts else 0
            print(f"    {label:<14} {stream:<30} {average:>7.0f} docs per {interval}")
        print()
        print("    Both streams receive identical documents, so the difference is configuration alone.")

    buckets = metrics.get("aggregations", {}).get("per_metric", {}).get("buckets", [])
    print()
    print(f"  Peak value per metric over the last {args.window} ({interval} interval, summed across nodes)")
    if not buckets:
        print("    no derived metrics yet; give it an interval or two")
    for bucket in buckets:
        latest = max(
            bucket.get("latest", {}).get("value") or 0.0,
            bucket.get("latest_counter", {}).get("value") or 0.0,
        )
        p99 = (bucket.get("p99", {}).get("values") or {}).get("99.0")
        if p99 is not None:
            # a distribution, so report the shape rather than a sum that does not exist
            print(f"    {bucket['key']:<24} {p99:>16,.2f}   ({bucket['doc_count']} series-buckets, p99)")
        else:
            print(
                f"    {bucket['key']:<24} {0.0 if latest is None else latest:>16,.2f}"
                f"   ({bucket['doc_count']} series-buckets)"
            )
    print()


if __name__ == "__main__":
    main()
