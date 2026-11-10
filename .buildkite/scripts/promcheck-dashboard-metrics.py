#!/usr/bin/env python3
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""Recover dashboard ownership from the pinned pre-Go export and measure complete queries.

The release lost ownership and some queries during conversion. Keep release cases intact,
restore missing instant/range cases with the same time parameters, and reject any query
that cannot be attributed. A dashboard query is supported only when BOTH variants match
with actual result points; empty answers cannot demonstrate support for recovered queries
whose metrics might be absent from the release's synthetic dataset.
"""

import argparse
from collections import defaultdict
import io
import json
from pathlib import Path
import re
import tarfile


# Defaults for variables absent from the old fixture header. Label values follow that
# fixture's synthetic labels; durations remain durations, not the label value "default".
EXTRA_DEFAULTS = dict.fromkeys(
    "CPU ClientId Database Deployment Disk VHost app dstns dstsvc dstwl env exported_service "
    "hikaricp icmp_job jvm_buffer_pool jvm_memory_pool_heap jvm_memory_pool_nonheap "
    "memory_allocator memory_pool_heap memory_pool_nonheap mode persistentvolumeclaim port protocol".split(),
    "default",
)
EXTRA_DEFAULTS.update(RateInterval="5m", hosts="test:9999", job_insert="promcheck")
VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z_0-9]*)\}|\$([A-Za-z_][A-Za-z_0-9]*)")


def canonical(query):
    """Ignore formatting whitespace outside string literals, preserving quoted values."""
    out, quote, escaped, space = [], None, False, False
    for char in query:
        if quote:
            out.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
        elif char.isspace():
            space = bool(out)
        else:
            if space:
                out.append(" ")
                space = False
            out.append(char)
            if char in "\"'`":
                quote = char
    return "".join(out)


def substitute(query, defaults):
    def replace(match):
        name = match[1] or match[2]
        if name not in defaults:
            raise ValueError(f"No fixture default for ${name}")
        return defaults[name]
    # Numeric $1 back-references are deliberately outside VARIABLE's pattern.
    return canonical(VARIABLE.sub(replace, query))


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def prepare(source, legacy, bundle, output, manifest_path, audit_path, expected_dashboards=100):
    """Join every historical query to its owners and recover all missing release cases."""
    dashboards = json.loads(Path(source).read_text())
    if len(dashboards) != expected_dashboards or len({d["id"] for d in dashboards}) != len(dashboards):
        raise ValueError("Unexpected dashboard count or duplicate dashboard IDs")
    defaults = dict(EXTRA_DEFAULTS)
    for line in Path(legacy).read_text().splitlines():
        if line.startswith("// @set "):
            key, value = line[8:].split(None, 1)
            defaults[key] = value
    expected = set()
    raw_queries = set()
    owners = []
    for dashboard in dashboards:
        raw = sorted(set(dashboard["queries"]))
        if any(not isinstance(query, str) or not query.strip() for query in raw):
            raise ValueError("Empty or invalid dashboard query")
        queries = [substitute(query, defaults) for query in raw]
        # Preserve each distinct original query even if template defaults collapse two
        # expressions. Execute the shared expression once, credit every owning dashboard.
        owners.append({"id": dashboard["id"], "name": dashboard["name"], "queries": queries})
        expected.update(queries)
        raw_queries.update(raw)
    if not expected:
        raise ValueError("Dashboard export has no queries")

    with tarfile.open(bundle, "r:gz") as archive:
        names = archive.getnames()
        if set(names) != {"dataset.csv.gz", "queries.jsonl", "NOTICE"} or len(names) != 3:
            raise ValueError("Unexpected release bundle members")
        members = {name: archive.extractfile(name).read() for name in names}
    cases = [json.loads(line) for line in members["queries.jsonl"].splitlines() if line.strip()]
    seen = set()
    for case in cases:
        key = (canonical(case["query"]), "time" in case)
        if key[0] not in expected or key in seen:
            raise ValueError("Unattributed or duplicate release case")
        seen.add(key)
    release_queries = {query for query, _ in seen}
    recovered = 0
    for query in sorted(expected):
        for instant in (False, True):
            if (query, instant) not in seen:
                params = {"time": 0} if instant else {"start": -3600, "end": 0, "step": 60}
                cases.append({"query": query, **params})
                recovered += 1
    members["queries.jsonl"] = b"".join((json.dumps(case) + "\n").encode() for case in cases)
    with tarfile.open(output, "w:gz") as archive:
        for name in names:
            info = tarfile.TarInfo(name)
            info.size = len(members[name])
            archive.addfile(info, io.BytesIO(members[name]))
    manifest = {"dashboards": owners, "queries": sorted(expected), "cases": len(cases)}
    write_json(manifest_path, manifest)
    write_json(audit_path, {
        "dashboards": len(dashboards), "empty_dashboards": sum(not d["queries"] for d in owners),
        "raw_distinct_queries": len(raw_queries), "normalized_queries": len(expected),
        "release_queries": len(release_queries), "recovered_cases": recovered, "cases": len(cases),
        "unattributed_queries": 0, "missing_queries": 0,
    })


def report(manifest_path, log_path, output, details_path):
    """Return cumulative thresholds over all source dashboards, counting each dashboard once."""
    manifest = json.loads(Path(manifest_path).read_text())
    expected = {(query, instant) for query in manifest["queries"] for instant in (False, True)}
    results, summaries = {}, []
    with Path(log_path).open() as stream:
        for line in stream:
            row = json.loads(line)
            if row["type"] == "summary":
                summaries.append(row)
            elif row["type"] == "query":
                if not isinstance(row["instant"], bool) or row["status"] not in {"match", "mismatch", "error", "skipped"}:
                    raise ValueError("Invalid query verdict")
                key = (canonical(row["query"]), row["instant"])
                if key not in expected or key in results:
                    raise ValueError("Unexpected or duplicate query verdict")
                points = row["points"]
                if type(points) is not int or points < 0:
                    raise ValueError("Invalid result point count")
                results[key] = row
    if results.keys() != expected or len(summaries) != 1:
        raise ValueError("Missing query verdicts or summary; refusing partial dashboard coverage")
    summary = summaries[0]
    if (summary["total"] != len(expected) or summary["inspected"] != len(expected)
            or summary["match"] != sum(row["status"] == "match" for row in results.values())):
        raise ValueError("Query verdicts disagree with the summary")
    supported = {
        query for query in manifest["queries"]
        if all(results[query, instant]["status"] == "match" and results[query, instant]["points"] > 0
               for instant in (False, True))
    }
    details = []
    for dashboard in manifest["dashboards"]:
        queries = dashboard["queries"]
        success = sum(query in supported for query in queries)
        details.append({"id": dashboard["id"], "name": dashboard["name"], "query_total": len(queries),
                        "query_supported": success,
                        "support_pct": 100 * success / len(queries) if queries else None})
    total = len(details)
    empty = sum(d["query_total"] == 0 for d in details)
    buckets = []
    for threshold in range(0, 101, 10):
        # Compare integer counts before rounding: 99.99% must never enter the 100% bucket.
        count = sum(d["query_total"] > 0 and 100 * d["query_supported"] >= threshold * d["query_total"] for d in details)
        buckets.append({"threshold_pct": threshold, "dashboard_count": count,
                        "dashboard_pct": 100 * count / total, "dashboard_total": total,
                        "empty_dashboards": empty})
    write_json(details_path, details)
    write_json(output, buckets)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    for name in ("source", "legacy", "bundle", "output", "manifest", "audit"):
        prep.add_argument("--" + name, required=True)
    rep = commands.add_parser("report")
    for name in ("manifest", "log", "output", "details"):
        rep.add_argument("--" + name, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.source, args.legacy, args.bundle, args.output, args.manifest, args.audit)
    else:
        report(args.manifest, args.log, args.output, args.details)


if __name__ == "__main__":
    main()
