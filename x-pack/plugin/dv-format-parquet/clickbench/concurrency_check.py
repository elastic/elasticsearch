"""Runs a mix of queries from several clients at once against the mounted Parquet index (hits) and checks that every result equals the
result of a single client, that nothing fails, that the heap does not grow from round to round, and that no task is left behind.

The mix is ES|QL queries of the Rally track (queries.json, see compare_with_data_source.py), a few Query DSL searches, and gets of
single documents. It needs the process id of the node, to force a collection with `jcmd <pid> GC.run` between rounds.

  concurrency_check.py queries.json <node pid> [clients] [rounds]
"""
import concurrent.futures
import json
import random
import subprocess
import sys
import time
import urllib.request

ES = "http://localhost:9200"
# Deterministic queries of the track, without a long run time. A query whose LIMIT cuts a tie group (q39, q40) gives other rows on
# each run, even for one client, so it cannot be compared with a baseline.
NAMES = ["q0", "q1", "q2", "q3", "q4", "q6", "q7", "q8", "q12", "q15", "q19", "q25", "q26", "q29", "q36", "q37", "q42"]
GET_ROWS = [5, 123456, 99997496, 50000000]


def request(method, path, body=None, timeout=900):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ES + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def norm(value):
    if isinstance(value, list):
        return tuple(norm(v) for v in value)
    if isinstance(value, float):
        return float("%.9g" % value)
    return value


def tasks(queries):
    work = []
    for q in queries:
        if q["name"].split("_")[0] in NAMES:
            work.append((q["name"], lambda q=q: [norm(r) for r in request("POST", "/_query", {"query": q["query"]})["values"]]))
    work.append(("dsl range count", lambda: request("POST", "/hits/_search?request_cache=false", {"size": 0, "track_total_hits": True,
        "query": {"range": {"EventTime": {"gte": "2013-07-14T00:00:00Z", "lt": "2013-07-15T00:00:00Z"}}}})["hits"]["total"]["value"]))
    work.append(("dsl sort", lambda: [h["fields"]["EventTime"] for h in request("POST", "/hits/_search?request_cache=false", {"size": 2,
        "_source": False, "docvalue_fields": ["EventTime"], "sort": [{"EventTime": "desc"}]})["hits"]["hits"]]))
    work.append(("dsl terms", lambda: request("POST", "/hits/_search?request_cache=false", {"size": 0,
        "query": {"range": {"CounterID": {"gte": 0, "lte": 100}}}, "aggs": {"e": {"terms": {"field": "SearchEngineID", "size": 3}}}})
        ["aggregations"]["e"]["buckets"]))
    for row in GET_ROWS:
        work.append((f"get {row}", lambda row=row: request("GET", f"/hits/_doc/{row}?_source_includes=UserID,EventTime,URL")["_source"]))
    return work


def heap_after_gc(pid):
    subprocess.run(["jcmd", str(pid), "GC.run"], check=False, capture_output=True)
    time.sleep(1)
    stats = request("GET", "/_nodes/stats/jvm,breaker")
    node = next(iter(stats["nodes"].values()))
    return node["jvm"]["mem"]["heap_used_in_bytes"], node["breakers"]["request"]["estimated_size_in_bytes"]


def main():
    queries = json.load(open(sys.argv[1]))
    pid = int(sys.argv[2])
    clients = int(sys.argv[3]) if len(sys.argv) > 3 else 6
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 4
    work = tasks(queries)
    print(f"{len(work)} tasks, {clients} clients, {rounds} rounds", flush=True)
    baseline = {}
    start = time.time()
    for name, fn in work:
        baseline[name] = fn()
    print(f"baseline of one client: {time.time() - start:.0f} s", flush=True)
    heaps = []
    problems = []
    for rnd in range(rounds):
        def client(seed):
            rng = random.Random(seed)
            order = list(work)
            rng.shuffle(order)
            bad = []
            for name, fn in order:
                try:
                    result = fn()
                except Exception as e:  # noqa: BLE001
                    bad.append(f"{name}: {str(e)[:120]}")
                    continue
                if result != baseline[name]:
                    bad.append(f"{name}: result differs from the baseline")
            return bad
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(clients) as pool:
            results = list(pool.map(client, [rnd * 100 + c for c in range(clients)]))
        round_problems = [p for r in results for p in r]
        problems.extend(round_problems)
        heap, breaker = heap_after_gc(pid)
        heaps.append(heap)
        print(f"round {rnd + 1}: {time.time() - start:5.0f} s, problems {len(round_problems)}, heap after gc {heap / 1e6:7.0f} MB, "
              f"request breaker {breaker} B", flush=True)
    running = request("GET", "/_tasks?actions=*esql*").get("nodes", {})
    left = sum(len(n.get("tasks", {})) for n in running.values())
    print(f"tasks left: {left}", flush=True)
    for p in problems[:10]:
        print("PROBLEM", p)
    growth = heaps[-1] - heaps[0]
    print(f"heap after gc: first round {heaps[0] / 1e6:.0f} MB, last round {heaps[-1] / 1e6:.0f} MB, growth {growth / 1e6:.0f} MB")
    print("RESULT:", "ok" if not problems and left == 0 else "FAILED")


main()
