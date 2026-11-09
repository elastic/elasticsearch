"""Checks the Parquet-backed index against the ES|QL Parquet reader, query by query.

The mounted index (hits) is read through the doc values format of this plugin. The oracle (hits_ds) is the same file read through an
ES|QL data source, which uses the ES|QL Parquet reader and no doc values format, so a difference is a bug in this plugin's iterators,
skippers, conversions or mount, or in the way ES|QL reads them. It cannot find a bug of the ES|QL reader itself, because both sides use
its decoding.

Results are compared in order with a small tolerance for floating point sums. A SORT with a LIMIT may legitimately return other rows among
ties, so then the sort keys must be equal in order and every row of ours must be a row of the oracle (a group that is part of a tie group
too long to fetch is checked on its own, with a query for that group). A LIMIT without a SORT picks any groups, so each of our groups is
checked with a query for it.

Setup and usage: see the README of the plugin ("Checking the results against an ES|QL data source").

  compare_with_data_source.py queries.json [name-prefix ...]

queries.json is a list of {"name", "query"}, and the queries start with "FROM hits |".
"""
import json
import re
import sys
import time
import urllib.request

ES = "http://localhost:9200"
OURS = "hits"
ORACLE = "hits_ds"
# the mount converts the time columns of the file (bare integers) to dates; the data source has to do the same in the query
ORACLE_PREFIX = (
    "FROM hits_ds | EVAL tmp_ed = TO_DATETIME(TO_LONG(EventDate) * 86400000), tmp_et = TO_DATETIME(TO_LONG(EventTime) * 1000), "
    "tmp_cet = TO_DATETIME(TO_LONG(ClientEventTime) * 1000), tmp_let = TO_DATETIME(TO_LONG(LocalEventTime) * 1000) "
    "| DROP EventDate, EventTime, ClientEventTime, LocalEventTime "
    "| RENAME tmp_ed AS EventDate, tmp_et AS EventTime, tmp_cet AS ClientEventTime, tmp_let AS LocalEventTime |"
)
# Why not EVAL EventDate = ...: the federated aggregate pushdown then answers MIN and MAX from the footer statistics of the raw column
# under the shadowed name, and returns the raw value as a date.
TIMEOUT = 1800
TIE_LIMIT = 10000


def esql(query):
    body = json.dumps({"query": query}).encode()
    req = urllib.request.Request(ES + "/_query", data=body, headers={"Content-Type": "application/json"})
    start = time.time()
    with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
        result = json.load(response)
    names = [c["name"] for c in result["columns"]]
    rows = [dict(zip(names, row)) for row in result["values"]]
    return names, rows, time.time() - start


def oracle_query(query):
    assert query.startswith("FROM hits |"), query
    return ORACLE_PREFIX + query[len("FROM hits |"):]


def norm(value):
    """A value in a form that two engines agree on: numbers as floats with 9 significant digits, lists element by element."""
    if isinstance(value, list):
        return tuple(norm(v) for v in value)
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    return float("%.9g" % value)


def same_value(a, b):
    if isinstance(a, list) or isinstance(b, list):
        a = a if isinstance(a, list) else [a]
        b = b if isinstance(b, list) else [b]
        return len(a) == len(b) and all(same_value(x, y) for x, y in zip(a, b))
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)
    return a == b


def same_row(a, b, columns=None):
    columns = columns if columns is not None else list(a.keys())
    return all(c in b and same_value(a[c], b[c]) for c in columns)


def row_key(row, columns):
    return tuple(norm(row.get(c)) for c in columns)


def sort_columns(query):
    """The columns of the last SORT before the last LIMIT, with their direction, or None when the query has no SORT."""
    matches = re.findall(r"\|\s*SORT\s+(.*?)(?=\s*\||$)", query)
    if not matches:
        return None
    columns = []
    for item in matches[-1].split(","):
        parts = item.split()
        columns.append((parts[0], not (len(parts) > 1 and parts[1].upper() == "DESC")))
    return columns


def last_limit(query):
    matches = re.findall(r"\|\s*LIMIT\s+(\d+)\s*$", query.strip())
    return int(matches[-1]) if matches else None


def exact(names_a, rows_a, rows_b):
    return len(rows_a) == len(rows_b) and all(same_row(a, b, names_a) for a, b in zip(rows_a, rows_b))


def unordered_same(rows_a, rows_b, columns):
    return len(rows_a) == len(rows_b) and sorted(map(repr, (row_key(r, columns) for r in rows_a))) == sorted(
        map(repr, (row_key(r, columns) for r in rows_b))
    )


def strip_keep_and_limit(query, limit):
    query = re.sub(r"\|\s*KEEP\s+[^|]*", "", query)
    return re.sub(r"\|\s*LIMIT\s+\d+\s*$", "| LIMIT %d" % limit, query.strip())


def literal(value):
    if value is None:
        return None
    if isinstance(value, str):
        escaped = value.replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))
        escaped = escaped.replace(chr(10), chr(92) + "n").replace(chr(13), chr(92) + "r").replace(chr(9), chr(92) + "t")
        return chr(34) + escaped + chr(34)
    return str(value)



def point_check(query, row):
    """Recomputes the group of one row of ours on the oracle: the same query up to its STATS, limited to the group by its keys."""
    head, _, rest = query.partition("| STATS")
    stats = "| STATS" + re.split(r"\s*\|\s*(?:SORT|LIMIT|WHERE)", rest)[0]
    by = [c.strip() for c in re.search(r"BY\s+(.*)$", stats).group(1).split(",")]
    conditions = []
    for c in by:
        lit = literal(row[c])
        conditions.append("%s IS NULL" % c if lit is None else "%s == %s" % (c, lit))
    check = oracle_query(head.rstrip() + " | WHERE " + " AND ".join(conditions) + " " + stats)
    _, rows, _ = esql(check)
    if len(rows) != 1:
        return "the oracle has %d rows for the group %s" % (len(rows), {c: row[c] for c in by})
    columns = [c for c in row.keys() if c in rows[0]]
    if not same_row(row, rows[0], columns):
        return "group %s: ours %s, oracle %s" % ({c: row[c] for c in by}, json.dumps(row)[:150], json.dumps(rows[0])[:150])
    return None


def check_unsorted_limit(name, query, ours_names, ours_rows):
    """A LIMIT without a SORT picks any groups. Each group that we return must have exactly the values that the oracle computes."""
    for row in ours_rows:
        problem = point_check(query, row)
        if problem:
            return problem
    return None


def compare(name, query):
    ours_names, ours_rows, ours_time = esql(query)
    _, oracle_rows, oracle_time = esql(oracle_query(query))
    timing = "ours %6.1fs oracle %6.1fs" % (ours_time, oracle_time)
    if exact(ours_names, ours_rows, oracle_rows):
        return "PASS", timing, "%d rows, equal" % len(ours_rows)
    keys = sort_columns(query)
    limit = last_limit(query)
    if keys is None and limit is not None:
        problem = check_unsorted_limit(name, query, ours_names, ours_rows)
        count_ok = len(ours_rows) == len(oracle_rows)
        if problem is None and count_ok:
            return "PASS", timing, "%d rows, a LIMIT without SORT: every group has the oracle's count" % len(ours_rows)
        return "FAIL", timing, problem or "row counts differ: %d vs %d" % (len(ours_rows), len(oracle_rows))
    if keys is None:
        columns = ours_names
        if unordered_same(ours_rows, oracle_rows, columns):
            return "PASS", timing, "%d rows, equal as a set" % len(ours_rows)
        return "FAIL", timing, "no SORT and the rows differ (%d vs %d rows)" % (len(ours_rows), len(oracle_rows))
    # a SORT: the rows may differ among ties, so compare the sort keys in order and check each row of ours against the oracle's rows
    key_names = [k[0] for k in keys]
    if limit is None:
        full_ours, full_oracle = ours_rows, oracle_rows
        full_names = ours_names
    else:
        expanded_limit = max(TIE_LIMIT, limit)
        names2, full_ours, _ = esql(strip_keep_and_limit(query, limit))
        _, full_oracle, _ = esql(oracle_query(strip_keep_and_limit(query, expanded_limit)))
        full_names = names2
    if len(full_ours) != (len(full_oracle) if limit is None else min(limit, len(full_oracle))):
        return "FAIL", timing, "row counts differ: %d vs %d" % (len(full_ours), len(full_oracle))
    ours_keys = [row_key(r, key_names) for r in full_ours]
    oracle_keys = [row_key(r, key_names) for r in full_oracle[: len(full_ours)]]
    if ours_keys != oracle_keys:
        first = next(i for i, (a, b) in enumerate(zip(ours_keys, oracle_keys)) if a != b)
        return "FAIL", timing, "sort keys differ at row %d: ours %s, oracle %s" % (first, ours_keys[first], oracle_keys[first])
    oracle_index = {}
    for r in full_oracle:
        oracle_index.setdefault(row_key(r, full_names), 0)
        oracle_index[row_key(r, full_names)] += 1
    missing = [r for r in full_ours if row_key(r, full_names) not in oracle_index]
    if missing:
        if "| STATS" in query and re.search(r"BY", query):
            # the tie group at the end of the limit is longer than the rows that we fetched from the oracle: check each row by itself
            for row in missing:
                problem = point_check(query, row)
                if problem:
                    return "FAIL", timing, problem
            return "PASS", timing, "%d rows, sort keys equal; %d rows of a long tie group verified one by one" % (len(full_ours), len(missing))
        return "FAIL", timing, "a row of ours is not in the oracle's rows: %s" % json.dumps(missing[0])[:200]
    return "PASS", timing, "%d rows, equal up to the order of ties" % len(full_ours)


def main():
    queries = json.load(open(sys.argv[1]))
    prefixes = sys.argv[2:]
    selected = [q for q in queries if not prefixes or any(q["name"].startswith(p) for p in prefixes)]
    # a result can have more rows than the default truncation
    req = urllib.request.Request(
        ES + "/_cluster/settings",
        data=json.dumps({"persistent": {"esql.query.result_truncation_max_size": 20000}}).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    urllib.request.urlopen(req).read()
    counts = {"PASS": 0, "FAIL": 0, "ERROR": 0}
    for q in selected:
        try:
            status, timing, detail = compare(q["name"], q["query"])
        except Exception as e:  # noqa: BLE001
            status, timing, detail = "ERROR", "", str(e)[:300]
        counts[status] += 1
        print("%-5s %-38s %s  %s" % (status, q["name"][:38], timing, detail), flush=True)
    print("summary:", counts, flush=True)


main()
