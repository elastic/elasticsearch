"""Writes the expected results of the 43 ES|QL queries of the internal ClickBench Rally track (rally-internal-tracks,
clickbench/operations/02_all_queries.json) from DuckDB, for ParquetClickBenchManualIT#testRallyTrackQueriesGiveWhatDuckDbComputes.
The queries run unchanged, so their sort keys have ties: for every query the file holds the top rows in DuckDB's order, the sort keys,
and the rows that tie with the last row, so that the check (ClickBenchResultCheck) accepts every valid choice among the ties.
The SQL of every query is ours. The ES|QL comes from the Rally file, parsed by parse_rally_queries.py.
Usage: internal_expected.py hits.parquet rally_queries.json out.json [name-prefix ...]
With name prefixes only those queries are written, and out.json is overwritten."""
import decimal, json, sys, time, duckdb

hits, queries, out = sys.argv[1], sys.argv[2], sys.argv[3]
only = sys.argv[4:]
internal = {q["name"]: q["query"] for q in json.load(open(queries))}
TIES_CAP = 20000

def TS(e): return f"strftime(make_timestamp(({e})::BIGINT * 1000000), '%Y-%m-%dT%H:%M:%S.000Z')"
def DS(e): return f"strftime(make_timestamp(({e})::BIGINT * 86400 * 1000000), '%Y-%m-%dT%H:%M:%S.000Z')"
JUL = "CounterID = 62 AND EventDate BETWEEN 15887 AND 15917"

S = []
def spec(name, sql, sort=(), limit=1, mode="ties", tol=1e-6, group=None, outcols=None, k=0):
    S.append(dict(name=name, sql=sql, sort=list(sort), limit=limit, mode=mode, tol=tol, group=group, outcols=outcols, k=k))

spec("q0_count_hits", "SELECT count(*) count FROM hits")
spec("q1_count_advengineid_nonzero", "SELECT count(*) count FROM hits WHERE AdvEngineID <> 0")
spec("q2_stats_advengineid_resolution", "SELECT sum(AdvEngineID) sum_AdvEngineID, count(*) count, avg(ResolutionWidth) avg_ResolutionWidth FROM hits")
spec("q3_stats_avg_userid", "SELECT avg(UserID) avg_UserID FROM hits")
spec("q4_stats_distinct_userid", "SELECT count(DISTINCT UserID) u FROM hits", tol=0.05)
spec("q5_stats_distinct_searchphrase", "SELECT count(DISTINCT SearchPhrase) u FROM hits", tol=0.05)
spec("q6_stats_min_max_eventdate", f"SELECT {DS('min(EventDate)')} min_EventDate, {DS('max(EventDate)')} max_EventDate FROM hits")
spec("q7_stats_advengineid_by_advengineid", "SELECT count(*) c, AdvEngineID FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID", [("c", "desc")], 1000)
spec("q8_stats_distinct_userid_by_region", "SELECT count(DISTINCT UserID) u, RegionID FROM hits GROUP BY RegionID", [("u", "desc")], 10, "approx_rank", 0.05, ["RegionID"], k=100)
spec("q9_stats_region_aggregates", "SELECT sum(AdvEngineID) sum_AdvEngineID, count(*) c, avg(ResolutionWidth) avg_ResolutionWidth, count(DISTINCT UserID) count_distinct_UserID, RegionID FROM hits GROUP BY RegionID", [("c", "desc")], 10, tol=[0, 0, 1e-6, 0.05, 0])
spec("q10_stats_mobile_phone_model_users", "SELECT count(DISTINCT UserID) u, MobilePhoneModel FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel", [("u", "desc")], 10, "approx_rank", 0.05, ["MobilePhoneModel"], k=100)
spec("q11_stats_mobile_phone_users", "SELECT count(DISTINCT UserID) u, MobilePhone, MobilePhoneModel FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel", [("u", "desc")], 10, "approx_rank", 0.05, ["MobilePhone", "MobilePhoneModel"], k=100)
spec("q12_stats_searchphrase_count", "SELECT count(*) c, SearchPhrase FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase", [("c", "desc")], 10)
spec("q13_stats_searchphrase_users", "SELECT count(DISTINCT UserID) u, SearchPhrase FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase", [("u", "desc")], 10, "approx_rank", 0.2, ["SearchPhrase"], k=300)
spec("q14_stats_search_engine_phrase", "SELECT count(*) c, SearchEngineID, SearchPhrase FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase", [("c", "desc")], 10)
spec("q15_stats_userid_count", "SELECT count(*) c, UserID FROM hits GROUP BY UserID", [("c", "desc")], 10)
spec("q16_stats_userid_searchphrase_count", "SELECT count(*) c, UserID, SearchPhrase FROM hits GROUP BY UserID, SearchPhrase", [("c", "desc")], 10)
spec("q17_stats_userid_searchphrase_limit", "SELECT count(*) c, UserID, SearchPhrase FROM hits GROUP BY UserID, SearchPhrase", [], 10, "count_only")
spec("q18_stats_userid_minute_searchphrase", "SELECT count(*) c, UserID, (EventTime // 60) % 60 m, SearchPhrase FROM hits GROUP BY UserID, m, SearchPhrase", [("c", "desc")], 10)
spec("q19_filter_userid_435090932899640449", "SELECT UserID FROM hits WHERE UserID = 435090932899640449", [], 1000)
spec("q20_stats_google_url_count", "SELECT count(*) count FROM hits WHERE URL LIKE '%google%'")
spec("q21_stats_google_url_searchphrase", "SELECT min(URL) min_URL, count(*) c, SearchPhrase FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase", [("c", "desc")], 10)
spec("q22_stats_google_title_not_url", "SELECT min(URL) min_URL, min(Title) min_Title, count(*) c, count(DISTINCT UserID) count_distinct_UserID, SearchPhrase FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase", [("c", "desc")], 10, tol=[0, 0, 0, 0.05, 0])
cols = [r[0] for r in duckdb.connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{hits}')").fetchall()]
def sel(c):
    if c in ("EventTime", "ClientEventTime", "LocalEventTime"): return f"{TS(c)} AS {c}"
    if c == "EventDate": return f"{DS(c)} AS {c}"
    return c
spec("q23_sort_google_url_by_time", "SELECT " + ", ".join(sel(c) for c in cols) + " FROM hits WHERE URL LIKE '%google%'", [("EventTime", "asc")], 10)
spec("q24_sort_searchphrase_by_time", f"SELECT SearchPhrase, {TS('EventTime')} EventTime FROM hits WHERE SearchPhrase <> ''", [("EventTime", "asc")], 10, outcols=["SearchPhrase"])
spec("q25_sort_searchphrase_asc", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> ''", [("SearchPhrase", "asc")], 10)
spec("q26_sort_searchphrase_time_asc", f"SELECT SearchPhrase, {TS('EventTime')} EventTime FROM hits WHERE SearchPhrase <> ''", [("EventTime", "asc"), ("SearchPhrase", "asc")], 10, outcols=["SearchPhrase"])
spec("q27_stats_url_length_by_counter", "SELECT avg(strlen(URL)) l, count(*) c, CounterID FROM hits WHERE URL <> '' GROUP BY CounterID HAVING count(*) > 100000", [("l", "desc")], 25)
spec("q28_stats_referer_domain_analysis", r"SELECT avg(strlen(Referer)) l, count(*) c, min(Referer) min_Referer, regexp_replace(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') k FROM hits WHERE Referer <> '' GROUP BY k HAVING count(*) > 100000", [("l", "desc")], 25, tol=1e-5)
spec("q29_stats_resolution_width_sums", "SELECT " + ", ".join(f"sum(ResolutionWidth + {k}) sum_{k}" for k in range(90)) + " FROM hits")
spec("q30_stats_search_engine_clientip", "SELECT count(*) c, sum(IsRefresh) sum_IsRefresh, avg(ResolutionWidth) avg_ResolutionWidth, SearchEngineID, ClientIP FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP", [("c", "desc")], 10)
spec("q31_stats_watchid_clientip", "SELECT count(*) c, sum(IsRefresh) sum_IsRefresh, avg(ResolutionWidth) avg_ResolutionWidth, WatchID, ClientIP FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP", [("c", "desc")], 10)
spec("q32_stats_watchid_clientip_all", "SELECT count(*) c, sum(IsRefresh) sum_IsRefresh, avg(ResolutionWidth) avg_ResolutionWidth, WatchID, ClientIP FROM hits GROUP BY WatchID, ClientIP", [("c", "desc")], 10)
spec("q33_stats_url_count", "SELECT count(*) c, URL FROM hits GROUP BY URL", [("c", "desc")], 10)
spec("q34_stats_const_url_count", "SELECT count(*) c, 1 const1, URL FROM hits GROUP BY 2, URL", [("c", "desc")], 10)
spec("q35_stats_clientip_variations", "SELECT count(*) c, ClientIP, ClientIP - 1 ip_m1, ClientIP - 2 ip_m2, ClientIP - 3 ip_m3 FROM hits GROUP BY ClientIP, ip_m1, ip_m2, ip_m3", [("c", "desc")], 10)
spec("q36_stats_counter62_july_urls", f"SELECT count(*) PageViews, URL FROM hits WHERE {JUL} AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL", [("PageViews", "desc")], 10)
spec("q37_stats_counter62_july_titles", f"SELECT count(*) PageViews, Title FROM hits WHERE {JUL} AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title", [("PageViews", "desc")], 10)
spec("q38_stats_counter62_july_links", f"SELECT count(*) PageViews, URL FROM hits WHERE {JUL} AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL", [("PageViews", "desc")], 1010)
spec("q39_stats_counter62_july_traffic_sources", f"SELECT count(*) PageViews, TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END Src, URL Dst FROM hits WHERE {JUL} AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst", [("PageViews", "desc")], 1010)
spec("q40_stats_counter62_july_referer_hash", f"SELECT count(*) PageViews, URLHash, {DS('EventDate')} EventDate FROM hits WHERE {JUL} AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate", [("PageViews", "desc")], 110)
spec("q41_stats_counter62_july_window_client", f"SELECT count(*) PageViews, WindowClientWidth, WindowClientHeight FROM hits WHERE {JUL} AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight", [("PageViews", "desc")], 10010)
spec("q42_stats_counter62_july14_15_minutes", f"SELECT count(*) PageViews, strftime(make_timestamp((EventTime // 60 * 60) * 1000000), '%Y-%m-%dT%H:%M:00.000Z') M FROM hits WHERE CounterID = 62 AND EventDate BETWEEN 15900 AND 15901 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY M", [("M", "asc")], 1010)

def norm(v):
    if isinstance(v, decimal.Decimal): return int(v) if v == v.to_integral_value() else float(v)
    return v

con = duckdb.connect()
con.execute(f"CREATE VIEW hits AS SELECT * FROM read_parquet('{hits}')")
result = []
for s in S:
    if only and not any(s["name"].startswith(p) for p in only): continue
    t = time.time()
    cur = con.execute(f"SELECT * FROM ({s['sql']}) LIMIT 0")
    names = [d[0] for d in cur.description]
    order = ", ".join(f'"{c}" {d}' for c, d in s["sort"]) + ("," if s["sort"] else "") + ", ".join(f'"{c}"' for c in names)
    n = s["k"] if s["mode"] == "approx_rank" else s["limit"]
    rows = [[norm(v) for v in r] for r in con.execute(f"SELECT * FROM ({s['sql']}) ORDER BY {order} LIMIT {n}").fetchall()]
    ties, ties_complete = [], True
    if s["sort"] and rows and s["mode"] == "ties":
        cut = [rows[-1][names.index(c)] for c, _ in s["sort"]]
        cond = " AND ".join(f'"{c}" = ?' for c, _ in s["sort"])
        ties = [[norm(v) for v in r] for r in con.execute(f"SELECT * FROM ({s['sql']}) WHERE {cond} LIMIT {TIES_CAP + 1}", cut).fetchall()]
        if len(ties) > TIES_CAP:
            ties, ties_complete = [], False
    tol = s["tol"]
    if not isinstance(tol, list): tol = [tol] * len(names)
    print(f"{s['name']}: {len(rows)} rows, {len(ties)} ties{'' if ties_complete else ' (too many)'} in {time.time()-t:.1f}s", flush=True)
    result.append(dict(name=s["name"], esql=internal[s["name"]], cols=names, outcols=s["outcols"] or names, sort=s["sort"],
                       limit=s["limit"], mode=s["mode"], tolerance=tol, group=s["group"], rows=rows, ties=ties, tiesComplete=ties_complete))
json.dump(result, open(out, "w"))
