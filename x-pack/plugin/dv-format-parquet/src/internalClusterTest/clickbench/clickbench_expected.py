"""Writes the ClickBench-style ES|QL queries that we wrote ourselves (not the Rally ones) with the rows that DuckDB computes from the
same hits.parquet, for ParquetClickBenchManualIT#testQueriesFromFileGiveWhatDuckDbComputes.
Usage: clickbench_expected.py hits.parquet out.json [name-prefix ...]"""
import json, sys, time, duckdb

hits, out = sys.argv[1], sys.argv[2]
only = sys.argv[3:]
JUL = 'EventDate >= "2013-07-01T00:00:00Z" AND EventDate <= "2013-07-31T00:00:00Z"'
Q = []  # (name, esql, sql, tolerance)
def q(name, esql, sql, tol=1e-6, offset=0): Q.append((name, esql, sql, tol, offset))

q("q04_avg_userid", "FROM hits | STATS a = AVG(UserID)", "SELECT avg(UserID) FROM hits")
q("q05_distinct_userid", "FROM hits | STATS c = COUNT_DISTINCT(UserID)", "SELECT count(DISTINCT UserID) FROM hits", 0.05)
q("q06_distinct_phrase", "FROM hits | STATS c = COUNT_DISTINCT(SearchPhrase)", "SELECT count(DISTINCT SearchPhrase) FROM hits", 0.05)
q("q15_engine_phrase", 'FROM hits | WHERE SearchPhrase != "" | STATS c = COUNT(*) BY SearchEngineID, SearchPhrase | SORT c DESC, SearchEngineID, SearchPhrase | LIMIT 10 | KEEP SearchEngineID, SearchPhrase, c',
  "SELECT SearchEngineID, SearchPhrase, count(*) c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC, SearchEngineID, SearchPhrase LIMIT 10")
q("q16_userid_count", "FROM hits | STATS c = COUNT(*) BY UserID | SORT c DESC, UserID | LIMIT 10 | KEEP UserID, c",
  "SELECT UserID, count(*) c FROM hits GROUP BY UserID ORDER BY c DESC, UserID LIMIT 10")
q("q20_point_lookup", "FROM hits | WHERE UserID == 435090932899640449 | KEEP UserID", "SELECT UserID FROM hits WHERE UserID = 435090932899640449")
q("q22_google_phrase", 'FROM hits | WHERE URL LIKE "*google*" AND SearchPhrase != "" | STATS u = MIN(URL), c = COUNT(*) BY SearchPhrase | SORT c DESC, SearchPhrase | LIMIT 10 | KEEP SearchPhrase, u, c',
  "SELECT SearchPhrase, min(URL) u, count(*) c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase LIMIT 10")
q("q27_phrase_by_time", 'FROM hits | WHERE SearchPhrase != "" | SORT @timestamp, SearchPhrase | LIMIT 10 | KEEP SearchPhrase',
  "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10")
q("q26_phrase_sorted", 'FROM hits | WHERE SearchPhrase != "" | SORT SearchPhrase | LIMIT 10 | KEEP SearchPhrase',
  "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10")
q("q28_url_length", 'FROM hits | WHERE URL != "" | EVAL len = LENGTH(URL) | STATS l = AVG(len), c = COUNT(*) BY CounterID | WHERE c > 100000 | SORT l DESC, CounterID | LIMIT 25 | KEEP CounterID, l, c',
  "SELECT CounterID, avg(length(URL)) l, count(*) c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC, CounterID LIMIT 25")
q("q30_sums", "FROM hits | STATS " + ", ".join(f"s{k} = SUM(ResolutionWidth + {k})" for k in range(90)),
  "SELECT " + ", ".join(f"sum(ResolutionWidth + {k})" for k in range(90)) + " FROM hits")
q("q31_engine_ip", 'FROM hits | WHERE SearchPhrase != "" | STATS c = COUNT(*), r = SUM(IsRefresh), w = AVG(ResolutionWidth) BY SearchEngineID, ClientIP | SORT c DESC, SearchEngineID, ClientIP | LIMIT 10 | KEEP SearchEngineID, ClientIP, c, r, w',
  "SELECT SearchEngineID, ClientIP, count(*) c, sum(IsRefresh) r, avg(ResolutionWidth) w FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC, SearchEngineID, ClientIP LIMIT 10")
q("q37_urls_counter62", f'FROM hits | WHERE CounterID == 62 AND {JUL} AND DontCountHits == 0 AND IsRefresh == 0 AND URL != "" | STATS pv = COUNT(*) BY URL | SORT pv DESC, URL | LIMIT 10 | KEEP URL, pv',
  "SELECT URL, count(*) pv FROM hits WHERE CounterID = 62 AND EventDate BETWEEN 15887 AND 15917 AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY pv DESC, URL LIMIT 10")
q("q38_titles_counter62", f'FROM hits | WHERE CounterID == 62 AND {JUL} AND DontCountHits == 0 AND IsRefresh == 0 AND Title != "" | STATS pv = COUNT(*) BY Title | SORT pv DESC, Title | LIMIT 10 | KEEP Title, pv',
  "SELECT Title, count(*) pv FROM hits WHERE CounterID = 62 AND EventDate BETWEEN 15887 AND 15917 AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY pv DESC, Title LIMIT 10")
q("q43_minutes", 'FROM hits | WHERE CounterID == 62 AND EventDate >= "2013-07-14T00:00:00Z" AND EventDate <= "2013-07-15T00:00:00Z" AND IsRefresh == 0 AND DontCountHits == 0 | STATS pv = COUNT(*) BY m = DATE_TRUNC(1 minute, @timestamp) | SORT m | LIMIT 10 | EVAL m = TO_STRING(m) | KEEP m, pv',
  "SELECT strftime(make_timestamp((EventTime // 60 * 60) * 1000000), '%Y-%m-%dT%H:%M:00.000Z') m, count(*) pv FROM hits WHERE CounterID = 62 AND EventDate BETWEEN 15900 AND 15901 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY m ORDER BY m LIMIT 10")


JUL_SQL = "CounterID = 62 AND EventDate BETWEEN 15887 AND 15917"
# exact distinct counts: first one group for every (key, UserID), then a count of those groups
q("q09_region_users", "FROM hits | STATS c = COUNT(*) BY RegionID, UserID | STATS u = COUNT(*) BY RegionID | SORT u DESC, RegionID | LIMIT 10 | KEEP RegionID, u",
  "SELECT RegionID, count(DISTINCT UserID) u FROM hits GROUP BY RegionID ORDER BY u DESC, RegionID LIMIT 10")
q("q10_region_stats", "FROM hits | STATS s = SUM(AdvEngineID), c = COUNT(*), w = AVG(ResolutionWidth), u = COUNT_DISTINCT(UserID) BY RegionID | SORT c DESC, RegionID | LIMIT 10 | KEEP RegionID, s, c, w, u",
  "SELECT RegionID, sum(AdvEngineID) s, count(*) c, avg(ResolutionWidth) w, count(DISTINCT UserID) u FROM hits GROUP BY RegionID ORDER BY c DESC, RegionID LIMIT 10", [0, 0, 0, 1e-6, 0.05])
q("q11_phone_model_users", 'FROM hits | WHERE MobilePhoneModel != "" | STATS c = COUNT(*) BY MobilePhoneModel, UserID | STATS u = COUNT(*) BY MobilePhoneModel | SORT u DESC, MobilePhoneModel | LIMIT 10 | KEEP MobilePhoneModel, u',
  "SELECT MobilePhoneModel, count(DISTINCT UserID) u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC, MobilePhoneModel LIMIT 10")
q("q12_phone_users", 'FROM hits | WHERE MobilePhoneModel != "" | STATS c = COUNT(*) BY MobilePhone, MobilePhoneModel, UserID | STATS u = COUNT(*) BY MobilePhone, MobilePhoneModel | SORT u DESC, MobilePhone, MobilePhoneModel | LIMIT 10 | KEEP MobilePhone, MobilePhoneModel, u',
  "SELECT MobilePhone, MobilePhoneModel, count(DISTINCT UserID) u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC, MobilePhone, MobilePhoneModel LIMIT 10")
q("q14_phrase_users", 'FROM hits | WHERE SearchPhrase != "" | STATS c = COUNT(*) BY SearchPhrase, UserID | STATS u = COUNT(*) BY SearchPhrase | SORT u DESC, SearchPhrase | LIMIT 10 | KEEP SearchPhrase, u',
  "SELECT SearchPhrase, count(DISTINCT UserID) u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC, SearchPhrase LIMIT 10")
q("q17_user_phrase", "FROM hits | STATS c = COUNT(*) BY UserID, SearchPhrase | SORT c DESC, UserID, SearchPhrase | LIMIT 10 | KEEP UserID, SearchPhrase, c",
  "SELECT UserID, SearchPhrase, count(*) c FROM hits GROUP BY UserID, SearchPhrase ORDER BY c DESC, UserID, SearchPhrase LIMIT 10")
# Q18 has a LIMIT without an order, so any ten groups are right: the check is that ten groups come back
q("q18_user_phrase_any", "FROM hits | STATS c = COUNT(*) BY UserID, SearchPhrase | LIMIT 10 | STATS n = COUNT(*)", "SELECT 10")
q("q19_user_minute", 'FROM hits | EVAL m = DATE_EXTRACT("minute_of_hour", @timestamp) | STATS c = COUNT(*) BY UserID, m, SearchPhrase | SORT c DESC, UserID, m, SearchPhrase | LIMIT 10 | KEEP UserID, m, SearchPhrase, c',
  "SELECT UserID, (EventTime // 60) % 60 m, SearchPhrase, count(*) c FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY c DESC, UserID, m, SearchPhrase LIMIT 10")
q("q23_title_google", 'FROM hits | WHERE Title LIKE "*Google*" AND NOT (URL LIKE "*.google.*") AND SearchPhrase != "" | STATS mu = MIN(URL), mt = MIN(Title), c = COUNT(*), d = COUNT_DISTINCT(UserID) BY SearchPhrase | SORT c DESC, SearchPhrase | LIMIT 10 | KEEP SearchPhrase, mu, mt, c, d',
  "SELECT SearchPhrase, min(URL) mu, min(Title) mt, count(*) c, count(DISTINCT UserID) d FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, SearchPhrase LIMIT 10", [0, 0, 0, 0, 0.05])
# Q24 selects every column: the time columns are strings in the response, so the SQL turns them into the same strings
_cols = [r[0] for r in duckdb.connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{hits}')").fetchall()]
def _sql_col(c):
    if c == "EventTime": return "strftime(make_timestamp(EventTime * 1000000), '%Y-%m-%dT%H:%M:%S.000Z')"
    if c == "EventDate": return "strftime(make_timestamp(EventDate::BIGINT * 86400 * 1000000), '%Y-%m-%dT%H:%M:%S.000Z')"
    return c
def _esql_col(c): return "`@timestamp`" if c == "EventTime" else c
q("q24_select_all", 'FROM hits | WHERE URL LIKE "*google*" | SORT @timestamp, WatchID | LIMIT 10 | EVAL EventDate = TO_STRING(EventDate), ts = TO_STRING(@timestamp) | KEEP ' + ", ".join("ts" if c == "EventTime" else c for c in _cols),
  "SELECT " + ", ".join(_sql_col(c) for c in _cols) + " FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime, WatchID LIMIT 10")
# Q25 sorts by EventTime only, so the phrases of tied rows are arbitrary: the check is the ten smallest times
q("q25_times_only", 'FROM hits | WHERE SearchPhrase != "" | SORT @timestamp | LIMIT 10 | EVAL ts = TO_STRING(@timestamp) | KEEP ts',
  "SELECT strftime(make_timestamp(EventTime * 1000000), '%Y-%m-%dT%H:%M:%S.000Z') FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10")
# RE2 (DuckDB) only treats \n as a line end and its $ only matches at the end of the text; in Java regex, which ES|QL uses, that is (?d) and \z
q("q29_referer_domain", 'FROM hits | WHERE Referer != "" | EVAL k = REPLACE(Referer, "(?d)^https?://(?:www\\\\.)?([^/]+)/.*\\\\z", "$1"), len = LENGTH(Referer) | STATS l = AVG(len), c = COUNT(*), m = MIN(Referer) BY k | WHERE c > 100000 | SORT l DESC, k | LIMIT 25 | KEEP k, l, c, m',
  r"SELECT regexp_replace(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') k, avg(length(Referer)) l, count(*) c, min(Referer) m FROM hits WHERE Referer <> '' GROUP BY k HAVING count(*) > 100000 ORDER BY l DESC, k LIMIT 25")
q("q32_watch_ip_filtered", 'FROM hits | WHERE SearchPhrase != "" | STATS c = COUNT(*), r = SUM(IsRefresh), w = AVG(ResolutionWidth) BY WatchID, ClientIP | SORT c DESC, WatchID, ClientIP | LIMIT 10 | KEEP WatchID, ClientIP, c, r, w',
  "SELECT WatchID, ClientIP, count(*) c, sum(IsRefresh) r, avg(ResolutionWidth) w FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC, WatchID, ClientIP LIMIT 10")
q("q33_watch_ip", 'FROM hits | STATS c = COUNT(*), r = SUM(IsRefresh), w = AVG(ResolutionWidth) BY WatchID, ClientIP | SORT c DESC, WatchID, ClientIP | LIMIT 10 | KEEP WatchID, ClientIP, c, r, w',
  "SELECT WatchID, ClientIP, count(*) c, sum(IsRefresh) r, avg(ResolutionWidth) w FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC, WatchID, ClientIP LIMIT 10")
q("q34_url_count", "FROM hits | STATS c = COUNT(*) BY URL | SORT c DESC, URL | LIMIT 10 | KEEP URL, c",
  "SELECT URL, count(*) c FROM hits GROUP BY URL ORDER BY c DESC, URL LIMIT 10")
q("q35_one_url_count", "FROM hits | EVAL one = 1 | STATS c = COUNT(*) BY one, URL | SORT c DESC, URL | LIMIT 10 | KEEP one, URL, c",
  "SELECT 1 one, URL, count(*) c FROM hits GROUP BY 1, URL ORDER BY c DESC, URL LIMIT 10")
q("q36_ip_arithmetic", "FROM hits | EVAL a = ClientIP - 1, b = ClientIP - 2, d = ClientIP - 3 | STATS c = COUNT(*) BY ClientIP, a, b, d | SORT c DESC, ClientIP | LIMIT 10 | KEEP ClientIP, a, b, d, c",
  "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, count(*) c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC, ClientIP LIMIT 10")
q("q39_links_offset", f'FROM hits | WHERE CounterID == 62 AND {JUL} AND IsRefresh == 0 AND IsLink != 0 AND IsDownload == 0 | STATS pv = COUNT(*) BY URL | SORT pv DESC, URL | LIMIT 1010 | KEEP URL, pv',
  f"SELECT URL, count(*) pv FROM hits WHERE {JUL_SQL} AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY pv DESC, URL LIMIT 10 OFFSET 1000", 1e-6, 1000)
q("q40_sources_offset", f'FROM hits | WHERE CounterID == 62 AND {JUL} AND IsRefresh == 0 | EVAL Src = CASE(SearchEngineID == 0 AND AdvEngineID == 0, Referer, ""), Dst = URL | STATS pv = COUNT(*) BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst | SORT pv DESC, TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst | LIMIT 1010 | KEEP TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst, pv',
  f"SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, count(*) pv FROM hits WHERE {JUL_SQL} AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY pv DESC, TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst LIMIT 10 OFFSET 1000", 1e-6, 1000)
q("q41_hash_date_offset", f'FROM hits | WHERE CounterID == 62 AND {JUL} AND IsRefresh == 0 AND TraficSourceID IN (-1, 6) AND RefererHash == 3594120000172545465 | STATS pv = COUNT(*) BY URLHash, EventDate | SORT pv DESC, URLHash, EventDate | LIMIT 110 | EVAL EventDate = TO_STRING(EventDate) | KEEP URLHash, EventDate, pv',
  f"SELECT URLHash, strftime(make_timestamp(EventDate::BIGINT * 86400 * 1000000), '%Y-%m-%dT%H:%M:%S.000Z') EventDate, count(*) pv FROM hits WHERE {JUL_SQL} AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY pv DESC, URLHash, EventDate LIMIT 10 OFFSET 100", 1e-6, 100)
q("q42_window_offset", f'FROM hits | WHERE CounterID == 62 AND {JUL} AND IsRefresh == 0 AND DontCountHits == 0 AND URLHash == 2868770270353813622 | STATS pv = COUNT(*) BY WindowClientWidth, WindowClientHeight | SORT pv DESC, WindowClientWidth, WindowClientHeight | LIMIT 10010 | KEEP WindowClientWidth, WindowClientHeight, pv',
  f"SELECT WindowClientWidth, WindowClientHeight, count(*) pv FROM hits WHERE {JUL_SQL} AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY pv DESC, WindowClientWidth, WindowClientHeight LIMIT 10 OFFSET 10000", 1e-6, 10000)
q("q43_minutes_offset", 'FROM hits | WHERE CounterID == 62 AND EventDate >= "2013-07-14T00:00:00Z" AND EventDate <= "2013-07-15T00:00:00Z" AND IsRefresh == 0 AND DontCountHits == 0 | STATS pv = COUNT(*) BY m = DATE_TRUNC(1 minute, @timestamp) | SORT m | LIMIT 1010 | EVAL m = TO_STRING(m) | KEEP m, pv',
  "SELECT strftime(make_timestamp((EventTime // 60 * 60) * 1000000), '%Y-%m-%dT%H:%M:00.000Z') m, count(*) pv FROM hits WHERE CounterID = 62 AND EventDate BETWEEN 15900 AND 15901 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY m ORDER BY m LIMIT 10 OFFSET 1000", 1e-6, 1000)

con = duckdb.connect()
con.execute(f"CREATE VIEW hits AS SELECT * FROM read_parquet('{hits}')")
result = []
for name, esql, sql, tol, offset in Q:
    if only and not any(name.startswith(p) for p in only):
        continue
    t = time.time()
    rows = [[float(v) if hasattr(v, "as_integer_ratio") and not isinstance(v, int) else v for v in r] for r in con.execute(sql).fetchall()]
    rows = [[int(v) if isinstance(v, __import__("decimal").Decimal) and v == v.to_integral_value() else (float(v) if isinstance(v, __import__("decimal").Decimal) else v) for v in r] for r in rows]
    print(f"{name}: {len(rows)} rows in {time.time()-t:.1f}s", flush=True)
    result.append({"name": name, "esql": esql, "tolerance": tol, "offset": offset, "rows": rows})
json.dump(result, open(out, "w"))
