% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

A selector without a range, such as `http_requests_total`, returns the latest sample of each series at every evaluation time.

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 50 | $$$promql-limitations-instant-query$$$ Instant queries: `/api/v1/query` | Looks back a fixed five minutes for the latest sample. The `lookback_delta` parameter isn't supported | Looks back five minutes by default, configurable with `lookback_delta` |
| 51 | $$$promql-limitations-range-query$$$ Range queries: a series that stops reporting, in `/api/v1/query_range` | Looks back one `step`. The series disappears at the first step without a sample of its own | Keeps returning the last sample for up to five minutes |
| 52 | $$$promql-limitations-staleness$$$ Staleness markers: a scrape target that disappears | Ignores staleness markers. The series disappears only once all its samples fall outside the lookback window | Drops the series as soon as the staleness marker arrives |
| 53 | A negative offset near the end of the query range: `x offset -5m` | Reads no samples after the end of the query range, so the last evaluation times have no value | Reads samples after the end of the query range |
| 54 | {applies_to}`stack: preview =9.4` Instant queries: `/api/v1/query` | A five-minute range query ending at `time`, returning the last sample per series | Evaluates the expression at `time` |
| 55 | {applies_to}`stack: preview =9.4` Time buckets of range queries | Aligned to fixed calendar boundaries, so results can differ slightly, especially for short ranges or large steps | Aligned to the query start time |
