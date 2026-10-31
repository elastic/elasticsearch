% Section of prometheus-compatibility.md, also included in other pages. Rows are numbered across all sections.

Further differences are listed under **Differences from Prometheus** on each [function's reference entry](/reference/query-languages/promql/functions.md).

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 54 | `rate` and `increase` | Computed over fixed time buckets. Values can differ, especially when an evaluation time falls exactly on a sample | Extrapolated within each range window | - |
| 55 | `quantile` and `quantile_over_time` | Approximate, computed with the t-digest percentile aggregation | Exact, with linear interpolation | - |
| 56 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `limitk` and `limit_ratio` | Select a different, equally valid subset of series | `limitk` takes the first series in storage order. `limit_ratio` samples by a hash of the labels | - |
| 57 | `absent_over_time` for a selector without data, or over a gap: `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers | - |
| 58 | {applies_to}`stack: ga =9.5` Results that are `NaN` or infinite: `x / 0`, `ln(vector(-1))`, `asin(x)` outside its domain, `histogram_quantile(NaN, ...)`, `scalar()` of several series | Leaves those series out | Returns `NaN`, `+Inf`, or `-Inf` | - |
| 59 | {applies_to}`stack: ga =9.5` A string literal as the query: `"text"` | Instant queries return an empty scalar. Range queries run | Instant queries return the string. Range queries return an error | - |
