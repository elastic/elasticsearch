% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

Further differences are listed under **Differences from Prometheus** on each [function's reference entry](/reference/query-languages/promql/functions.md).

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 67 | `rate` and `increase` | Computed over fixed time buckets. Values can differ, especially when an evaluation time falls exactly on a sample | Extrapolated within each range window |
| 68 | `rate` or `increase` with a single sample in the window: `rate(x[15s])` with a 15-second scrape interval | Returns a value | No result: a rate needs two samples |
| 69 | `deriv` | Values differ, and some windows return no result | Least-squares regression over the samples in the window |
| 70 | `quantile` or `quantile_over_time` with a quantile outside the range 0 to 1: `quantile(1.5, x)` | No results | `+Inf` or `-Inf`, with a warning |
| 71 | `quantile` and `quantile_over_time` | Approximate, computed with the t-digest percentile aggregation | Exact, with linear interpolation |
| 72 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `limitk` and `limit_ratio` | Select a different, equally valid subset of series | `limitk` takes the first series in storage order. `limit_ratio` samples by a hash of the labels |
| 73 | `absent_over_time` for a selector without data, or over a gap: `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers |
| 74 | {applies_to}`stack: ga 9.5-9.6` `absent_over_time` for series that have samples: `absent_over_time(up[5m])` | Returns those series | No results |
| 75 | {applies_to}`stack: ga =9.5` Results that are `NaN` or infinite: `x / 0`, `ln(vector(-1))`, `asin(x)` outside its domain, `histogram_quantile(NaN, ...)`, `scalar()` of several series | Leaves those series out | Returns `NaN`, `+Inf`, or `-Inf` |
| 76 | {applies_to}`stack: ga =9.5` `clamp` with a minimum above the maximum: `clamp(x, 10, 0)` | Returns the series | No results |
| 77 | {applies_to}`stack: ga =9.5` `histogram_quantile` over a `without(...)` aggregation | No results | Evaluates |
| 78 | {applies_to}`stack: ga =9.5` A string literal as the query: `"text"` | Instant queries return an empty scalar. Range queries run | Instant queries return the string. Range queries return an error |
| 79 | {applies_to}`stack: ga =9.5` The metric name as a matcher next to other matchers: `{__name__="up", job="api"}`, `{"up", job="api"}` | No results | Every matching series |
