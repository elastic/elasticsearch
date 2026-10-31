% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

Prometheus drops the metric name (`__name__`) from the result of most functions and of arithmetic, because the result is no longer a sample of that metric.

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 49 | Functions over a series: math functions such as `abs`, `round`, and `clamp`; `timestamp` and the date functions with an argument; `rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, and `changes`; the `_over_time` functions other than `first_over_time` and `last_over_time`; `histogram_quantile` and `histogram_fraction` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 50 | Arithmetic between two instant vectors of the same metric: `max_over_time(x[5m]) - min_over_time(x[5m])` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 51 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `topk` or `bottomk` combined with a number: `topk(2, x) * 2` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 52 | Arithmetic or a `bool` comparison with a number, and unary minus: `x * 2`, `x > bool 100`, `-x` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 53 | Aggregations with `without(...)`: `sum without (instance) (x)` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
