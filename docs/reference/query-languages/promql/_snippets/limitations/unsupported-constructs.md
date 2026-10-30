% Included in both the PromQL limitations page and the ES|QL PROMQL command page. The command page
% is copied verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt)
% and the elasticsearch-esql agent skill (references/promql-command.md).
% Rows are numbered across all snippets in _snippets/limitations/. A row without a version badge applies to every
% version.

The majority of PromQL expressions run unchanged.
The following constructs are not evaluated yet. A query that uses one of them returns an error.

**Syntax**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 1 | The `@` modifier: `x @ 1700000000`, `x @ start()`, `x @ end()` | Error | Evaluates the selector at the given time |
| 2 | Subqueries: `max_over_time(rate(x[5m])[1h:1m])`, `max_over_time(x[10m:])` | Error | Evaluates the inner expression at each step of the subquery range |
| 3 | A range vector as the result of an instant query: `x[5m]` | Error | Returns the raw samples of each series |
| 4 | Number literals with underscores: `1_000` | Error | `1000` |
| 5 | {applies_to}`stack: ga =9.5` Durations as numbers: `1h`, `90s`, `5m - 1m` | Error | The number of seconds, such as `3600` for `1h` |
| 6 | An `offset` of zero: `x offset 0s` | Error | Same as no offset |
| 7 | An `offset` directly followed by a binary operator: `x offset 5m + y` | Error | `(x offset 5m) + y` |
| 8 | Syntax that Prometheus enables with a feature flag: the duration functions `step()`, `range()`, `min_of()`, `max_of()`, `start()`, and `end()`; the `anchored` and `smoothed` range modifiers; the `fill()`, `fill_left()`, and `fill_right()` binary operator modifiers | Error. Arithmetic in durations, such as `rate(x[5m + 1m])`, is supported | Evaluates when the feature flag is enabled |

**Selectors**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 9 | Selectors without a metric name: `{job="api"}` | Error | Every series that matches |
| 10 | Regex matchers on `__name__`: `{__name__=~"node_.*"}`, `{__name__!~"up", job="node"}` | Error | Every series whose name matches |
| 11 | A regex with an empty alternative: `x{code=~"200\|"}` | Error | Matches `200` and the empty value |

**Operators**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 12 | The `atan2` operator: `x atan2 y` | Error | The arc tangent of `x / y` |
| 13 | A filtering comparison with a scalar on the left and an instant vector on the right: `100 < x`, `time() <= x` | Error. Write it the other way round: `x > 100` | The series of `x` for which the comparison holds |
| 14 | A comparison with a non-literal scalar on the right: `x > time()`, `x > scalar(y)`, `1 < bool time()` | Error | Evaluates |
| 15 | A comparison between two instant vectors, and a `bool` comparison with a scalar on the left: `x > y`, `sum by (job) (a) > sum by (job) (b)`, `100 > bool x` | Error | Evaluates |
| 16 | A comparison inside another expression: `x > 100 > 200`, `(x > 100) + 1`, `count(x > 100)` | Error | Evaluates |
| 17 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Arithmetic between two different metrics that aren't aggregated: `a / b` | Error. Aggregate both by the labels to match on, such as `sum by (instance) (a) / sum by (instance) (b)` | Matches the series of both metrics on their labels |
| 18 | `on(...)`, `ignoring(...)`, `group_left`, or `group_right` with an operand that isn't aggregated: `a / on(instance) b`, `a * on(pod) group_left(node) b` | Error | Evaluates |
| 19 | {applies_to}`stack: ga =9.5` `on(...)`, `ignoring(...)`, `group_left`, or `group_right` between aggregations: `sum by (job) (a) / on(job) sum by (job) (b)` | Error | Evaluates |
| 20 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Operands that read metrics and aren't aggregated alike. Both must be either not aggregated, such as `a / b`, or aggregated once across series, such as `sum(a) / sum(b)`. This covers `sum(a) / b`, `scalar()` of a metric next to an operand with labels such as `b / scalar(sum(a))`, and `topk`, `bottomk`, `limitk`, or `limit_ratio` in an operand such as `topk(2, a) * b`. Operations with a number, such as `topk(1, a) * 2`, and `or` are supported | Error | Evaluates |
| 21 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` An operand that nests an aggregation inside another, next to an operand that reads metrics: `count(count by (pod) (a)) / sum(b)`. Nested aggregations on their own, such as `sum(sum by (pod) (a))`, are supported | Error | Evaluates |
| 22 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Operands with different `offset` values: `rate(x[5m]) / rate(x[5m] offset 1h)`. `or` is supported | Error | Evaluates |
| 23 | `label_replace` or `label_join` as an operand: `label_replace(a, ...) * on(pod) group_left(node) b` | Error | Evaluates |

**Set operators**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 24 | `and` and `unless` | Error | Evaluates |
| 25 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `or` with `on(...)` or `ignoring(...)`, except the empty-guard idiom `<labelless> or on () <labelless>`, such as `sum(a) or on () vector(1)`. An `or` chain supports at most 8 operands by default (`esql.query.max_branch_count_per_merge`) | Error | Evaluates |
| 26 | {applies_to}`stack: ga =9.5` `or` with `on(...)` or `ignoring(...)`, except the empty-guard idiom `<labelless> or on () <labelless>`, such as `sum(a) or on () vector(1)`. An `or` chain supports at most 8 operands | Error | Evaluates |
| 27 | An `or` of aggregations inside arithmetic: `sum(a) + (sum(a) or sum(b))` | Error | Evaluates |

**Aggregations**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 28 | `group` and `count_values` | Error | Evaluates |
| 29 | {applies_to}`stack: ga =9.5` `topk`, `bottomk`, `limitk`, and `limit_ratio` | Error | Evaluates |
| 30 | `topk`, `bottomk`, `limitk`, or `limit_ratio` with `without(...)`: `topk without (instance) (1, x)` | Error | Evaluates |
| 31 | A `k` above the 32-bit integer range: `topk(1e10, x)` | Error | Every series |
| 32 | Parameters that aren't number literals: `topk(scalar(a), b)`, `quantile(scalar(a), b)`, `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | Error | Evaluates |
| 33 | A `without(...)` aggregation nested inside another: `sum without (pod) (max without (container) (x))`. Use `by(...)` instead | Error | Evaluates |
| 34 | Grouping by a label named `step`: `sum by (step) (x)` | Error. `step` is a built-in output column of the `PROMQL` command | Evaluates |

**Functions**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 35 | Functions listed under [Not yet supported](/reference/query-languages/promql/functions.md#promql-not-supported), such as `absent`, `resets`, `info`, and `sort_by_label` | Error | Evaluates |
| 36 | {applies_to}`stack: ga =9.5` `sort`, `sort_desc`, `timestamp`, `changes`, `histogram_fraction`, `label_replace`, and `label_join` | Error | Evaluates |
| 37 | `predict_linear` | Error | Evaluates |
| 38 | `sort` or `sort_desc` below the top level: `sum(sort(x))` | Error | Evaluates. Sorting has no effect there |
| 39 | `timestamp()` over the result of a range vector function: `timestamp(rate(x[5m]))` | Error | Evaluates |
| 40 | `label_replace` or `label_join` outside a `by(...)` aggregation: `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | Error. Supported inside `sum by (dst) (label_replace(...))` | Evaluates |
| 41 | `label_replace` or `label_join` over an aggregation: `label_replace(sum by (job) (x), ...)` | Error | Evaluates |
| 42 | `label_replace` or `label_join` that writes a label named `value` or `step` | Error | Evaluates |
| 43 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio`, unless the input is first aggregated with `by(...)`: `histogram_quantile(0.9, topk(5, a))`. `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` is supported | Error | Evaluates |
| 44 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `histogram_quantile` or `histogram_fraction` over a `without(...)` aggregation: `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))`. Use `by(...)` instead | Error | Evaluates |
| 45 | `histogram_count`, `histogram_sum`, or `histogram_avg` over classic histogram buckets (float series with an `le` label) | Error. They accept only native histograms | No results |

**Earlier versions**

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 46 | {applies_to}`stack: preview =9.4` The `or` operator | Error | Evaluates |
| 47 | {applies_to}`stack: preview =9.4` The `offset` modifier | Error | Evaluates |

The following constructs return no results instead of an error:

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 48 | A negative matcher on `__name__`: `{__name__!="up", job="node"}` | No results | Every other metric that matches |
| 49 | Regex matchers with inline flags or explicit anchors: `{job=~"(?i)api"}`, `{code=~"^200$"}`. Regex matchers are always fully anchored, so drop `^` and `$`, and list the case variants instead of using `(?i)` | No results | Matches |
