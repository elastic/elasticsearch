% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/operators/. Rows are numbered across all
% snippets in _snippets/compatibility/. A row without a version badge applies to every version.

These rows follow [Operators](https://prometheus.io/docs/prometheus/latest/querying/operators/) in the Prometheus documentation.
Further differences are listed under **Differences from Prometheus** on each [operator's reference entry](/reference/query-languages/promql/operators.md).
Prometheus drops the metric name (`__name__`) from the result of arithmetic, because the result is no longer a sample of that metric.

### Unary operator [prometheus-compatibility-unary-operator]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 20 | Unary minus: `-x` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |

### Binary operators [prometheus-compatibility-binary-operators]

#### Arithmetic binary operators [prometheus-compatibility-arithmetic-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 21 | Arithmetic between two instant vectors of the same metric: `max_over_time(x[5m]) - min_over_time(x[5m])` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 22 | Arithmetic with a number: `x * 2` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 23 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `topk` or `bottomk` combined with a number: `topk(2, x) * 2` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 24 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Arithmetic on native histograms: `+`, `-`, `*`, `/` | Error | Evaluates | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |

#### Trigonometric binary operators [prometheus-compatibility-trigonometric-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 25 | The `atan2` operator: `x atan2 y` | Error | The arc tangent of `x / y` | [#164153](https://github.com/elastic/elasticsearch/issues/164153) |

#### Comparison binary operators [prometheus-compatibility-comparison-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 26 | A filtering comparison with a scalar as the first operand and an instant vector as the second: `100 < x`, `time() <= x` | Error. Swap the operands: `x > 100` | The series of `x` for which the comparison holds | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 27 | A comparison with a non-literal scalar as the second operand: `x > time()`, `x > scalar(y)`, `1 < bool time()` | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 28 | A comparison between two instant vectors, and a `bool` comparison with a scalar as the first operand: `x > y`, `sum by (job) (a) > sum by (job) (b)`, `100 > bool x` | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 29 | A comparison inside another expression: `x > 100 > 200`, `(x > 100) + 1`, `count(x > 100)` | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 30 | A `bool` comparison with a number: `x > bool 100` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |

#### Logical/set binary operators [prometheus-compatibility-logical-set-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 31 | `and` and `unless` | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 32 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `or` with `on(...)` or `ignoring(...)`, except the empty-guard idiom `<labelless> or on () <labelless>`, such as `sum(a) or on () vector(1)`. An `or` chain supports at most 8 operands by default (`esql.query.max_branch_count_per_merge`) | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 33 | {applies_to}`stack: ga =9.5` `or` with `on(...)` or `ignoring(...)`, except the empty-guard idiom `<labelless> or on () <labelless>`, such as `sum(a) or on () vector(1)`. An `or` chain supports at most 8 operands | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 34 | {applies_to}`stack: preview =9.4` The `or` operator | Error | Evaluates | - |

### Vector matching [prometheus-compatibility-vector-matching]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 35 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Arithmetic between two different metrics that aren't aggregated: `a / b` | Error. Aggregate both by the labels to match on, such as `sum by (instance) (a) / sum by (instance) (b)` | Matches the series of both metrics on their labels | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 36 | `on(...)`, `ignoring(...)`, `group_left`, or `group_right` with an operand that isn't aggregated: `a / on(instance) b`, `a * on(pod) group_left(node) b` | Error | Evaluates | [#157669](https://github.com/elastic/elasticsearch/issues/157669) |
| 37 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Operands that read metrics and aren't aggregated alike. Both must be either not aggregated, such as `a / b`, or aggregated once across series, such as `sum(a) / sum(b)`. This covers `sum(a) / b`, `scalar()` of a metric next to an operand with labels such as `b / scalar(sum(a))`, and `topk`, `bottomk`, `limitk`, or `limit_ratio` in an operand such as `topk(2, a) * b`. Operations with a number, such as `topk(1, a) * 2`, and `or` are supported | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 38 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` An operand that nests an aggregation inside another, next to an operand that reads metrics: `count(count by (pod) (a)) / sum(b)`. Nested aggregations on their own, such as `sum(sum by (pod) (a))`, are supported | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 39 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Operands with different `offset` values: `rate(x[5m]) / rate(x[5m] offset 1h)`. `or` is supported | Error | Evaluates | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 40 | `label_replace` or `label_join` as an operand: `label_replace(a, ...) * on(pod) group_left(node) b` | Error | Evaluates | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 41 | The `fill()`, `fill_left()`, and `fill_right()` modifiers: `a + fill(0) b` | Error | Evaluates when the `promql-binop-fill-modifiers` feature flag is enabled | - |
| 42 | {applies_to}`stack: ga =9.5` `on(...)`, `ignoring(...)`, `group_left`, or `group_right` between aggregations: `sum by (job) (a) / on(job) sum by (job) (b)` | Error | Evaluates | - |
| 43 | {applies_to}`stack: ga =9.5` Arithmetic between two different metrics that aren't aggregated: `a / b` | No results | Matches the series of both metrics on their labels | - |

### Aggregation operators [prometheus-compatibility-aggregation-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 44 | `group` and `count_values` | Error | Evaluates | [#164009](https://github.com/elastic/elasticsearch/issues/164009), [#136248](https://github.com/elastic/elasticsearch/issues/136248) |
| 45 | `topk`, `bottomk`, `limitk`, or `limit_ratio` with `without(...)`: `topk without (instance) (1, x)` | Error | Evaluates | - |
| 46 | Parameters that aren't number literals: `topk(scalar(a), b)`, `quantile(scalar(a), b)` | Error | Evaluates | - |
| 47 | A `without(...)` aggregation nested inside another: `sum without (pod) (max without (container) (x))`. Use `by(...)` instead | Error | Evaluates | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 48 | Grouping by a label named `step`: `sum by (step) (x)` | Error. `step` is a built-in output column of the `PROMQL` command | Evaluates | - |
| 49 | Aggregations with `without(...)`: `sum without (instance) (x)` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 50 | `quantile` | Approximate, computed with the t-digest percentile aggregation | Exact, with linear interpolation | - |
| 51 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `limitk` and `limit_ratio` | Select a different, equally valid subset of series | `limitk` takes the first series in storage order. `limit_ratio` samples by a hash of the labels | - |
| 52 | {applies_to}`stack: ga =9.5` `topk`, `bottomk`, `limitk`, and `limit_ratio` | Error | Evaluates | - |
