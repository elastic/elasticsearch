% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/operators/. Rows are numbered across all
% snippets in _snippets/compatibility/. A row without a version badge applies to every version.

Sections follow [Operators](https://prometheus.io/docs/prometheus/latest/querying/operators/) in the Prometheus documentation.
Each [operator reference entry](/reference/query-languages/promql/operators.md) lists further differences.
Prometheus drops the metric name (`__name__`) from arithmetic results.

### Unary operator [prometheus-compatibility-unary-operator]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 20 | Unary minus: `-x` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |

### Binary operators [prometheus-compatibility-binary-operators]

#### Arithmetic binary operators [prometheus-compatibility-arithmetic-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 21 | Two vectors of the same metric: `max_over_time(x[5m]) - min_over_time(x[5m])` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 22 | A vector and a number: `x * 2` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 23 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `topk` or `bottomk` and a number: `topk(2, x) * 2` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 24 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Native histogram operands: `+`, `-`, `*`, `/` | Unsupported | Supported | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |

#### Trigonometric binary operators [prometheus-compatibility-trigonometric-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 25 | `atan2`: `x atan2 y` | Unsupported | Supported | [#164153](https://github.com/elastic/elasticsearch/issues/164153) |

#### Comparison binary operators [prometheus-compatibility-comparison-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 26 | Filter with the scalar as the first operand: `100 < x`, `time() <= x` | Unsupported. Swap the operands: `x > 100` | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 27 | Non-literal scalar as the second operand: `x > time()`, `x > scalar(y)`, `1 < bool time()` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 28 | Two vectors, such as `x > y`, or `bool` with the scalar as the first operand, such as `100 > bool x` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 29 | Comparison inside another expression: `x > 100 > 200`, `(x > 100) + 1`, `count(x > 100)` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 30 | `bool` with a number: `x > bool 100` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |

#### Logical/set binary operators [prometheus-compatibility-logical-set-binary-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 31 | `and`, `unless` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 32 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `or` with `on(...)` or `ignoring(...)` | Unsupported, except between operands without labels, such as `sum(a) or on () vector(1)`. An `or` chain takes at most 8 operands by default (`esql.query.max_branch_count_per_merge`) | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 33 | {applies_to}`stack: ga =9.5` `or` with `on(...)` or `ignoring(...)` | Unsupported, except between operands without labels, such as `sum(a) or on () vector(1)`. An `or` chain takes at most 8 operands | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 34 | {applies_to}`stack: preview =9.4` `or` | Unsupported | Supported | - |

### Vector matching [prometheus-compatibility-vector-matching]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 35 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Two metrics, not aggregated: `a / b` | Unsupported. Aggregate both by the labels to match on: `sum by (instance) (a) / sum by (instance) (b)` | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 36 | `on`, `ignoring`, `group_left`, or `group_right` with an operand that isn't aggregated: `a / on(instance) b`, `a * on(pod) group_left(node) b` | Unsupported | Supported | [#157669](https://github.com/elastic/elasticsearch/issues/157669) |
| 37 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Operands aggregated differently: `sum(a) / b`, `b / scalar(sum(a))`, `topk(2, a) * b` | Unsupported. Both operands must be either not aggregated (`a / b`) or aggregated once across series (`sum(a) / sum(b)`). A number, such as `topk(1, a) * 2`, and `or` are supported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 38 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Nested aggregation next to an operand that reads a metric: `count(count by (pod) (a)) / sum(b)` | Unsupported. A nested aggregation on its own, such as `sum(sum by (pod) (a))`, is supported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 39 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` A different `offset` per operand: `rate(x[5m]) / rate(x[5m] offset 1h)` | Unsupported, except with `or` | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 40 | `label_replace` or `label_join` as an operand: `label_replace(a, ...) * on(pod) group_left(node) b` | Unsupported | Supported | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 41 | `fill()`, `fill_left()`, and `fill_right()`: `a + fill(0) b` | Unsupported | Supported with the `promql-binop-fill-modifiers` feature flag | - |
| 42 | {applies_to}`stack: ga =9.5` `on`, `ignoring`, `group_left`, or `group_right` between aggregations: `sum by (job) (a) / on(job) sum by (job) (b)` | Unsupported | Supported | - |
| 43 | {applies_to}`stack: ga =9.5` Two metrics, not aggregated: `a / b` | No results | Matches series on their labels | - |

### Aggregation operators [prometheus-compatibility-aggregation-operators]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 44 | `group`, `count_values` | Unsupported | Supported | [#164009](https://github.com/elastic/elasticsearch/issues/164009), [#136248](https://github.com/elastic/elasticsearch/issues/136248) |
| 45 | `topk`, `bottomk`, `limitk`, or `limit_ratio` with `without(...)`: `topk without (instance) (1, x)` | Unsupported | Supported | - |
| 46 | Parameter that isn't a number literal: `topk(scalar(a), b)`, `quantile(scalar(a), b)` | Unsupported | Supported | - |
| 47 | Nested `without(...)`: `sum without (pod) (max without (container) (x))` | Unsupported. Use `by(...)` | Supported | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 48 | Grouping by a label named `step`: `sum by (step) (x)` | Unsupported. `step` is an output column of the `PROMQL` command | Supported | - |
| 49 | `without(...)`: `sum without (instance) (x)` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 50 | `quantile` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 51 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `limitk`, `limit_ratio` | Selects a different, equally valid subset | `limitk`: the first series in storage order. `limit_ratio`: sampled by a hash of the labels | - |
| 52 | {applies_to}`stack: ga =9.5` `topk`, `bottomk`, `limitk`, `limit_ratio` | Unsupported | Supported | - |
