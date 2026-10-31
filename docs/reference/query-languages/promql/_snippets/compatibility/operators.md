% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/operators/. Rows are numbered across all
% snippets in _snippets/compatibility/. A row without a version badge applies to every version.

Sections follow [Operators](https://prometheus.io/docs/prometheus/latest/querying/operators/) in the Prometheus documentation.
Each [operator reference entry](/reference/query-languages/promql/operators.md) lists further differences.

### Binary operators [prometheus-compatibility-binary-operators]

#### Arithmetic binary operators [prometheus-compatibility-arithmetic-binary-operators]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 14 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Native histogram operands: `+`, `-`, `*`, `/` | Unsupported | Supported | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
:::

#### Trigonometric binary operators [prometheus-compatibility-trigonometric-binary-operators]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 15 | `atan2`: `x atan2 y` | Unsupported | Supported | [#164153](https://github.com/elastic/elasticsearch/issues/164153) |
:::

#### Comparison binary operators [prometheus-compatibility-comparison-binary-operators]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 16 | Filter with the scalar as the first operand: `100 < x`, `time() <= x` | Unsupported. Swap the operands: `x > 100` | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 17 | Non-literal scalar as the second operand: `x > time()`, `x > scalar(y)`, `1 < bool time()` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 18 | Two vectors, such as `x > y`, or `bool` with the scalar as the first operand, such as `100 > bool x` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 19 | Comparison inside another expression: `x > 100 > 200`, `(x > 100) + 1`, `count(x > 100)` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
:::

#### Logical/set binary operators [prometheus-compatibility-logical-set-binary-operators]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 20 | `and`, `unless` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 21 | `or` with `on(...)` or `ignoring(...)`: `a or on(job) b` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 22 | `or` inside another expression: `(sum(a) or sum(b)) * 2`, `count(a or b)` | Unsupported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 23 | An `or` chain with more than 8 operands | Unsupported. {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` The limit is the `esql.query.max_branch_count_per_merge` cluster setting | Supported | - |
:::

### Vector matching [prometheus-compatibility-vector-matching]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 24 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Two metrics, not aggregated: `a / b` | Unsupported. Aggregate both by the labels to match on: `sum by (instance) (a) / sum by (instance) (b)` | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 25 | `on`, `ignoring`, `group_left`, or `group_right` with an operand that isn't aggregated: `a / on(instance) b`, `a * on(pod) group_left(node) b` | Unsupported | Supported | [#157669](https://github.com/elastic/elasticsearch/issues/157669) |
| 26 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Operands aggregated differently: `sum(a) / b`, `b / scalar(sum(a))`, `topk(2, a) * b` | Unsupported. Both operands must be either not aggregated (`a / b`) or aggregated once across series (`sum(a) / sum(b)`). A number, such as `sum(a) * 2`, and `or` are supported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 27 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Nested aggregation next to an operand that reads a metric: `count(count by (pod) (a)) / sum(b)` | Unsupported. A nested aggregation on its own, such as `sum(sum by (pod) (a))`, is supported | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 28 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` A different `offset` per operand: `rate(x[5m]) / rate(x[5m] offset 1h)` | Unsupported, except with `or` | Supported | [#158184](https://github.com/elastic/elasticsearch/issues/158184) |
| 29 | `label_replace` or `label_join` as an operand: `label_replace(a, ...) * on(pod) group_left(node) b` | Unsupported | Supported | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 30 | `fill()`, `fill_left()`, and `fill_right()`: `a + fill(0) b` | Unsupported | Supported with the `promql-binop-fill-modifiers` feature flag | - |
:::

### Aggregation operators [prometheus-compatibility-aggregation-operators]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 31 | `group`, `count_values` | Unsupported | Supported | [#164009](https://github.com/elastic/elasticsearch/issues/164009), [#136248](https://github.com/elastic/elasticsearch/issues/136248) |
| 32 | `topk`, `bottomk`, `limitk`, or `limit_ratio` with `without(...)`: `topk without (instance) (1, x)` | Unsupported | Supported | - |
| 33 | Parameter that isn't a number literal: `topk(scalar(a), b)`, `quantile(scalar(a), b)` | Unsupported | Supported | - |
| 34 | Nested `without(...)`: `sum without (pod) (max without (container) (x))` | Unsupported. Use `by(...)` | Supported | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 35 | `without(...)` as an operand: `sum without (method) (x) * 2`, `sum without (method) (a) / sum without (method) (b)` | Unsupported. Use `by(...)` | Supported | [#145308](https://github.com/elastic/elasticsearch/issues/145308) |
| 36 | Grouping by a label named `step`: `sum by (step) (x)` | Unsupported. `step` is an output column of the `PROMQL` command | Supported | - |
| 37 | `quantile` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 38 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `limitk`, `limit_ratio` | Selects a different, equally valid subset | Selects a deterministic pseudo-random subset, with the `promql-experimental-functions` feature flag | - |
:::
