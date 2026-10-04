% Included in both the PromQL limitations page and the ES|QL PROMQL command page. The command page
% is copied verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt)
% and the elasticsearch-esql agent skill (references/promql-command.md).

The majority of PromQL expressions run unchanged.
The following constructs are not evaluated yet, so they return a client error (4xx):

- Binary set operators: `and` and `unless`.
- {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Binary set operator `or`, except at the top level of an expression. A top-level `or` chain supports at most 8 operands and can't use `on(...)` or `ignoring(...)`. A nested `or`, a chain of more than 8 operands, or an `or` with `on(...)` or `ignoring(...)` returns a client error (4xx).
- Comparison operators: evaluated only at the top level of an expression and only with a scalar literal on the right-hand side. Comparisons between two instant vectors, and nested comparisons, return a client error (4xx).
- Group modifiers: `on(...)`, `ignoring(...)`, `group_left`, `group_right`.
- The `@` modifier.
- Subqueries, such as `max_over_time(rate(http_requests_total)[1h:])`.
- Selectors without a metric name, and regex matchers on `__name__`, such as `{__name__=~"node_.*"}`.
- Binary expressions with a `without(...)` aggregation as an operand, and `without(...)` aggregations nested inside another `without(...)` aggregation, such as `sum without (pod) (max without (container) (...))`. Use `by(...)` instead.
- {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Binary expressions whose operands use different `offset` values, such as `rate(http_requests_total) / rate(http_requests_total offset 1h)`. This restriction doesn't apply to `or`.
- {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Binary expressions where both operands read metrics and one of them nests an aggregation inside another aggregation, such as `count(count by (pod) (up)) / sum(machine_count)`. Nested aggregations on their own, such as `sum(sum by (pod) (...))`, are supported.
- Functions: refer to [Not yet supported](/reference/query-languages/promql/functions.md#promql-not-supported) for the full list of recognized but unimplemented functions. Some supported functions have restrictions, which are listed under **Differences from Prometheus** on each function's reference entry.
- {applies_to}`stack: preview =9.4` Binary set operator `or`.
- {applies_to}`stack: preview =9.4` The `offset` modifier.

The following constructs return no results instead of an error:

- Binary expressions between two operands that read different metrics without aggregating them, such as
  `node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes`. Unlike in Prometheus, the series of the two
  metrics aren't matched on their other labels. Aggregate both operands by the labels to keep instead, such as
  `avg by (instance) (node_memory_MemAvailable_bytes) / avg by (instance) (node_memory_MemTotal_bytes)`.
