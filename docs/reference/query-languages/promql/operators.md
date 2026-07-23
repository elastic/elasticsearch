---
description: PromQL operator reference for Elasticsearch, grouped by category, with Elasticsearch-specific differences from Prometheus.
navigation_title: Operators
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
products:
  - id: elasticsearch
---

# PromQL operators [promql-operators]

These operators are based on the [Prometheus operators](https://prometheus.io/docs/prometheus/latest/querying/operators/). Refer to the Prometheus documentation for the full semantics, and note any {{es}}-specific differences called out per operator.

## Operators overview [promql-operators-overview]

### Arithmetic operators

Binary operators that perform arithmetic between scalars and instant vectors.

::::{dropdown} Arithmetic operator list
:open:
:::{include} _snippets/generated/x-pack-esql/operators/lists/arithmetic-overview.md
:::
::::

### Comparison operators

Binary operators that filter instant vectors or, with the `bool` modifier, return `0` or `1`.

::::{dropdown} Comparison operator list
:open:
:::{include} _snippets/generated/x-pack-esql/operators/lists/comparison-overview.md
:::
::::

### Logical/set operators

Binary operators that combine two instant vectors by matching their label sets.

::::{dropdown} Logical/set operator list
:open:
:::{include} _snippets/generated/x-pack-esql/operators/lists/logical-set-overview.md
:::
::::

### Unary operators

Operators that apply to a single instant vector or scalar.

::::{dropdown} Unary operator list
:open:
:::{include} _snippets/generated/x-pack-esql/operators/lists/unary-overview.md
:::
::::

### Label matching operators

Operators used inside instant vector selectors (`{...}`) to match series by label.

::::{dropdown} Label matching operator list
:open:
:::{include} _snippets/generated/x-pack-esql/operators/lists/label-matching-overview.md
:::
::::

## Binary operator precedence [promql-operators-precedence]

PromQL follows the same binary operator precedence as Prometheus. The following list orders operators from highest to lowest precedence; operators listed together share the same precedence level:

1. `^`
2. `*`, `/`, `%`, `atan2`
3. `+`, `-`
4. `==`, `!=`, `<=`, `<`, `>=`, `>`
5. `and`, `unless`
6. `or`

`^` is right-associative; all other binary operators are left-associative. Use parentheses to override the default precedence.

::::{note}
`atan2`, `and`, and `unless` are listed here for completeness, but are not evaluated yet in {{es}}. See [Not yet supported](#promql-operators-not-supported) and [PromQL limitations](/reference/query-languages/promql/promql-limitations.md#promql-limitations-unsupported-constructs).
::::

## Vector matching [promql-operators-vector-matching]

When a binary operator is applied between two instant vectors, PromQL matches samples that have identical label sets (one-to-one matching). Prometheus provides modifiers that change which labels are considered (`on(...)` and `ignoring(...)`) and that enable many-to-one and one-to-many matching (`group_left` and `group_right`).

{{es}} does not support the `on(...)`, `ignoring(...)`, `group_left`, or `group_right` vector matching modifiers yet. Using them returns a client error (4xx). See [PromQL limitations](/reference/query-languages/promql/promql-limitations.md#promql-limitations-unsupported-constructs).

## The `bool` modifier [promql-operators-bool-modifier]

By default, a comparison operator between two instant vectors acts as a filter: samples that do not satisfy the comparison are dropped, and matching samples keep their original values. Appending the `bool` modifier changes this behavior so that the comparison returns `0` for false and `1` for true, and keeps every sample. The `bool` modifier is required when comparing two scalars.

**Example**

```
requests_total > bool 256
```

## Aggregation operators [promql-operators-aggregation]

Prometheus documents aggregation operators (such as `sum`, `avg`, `min`, `max`, and `count`) on its operators page. In {{es}} PromQL, these are documented as [aggregation functions](functions/aggregation.md).

## Not yet supported [promql-operators-not-supported]

The following Prometheus operators are not supported in {{es}} PromQL yet:

:::{include} _snippets/generated/x-pack-esql/operators/not-supported.md
:::
