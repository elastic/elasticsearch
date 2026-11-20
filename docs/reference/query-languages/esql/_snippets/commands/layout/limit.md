```yaml {applies_to}
serverless: ga
stack: ga
```

The `LIMIT` processing command limits the number of rows returned.

## Syntax

::::{applies-switch}

:::{applies-item} { stack: ga 9.0+}
```esql
LIMIT max_number_of_rows
```
:::

:::{applies-item} { "stack": "preview 9.4-9.5, ga 9.6+", "serverless": "ga" }
```esql
LIMIT max_number_of_rows BY grouping_expr1[, ..., grouping_exprN]
```
:::
::::

## Parameters

`max_number_of_rows`
:   The maximum number of rows to return. When `BY` is specified, the maximum
number of rows to return **per group**.

`grouping_exprX` {applies_to}`serverless: ga` {applies_to}`stack: ga 9.6+`
:   An expression that outputs the values to group by.

## Description

Use the `LIMIT` processing command to limit the number of rows returned.

When `BY` is specified, up to `max_number_of_rows` rows are retained for each
distinct combination of the grouping expressions.
Precede `LIMIT <N> BY` with a `SORT` to keep the top N for each group.

`null` grouping values compare equal, so rows whose grouping values are all
`null` form one group. A multivalued grouping value is compared as a whole, in
order: `[a, b]`, `[b, a]` and `[a, a, b]` are three distinct groups, unlike
[`STATS ... BY`](/reference/query-languages/esql/commands/stats-by.md), which
puts such a row into one group per value. `_source` and spatial shapes
(`geo_shape`, `cartesian_shape`) are compared by their stored byte
representation, so the same logical value encoded differently counts as a
separate group.

{applies_to}`stack: preview 9.6+` {applies_to}`serverless: preview` To remove
duplicate rows, use [`DEDUP`](/reference/query-languages/esql/commands/dedup.md)
instead. It is equivalent to `LIMIT 1 BY` for every column in scope, without having
to list the columns.

:::{include} ../../common/result-set-size-limitation.md
:::

## Examples

### Limit

:::{include} ../../generated/x-pack-esql/commands/examples/limit.csv-spec/basic.md
:::

### With groups
```{applies_to}
stack: preview 9.4-9.5, ga 9.6+
serverless: ga
```

:::{include} ../../generated/x-pack-esql/commands/examples/limit.csv-spec/limitBy.md
:::

### Group by multiple values
```{applies_to}
stack: preview 9.4-9.5, ga 9.6+
serverless: ga
```

:::{include} ../../generated/x-pack-esql/commands/examples/limit.csv-spec/limitByMultipleGroups.md
:::
