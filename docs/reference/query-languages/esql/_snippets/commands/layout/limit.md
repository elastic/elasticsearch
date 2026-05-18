```yaml {applies_to}
serverless: ga
stack: ga
```

The `LIMIT` processing command limits the number of rows returned.

## Syntax

::::{applies-switch}

:::{applies-item} { "stack": "preview 9.4+", "serverless": "preview" }
```esql
LIMIT max_number_of_rows [BY grouping_expr1[, ..., grouping_exprN]]
```
:::

:::{applies-item} { stack: ga 9.0+}
```esql
LIMIT max_number_of_rows
```
:::
::::

## Parameters

`max_number_of_rows`
:   The maximum number of rows to return. When `BY` is specified, the maximum
number of rows to return **per group**.

`grouping_exprX` {applies_to}`serverless: preview` {applies_to}`stack: preview 9.4+`
:   An expression that outputs the values to group by.

## Description

Use the `LIMIT` processing command to limit the number of rows returned.

When `BY` is specified, up to `max_number_of_rows` rows are retained for each
distinct combination of the grouping expressions.
Precede `LIMIT <N> BY` with a `SORT` to keep the top N for each group.

:::{include} ../../common/result-set-size-limitation.md
:::

## Examples

### Limit

:::{include} ../examples/limit.csv-spec/basic.md
:::

### With groups
```{applies_to}
stack: preview 9.4
serverless: preview
```

:::{include} ../examples/limit.csv-spec/limitBy.md
:::

### Group by multiple values
```{applies_to}
stack: preview 9.4
serverless: preview
```

:::{include} ../examples/limit.csv-spec/limitByMultipleGroups.md
:::
