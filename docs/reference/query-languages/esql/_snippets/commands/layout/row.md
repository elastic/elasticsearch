```yaml {applies_to}
serverless: ga
stack: ga
```

The `ROW` source command produces a row with one or more columns with values
that you specify. This can be useful for testing.

## Syntax

```esql
ROW column1 = value1[, ..., columnN = valueN]
```

## Parameters

`columnX`
:   The column name.
    In case of duplicate column names, only the rightmost duplicate creates a column.

`valueX`
:   The value for the column. Can be a literal, an expression, or a
    [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).

## Examples

The following examples demonstrate common `ROW` patterns.

### Create a row with literal values

:::{include} ../examples/row.csv-spec/example.md
:::

### Create multivalued columns

Use square brackets to create a column with multiple values:

:::{include} ../examples/row.csv-spec/multivalue.md
:::

### Use functions as values

:::{include} ../examples/row.csv-spec/function.md
:::
