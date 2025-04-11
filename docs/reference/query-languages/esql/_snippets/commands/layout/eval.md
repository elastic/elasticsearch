## `EVAL` [esql-eval]

The `EVAL` processing command enables you to append new columns with calculated
values.

**Syntax**

```esql
EVAL [column1 =] value1[, ..., [columnN =] valueN]
```

**Parameters**

`columnX`
:   The column name.
    If a column with the same name already exists, the existing column is dropped.
    If a column name is used more than once, only the rightmost duplicate creates a column.

`valueX`
:   The value for the column. Can be a literal, an expression, or a
    [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).
    Can use columns defined left of this one.

**Description**

The `EVAL` processing command enables you to append new columns with calculated
values. `EVAL` supports various functions for calculating values. Refer to
[Functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions) for more information.

**Examples**

:::{include} ../examples/eval.csv-spec/eval.md
:::

If the specified column already exists, the existing column will be dropped, and
the new column will be appended to the table:

:::{include} ../examples/eval.csv-spec/evalReplace.md
:::

Specifying the output column name is optional. If not specified, the new column
name is equal to the expression. The following query adds a column named
`height*3.281`:

:::{include} ../examples/eval.csv-spec/evalUnnamedColumn.md
:::

Because this name contains special characters,
[it needs to be quoted](/reference/query-languages/esql/esql-syntax.md#esql-identifiers)
with backticks (```) when using it in subsequent commands:

:::{include} ../examples/eval.csv-spec/evalUnnamedColumnStats.md
:::
