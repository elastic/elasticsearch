## `ROW` [esql-row]

The `ROW` source command produces a row with one or more columns with values that you specify. This can be useful for testing.

**Syntax**

```esql
ROW column1 = value1[, ..., columnN = valueN]
```

**Parameters**

`columnX`
:   The column name. In case of duplicate column names, only the rightmost duplicate creates a column.

`valueX`
:   The value for the column. Can be a literal, an expression, or a [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).

**Examples**

```esql
ROW a = 1, b = "two", c = null
```

| a:integer | b:keyword | c:null |
| --- | --- | --- |
| 1 | "two" | null |

Use square brackets to create multi-value columns:

```esql
ROW a = [2, 1]
```

`ROW` supports the use of [functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions):

```esql
ROW a = ROUND(1.23, 0)
```


