## `EVAL` [esql-eval]

The `EVAL` processing command enables you to append new columns with calculated values.

**Syntax**

```esql
EVAL [column1 =] value1[, ..., [columnN =] valueN]
```

**Parameters**

`columnX`
:   The column name. If a column with the same name already exists, the existing column is dropped. If a column name is used more than once, only the rightmost duplicate creates a column.

`valueX`
:   The value for the column. Can be a literal, an expression, or a [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions). Can use columns defined left of this one.

**Description**

The `EVAL` processing command enables you to append new columns with calculated values. `EVAL` supports various functions for calculating values. Refer to [Functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions) for more information.

**Examples**

```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height_feet = height * 3.281, height_cm = height * 100
```

| first_name:keyword | last_name:keyword | height:double | height_feet:double | height_cm:double |
| --- | --- | --- | --- | --- |
| Georgi | Facello | 2.03 | 6.66043 | 202.99999999999997 |
| Bezalel | Simmel | 2.08 | 6.82448 | 208.0 |
| Parto | Bamford | 1.83 | 6.004230000000001 | 183.0 |

If the specified column already exists, the existing column will be dropped, and the new column will be appended to the table:

```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height = height * 3.281
```

| first_name:keyword | last_name:keyword | height:double |
| --- | --- | --- |
| Georgi | Facello | 6.66043 |
| Bezalel | Simmel | 6.82448 |
| Parto | Bamford | 6.004230000000001 |

Specifying the output column name is optional. If not specified, the new column name is equal to the expression. The following query adds a column named `height*3.281`:

```esql
FROM employees
| SORT emp_no
| KEEP first_name, last_name, height
| EVAL height * 3.281
```

| first_name:keyword | last_name:keyword | height:double | height * 3.281:double |
| --- | --- | --- | --- |
| Georgi | Facello | 2.03 | 6.66043 |
| Bezalel | Simmel | 2.08 | 6.82448 |
| Parto | Bamford | 1.83 | 6.004230000000001 |

Because this name contains special characters, [it needs to be quoted](/reference/query-languages/esql/esql-syntax.md#esql-identifiers) with backticks (```) when using it in subsequent commands:

```esql
FROM employees
| EVAL height * 3.281
| STATS avg_height_feet = AVG(`height * 3.281`)
```

| avg_height_feet:double |
| --- |
| 5.801464200000001 |


