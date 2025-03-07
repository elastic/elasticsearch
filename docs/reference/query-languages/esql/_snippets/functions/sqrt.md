## `SQRT` [esql-sqrt]

**Syntax**

:::{image} ../../../../../images/sqrt.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the square root of a number. The input can be any numeric value, the return value is always a double. Square roots of negative numbers and infinities are null.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 100.0
| EVAL s = SQRT(d)
```

| d: double | s:double |
| --- | --- |
| 100.0 | 10.0 |


