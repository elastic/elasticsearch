## `CBRT` [esql-cbrt]

**Syntax**

:::{image} ../../../../../images/cbrt.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the cube root of a number. The input can be any numeric value, the return value is always a double. Cube roots of infinities are null.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 1000.0
| EVAL c = cbrt(d)
```

| d: double | c:double |
| --- | --- |
| 1000.0 | 10.0 |


