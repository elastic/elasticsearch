## `LOG10` [esql-log10]

**Syntax**

:::{image} ../../../../../images/log10.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the logarithm of a value to base 10. The input can be any numeric value, the return value is always a double.  Logs of 0 and negative numbers return `null` as well as a warning.

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
| EVAL s = LOG10(d)
```

| d: double | s:double |
| --- | --- |
| 1000.0 | 3.0 |


