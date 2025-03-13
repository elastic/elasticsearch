## `HYPOT` [esql-hypot]

**Syntax**

:::{image} ../../../../../images/hypot.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number1`
:   Numeric expression. If `null`, the function returns `null`.

`number2`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the hypotenuse of two numbers. The input can be any numeric values, the return value is always a double. Hypotenuses of infinities are null.

**Supported types**

| number1 | number2 | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| double | unsigned_long | double |
| integer | double | double |
| integer | integer | double |
| integer | long | double |
| integer | unsigned_long | double |
| long | double | double |
| long | integer | double |
| long | long | double |
| long | unsigned_long | double |
| unsigned_long | double | double |
| unsigned_long | integer | double |
| unsigned_long | long | double |
| unsigned_long | unsigned_long | double |

**Example**

```esql
ROW a = 3.0, b = 4.0
| EVAL c = HYPOT(a, b)
```

| a:double | b:double | c:double |
| --- | --- | --- |
| 3.0 | 4.0 | 5.0 |


