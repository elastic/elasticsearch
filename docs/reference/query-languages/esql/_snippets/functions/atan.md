## `ATAN` [esql-atan]

**Syntax**

:::{image} ../../../../../images/atan.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [arctangent](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) of the input numeric expression as an angle, expressed in radians.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=12.9
| EVAL atan=ATAN(a)
```

| a:double | atan:double |
| --- | --- |
| 12.9 | 1.4934316673669235 |


