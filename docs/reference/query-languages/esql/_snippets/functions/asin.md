## `ASIN` [esql-asin]

**Syntax**

:::{image} ../../../../../images/asin.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Number between -1 and 1. If `null`, the function returns `null`.

**Description**

Returns the [arcsine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) of the input numeric expression as an angle, expressed in radians.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=.9
| EVAL asin=ASIN(a)
```

| a:double | asin:double |
| --- | --- |
| .9 | 1.1197695149986342 |


