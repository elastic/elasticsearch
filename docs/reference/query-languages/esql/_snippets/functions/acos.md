## `ACOS` [esql-acos]

**Syntax**

:::{image} ../../../../../images/acos.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Number between -1 and 1. If `null`, the function returns `null`.

**Description**

Returns the [arccosine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) of `n` as an angle, expressed in radians.

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
| EVAL acos=ACOS(a)
```

| a:double | acos:double |
| --- | --- |
| .9 | 0.45102681179626236 |


