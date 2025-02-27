## `TO_RADIANS` [esql-to_radians]

**Syntax**

:::{image} ../../../../../images/to_radians.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts a number in [degrees](https://en.wikipedia.org/wiki/Degree_(angle)) to [radians](https://en.wikipedia.org/wiki/Radian).

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW deg = [90.0, 180.0, 270.0]
| EVAL rad = TO_RADIANS(deg)
```

| deg:double | rad:double |
| --- | --- |
| [90.0, 180.0, 270.0] | [1.5707963267948966, 3.141592653589793, 4.71238898038469] |


