## `TAN` [esql-tan]

**Syntax**

:::{image} ../../../../../images/tan.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`angle`
:   An angle, in radians. If `null`, the function returns `null`.

**Description**

Returns the [tangent](https://en.wikipedia.org/wiki/Sine_and_cosine) of an angle.

**Supported types**

| angle | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=1.8
| EVAL tan=TAN(a)
```

| a:double | tan:double |
| --- | --- |
| 1.8 | -4.286261674628062 |


