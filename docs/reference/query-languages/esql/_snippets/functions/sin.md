## `SIN` [esql-sin]

**Syntax**

:::{image} ../../../../../images/sin.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`angle`
:   An angle, in radians. If `null`, the function returns `null`.

**Description**

Returns the [sine](https://en.wikipedia.org/wiki/Sine_and_cosine) of an angle.

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
| EVAL sin=SIN(a)
```

| a:double | sin:double |
| --- | --- |
| 1.8 | 0.9738476308781951 |


