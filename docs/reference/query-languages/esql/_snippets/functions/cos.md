## `COS` [esql-cos]

**Syntax**

:::{image} ../../../../../images/cos.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`angle`
:   An angle, in radians. If `null`, the function returns `null`.

**Description**

Returns the [cosine](https://en.wikipedia.org/wiki/Sine_and_cosine) of an angle.

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
| EVAL cos=COS(a)
```

| a:double | cos:double |
| --- | --- |
| 1.8 | -0.2272020946930871 |


