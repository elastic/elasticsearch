## {{esql}} mathematical functions [esql-math-functions]


{{esql}} supports these mathematical functions:

:::{include} lists/math-functions.md
:::

## `ABS` [esql-abs]

**Syntax**

:::{image} ../../../../images/abs.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the absolute value.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Examples**

```esql
ROW number = -1.0
| EVAL abs_number = ABS(number)
```

| number:double | abs_number:double |
| --- | --- |
| -1.0 | 1.0 |

```esql
FROM employees
| KEEP first_name, last_name, height
| EVAL abs_height = ABS(0.0 - height)
```

| first_name:keyword | last_name:keyword | height:double | abs_height:double |
| --- | --- | --- | --- |
| Alejandro | McAlpine | 1.48 | 1.48 |
| Amabile | Gomatam | 2.09 | 2.09 |
| Anneke | Preusig | 1.56 | 1.56 |


## `ACOS` [esql-acos]

**Syntax**

:::{image} ../../../../images/acos.svg
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


## `ASIN` [esql-asin]

**Syntax**

:::{image} ../../../../images/asin.svg
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


## `ATAN` [esql-atan]

**Syntax**

:::{image} ../../../../images/atan.svg
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


## `ATAN2` [esql-atan2]

**Syntax**

:::{image} ../../../../images/atan2.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`y_coordinate`
:   y coordinate. If `null`, the function returns `null`.

`x_coordinate`
:   x coordinate. If `null`, the function returns `null`.

**Description**

The [angle](https://en.wikipedia.org/wiki/Atan2) between the positive x-axis and the ray from the origin to the point (x , y) in the Cartesian plane, expressed in radians.

**Supported types**

| y_coordinate | x_coordinate | result |
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
ROW y=12.9, x=.6
| EVAL atan2=ATAN2(y, x)
```

| y:double | x:double | atan2:double |
| --- | --- | --- |
| 12.9 | 0.6 | 1.5243181954438936 |


## `CBRT` [esql-cbrt]

**Syntax**

:::{image} ../../../../images/cbrt.svg
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


## `CEIL` [esql-ceil]

**Syntax**

:::{image} ../../../../images/ceil.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Round a number up to the nearest integer.

::::{note}
This is a noop for `long` (including unsigned) and `integer`. For `double` this picks the closest `double` value to the integer similar to [Math.ceil](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Math.md#ceil(double)).
::::


**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW a=1.8
| EVAL a=CEIL(a)
```

| a:double |
| --- |
| 2 |


## `COS` [esql-cos]

**Syntax**

:::{image} ../../../../images/cos.svg
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


## `COSH` [esql-cosh]

**Syntax**

:::{image} ../../../../images/cosh.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [hyperbolic cosine](https://en.wikipedia.org/wiki/Hyperbolic_functions) of a number.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=1.8
| EVAL cosh=COSH(a)
```

| a:double | cosh:double |
| --- | --- |
| 1.8 | 3.1074731763172667 |


## `E` [esql-e]

**Syntax**

:::{image} ../../../../images/e.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

**Description**

Returns [Euler’s number](https://en.wikipedia.org/wiki/E_(mathematical_constant)).

**Supported types**

| result |
| --- |
| double |

**Example**

```esql
ROW E()
```

| E():double |
| --- |
| 2.718281828459045 |


## `EXP` [esql-exp]

**Syntax**

:::{image} ../../../../images/exp.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the value of e raised to the power of the given number.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 5.0
| EVAL s = EXP(d)
```

| d: double | s:double |
| --- | --- |
| 5.0 | 148.413159102576603 |


## `FLOOR` [esql-floor]

**Syntax**

:::{image} ../../../../images/floor.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Round a number down to the nearest integer.

::::{note}
This is a noop for `long` (including unsigned) and `integer`. For `double` this picks the closest `double` value to the integer similar to [Math.floor](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Math.md#floor(double)).
::::


**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW a=1.8
| EVAL a=FLOOR(a)
```

| a:double |
| --- |
| 1 |


## `HYPOT` [esql-hypot]

**Syntax**

:::{image} ../../../../images/hypot.svg
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


## `LOG` [esql-log]

**Syntax**

:::{image} ../../../../images/log.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`base`
:   Base of logarithm. If `null`, the function returns `null`. If not provided, this function returns the natural logarithm (base e) of a value.

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the logarithm of a value to a base. The input can be any numeric value, the return value is always a double.  Logs of zero, negative numbers, and base of one return `null` as well as a warning.

**Supported types**

| base | number | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| double | unsigned_long | double |
| double |  | double |
| integer | double | double |
| integer | integer | double |
| integer | long | double |
| integer | unsigned_long | double |
| integer |  | double |
| long | double | double |
| long | integer | double |
| long | long | double |
| long | unsigned_long | double |
| long |  | double |
| unsigned_long | double | double |
| unsigned_long | integer | double |
| unsigned_long | long | double |
| unsigned_long | unsigned_long | double |
| unsigned_long |  | double |

**Examples**

```esql
ROW base = 2.0, value = 8.0
| EVAL s = LOG(base, value)
```

| base: double | value: double | s:double |
| --- | --- | --- |
| 2.0 | 8.0 | 3.0 |

```esql
row value = 100
| EVAL s = LOG(value);
```

| value: integer | s:double |
| --- | --- |
| 100 | 4.605170185988092 |


## `LOG10` [esql-log10]

**Syntax**

:::{image} ../../../../images/log10.svg
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


## `PI` [esql-pi]

**Syntax**

:::{image} ../../../../images/pi.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

**Description**

Returns [Pi](https://en.wikipedia.org/wiki/Pi), the ratio of a circle’s circumference to its diameter.

**Supported types**

| result |
| --- |
| double |

**Example**

```esql
ROW PI()
```

| PI():double |
| --- |
| 3.141592653589793 |


## `POW` [esql-pow]

**Syntax**

:::{image} ../../../../images/pow.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`base`
:   Numeric expression for the base. If `null`, the function returns `null`.

`exponent`
:   Numeric expression for the exponent. If `null`, the function returns `null`.

**Description**

Returns the value of `base` raised to the power of `exponent`.

::::{note}
It is still possible to overflow a double result here; in that case, null will be returned.
::::


**Supported types**

| base | exponent | result |
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

**Examples**

```esql
ROW base = 2.0, exponent = 2
| EVAL result = POW(base, exponent)
```

| base:double | exponent:integer | result:double |
| --- | --- | --- |
| 2.0 | 2 | 4.0 |

The exponent can be a fraction, which is similar to performing a root. For example, the exponent of `0.5` will give the square root of the base:

```esql
ROW base = 4, exponent = 0.5
| EVAL s = POW(base, exponent)
```

| base:integer | exponent:double | s:double |
| --- | --- | --- |
| 4 | 0.5 | 2.0 |


## `ROUND` [esql-round]

**Syntax**

:::{image} ../../../../images/round.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   The numeric value to round. If `null`, the function returns `null`.

`decimals`
:   The number of decimal places to round to. Defaults to 0. If `null`, the function returns `null`.

**Description**

Rounds a number to the specified number of decimal places. Defaults to 0, which returns the nearest integer. If the precision is a negative number, rounds to the number of digits left of the decimal point.

**Supported types**

| number | decimals | result |
| --- | --- | --- |
| double | integer | double |
| double | long | double |
| double |  | double |
| integer | integer | integer |
| integer | long | integer |
| integer |  | integer |
| long | integer | long |
| long | long | long |
| long |  | long |
| unsigned_long | integer | unsigned_long |
| unsigned_long | long | unsigned_long |
| unsigned_long |  | unsigned_long |

**Example**

```esql
FROM employees
| KEEP first_name, last_name, height
| EVAL height_ft = ROUND(height * 3.281, 1)
```

| first_name:keyword | last_name:keyword | height:double | height_ft:double |
| --- | --- | --- | --- |
| Arumugam | Ossenbruggen | 2.1 | 6.9 |
| Kwee | Schusler | 2.1 | 6.9 |
| Saniya | Kalloufi | 2.1 | 6.9 |


## `SIGNUM` [esql-signum]

**Syntax**

:::{image} ../../../../images/signum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the sign of the given number. It returns `-1` for negative numbers, `0` for `0` and `1` for positive numbers.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 100.0
| EVAL s = SIGNUM(d)
```

| d: double | s:double |
| --- | --- |
| 100 | 1.0 |


## `SIN` [esql-sin]

**Syntax**

:::{image} ../../../../images/sin.svg
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


## `SINH` [esql-sinh]

**Syntax**

:::{image} ../../../../images/sinh.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [hyperbolic sine](https://en.wikipedia.org/wiki/Hyperbolic_functions) of a number.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=1.8
| EVAL sinh=SINH(a)
```

| a:double | sinh:double |
| --- | --- |
| 1.8 | 2.94217428809568 |


## `SQRT` [esql-sqrt]

**Syntax**

:::{image} ../../../../images/sqrt.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the square root of a number. The input can be any numeric value, the return value is always a double. Square roots of negative numbers and infinities are null.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW d = 100.0
| EVAL s = SQRT(d)
```

| d: double | s:double |
| --- | --- |
| 100.0 | 10.0 |


## `TAN` [esql-tan]

**Syntax**

:::{image} ../../../../images/tan.svg
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


## `TANH` [esql-tanh]

**Syntax**

:::{image} ../../../../images/tanh.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Numeric expression. If `null`, the function returns `null`.

**Description**

Returns the [hyperbolic tangent](https://en.wikipedia.org/wiki/Hyperbolic_functions) of a number.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=1.8
| EVAL tanh=TANH(a)
```

| a:double | tanh:double |
| --- | --- |
| 1.8 | 0.9468060128462683 |


## `TAU` [esql-tau]

**Syntax**

:::{image} ../../../../images/tau.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

**Description**

Returns the [ratio](https://tauday.com/tau-manifesto) of a circle’s circumference to its radius.

**Supported types**

| result |
| --- |
| double |

**Example**

```esql
ROW TAU()
```

| TAU():double |
| --- |
| 6.283185307179586 |
