---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-math.html
---

# Mathematical functions [sql-functions-math]

All math and trigonometric functions require their input (where applicable) to be numeric.


## Generic [sql-functions-math-generic]

## `ABS` [sql-functions-math-abs]

```sql
ABS(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: numeric

**Description**: Returns the [absolute value](https://en.wikipedia.org/wiki/Absolute_value) of `numeric_exp`. The return type is the same as the input type.

```sql
SELECT ABS(-123.5), ABS(55);

  ABS(-123.5)  |    ABS(55)
---------------+---------------
123.5          |55
```


## `CBRT` [sql-functions-math-cbrt]

```sql
CBRT(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [cube root](https://en.wikipedia.org/wiki/Cube_root) of `numeric_exp`.

```sql
SELECT CBRT(-125.5);

   CBRT(-125.5)
-------------------
-5.0066577974783435
```


## `CEIL/CEILING` [sql-functions-math-ceil]

```sql
CEIL(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: integer or long numeric value

**Description**: Returns the smallest integer greater than or equal to `numeric_exp`.

```sql
SELECT CEIL(125.01), CEILING(-125.99);

 CEIL(125.01)  |CEILING(-125.99)
---------------+----------------
126            |-125
```


## `E` [sql-functions-math-e]

```sql
E()
```

**Input**: *none*

**Output**: `2.718281828459045`

**Description**: Returns [Euler’s number](https://en.wikipedia.org/wiki/E_%28mathematical_constant%29).

```sql
SELECT E(), CEIL(E());

       E()       |   CEIL(E())
-----------------+---------------
2.718281828459045|3
```


## `EXP` [sql-functions-math-exp]

```sql
EXP(numeric_exp) <1>
```

**Input**:

1. float numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns [Euler’s number at the power](https://en.wikipedia.org/wiki/Exponential_function) of `numeric_exp` enumeric_exp.

```sql
SELECT EXP(1), E(), EXP(2), E() * E();

     EXP(1)      |       E()       |     EXP(2)     |     E() * E()
-----------------+-----------------+----------------+------------------
2.718281828459045|2.718281828459045|7.38905609893065|7.3890560989306495
```


## `EXPM1` [sql-functions-math-expm1]

```sql
EXPM1(numeric_exp) <1>
```

**Input**:

1. float numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns [Euler’s number at the power](https://en.wikipedia.org/wiki/Exponential_function) of `numeric_exp` minus 1 (enumeric_exp - 1).

```sql
SELECT E(), EXP(2), EXPM1(2);

       E()       |     EXP(2)     |    EXPM1(2)
-----------------+----------------+----------------
2.718281828459045|7.38905609893065|6.38905609893065
```


## `FLOOR` [sql-functions-math-floor]

```sql
FLOOR(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: integer or long numeric value

**Description**: Returns the largest integer less than or equal to `numeric_exp`.

```sql
SELECT FLOOR(125.01), FLOOR(-125.99);

 FLOOR(125.01) |FLOOR(-125.99)
---------------+---------------
125            |-126
```


## `LOG` [sql-functions-math-log]

```sql
LOG(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [natural logarithm](https://en.wikipedia.org/wiki/Natural_logarithm) of `numeric_exp`.

```sql
SELECT EXP(3), LOG(20.085536923187668);

      EXP(3)      |LOG(20.085536923187668)
------------------+-----------------------
20.085536923187668|3.0
```


## `LOG10` [sql-functions-math-log10]

```sql
LOG10(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [base 10 logarithm](https://en.wikipedia.org/wiki/Common_logarithm) of `numeric_exp`.

```sql
SELECT LOG10(5), LOG(5)/LOG(10);

     LOG10(5)     |    LOG(5)/LOG(10)
------------------+-----------------------
0.6989700043360189|0.6989700043360187
```


## `PI` [sql-functions-math-pi]

```sql
PI()
```

**Input**: *none*

**Output**: `3.141592653589793`

**Description**: Returns [PI number](https://en.wikipedia.org/wiki/Pi).

```sql
SELECT PI();

      PI()
-----------------
3.141592653589793
```


## `POWER` [sql-functions-math-power]

```sql
POWER(
    numeric_exp, <1>
    integer_exp) <2>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.
2. integer expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the value of `numeric_exp` to the power of `integer_exp`.

```sql
SELECT POWER(3, 2), POWER(3, 3);

  POWER(3, 2)  |  POWER(3, 3)
---------------+---------------
9.0            |27.0
```

```sql
SELECT POWER(5, -1), POWER(5, -2);

  POWER(5, -1) |  POWER(5, -2)
---------------+---------------
0.2            |0.04
```


## `RANDOM/RAND` [sql-functions-math-random]

```sql
RANDOM(seed) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns a random double using the given seed.

```sql
SELECT RANDOM(123);

   RANDOM(123)
------------------
0.7231742029971469
```


## `ROUND` [sql-functions-math-round]

```sql
ROUND(
    numeric_exp      <1>
    [, integer_exp]) <2>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.
2. integer expression; optional. If `null`, the function returns `null`.


**Output**: numeric

**Description**: Returns `numeric_exp` rounded to `integer_exp` places right of the decimal point. If `integer_exp` is negative, `numeric_exp` is rounded to |`integer_exp`| places to the left of the decimal point. If `integer_exp` is omitted, the function will perform as if `integer_exp` would be 0. The returned numeric data type is the same as the data type of `numeric_exp`.

```sql
SELECT ROUND(-345.153, 1) AS rounded;

    rounded
---------------
-345.2
```

```sql
SELECT ROUND(-345.153, -1) AS rounded;

    rounded
---------------
-350.0
```


## `SIGN/SIGNUM` [sql-functions-math-sign]

```sql
SIGN(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: [-1, 0, 1]

**Description**: Returns an indicator of the sign of `numeric_exp`. If `numeric_exp` is less than zero, –1 is returned. If `numeric_exp` equals zero, 0 is returned. If `numeric_exp` is greater than zero, 1 is returned.

```sql
SELECT SIGN(-123), SIGN(0), SIGN(415);

  SIGN(-123)   |    SIGN(0)    |   SIGN(415)
---------------+---------------+---------------
-1             |0              |1
```


## `SQRT` [sql-functions-math-sqrt]

```sql
SQRT(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns [square root](https://en.wikipedia.org/wiki/Square_root) of `numeric_exp`.

```sql
SELECT SQRT(EXP(2)), E(), SQRT(25);

  SQRT(EXP(2))   |       E()       |   SQRT(25)
-----------------+-----------------+---------------
2.718281828459045|2.718281828459045|5.0
```


## `TRUNCATE/TRUNC` [sql-functions-math-truncate]

```sql
TRUNCATE(
    numeric_exp      <1>
    [, integer_exp]) <2>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.
2. integer expression; optional. If `null`, the function returns `null`.


**Output**: numeric

**Description**: Returns `numeric_exp` truncated to `integer_exp` places right of the decimal point. If `integer_exp` is negative, `numeric_exp` is truncated to |`integer_exp`| places to the left of the decimal point. If `integer_exp` is omitted, the function will perform as if `integer_exp` would be 0. The returned numeric data type is the same as the data type of `numeric_exp`.

```sql
SELECT TRUNC(-345.153, 1) AS trimmed;

    trimmed
---------------
-345.1
```

```sql
SELECT TRUNCATE(-345.153, -1) AS trimmed;

    trimmed
---------------
-340.0
```


## Trigonometric [sql-functions-math-trigonometric]


## `ACOS` [sql-functions-math-acos]

```sql
ACOS(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [arccosine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) of `numeric_exp` as an angle, expressed in radians.

```sql
SELECT ACOS(COS(PI())), PI();

 ACOS(COS(PI())) |      PI()
-----------------+-----------------
3.141592653589793|3.141592653589793
```


## `ASIN` [sql-functions-math-asin]

```sql
ASIN(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [arcsine](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) of `numeric_exp` as an angle, expressed in radians.

```sql
SELECT ROUND(DEGREES(ASIN(0.7071067811865475))) AS "ASIN(0.707)", ROUND(SIN(RADIANS(45)), 3) AS "SIN(45)";

  ASIN(0.707)  |    SIN(45)
---------------+---------------
45.0           |0.707
```


## `ATAN` [sql-functions-math-atan]

```sql
ATAN(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [arctangent](https://en.wikipedia.org/wiki/Inverse_trigonometric_functions) of `numeric_exp` as an angle, expressed in radians.

```sql
SELECT DEGREES(ATAN(TAN(RADIANS(90))));

DEGREES(ATAN(TAN(RADIANS(90))))
-------------------------------
90.0
```


## `ATAN2` [sql-functions-math-atan2]

```sql
ATAN2(
    ordinate, <1>
    abscisa)  <2>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.
2. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [arctangent of the `ordinate` and `abscisa` coordinates](https://en.wikipedia.org/wiki/Atan2) specified as an angle, expressed in radians.

```sql
SELECT ATAN2(5 * SIN(RADIANS(45)), 5 * COS(RADIANS(45))) AS "ATAN2(5*SIN(45), 5*COS(45))", RADIANS(45);

ATAN2(5*SIN(45), 5*COS(45))|   RADIANS(45)
---------------------------+------------------
0.7853981633974483         |0.7853981633974483
```


## `COS` [sql-functions-math-cos]

```sql
COS(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [cosine](https://en.wikipedia.org/wiki/Trigonometric_functions#cosine) of `numeric_exp`, where `numeric_exp` is an angle expressed in radians.

```sql
SELECT COS(RADIANS(180)), POWER(SIN(RADIANS(54)), 2) + POWER(COS(RADIANS(54)), 2) AS pythagorean_identity;

COS(RADIANS(180))|pythagorean_identity
-----------------+--------------------
-1.0             |1.0
```


## `COSH` [sql-functions-math-cosh]

```sql
COSH(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [hyperbolic cosine](https://en.wikipedia.org/wiki/Hyperbolic_function) of `numeric_exp`.

```sql
SELECT COSH(5), (POWER(E(), 5) + POWER(E(), -5)) / 2 AS "(e^5 + e^-5)/2";

     COSH(5)     | (e^5 + e^-5)/2
-----------------+-----------------
74.20994852478785|74.20994852478783
```


## `COT` [sql-functions-math-cot]

```sql
COT(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [cotangent](https://en.wikipedia.org/wiki/Trigonometric_functions#Cosecant,_secant,_and_cotangent) of `numeric_exp`, where `numeric_exp` is an angle expressed in radians.

```sql
SELECT COT(RADIANS(30)) AS "COT(30)", COS(RADIANS(30)) / SIN(RADIANS(30)) AS "COS(30)/SIN(30)";

     COT(30)      | COS(30)/SIN(30)
------------------+------------------
1.7320508075688774|1.7320508075688776
```


## `DEGREES` [sql-functions-math-degrees]

```sql
DEGREES(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Convert from [radians](https://en.wikipedia.org/wiki/Radian) to [degrees](https://en.wikipedia.org/wiki/Degree_(angle)).

```sql
SELECT DEGREES(PI() * 2), DEGREES(PI());

DEGREES(PI() * 2)| DEGREES(PI())
-----------------+---------------
360.0            |180.0
```


## `RADIANS` [sql-functions-math-radians]

```sql
RADIANS(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Convert from [degrees](https://en.wikipedia.org/wiki/Degree_(angle)) to [radians](https://en.wikipedia.org/wiki/Radian).

```sql
SELECT RADIANS(90), PI()/2;

   RADIANS(90)    |      PI()/2
------------------+------------------
1.5707963267948966|1.5707963267948966
```


## `SIN` [sql-functions-math-sin]

```sql
SIN(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [sine](https://en.wikipedia.org/wiki/Trigonometric_functions#sine) of `numeric_exp`, where `numeric_exp` is an angle expressed in radians.

```sql
SELECT SIN(RADIANS(90)), POWER(SIN(RADIANS(67)), 2) + POWER(COS(RADIANS(67)), 2) AS pythagorean_identity;

SIN(RADIANS(90))|pythagorean_identity
----------------+--------------------
1.0             |1.0
```


## `SINH` [sql-functions-math-sinh]

```sql
SINH(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [hyperbolic sine](https://en.wikipedia.org/wiki/Hyperbolic_function) of `numeric_exp`.

```sql
SELECT SINH(5), (POWER(E(), 5) - POWER(E(), -5)) / 2 AS "(e^5 - e^-5)/2";

     SINH(5)     | (e^5 - e^-5)/2
-----------------+-----------------
74.20321057778875|74.20321057778874
```


## `TAN` [sql-functions-math-tan]

```sql
TAN(numeric_exp) <1>
```

**Input**:

1. numeric expression. If `null`, the function returns `null`.


**Output**: double numeric value

**Description**: Returns the [tangent](https://en.wikipedia.org/wiki/Trigonometric_functions#tangent) of `numeric_exp`, where `numeric_exp` is an angle expressed in radians.

```sql
SELECT TAN(RADIANS(66)) AS "TAN(66)", SIN(RADIANS(66))/COS(RADIANS(66)) AS "SIN(66)/COS(66)=TAN(66)";

     TAN(66)      |SIN(66)/COS(66)=TAN(66)
------------------+-----------------------
2.2460367739042164|2.246036773904216
```


