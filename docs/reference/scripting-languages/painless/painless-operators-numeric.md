---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators-numeric.html
---

# Operators: Numeric [painless-operators-numeric]

## Post Increment [post-increment-operator]

Use the `post increment operator '++'` to INCREASE the value of a numeric type variable/field by `1`. An extra implicit cast is necessary to return the promoted numeric type value to the original numeric type value of the variable/field for the following types: `byte`, `short`, and `char`. If a variable/field is read as part of an expression the value is loaded prior to the increment.

**Errors**

* If the variable/field is a non-numeric type.

**Grammar**

```text
post_increment: ( variable | field ) '++';
```

**Promotion**

| original | promoted | implicit |
| --- | --- | --- |
| byte | int | byte |
| short | int | short |
| char | int | char |
| int | int |  |
| long | long |  |
| float | float |  |
| double | double |  |
| def | def |  |

**Examples**

* Post increment with different numeric types.

    ```painless
    short i = 0; <1>
    i++;         <2>
    long j = 1;  <3>
    long k;      <4>
    k = j++;     <5>
    ```

    1. declare `short i`; store `short 0` to `i`
    2. load from `i` → `short 0`; promote `short 0`: result `int`; add `int 0` and `int 1` → `int 1`; implicit cast `int 1` to `short 1`; store `short 1` to `i`
    3. declare `long j`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `j`
    4. declare `long k`; store default `long 0` to `k`
    5. load from `j` → `long 1`; store `long 1` to `k`; add `long 1` and `long 1` → `long 2`; store `long 2` to `j`

* Post increment with the `def` type.

    ```painless
    def x = 1; <1>
    x++;       <2>
    ```

    1. declare `def x`; implicit cast `int 1` to `def` → `def`; store `def` to `x`
    2. load from `x` → `def`; implicit cast `def` to `int 1`; add `int 1` and `int 1` → `int 2`; implicit cast `int 2` to `def`; store `def` to `x`



## Post Decrement [post-decrement-operator]

Use the `post decrement operator '--'` to DECREASE the value of a numeric type variable/field by `1`. An extra implicit cast is necessary to return the promoted numeric type value to the original numeric type value of the variable/field for the following types: `byte`, `short`, and `char`. If a variable/field is read as part of an expression the value is loaded prior to the decrement.

**Errors**

* If the variable/field is a non-numeric type.

**Grammar**

```text
post_decrement: ( variable | field ) '--';
```

**Promotion**

| original | promoted | implicit |
| --- | --- | --- |
| byte | int | byte |
| short | int | short |
| char | int | char |
| int | int |  |
| long | long |  |
| float | float |  |
| double | double |  |
| def | def |  |

**Examples**

* Post decrement with different numeric types.

    ```painless
    short i = 0; <1>
    i--;         <2>
    long j = 1;  <3>
    long k;      <4>
    k = j--;     <5>
    ```

    1. declare `short i`; store `short 0` to `i`
    2. load from `i` → `short 0`; promote `short 0`: result `int`; subtract `int 1` from `int 0` → `int -1`; implicit cast `int -1` to `short -1`; store `short -1` to `i`
    3. declare `long j`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `j`
    4. declare `long k`; store default `long 0` to `k`
    5. load from `j` → `long 1`; store `long 1` to `k`; subtract `long 1` from `long 1` → `long 0`; store `long 0` to `j`

* Post decrement with the `def` type.

    ```painless
    def x = 1; <1>
    x--;       <2>
    ```

    1. declare `def x`; implicit cast `int 1` to `def` → `def`; store `def` to `x`
    2. load from `x` → `def`; implicit cast `def` to `int 1`; subtract `int 1` from `int 1` → `int 0`; implicit cast `int 0` to `def`; store `def` to `x`



## Pre Increment [pre-increment-operator]

Use the `pre increment operator '++'` to INCREASE the value of a numeric type variable/field by `1`. An extra implicit cast is necessary to return the promoted numeric type value to the original numeric type value of the variable/field for the following types: `byte`, `short`, and `char`. If a variable/field is read as part of an expression the value is loaded after the increment.

**Errors**

* If the variable/field is a non-numeric type.

**Grammar**

```text
pre_increment: '++' ( variable | field );
```

**Promotion**

| original | promoted | implicit |
| --- | --- | --- |
| byte | int | byte |
| short | int | short |
| char | int | char |
| int | int |  |
| long | long |  |
| float | float |  |
| double | double |  |
| def | def |  |

**Examples**

* Pre increment with different numeric types.

    ```painless
    short i = 0; <1>
    ++i;         <2>
    long j = 1;  <3>
    long k;      <4>
    k = ++j;     <5>
    ```

    1. declare `short i`; store `short 0` to `i`
    2. load from `i` → `short 0`; promote `short 0`: result `int`; add `int 0` and `int 1` → `int 1`; implicit cast `int 1` to `short 1`; store `short 1` to `i`
    3. declare `long j`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `j`
    4. declare `long k`; store default `long 0` to `k`
    5. load from `j` → `long 1`; add `long 1` and `long 1` → `long 2`; store `long 2` to `j`; store `long 2` to `k`

* Pre increment with the `def` type.

    ```painless
    def x = 1; <1>
    ++x;       <2>
    ```

    1. declare `def x`; implicit cast `int 1` to `def` → `def`; store `def` to `x`
    2. load from `x` → `def`; implicit cast `def` to `int 1`; add `int 1` and `int 1` → `int 2`; implicit cast `int 2` to `def`; store `def` to `x`



## Pre Decrement [pre-decrement-operator]

Use the `pre decrement operator '--'` to DECREASE the value of a numeric type variable/field by `1`. An extra implicit cast is necessary to return the promoted numeric type value to the original numeric type value of the variable/field for the following types: `byte`, `short`, and `char`. If a variable/field is read as part of an expression the value is loaded after the decrement.

**Errors**

* If the variable/field is a non-numeric type.

**Grammar**

```text
pre_decrement: '--' ( variable | field );
```

**Promotion**

| original | promoted | implicit |
| --- | --- | --- |
| byte | int | byte |
| short | int | short |
| char | int | char |
| int | int |  |
| long | long |  |
| float | float |  |
| double | double |  |
| def | def |  |

**Examples**

* Pre decrement with different numeric types.

    ```painless
    short i = 0; <1>
    --i;         <2>
    long j = 1;  <3>
    long k;      <4>
    k = --j;     <5>
    ```

    1. declare `short i`; store `short 0` to `i`
    2. load from `i` → `short 0`; promote `short 0`: result `int`; subtract `int 1` from `int 0` → `int -1`; implicit cast `int -1` to `short -1`; store `short -1` to `i`
    3. declare `long j`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `j`
    4. declare `long k`; store default `long 0` to `k`
    5. load from `j` → `long 1`; subtract `long 1` from `long 1` → `long 0`; store `long 0` to `j` store `long 0` to `k`;

* Pre decrement operator with the `def` type.

    ```painless
    def x = 1; <1>
    --x;       <2>
    ```

    1. declare `def x`; implicit cast `int 1` to `def` → `def`; store `def` to `x`
    2. load from `x` → `def`; implicit cast `def` to `int 1`; subtract `int 1` from `int 1` → `int 0`; implicit cast `int 0` to `def`; store `def` to `x`



## Unary Positive [unary-positive-operator]

Use the `unary positive operator '+'` to the preserve the IDENTITY of a numeric type value.

**Errors**

* If the value is a non-numeric type.

**Grammar**

```text
unary_positive: '+' expression;
```

**Examples**

* Unary positive with different numeric types.

    ```painless
    int x = +1;  <1>
    long y = +x; <2>
    ```

    1. declare `int x`; identity `int 1` → `int 1`; store `int 1` to `x`
    2. declare `long y`; load from `x` → `int 1`; identity `int 1` → `int 1`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `y`

* Unary positive with the `def` type.

    ```painless
    def z = +1; <1>
    int i = +z; <2>
    ```

    1. declare `def z`; identity `int 1` → `int 1`; implicit cast `int 1` to `def`; store `def` to `z`
    2. declare `int i`; load from `z` → `def`; implicit cast `def` to `int 1`; identity `int 1` → `int 1`; store `int 1` to `i`;



## Unary Negative [unary-negative-operator]

Use the `unary negative operator '-'` to NEGATE a numeric type value.

**Errors**

* If the value is a non-numeric type.

**Grammar**

```text
unary_negative: '-' expression;
```

**Examples**

* Unary negative with different numeric types.

    ```painless
    int x = -1;  <1>
    long y = -x; <2>
    ```

    1. declare `int x`; negate `int 1` → `int -1`; store `int -1` to `x`
    2. declare `long y`; load from `x` → `int 1`; negate `int -1` → `int 1`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `y`

* Unary negative with the `def` type.

    ```painless
    def z = -1; <1>
    int i = -z; <2>
    ```

    1. declare `def z`; negate `int 1` → `int -1`; implicit cast `int -1` to `def`; store `def` to `z`
    2. declare `int i`; load from `z` → `def`; implicit cast `def` to `int -1`; negate `int -1` → `int 1`; store `int 1` to `i`;



## Bitwise Not [bitwise-not-operator]

Use the `bitwise not operator '~'` to NOT each bit in an integer type value where a `1-bit` is flipped to a resultant `0-bit` and a `0-bit` is flipped to a resultant `1-bit`.

**Errors**

* If the value is a non-integer type.

**Bits**

| original | result |
| --- | --- |
| 1 | 0 |
| 0 | 1 |

**Grammar**

```text
bitwise_not: '~' expression;
```

**Promotion**

| original | promoted |
| --- | --- |
| byte | int |
| short | int |
| char | int |
| int | int |
| long | long |
| def | def |

**Examples**

* Bitwise not with different numeric types.

    ```painless
    byte b = 1;  <1>
    int i = ~b;  <2>
    long l = ~i; <3>
    ```

    1. declare `byte x`; store `byte 1` to b
    2. declare `int i`; load from `b` → `byte 1`; implicit cast `byte 1` to `int 1` → `int 1`; bitwise not `int 1` → `int -2`; store `int -2` to `i`
    3. declare `long l`; load from `i` → `int -2`; implicit cast `int -2` to `long -2` → `long -2`; bitwise not `long -2` → `long 1`; store `long 1` to `l`

* Bitwise not with the `def` type.

    ```painless
    def d = 1;  <1>
    def e = ~d; <2>
    ```

    1. declare `def d`; implicit cast `int 1` to `def` → `def`; store `def` to `d`;
    2. declare `def e`; load from `d` → `def`; implicit cast `def` to `int 1` → `int 1`; bitwise not `int 1` → `int -2`; implicit cast `int 1` to `def` → `def`; store `def` to `e`



## Multiplication [multiplication-operator]

Use the `multiplication operator '*'` to MULTIPLY together two numeric type values. Rules for resultant overflow and NaN values follow the JVM specification.

**Errors**

* If either of the values is a non-numeric type.

**Grammar**

```text
multiplication: expression '*' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double | def |
| byte | int | int | int | int | long | float | double | def |
| short | int | int | int | int | long | float | double | def |
| char | int | int | int | int | long | float | double | def |
| int | int | int | int | int | long | float | double | def |
| long | long | long | long | long | long | float | double | def |
| float | float | float | float | float | float | float | double | def |
| double | double | double | double | double | double | double | double | def |
| def | def | def | def | def | def | def | def | def |

**Examples**

* Multiplication with different numeric types.

    ```painless
    int i = 5*4;      <1>
    double d = i*7.0; <2>
    ```

    1. declare `int i`; multiply `int 4` by `int 5` → `int 20`; store `int 20` in `i`
    2. declare `double d`; load from `int i` → `int 20`; promote `int 20` and `double 7.0`: result `double`; implicit cast `int 20` to `double 20.0` → `double 20.0`; multiply `double 20.0` by `double 7.0` → `double 140.0`; store `double 140.0` to `d`

* Multiplication with the `def` type.

    ```painless
    def x = 5*4; <1>
    def y = x*2; <2>
    ```

    1. declare `def x`; multiply `int 5` by `int 4` → `int 20`; implicit cast `int 20` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 20`; multiply `int 20` by `int 2` → `int 40`; implicit cast `int 40` to `def` → `def`; store `def` to `y`



## Division [division-operator]

Use the `division operator '/'` to DIVIDE one numeric type value by another. Rules for NaN values and division by zero follow the JVM specification. Division with integer values drops the remainder of the resultant value.

**Errors**

* If either of the values is a non-numeric type.
* If a left-hand side integer type value is divided by a right-hand side integer type value of `0`.

**Grammar**

```text
division: expression '/' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double | def |
| byte | int | int | int | int | long | float | double | def |
| short | int | int | int | int | long | float | double | def |
| char | int | int | int | int | long | float | double | def |
| int | int | int | int | int | long | float | double | def |
| long | long | long | long | long | long | float | double | def |
| float | float | float | float | float | float | float | double | def |
| double | double | double | double | double | double | double | double | def |
| def | def | def | def | def | def | def | def | def |

**Examples**

* Division with different numeric types.

    ```painless
    int i = 29/4;     <1>
    double d = i/7.0; <2>
    ```

    1. declare `int i`; divide `int 29` by `int 4` → `int 7`; store `int 7` in `i`
    2. declare `double d`; load from `int i` → `int 7`; promote `int 7` and `double 7.0`: result `double`; implicit cast `int 7` to `double 7.0` → `double 7.0`; divide `double 7.0` by `double 7.0` → `double 1.0`; store `double 1.0` to `d`

* Division with the `def` type.

    ```painless
    def x = 5/4; <1>
    def y = x/2; <2>
    ```

    1. declare `def x`; divide `int 5` by `int 4` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 1`; divide `int 1` by `int 2` → `int 0`; implicit cast `int 0` to `def` → `def`; store `def` to `y`



## Remainder [remainder-operator]

Use the `remainder operator '%'` to calculate the REMAINDER for division between two numeric type values. Rules for NaN values and division by zero follow the JVM specification.

**Errors**

* If either of the values is a non-numeric type.

**Grammar**

```text
remainder: expression '%' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double | def |
| byte | int | int | int | int | long | float | double | def |
| short | int | int | int | int | long | float | double | def |
| char | int | int | int | int | long | float | double | def |
| int | int | int | int | int | long | float | double | def |
| long | long | long | long | long | long | float | double | def |
| float | float | float | float | float | float | float | double | def |
| double | double | double | double | double | double | double | double | def |
| def | def | def | def | def | def | def | def | def |

**Examples**

* Remainder with different numeric types.

    ```painless
    int i = 29%4;     <1>
    double d = i%7.0; <2>
    ```

    1. declare `int i`; remainder `int 29` by `int 4` → `int 1`; store `int 7` in `i`
    2. declare `double d`; load from `int i` → `int 1`; promote `int 1` and `double 7.0`: result `double`; implicit cast `int 1` to `double 1.0` → `double 1.0`; remainder `double 1.0` by `double 7.0` → `double 1.0`; store `double 1.0` to `d`

* Remainder with the `def` type.

    ```painless
    def x = 5%4; <1>
    def y = x%2; <2>
    ```

    1. declare `def x`; remainder `int 5` by `int 4` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 1`; remainder `int 1` by `int 2` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` to `y`



## Addition [addition-operator]

Use the `addition operator '+'` to ADD together two numeric type values. Rules for resultant overflow and NaN values follow the JVM specification.

**Errors**

* If either of the values is a non-numeric type.

**Grammar**

```text
addition: expression '+' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double | def |
| byte | int | int | int | int | long | float | double | def |
| short | int | int | int | int | long | float | double | def |
| char | int | int | int | int | long | float | double | def |
| int | int | int | int | int | long | float | double | def |
| long | long | long | long | long | long | float | double | def |
| float | float | float | float | float | float | float | double | def |
| double | double | double | double | double | double | double | double | def |
| def | def | def | def | def | def | def | def | def |

**Examples**

* Addition operator with different numeric types.

    ```painless
    int i = 29+4;     <1>
    double d = i+7.0; <2>
    ```

    1. declare `int i`; add `int 29` and `int 4` → `int 33`; store `int 33` in `i`
    2. declare `double d`; load from `int i` → `int 33`; promote `int 33` and `double 7.0`: result `double`; implicit cast `int 33` to `double 33.0` → `double 33.0`; add `double 33.0` and `double 7.0` → `double 40.0`; store `double 40.0` to `d`

* Addition with the `def` type.

    ```painless
    def x = 5+4; <1>
    def y = x+2; <2>
    ```

    1. declare `def x`; add `int 5` and `int 4` → `int 9`; implicit cast `int 9` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 9`; add `int 9` and `int 2` → `int 11`; implicit cast `int 11` to `def` → `def`; store `def` to `y`



## Subtraction [subtraction-operator]

Use the `subtraction operator '-'` to SUBTRACT a right-hand side numeric type value from a left-hand side numeric type value. Rules for resultant overflow and NaN values follow the JVM specification.

**Errors**

* If either of the values is a non-numeric type.

**Grammar**

```text
subtraction: expression '-' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double | def |
| byte | int | int | int | int | long | float | double | def |
| short | int | int | int | int | long | float | double | def |
| char | int | int | int | int | long | float | double | def |
| int | int | int | int | int | long | float | double | def |
| long | long | long | long | long | long | float | double | def |
| float | float | float | float | float | float | float | double | def |
| double | double | double | double | double | double | double | double | def |
| def | def | def | def | def | def | def | def | def |

**Examples**

* Subtraction with different numeric types.

    ```painless
    int i = 29-4;     <1>
    double d = i-7.5; <2>
    ```

    1. declare `int i`; subtract `int 4` from `int 29` → `int 25`; store `int 25` in `i`
    2. declare `double d` load from `int i` → `int 25`; promote `int 25` and `double 7.5`: result `double`; implicit cast `int 25` to `double 25.0` → `double 25.0`; subtract `double 33.0` by `double 7.5` → `double 25.5`; store `double 25.5` to `d`

* Subtraction with the `def` type.

    ```painless
    def x = 5-4; <1>
    def y = x-2; <2>
    ```

    1. declare `def x`; subtract `int 4` and `int 5` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 1`; subtract `int 2` from `int 1` → `int -1`; implicit cast `int -1` to `def` → `def`; store `def` to `y`



## Left Shift [left-shift-operator]

Use the `left shift operator '<<'` to SHIFT lower order bits to higher order bits in a left-hand side integer type value by the distance specified in a right-hand side integer type value.

**Errors**

* If either of the values is a non-integer type.
* If the right-hand side value cannot be cast to an int type.

**Grammar**

```text
left_shift: expression '<<' expression;
```

**Promotion**

The left-hand side integer type value is promoted as specified in the table below. The right-hand side integer type value is always implicitly cast to an `int` type value and truncated to the number of bits of the promoted type value.

| original | promoted |
| --- | --- |
| byte | int |
| short | int |
| char | int |
| int | int |
| long | long |
| def | def |

**Examples**

* Left shift with different integer types.

    ```painless
    int i = 4 << 1;   <1>
    long l = i << 2L; <2>
    ```

    1. declare `int i`; left shift `int 4` by `int 1` → `int 8`; store `int 8` in `i`
    2. declare `long l` load from `int i` → `int 8`; implicit cast `long 2` to `int 2` → `int 2`; left shift `int 8` by `int 2` → `int 32`; implicit cast `int 32` to `long 32` → `long 32`; store `long 32` to `l`

* Left shift with the `def` type.

    ```painless
    def x = 4 << 2; <1>
    def y = x << 1; <2>
    ```

    1. declare `def x`; left shift `int 4` by `int 2` → `int 16`; implicit cast `int 16` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 16`; left shift `int 16` by `int 1` → `int 32`; implicit cast `int 32` to `def` → `def`; store `def` to `y`



## Right Shift [right-shift-operator]

Use the `right shift operator '>>'` to SHIFT higher order bits to lower order bits in a left-hand side integer type value by the distance specified in a right-hand side integer type value. The highest order bit of the left-hand side integer type value is preserved.

**Errors**

* If either of the values is a non-integer type.
* If the right-hand side value cannot be cast to an int type.

**Grammar**

```text
right_shift: expression '>>' expression;
```

**Promotion**

The left-hand side integer type value is promoted as specified in the table below. The right-hand side integer type value is always implicitly cast to an `int` type value and truncated to the number of bits of the promoted type value.

| original | promoted |
| --- | --- |
| byte | int |
| short | int |
| char | int |
| int | int |
| long | long |
| def | def |

**Examples**

* Right shift with different integer types.

    ```painless
    int i = 32 >> 1;  <1>
    long l = i >> 2L; <2>
    ```

    1. declare `int i`; right shift `int 32` by `int 1` → `int 16`; store `int 16` in `i`
    2. declare `long l` load from `int i` → `int 16`; implicit cast `long 2` to `int 2` → `int 2`; right shift `int 16` by `int 2` → `int 4`; implicit cast `int 4` to `long 4` → `long 4`; store `long 4` to `l`

* Right shift with the `def` type.

    ```painless
    def x = 16 >> 2; <1>
    def y = x >> 1;  <2>
    ```

    1. declare `def x`; right shift `int 16` by `int 2` → `int 4`; implicit cast `int 4` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 4`; right shift `int 4` by `int 1` → `int 2`; implicit cast `int 2` to `def` → `def`; store `def` to `y`



## Unsigned Right Shift [unsigned-right-shift-operator]

Use the `unsigned right shift operator '>>>'` to SHIFT higher order bits to lower order bits in a left-hand side integer type value by the distance specified in a right-hand side type integer value. The highest order bit of the left-hand side integer type value is **not** preserved.

**Errors**

* If either of the values is a non-integer type.
* If the right-hand side value cannot be cast to an int type.

**Grammar**

```text
unsigned_right_shift: expression '>>>' expression;
```

**Promotion**

The left-hand side integer type value is promoted as specified in the table below. The right-hand side integer type value is always implicitly cast to an `int` type value and truncated to the number of bits of the promoted type value.

| original | promoted |
| --- | --- |
| byte | int |
| short | int |
| char | int |
| int | int |
| long | long |
| def | def |

**Examples**

* Unsigned right shift with different integer types.

    ```painless
    int i = -1 >>> 29; <1>
    long l = i >>> 2L; <2>
    ```

    1. declare `int i`; unsigned right shift `int -1` by `int 29` → `int 7`; store `int 7` in `i`
    2. declare `long l` load from `int i` → `int 7`; implicit cast `long 2` to `int 2` → `int 2`; unsigned right shift `int 7` by `int 2` → `int 3`; implicit cast `int 3` to `long 3` → `long 3`; store `long 3` to `l`

* Unsigned right shift with the `def` type.

    ```painless
    def x = 16 >>> 2; <1>
    def y = x >>> 1;  <2>
    ```

    1. declare `def x`; unsigned right shift `int 16` by `int 2` → `int 4`; implicit cast `int 4` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 4`; unsigned right shift `int 4` by `int 1` → `int 2`; implicit cast `int 2` to `def` → `def`; store `def` to `y`



## Bitwise And [bitwise-and-operator]

Use the `bitwise and operator '&'` to AND together each bit within two integer type values where if both bits at the same index are `1` the resultant bit is `1` and `0` otherwise.

**Errors**

* If either of the values is a non-integer type.

**Bits**

|     |     |     |
| --- | --- | --- |
|  | 1 | 0 |
| 1 | 1 | 0 |
| 0 | 0 | 0 |

**Grammar**

```text
bitwise_and: expression '&' expression;
```

**Promotion**

|     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | def |
| byte | int | int | int | int | long | def |
| short | int | int | int | int | long | def |
| char | int | int | int | int | long | def |
| int | int | int | int | int | long | def |
| long | long | long | long | long | long | def |
| def | def | def | def | def | def | def |

**Examples**

* Bitwise and with different integer types.

    ```painless
    int i = 5 & 6;   <1>
    long l = i & 5L; <2>
    ```

    1. declare `int i`; bitwise and `int 5` and `int 6` → `int 4`; store `int 4` in `i`
    2. declare `long l` load from `int i` → `int 4`; promote `int 4` and `long 5`: result `long`; implicit cast `int 4` to `long 4` → `long 4`; bitwise and `long 4` and `long 5` → `long 4`; store `long 4` to `l`

* Bitwise and with the `def` type.

    ```painless
    def x = 15 & 6; <1>
    def y = x & 5;  <2>
    ```

    1. declare `def x`; bitwise and `int 15` and `int 6` → `int 6`; implicit cast `int 6` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 6`; bitwise and `int 6` and `int 5` → `int 4`; implicit cast `int 4` to `def` → `def`; store `def` to `y`



## Bitwise Xor [bitwise-xor-operator]

Use the `bitwise xor operator '^'` to XOR together each bit within two integer type values where if one bit is a `1` and the other bit is a `0` at the same index the resultant bit is `1` otherwise the resultant bit is `0`.

**Errors**

* If either of the values is a non-integer type.

**Bits**

The following table illustrates the resultant bit from the xoring of two bits.

|     |     |     |
| --- | --- | --- |
|  | 1 | 0 |
| 1 | 0 | 1 |
| 0 | 1 | 0 |

**Grammar**

```text
bitwise_xor: expression '^' expression;
```

**Promotion**

|     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | def |
| byte | int | int | int | int | long | def |
| short | int | int | int | int | long | def |
| char | int | int | int | int | long | def |
| int | int | int | int | int | long | def |
| long | long | long | long | long | long | def |
| def | def | def | def | def | def | def |

**Examples**

* Bitwise xor with different integer types.

    ```painless
    int i = 5 ^ 6;   <1>
    long l = i ^ 5L; <2>
    ```

    1. declare `int i`; bitwise xor `int 5` and `int 6` → `int 3`; store `int 3` in `i`
    2. declare `long l` load from `int i` → `int 4`; promote `int 3` and `long 5`: result `long`; implicit cast `int 3` to `long 3` → `long 3`; bitwise xor `long 3` and `long 5` → `long 6`; store `long 6` to `l`

* Bitwise xor with the `def` type.

    ```painless
    def x = 15 ^ 6; <1>
    def y = x ^ 5;  <2>
    ```

    1. declare `def x`; bitwise xor `int 15` and `int 6` → `int 9`; implicit cast `int 9` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 9`; bitwise xor `int 9` and `int 5` → `int 12`; implicit cast `int 12` to `def` → `def`; store `def` to `y`



## Bitwise Or [bitwise-or-operator]

Use the `bitwise or operator '|'` to OR together each bit within two integer type values where if at least one bit is a `1` at the same index the resultant bit is `1` otherwise the resultant bit is `0`.

**Errors**

* If either of the values is a non-integer type.

**Bits**

The following table illustrates the resultant bit from the oring of two bits.

|     |     |     |
| --- | --- | --- |
|  | 1 | 0 |
| 1 | 1 | 1 |
| 0 | 1 | 0 |

**Grammar**

```text
bitwise_or: expression '|' expression;
```

**Promotion**

|     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | def |
| byte | int | int | int | int | long | def |
| short | int | int | int | int | long | def |
| char | int | int | int | int | long | def |
| int | int | int | int | int | long | def |
| long | long | long | long | long | long | def |
| def | def | def | def | def | def | def |

**Examples**

* Bitwise or with different integer types.

    ```painless
    int i = 5 | 6;   <1>
    long l = i | 8L; <2>
    ```

    1. declare `int i`; bitwise or `int 5` and `int 6` → `int 7`; store `int 7` in `i`
    2. declare `long l` load from `int i` → `int 7`; promote `int 7` and `long 8`: result `long`; implicit cast `int 7` to `long 7` → `long 7`; bitwise or `long 7` and `long 8` → `long 15`; store `long 15` to `l`

* Bitwise or with the `def` type.

    ```painless
    def x = 5 ^ 6; <1>
    def y = x ^ 8; <2>
    ```

    1. declare `def x`; bitwise or `int 5` and `int 6` → `int 7`; implicit cast `int 7` to `def` → `def`; store `def` in `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `int 7`; bitwise or `int 7` and `int 8` → `int 15`; implicit cast `int 15` to `def` → `def`; store `def` to `y`



