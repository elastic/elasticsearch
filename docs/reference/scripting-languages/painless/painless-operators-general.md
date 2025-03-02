---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators-general.html
---

# Operators: General [painless-operators-general]

## Precedence [precedence-operator]

Use the `precedence operator '()'` to guarantee the order of evaluation for an expression. An expression encapsulated by the precedence operator (enclosed in parentheses) overrides existing precedence relationships between operators and is evaluated prior to other expressions in inward-to-outward order.

**Grammar**

```text
precedence: '(' expression ')';
```

**Examples**

* Precedence with numeric operators.

    ```painless
    int x = (5+4)*6;   <1>
    int y = 12/(x-50); <2>
    ```

    1. declare `int x`; add `int 5` and `int 4` → `int 9`; multiply `int 9` and `int 6` → `int 54`; store `int 54` to `x`; (note the add is evaluated before the multiply due to the precedence operator)
    2. declare `int y`; load from `x` → `int 54`; subtract `int 50` from `int 54` → `int 4`; divide `int 12` by `int 4` → `int 3`; store `int 3` to `y`; (note the subtract is evaluated before the divide due to the precedence operator)



## Function Call [function-call-operator]

Use the `function call operator ()` to call an existing function. A [function call](/reference/scripting-languages/painless/painless-functions.md) is defined within a script.

**Grammar**

```text
function_call: ID '(' ( expression (',' expression)* )? ')'';
```

**Examples**

* A function call.

    ```painless
    int add(int x, int y) { <1>
          return x + y;
      }

    int z = add(1, 2); <2>
    ```

    1. define function `add` that returns `int` and has parameters (`int x`, `int y`)
    2. declare `int z`; call `add` with arguments (`int 1`, `int 2`) → `int 3`; store `int 3` to `z`



## Cast [cast-operator]

An explicit cast converts the value of an original type to the equivalent value of a target type forcefully as an operation. Use the `cast operator '()'` to specify an explicit cast. Refer to [casting](/reference/scripting-languages/painless/painless-casting.md) for more information.


## Conditional [conditional-operator]

A conditional consists of three expressions. The first expression is evaluated with an expected boolean result type. If the first expression evaluates to true then the second expression will be evaluated. If the first expression evaluates to false then the third expression will be evaluated. The second and third expressions will be [promoted](/reference/scripting-languages/painless/painless-casting.md#promotion) if the evaluated values are not the same type. Use the `conditional operator '? :'` as a shortcut to avoid the need for a full if/else branch in certain expressions.

**Errors**

* If the first expression does not evaluate to a boolean type value.
* If the values for the second and third expressions cannot be promoted.

**Grammar**

```text
conditional: expression '?' expression ':' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double | Reference | def |
| byte | int | int | int | int | long | float | double | - | def |
| short | int | int | int | int | long | float | double | - | def |
| char | int | int | int | int | long | float | double | - | def |
| int | int | int | int | int | long | float | double | - | def |
| long | long | long | long | long | long | float | double | - | def |
| float | float | float | float | float | float | float | double | - | def |
| double | double | double | double | double | double | double | double | - | def |
| Reference | - | - | - | - | - | - | - | Object @ | def |
| def | def | def | def | def | def | def | def | def | def |

@ If the two reference type values are the same then this promotion will not occur.

**Examples**

* Evaluation of conditionals.

    ```painless
    boolean b = true;        <1>
    int x = b ? 1 : 2;       <2>
    List y = x > 1 ? new ArrayList() : null; <3>
    def z = x < 2 ? x : 2.0; <4>
    ```

    1. declare `boolean b`; store `boolean true` to `b`
    2. declare `int x`; load from `b` → `boolean true` evaluate 1st expression: `int 1` → `int 1`; store `int 1` to `x`
    3. declare `List y`; load from `x` → `int 1`; `int 1` greater than `int 1` → `boolean false`; evaluate 2nd expression: `null` → `null`; store `null` to `y`;
    4. declare `def z`; load from `x` → `int 1`; `int 1` less than `int 2` → `boolean true`; evaluate 1st expression: load from `x` → `int 1`; promote `int 1` and `double 2.0`: result `double`; implicit cast `int 1` to `double 1.0` → `double 1.0`; implicit cast `double 1.0` to `def` → `def`; store `def` to `z`;



## Assignment [assignment-operator]

Use the `assignment operator '='` to store a value in a variable or reference type member field for use in subsequent operations. Any operation that produces a value can be assigned to any variable/field as long as the [types](/reference/scripting-languages/painless/painless-types.md) are the same or the resultant type can be [implicitly cast](/reference/scripting-languages/painless/painless-casting.md) to the variable/field type.

See [variable assignment](/reference/scripting-languages/painless/painless-variables.md#variable-assignment) for examples using variables.

**Errors**

* If the type of value is unable to match the type of variable or field.

**Grammar**

```text
assignment: field '=' expression
```

**Examples**

The examples use the following reference type definition:

```painless
name:
  Example

non-static member fields:
  * int x
  * def y
  * List z
```

* Field assignments of different type values.

    ```painless
    Example example = new Example(); <1>
    example.x = 1;                   <2>
    example.y = 2.0;                 <3>
    example.z = new ArrayList();     <4>
    ```

    1. declare `Example example`; allocate `Example` instance → `Example reference`; store `Example reference` to `example`
    2. load from `example` → `Example reference`; store `int 1` to `x` of `Example reference`
    3. load from `example` → `Example reference`; implicit cast `double 2.0` to `def` → `def`; store `def` to `y` of `Example reference`
    4. load from `example` → `Example reference`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `z` of `Example reference`

* A field assignment from a field access.

    ```painless
    Example example = new Example(); <1>
    example.x = 1;                   <2>
    example.y = example.x;           <3>
    ```

    1. declare `Example example`; allocate `Example` instance → `Example reference`; store `Example reference` to `example`
    2. load from `example` → `Example reference`; store `int 1` to `x` of `Example reference`
    3. load from `example` → `Example reference @0`; load from `example` → `Example reference @1`; load from `x` of `Example reference @1` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` to `y` of `Example reference @0`; (note `Example reference @0` and `Example reference @1` are the same)



## Compound Assignment [compound-assignment-operator]

Use the `compound assignment operator '$='` as a shortcut for an assignment where a binary operation would occur between the variable/field as the left-hand side expression and a separate right-hand side expression.

A compound assignment is equivalent to the expression below where V is the variable/field and T is the type of variable/member.

```painless
V = (T)(V op expression);
```

**Operators**

The table below shows the available operators for use in a compound assignment. Each operator follows the casting/promotion rules according to their regular definition. For numeric operations there is an extra implicit cast when necessary to return the promoted numeric type value to the original numeric type value of the variable/field and can result in data loss.

|     |     |
| --- | --- |
| Operator | Compound Symbol |
| Multiplication | *= |
| Division | /= |
| Remainder | %= |
| Addition | += |
| Subtraction | -= |
| Left Shift | <<= |
| Right Shift | >>= |
| Unsigned Right Shift | >>>= |
| Bitwise And | &= |
| Boolean And | &= |
| Bitwise Xor | ^= |
| Boolean Xor | ^= |
| Bitwise Or | &#124;= |
| Boolean Or | &#124;= |
| String Concatenation | += |

**Errors**

* If the type of value is unable to match the type of variable or field.

**Grammar**

```text
compound_assignment: ( ID | field ) '$=' expression;
```

Note the use of the `$=` represents the use of any of the possible binary operators.

**Examples**

* Compound assignment for each numeric operator.

    ```painless
    int i = 10; <1>
    i *= 2;     <2>
    i /= 5;     <3>
    i %= 3;     <4>
    i += 5;     <5>
    i -= 5;     <6>
    i <<= 2;    <7>
    i >>= 1;    <8>
    i >>>= 1;   <9>
    i &= 15;    <10>
    i ^= 12;    <11>
    i |= 2;     <12>
    ```

    1. declare `int i`; store `int 10` to `i`
    2. load from `i` → `int 10`; multiply `int 10` and `int 2` → `int 20`; store `int 20` to `i`; (note this is equivalent to `i = i*2`)
    3. load from `i` → `int 20`; divide `int 20` by `int 5` → `int 4`; store `int 4` to `i`; (note this is equivalent to `i = i/5`)
    4. load from `i` → `int 4`; remainder `int 4` by `int 3` → `int 1`; store `int 1` to `i`; (note this is equivalent to `i = i%3`)
    5. load from `i` → `int 1`; add `int 1` and `int 5` → `int 6`; store `int 6` to `i`; (note this is equivalent to `i = i+5`)
    6. load from `i` → `int 6`; subtract `int 5` from `int 6` → `int 1`; store `int 1` to `i`; (note this is equivalent to `i = i-5`)
    7. load from `i` → `int 1`; left shift `int 1` by `int 2` → `int 4`; store `int 4` to `i`; (note this is equivalent to `i = i<<2`)
    8. load from `i` → `int 4`; right shift `int 4` by `int 1` → `int 2`; store `int 2` to `i`; (note this is equivalent to `i = i>>1`)
    9. load from `i` → `int 2`; unsigned right shift `int 2` by `int 1` → `int 1`; store `int 1` to `i`; (note this is equivalent to `i = i>>>1`)
    10. load from `i` → `int 1`; bitwise and `int 1` and `int 15` → `int 1`; store `int 1` to `i`; (note this is equivalent to `i = i&2`)
    11. load from `i` → `int 1`; bitwise xor `int 1` and `int 12` → `int 13`; store `int 13` to `i`; (note this is equivalent to `i = i^2`)
    12. load from `i` → `int 13`; bitwise or `int 13` and `int 2` → `int 15`; store `int 15` to `i`; (note this is equivalent to `i = i|2`)

* Compound assignment for each boolean operator.

    ```painless
    boolean b = true; <1>
    b &= false;       <2>
    b ^= false;       <3>
    b |= true;        <4>
    ```

    1. declare `boolean b`; store `boolean true` in `b`;
    2. load from `b` → `boolean true`; boolean and `boolean true` and `boolean false` → `boolean false`; store `boolean false` to `b`; (note this is equivalent to `b = b && false`)
    3. load from `b` → `boolean false`; boolean xor `boolean false` and `boolean false` → `boolean false`; store `boolean false` to `b`; (note this is equivalent to `b = b ^ false`)
    4. load from `b` → `boolean true`; boolean or `boolean false` and `boolean true` → `boolean true`; store `boolean true` to `b`; (note this is equivalent to `b = b || true`)

* A compound assignment with the string concatenation operator.

    ```painless
    String s = 'compound'; <1>
    s += ' assignment';    <2>
    ```

    1. declare `String s`; store `String 'compound'` to `s`;
    2. load from `s` → `String 'compound'`; string concat `String 'compound'` and `String ' assignment''` → `String 'compound assignment'`; store `String 'compound assignment'` to `s`; (note this is equivalent to `s = s + ' assignment'`)

* A compound assignment with the `def` type.

    ```painless
    def x = 1; <1>
    x += 2;    <2>
    ```

    1. declare `def x`; implicit cast `int 1` to `def`; store `def` to `x`;
    2. load from `x` → `def`; implicit cast `def` to `int 1` → `int 1`; add `int 1` and `int 2` → `int 3`; implicit cast `int 3` to `def` → `def`; store `def` to `x`; (note this is equivalent to `x = x+2`)

* A compound assignment with an extra implicit cast.

    ```painless
    byte b = 1; <1>
    b += 2;     <2>
    ```

    1. declare `byte b`; store `byte 1` to `x`;
    2. load from `x` → `byte 1`; implicit cast `byte 1 to `int 1` → `int 1`; add `int 1` and `int 2` → `int 3`; implicit cast `int 3` to `byte 3` → `byte 3`; store `byte 3` to `b`; (note this is equivalent to `b = b+2`)



