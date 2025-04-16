---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators-boolean.html
---

# Operators: Boolean [painless-operators-boolean]

## Boolean Not [boolean-not-operator]

Use the `boolean not operator '!'` to NOT a `boolean` type value where `true` is flipped to `false` and `false` is flipped to `true`.

**Errors**

* If a value other than a `boolean` type value or a value that is castable to a `boolean` type value is given.

**Truth**

| original | result |
| --- | --- |
| true | false |
| false | true |

**Grammar**

```text
boolean_not: '!' expression;
```

**Examples**

* Boolean not with the `boolean` type.

    ```painless
    boolean x = !false; <1>
    boolean y = !x;     <2>
    ```

    1. declare `boolean x`; boolean not `boolean false` → `boolean true`; store `boolean true` to `x`
    2. declare `boolean y`; load from `x` → `boolean true`; boolean not `boolean true` → `boolean false`; store `boolean false` to `y`

* Boolean not with the `def` type.

    ```painless
    def y = true; <1>
    def z = !y;   <2>
    ```

    1. declare `def y`; implicit cast `boolean true` to `def` → `def`; store `true` to `y`
    2. declare `def z`; load from `y` → `def`; implicit cast `def` to `boolean true` → boolean `true`; boolean not `boolean true` → `boolean false`; implicit cast `boolean false` to `def` → `def`; store `def` to `z`



## Greater Than [greater-than-operator]

Use the `greater than operator '>'` to COMPARE two numeric type values where a resultant `boolean` type value is `true` if the left-hand side value is greater than to the right-hand side value and `false` otherwise.

**Errors**

* If either the evaluated left-hand side or the evaluated right-hand side is a non-numeric value.

**Grammar**

```text
greater_than: expression '>' expression;
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

* Greater than with different numeric types.

    ```painless
    boolean x = 5 > 4; <1>
    double y = 6.0;    <2>
    x = 6 > y;         <3>
    ```

    1. declare `boolean x`; greater than `int 5` and `int 4` → `boolean true`; store `boolean true` to `x`;
    2. declare `double y`; store `double 6.0` to `y`;
    3. load from `y` → `double 6.0 @0`; promote `int 6` and `double 6.0`: result `double`; implicit cast `int 6` to `double 6.0 @1` → `double 6.0 @1`; greater than `double 6.0 @1` and `double 6.0 @0` → `boolean false`; store `boolean false` to `x`

* Greater than with `def` type.

    ```painless
    int x = 5;       <1>
    def y = 7.0;     <2>
    def z = y > 6.5; <3>
    def a = x > y;   <4>
    ```

    1. declare `int x`; store `int 5` to `x`
    2. declare `def y`; implicit cast `double 7.0` to `def` → `def`; store `def` to `y`
    3. declare `def z`; load from `y` → `def`; implicit cast `def` to `double 7.0` → `double 7.0`; greater than `double 7.0` and `double 6.5` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `z`
    4. declare `def a`; load from `y` → `def`; implicit cast `def` to `double 7.0` → `double 7.0`; load from `x` → `int 5`; promote `int 5` and `double 7.0`: result `double`; implicit cast `int 5` to `double 5.0` → `double 5.0`; greater than `double 5.0` and `double 7.0` → `boolean false`; implicit cast `boolean false` to `def` → `def`; store `def` to `z`



## Greater Than Or Equal [greater-than-or-equal-operator]

Use the `greater than or equal operator '>='` to COMPARE two numeric type values where a resultant `boolean` type value is `true` if the left-hand side value is greater than or equal to the right-hand side value and `false` otherwise.

**Errors**

* If either the evaluated left-hand side or the evaluated right-hand side is a non-numeric value.

**Grammar**

```text
greater_than_or_equal: expression '>=' expression;
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

* Greater than or equal with different numeric types.

    ```painless
    boolean x = 5 >= 4; <1>
    double y = 6.0;     <2>
    x = 6 >= y;         <3>
    ```

    1. declare `boolean x`; greater than or equal `int 5` and `int 4` → `boolean true`; store `boolean true` to `x`
    2. declare `double y`; store `double 6.0` to `y`
    3. load from `y` → `double 6.0 @0`; promote `int 6` and `double 6.0`: result `double`; implicit cast `int 6` to `double 6.0 @1` → `double 6.0 @1`; greater than or equal `double 6.0 @1` and `double 6.0 @0` → `boolean true`; store `boolean true` to `x`

* Greater than or equal with the `def` type.

    ```painless
    int x = 5;        <1>
    def y = 7.0;      <2>
    def z = y >= 7.0; <3>
    def a = x >= y;   <4>
    ```

    1. declare `int x`; store `int 5` to `x`;
    2. declare `def y` implicit cast `double 7.0` to `def` → `def`; store `def` to `y`
    3. declare `def z`; load from `y` → `def`; implicit cast `def` to `double 7.0 @0` → `double 7.0 @0`; greater than or equal `double 7.0 @0` and `double 7.0 @1` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `z`
    4. declare `def a`; load from `y` → `def`; implicit cast `def` to `double 7.0` → `double 7.0`; load from `x` → `int 5`; promote `int 5` and `double 7.0`: result `double`; implicit cast `int 5` to `double 5.0` → `double 5.0`; greater than or equal `double 5.0` and `double 7.0` → `boolean false`; implicit cast `boolean false` to `def` → `def`; store `def` to `z`



## Less Than [less-than-operator]

Use the `less than operator '<'` to COMPARE two numeric type values where a resultant `boolean` type value is `true` if the left-hand side value is less than to the right-hand side value and `false` otherwise.

**Errors**

* If either the evaluated left-hand side or the evaluated right-hand side is a non-numeric value.

**Grammar**

```text
less_than: expression '<' expression;
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

* Less than with different numeric types.

    ```painless
    boolean x = 5 < 4; <1>
    double y = 6.0;    <2>
    x = 6 < y;         <3>
    ```

    1. declare `boolean x`; less than `int 5` and `int 4` → `boolean false`; store `boolean false` to `x`
    2. declare `double y`; store `double 6.0` to `y`
    3. load from `y` → `double 6.0 @0`; promote `int 6` and `double 6.0`: result `double`; implicit cast `int 6` to `double 6.0 @1` → `double 6.0 @1`; less than `double 6.0 @1` and `double 6.0 @0` → `boolean false`; store `boolean false` to `x`

* Less than with the `def` type.

    ```painless
    int x = 5;       <1>
    def y = 7.0;     <2>
    def z = y < 6.5; <3>
    def a = x < y;   <4>
    ```

    1. declare `int x`; store `int 5` to `x`
    2. declare `def y`; implicit cast `double 7.0` to `def` → `def`; store `def` to `y`
    3. declare `def z`; load from `y` → `def`; implicit cast `def` to `double 7.0` → `double 7.0`; less than `double 7.0` and `double 6.5` → `boolean false`; implicit cast `boolean false` to `def` → `def`; store `def` to `z`
    4. declare `def a`; load from `y` → `def`; implicit cast `def` to `double 7.0` → `double 7.0`; load from `x` → `int 5`; promote `int 5` and `double 7.0`: result `double`; implicit cast `int 5` to `double 5.0` → `double 5.0`; less than `double 5.0` and `double 7.0` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `z`



## Less Than Or Equal [less-than-or-equal-operator]

Use the `less than or equal operator '<='` to COMPARE two numeric type values where a resultant `boolean` type value is `true` if the left-hand side value is less than or equal to the right-hand side value and `false` otherwise.

**Errors**

* If either the evaluated left-hand side or the evaluated right-hand side is a non-numeric value.

**Grammar**

```text
greater_than_or_equal: expression '<=' expression;
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

* Less than or equal with different numeric types.

    ```painless
    boolean x = 5 <= 4; <1>
    double y = 6.0;     <2>
    x = 6 <= y;         <3>
    ```

    1. declare `boolean x`; less than or equal `int 5` and `int 4` → `boolean false`; store `boolean true` to `x`
    2. declare `double y`; store `double 6.0` to `y`
    3. load from `y` → `double 6.0 @0`; promote `int 6` and `double 6.0`: result `double`; implicit cast `int 6` to `double 6.0 @1` → `double 6.0 @1`; less than or equal `double 6.0 @1` and `double 6.0 @0` → `boolean true`; store `boolean true` to `x`

* Less than or equal with the `def` type.

    ```painless
    int x = 5;        <1>
    def y = 7.0;      <2>
    def z = y <= 7.0; <3>
    def a = x <= y;   <4>
    ```

    1. declare `int x`; store `int 5` to `x`;
    2. declare `def y`; implicit cast `double 7.0` to `def` → `def`; store `def` to `y`;
    3. declare `def z`; load from `y` → `def`; implicit cast `def` to `double 7.0 @0` → `double 7.0 @0`; less than or equal `double 7.0 @0` and `double 7.0 @1` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `z`
    4. declare `def a`; load from `y` → `def`; implicit cast `def` to `double 7.0` → `double 7.0`; load from `x` → `int 5`; promote `int 5` and `double 7.0`: result `double`; implicit cast `int 5` to `double 5.0` → `double 5.0`; less than or equal `double 5.0` and `double 7.0` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `z`



## Instanceof [instanceof-operator]

Use the `instanceof operator` to COMPARE the variable/field type to a specified reference type using the reference type name where a resultant `boolean` type value is `true` if the variable/field type is the same as or a descendant of the specified reference type and false otherwise.

**Errors**

* If the reference type name doesn’t exist as specified by the right-hand side.

**Grammar**

```text
instance_of: ID 'instanceof' TYPE;
```

**Examples**

* Instance of with different reference types.

    ```painless
    Map m = new HashMap();            <1>
    boolean a = m instanceof HashMap; <2>
    boolean b = m instanceof Map;     <3>
    ```

    1. declare `Map m`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Map reference`; store `Map reference` to `m`
    2. declare `boolean a`; load from `m` → `Map reference`; implicit cast `Map reference` to `HashMap reference` → `HashMap reference`; instanceof `HashMap reference` and `HashMap` → `boolean true`; store `boolean true` to `a`
    3. declare `boolean b`; load from `m` → `Map reference`; implicit cast `Map reference` to `HashMap reference` → `HashMap reference`; instanceof `HashMap reference` and `Map` → `boolean true`; store `true` to `b`; (note `HashMap` is a descendant of `Map`)

* Instance of with the `def` type.

    ```painless
    def d = new ArrayList();       <1>
    boolean a = d instanceof List; <2>
    boolean b = d instanceof Map;  <3>
    ```

    1. declare `def d`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def`; store `def` to `d`
    2. declare `boolean a`; load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; instanceof `ArrayList reference` and `List` → `boolean true`; store `boolean true` to `a`; (note `ArrayList` is a descendant of `List`)
    3. declare `boolean b`; load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; instanceof `ArrayList reference` and `Map` → `boolean false`; store `boolean false` to `a`; (note `ArrayList` is not a descendant of `Map`)



## Equality Equals [equality-equals-operator]

Use the `equality equals operator '=='` to COMPARE two values where a resultant `boolean` type value is `true` if the two values are equal and `false` otherwise. The member method, `equals`, is implicitly called when the values are reference type values where the first value is the target of the call and the second value is the argument. This operation is null-safe where if both values are `null` the resultant `boolean` type value is `true`, and if only one value is `null` the resultant `boolean` type value is `false`. A valid comparison is between `boolean` type values, numeric type values, or reference type values.

**Errors**

* If a comparison is made between a `boolean` type value and numeric type value.
* If a comparison is made between a primitive type value and a reference type value.

**Grammar**

```text
equality_equals: expression '==' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | boolean | byte | short | char | int | long | float | double | Reference | def |
| boolean | boolean | - | - | - | - | - | - | - | - | def |
| byte | - | int | int | int | int | long | float | double | - | def |
| short | - | int | int | int | int | long | float | double | - | def |
| char | - | int | int | int | int | long | float | double | - | def |
| int | - | int | int | int | int | long | float | double | - | def |
| long | - | long | long | long | long | long | float | double | - | def |
| float | - | float | float | float | float | float | float | double | - | def |
| double | - | double | double | double | double | double | double | double | - | def |
| Reference | - | - | - | - | - | - | - | - | Object | def |
| def | def | def | def | def | def | def | def | def | def | def |

**Examples**

* Equality equals with the `boolean` type.

    ```painless
    boolean a = true;  <1>
    boolean b = false; <2>
    a = a == false;    <3>
    b = a == b;        <4>
    ```

    1. declare `boolean a`; store `boolean true` to `a`
    2. declare `boolean b`; store `boolean false` to `b`
    3. load from `a` → `boolean true`; equality equals `boolean true` and `boolean false` → `boolean false`; store `boolean false` to `a`
    4. load from `a` → `boolean false @0`; load from `b` → `boolean false @1`; equality equals `boolean false @0` and `boolean false @1` → `boolean false`; store `boolean false` to `b`

* Equality equals with primitive types.

    ```painless
    int a = 1;          <1>
    double b = 2.0;     <2>
    boolean c = a == b; <3>
    c = 1 == a;         <4>
    ```

    1. declare `int a`; store `int 1` to `a`
    2. declare `double b`; store `double 1.0` to `b`
    3. declare `boolean c`; load from `a` → `int 1`; load from `b` → `double 2.0`; promote `int 1` and `double 2.0`: result `double`; implicit cast `int 1` to `double 1.0` → `double `1.0`; equality equals `double 1.0` and `double 2.0` → `boolean false`; store `boolean false` to `c`
    4. load from `a` → `int 1 @1`; equality equals `int 1 @0` and `int 1 @1` → `boolean true`; store `boolean true` to `c`

* Equal equals with reference types.

    ```painless
    List a = new ArrayList(); <1>
    List b = new ArrayList(); <2>
    a.add(1);                 <3>
    boolean c = a == b;       <4>
    b.add(1);                 <5>
    c = a == b;               <6>
    ```

    1. declare `List a`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `a`
    2. declare `List b`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `b`
    3. load from `a` → `List reference`; call `add` on `List reference` with arguments (`int 1)`
    4. declare `boolean c`; load from `a` → `List reference @0`; load from `b` → `List reference @1`; call `equals` on `List reference @0` with arguments (`List reference @1`) → `boolean false`; store `boolean false` to `c`
    5. load from `b` → `List reference`; call `add` on `List reference` with arguments (`int 1`)
    6. load from `a` → `List reference @0`; load from `b` → `List reference @1`; call `equals` on `List reference @0` with arguments (`List reference @1`) → `boolean true`; store `boolean true` to `c`

* Equality equals with `null`.

    ```painless
    Object a = null;       <1>
    Object b = null;       <2>
    boolean c = a == null; <3>
    c = a == b;            <4>
    b = new Object();      <5>
    c = a == b;            <6>
    ```

    1. declare `Object a`; store `null` to `a`
    2. declare `Object b`; store `null` to `b`
    3. declare `boolean c`; load from `a` → `null @0`; equality equals `null @0` and `null @1` → `boolean true`; store `boolean true` to `c`
    4. load from `a` → `null @0`; load from `b` → `null @1`; equality equals `null @0` and `null @1` → `boolean true`; store `boolean true` to `c`
    5. allocate `Object` instance → `Object reference`; store `Object reference` to `b`
    6. load from `a` → `Object reference`; load from `b` → `null`; call `equals` on `Object reference` with arguments (`null`) → `boolean false`; store `boolean false` to `c`

* Equality equals with the `def` type.

    ```painless
    def a = 0;               <1>
    def b = 1;               <2>
    boolean c = a == b;      <3>
    def d = new HashMap();   <4>
    def e = new ArrayList(); <5>
    c = d == e;              <6>
    ```

    1. declare `def a`; implicit cast `int 0` to `def` → `def`; store `def` to `a`;
    2. declare `def b`; implicit cast `int 1` to `def` → `def`; store `def` to `b`;
    3. declare `boolean c`; load from `a` → `def`; implicit cast `a` to `int 0` → `int 0`; load from `b` → `def`; implicit cast `b` to `int 1` → `int 1`; equality equals `int 0` and `int 1` → `boolean false`; store `boolean false` to `c`
    4. declare `def d`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def` store `def` to `d`;
    5. declare `def e`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def` store `def` to `d`;
    6. load from `d` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; load from `e` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `equals` on `HashMap reference` with arguments (`ArrayList reference`) → `boolean false`; store `boolean false` to `c`



## Equality Not Equals [equality-not-equals-operator]

Use the `equality not equals operator '!='` to COMPARE two values where a resultant `boolean` type value is `true` if the two values are NOT equal and `false` otherwise. The member method, `equals`, is implicitly called when the values are reference type values where the first value is the target of the call and the second value is the argument with the resultant `boolean` type value flipped. This operation is `null-safe` where if both values are `null` the resultant `boolean` type value is `false`, and if only one value is `null` the resultant `boolean` type value is `true`. A valid comparison is between boolean type values, numeric type values, or reference type values.

**Errors**

* If a comparison is made between a `boolean` type value and numeric type value.
* If a comparison is made between a primitive type value and a reference type value.

**Grammar**

```text
equality_not_equals: expression '!=' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | boolean | byte | short | char | int | long | float | double | Reference | def |
| boolean | boolean | - | - | - | - | - | - | - | - | def |
| byte | - | int | int | int | int | long | float | double | - | def |
| short | - | int | int | int | int | long | float | double | - | def |
| char | - | int | int | int | int | long | float | double | - | def |
| int | - | int | int | int | int | long | float | double | - | def |
| long | - | long | long | long | long | long | float | double | - | def |
| float | - | float | float | float | float | float | float | double | - | def |
| double | - | double | double | double | double | double | double | double | - | def |
| Reference | - | - | - | - | - | - | - | - | Object | def |
| def | def | def | def | def | def | def | def | def | def | def |

**Examples**

* Equality not equals with the `boolean` type.

    ```painless
    boolean a = true;  <1>
    boolean b = false; <2>
    a = a != false;    <3>
    b = a != b;        <4>
    ```

    1. declare `boolean a`; store `boolean true` to `a`
    2. declare `boolean b`; store `boolean false` to `b`
    3. load from `a` → `boolean true`; equality not equals `boolean true` and `boolean false` → `boolean true`; store `boolean true` to `a`
    4. load from `a` → `boolean true`; load from `b` → `boolean false`; equality not equals `boolean true` and `boolean false` → `boolean true`; store `boolean true` to `b`

* Equality not equals with primitive types.

    ```painless
    int a = 1;          <1>
    double b = 2.0;     <2>
    boolean c = a != b; <3>
    c = 1 != a;         <4>
    ```

    1. declare `int a`; store `int 1` to `a`
    2. declare `double b`; store `double 1.0` to `b`
    3. declare `boolean c`; load from `a` → `int 1`; load from `b` → `double 2.0`; promote `int 1` and `double 2.0`: result `double`; implicit cast `int 1` to `double 1.0` → `double `1.0`; equality not equals `double 1.0` and `double 2.0` → `boolean true`; store `boolean true` to `c`
    4. load from `a` → `int 1 @1`; equality not equals `int 1 @0` and `int 1 @1` → `boolean false`; store `boolean false` to `c`

* Equality not equals with reference types.

    ```painless
    List a = new ArrayList(); <1>
    List b = new ArrayList(); <2>
    a.add(1);                 <3>
    boolean c = a == b;       <4>
    b.add(1);                 <5>
    c = a == b;               <6>
    ```

    1. declare `List a`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `a`
    2. declare `List b`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `b`
    3. load from `a` → `List reference`; call `add` on `List reference` with arguments (`int 1)`
    4. declare `boolean c`; load from `a` → `List reference @0`; load from `b` → `List reference @1`; call `equals` on `List reference @0` with arguments (`List reference @1`) → `boolean false`; boolean not `boolean false` → `boolean true` store `boolean true` to `c`
    5. load from `b` → `List reference`; call `add` on `List reference` with arguments (`int 1`)
    6. load from `a` → `List reference @0`; load from `b` → `List reference @1`; call `equals` on `List reference @0` with arguments (`List reference @1`) → `boolean true`; boolean not `boolean true` → `boolean false`; store `boolean false` to `c`

* Equality not equals with `null`.

    ```painless
    Object a = null;       <1>
    Object b = null;       <2>
    boolean c = a == null; <3>
    c = a == b;            <4>
    b = new Object();      <5>
    c = a == b;            <6>
    ```

    1. declare `Object a`; store `null` to `a`
    2. declare `Object b`; store `null` to `b`
    3. declare `boolean c`; load from `a` → `null @0`; equality not equals `null @0` and `null @1` → `boolean false`; store `boolean false` to `c`
    4. load from `a` → `null @0`; load from `b` → `null @1`; equality not equals `null @0` and `null @1` → `boolean false`; store `boolean false` to `c`
    5. allocate `Object` instance → `Object reference`; store `Object reference` to `b`
    6. load from `a` → `Object reference`; load from `b` → `null`; call `equals` on `Object reference` with arguments (`null`) → `boolean false`; boolean not `boolean false` → `boolean true`; store `boolean true` to `c`

* Equality not equals with the `def` type.

    ```painless
    def a = 0;               <1>
    def b = 1;               <2>
    boolean c = a == b;      <3>
    def d = new HashMap();   <4>
    def e = new ArrayList(); <5>
    c = d == e;              <6>
    ```

    1. declare `def a`; implicit cast `int 0` to `def` → `def`; store `def` to `a`;
    2. declare `def b`; implicit cast `int 1` to `def` → `def`; store `def` to `b`;
    3. declare `boolean c`; load from `a` → `def`; implicit cast `a` to `int 0` → `int 0`; load from `b` → `def`; implicit cast `b` to `int 1` → `int 1`; equality equals `int 0` and `int 1` → `boolean false`; store `boolean false` to `c`
    4. declare `def d`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def` store `def` to `d`;
    5. declare `def e`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def` store `def` to `d`;
    6. load from `d` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; load from `e` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `equals` on `HashMap reference` with arguments (`ArrayList reference`) → `boolean false`; store `boolean false` to `c`



## Identity Equals [identity-equals-operator]

Use the `identity equals operator '==='` to COMPARE two values where a resultant `boolean` type value is `true` if the two values are equal and `false` otherwise. A reference type value is equal to another reference type value if both values refer to same instance on the heap or if both values are `null`. A valid comparison is between `boolean` type values, numeric type values, or reference type values.

**Errors**

* If a comparison is made between a `boolean` type value and numeric type value.
* If a comparison is made between a primitive type value and a reference type value.

**Grammar**

```text
identity_equals: expression '===' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | boolean | byte | short | char | int | long | float | double | Reference | def |
| boolean | boolean | - | - | - | - | - | - | - | - | def |
| byte | - | int | int | int | int | long | float | double | - | def |
| short | - | int | int | int | int | long | float | double | - | def |
| char | - | int | int | int | int | long | float | double | - | def |
| int | - | int | int | int | int | long | float | double | - | def |
| long | - | long | long | long | long | long | float | double | - | def |
| float | - | float | float | float | float | float | float | double | - | def |
| double | - | double | double | double | double | double | double | double | - | def |
| Reference | - | - | - | - | - | - | - | - | Object | def |
| def | def | def | def | def | def | def | def | def | def | def |

**Examples**

* Identity equals with reference types.

    ```painless
    List a = new ArrayList(); <1>
    List b = new ArrayList(); <2>
    List c = a;               <3>
    boolean c = a === b;      <4>
    c = a === c;              <5>
    ```

    1. declare `List a`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `a`
    2. declare `List b`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `b`
    3. load from `a` → `List reference`; store `List reference` to `c`
    4. declare `boolean c`; load from `a` → `List reference @0`; load from `b` → `List reference @1`; identity equals `List reference @0` and `List reference @1` → `boolean false` store `boolean false` to `c`
    5. load from `a` → `List reference @0`; load from `c` → `List reference @1`; identity equals `List reference @0` and `List reference @1` → `boolean true` store `boolean true` to `c` (note `List reference @0` and `List reference @1` refer to the same instance)

* Identity equals with `null`.

    ```painless
    Object a = null;        <1>
    Object b = null;        <2>
    boolean c = a === null; <3>
    c = a === b;            <4>
    b = new Object();       <5>
    c = a === b;            <6>
    ```

    1. declare `Object a`; store `null` to `a`
    2. declare `Object b`; store `null` to `b`
    3. declare `boolean c`; load from `a` → `null @0`; identity equals `null @0` and `null @1` → `boolean true`; store `boolean true` to `c`
    4. load from `a` → `null @0`; load from `b` → `null @1`; identity equals `null @0` and `null @1` → `boolean true`; store `boolean true` to `c`
    5. allocate `Object` instance → `Object reference`; store `Object reference` to `b`
    6. load from `a` → `Object reference`; load from `b` → `null`; identity equals `Object reference` and `null` → `boolean false`; store `boolean false` to `c`

* Identity equals with the `def` type.

    ```painless
    def a = new HashMap();   <1>
    def b = new ArrayList(); <2>
    boolean c = a === b;     <3>
    b = a;                   <4>
    c = a === b;             <5>
    ```

    1. declare `def d`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def` store `def` to `d`
    2. declare `def e`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def` store `def` to `d`
    3. declare `boolean c`; load from `a` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; load from `b` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; identity equals `HashMap reference` and `ArrayList reference` → `boolean false`; store `boolean false` to `c`
    4. load from `a` → `def`; store `def` to `b`
    5. load from `a` → `def`; implicit cast `def` to `HashMap reference @0` → `HashMap reference @0`; load from `b` → `def`; implicit cast `def` to `HashMap reference @1` → `HashMap reference @1`; identity equals `HashMap reference @0` and `HashMap reference @1` → `boolean true`; store `boolean true` to `b`; (note `HashMap reference @0` and `HashMap reference @1` refer to the same instance)



## Identity Not Equals [identity-not-equals-operator]

Use the `identity not equals operator '!=='` to COMPARE two values where a resultant `boolean` type value is `true` if the two values are NOT equal and `false` otherwise. A reference type value is not equal to another reference type value if both values refer to different instances on the heap or if one value is `null` and the other is not. A valid comparison is between `boolean` type values, numeric type values, or reference type values.

**Errors**

* If a comparison is made between a `boolean` type value and numeric type value.
* If a comparison is made between a primitive type value and a reference type value.

**Grammar**

```text
identity_not_equals: expression '!==' expression;
```

**Promotion**

|     |     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | boolean | byte | short | char | int | long | float | double | Reference | def |
| boolean | boolean | - | - | - | - | - | - | - | - | def |
| byte | - | int | int | int | int | long | float | double | - | def |
| short | - | int | int | int | int | long | float | double | - | def |
| char | - | int | int | int | int | long | float | double | - | def |
| int | - | int | int | int | int | long | float | double | - | def |
| long | - | long | long | long | long | long | float | double | - | def |
| float | - | float | float | float | float | float | float | double | - | def |
| double | - | double | double | double | double | double | double | double | - | def |
| Reference | - | - | - | - | - | - | - | - | Object | def |
| def | def | def | def | def | def | def | def | def | def | def |

**Examples**

* Identity not equals with reference type values.

    ```painless
    List a = new ArrayList(); <1>
    List b = new ArrayList(); <2>
    List c = a;               <3>
    boolean c = a !== b;      <4>
    c = a !== c;              <5>
    ```

    1. declare `List a`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `a`
    2. declare `List b`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `b`
    3. load from `a` → `List reference`; store `List reference` to `c`
    4. declare `boolean c`; load from `a` → `List reference @0`; load from `b` → `List reference @1`; identity not equals `List reference @0` and `List reference @1` → `boolean true` store `boolean true` to `c`
    5. load from `a` → `List reference @0`; load from `c` → `List reference @1`; identity not equals `List reference @0` and `List reference @1` → `boolean false` store `boolean false` to `c` (note `List reference @0` and `List reference @1` refer to the same instance)

* Identity not equals with `null`.

    ```painless
    Object a = null;        <1>
    Object b = null;        <2>
    boolean c = a !== null; <3>
    c = a !== b;            <4>
    b = new Object();       <5>
    c = a !== b;            <6>
    ```

    1. declare `Object a`; store `null` to `a`
    2. declare `Object b`; store `null` to `b`
    3. declare `boolean c`; load from `a` → `null @0`; identity not equals `null @0` and `null @1` → `boolean false`; store `boolean false` to `c`
    4. load from `a` → `null @0`; load from `b` → `null @1`; identity not equals `null @0` and `null @1` → `boolean false`; store `boolean false` to `c`
    5. allocate `Object` instance → `Object reference`; store `Object reference` to `b`
    6. load from `a` → `Object reference`; load from `b` → `null`; identity not equals `Object reference` and `null` → `boolean true`; store `boolean true` to `c`

* Identity not equals with the `def` type.

    ```painless
    def a = new HashMap();   <1>
    def b = new ArrayList(); <2>
    boolean c = a !== b;     <3>
    b = a;                   <4>
    c = a !== b;             <5>
    ```

    1. declare `def d`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def` store `def` to `d`
    2. declare `def e`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def` store `def` to `d`
    3. declare `boolean c`; load from `a` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; load from `b` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; identity not equals `HashMap reference` and `ArrayList reference` → `boolean true`; store `boolean true` to `c`
    4. load from `a` → `def`; store `def` to `b`
    5. load from `a` → `def`; implicit cast `def` to `HashMap reference @0` → `HashMap reference @0`; load from `b` → `def`; implicit cast `def` to `HashMap reference @1` → `HashMap reference @1`; identity not equals `HashMap reference @0` and `HashMap reference @1` → `boolean false`; store `boolean false` to `b`; (note `HashMap reference @0` and `HashMap reference @1` refer to the same instance)



## Boolean Xor [boolean-xor-operator]

Use the `boolean xor operator '^'` to XOR together two `boolean` type values where if one `boolean` type value is `true` and the other is `false` the resultant `boolean` type value is `true` and `false` otherwise.

**Errors**

* If either evaluated value is a value other than a `boolean` type value or a value that is castable to a `boolean` type value.

**Truth**

|     |     |     |
| --- | --- | --- |
|  | true | false |
| true | false | true |
| false | true | false |

**Grammar**

```text
boolean_xor: expression '^' expression;
```

**Examples**

* Boolean xor with the `boolean` type.

    ```painless
    boolean x = false;    <1>
    boolean y = x ^ true; <2>
    y = y ^ x;            <3>
    ```

    1. declare `boolean x`; store `boolean false` to `x`
    2. declare `boolean y`; load from `x` → `boolean false` boolean xor `boolean false` and `boolean true` → `boolean true`; store `boolean true` to `y`
    3. load from `y` → `boolean true @0`; load from `x` → `boolean true @1`; boolean xor `boolean true @0` and `boolean true @1` → `boolean false`; store `boolean false` to `y`

* Boolean xor with the `def` type.

    ```painless
    def x = false;    <1>
    def y = x ^ true; <2>
    y = y ^ x;        <3>
    ```

    1. declare `def x`; implicit cast `boolean false` to `def` → `def`; store `def` to `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `boolean false` → `boolean false`; boolean xor `boolean false` and `boolean true` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `y`
    3. load from `y` → `def`; implicit cast `def` to `boolean true @0` → `boolean true @0`; load from `x` → `def`; implicit cast `def` to `boolean true @1` → `boolean true @1`; boolean xor `boolean true @0` and `boolean true @1` → `boolean false`; implicit cast `boolean false` → `def`; store `def` to `y`



## Boolean And [boolean-and-operator]

Use the `boolean and operator '&&'` to AND together two `boolean` type values where if both `boolean` type values are `true` the resultant `boolean` type value is `true` and `false` otherwise.

**Errors**

* If either evaluated value is a value other than a `boolean` type value or a value that is castable to a `boolean` type value.

**Truth**

|     |     |     |
| --- | --- | --- |
|  | true | false |
| true | true | false |
| false | false | false |

**Grammar**

```text
boolean_and: expression '&&' expression;
```

**Examples**

* Boolean and with the `boolean` type.

    ```painless
    boolean x = true;      <1>
    boolean y = x && true; <2>
    x = false;             <3>
    y = y && x;            <4>
    ```

    1. declare `boolean x`; store `boolean true` to `x`
    2. declare `boolean y`; load from `x` → `boolean true @0`; boolean and `boolean true @0` and `boolean true @1` → `boolean true`; store `boolean true` to `y`
    3. store `boolean false` to `x`
    4. load from `y` → `boolean true`; load from `x` → `boolean false`; boolean and `boolean true` and `boolean false` → `boolean false`; store `boolean false` to `y`

* Boolean and with the `def` type.

    ```painless
    def x = true;      <1>
    def y = x && true; <2>
    x = false;         <3>
    y = y && x;        <4>
    ```

    1. declare `def x`; implicit cast `boolean true` to `def` → `def`; store `def` to `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `boolean true @0` → `boolean true @0`; boolean and `boolean true @0` and `boolean true @1` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `y`
    3. implicit cast `boolean false` to `def` → `def`; store `def` to `x`;
    4. load from `y` → `def`; implicit cast `def` to `boolean true` → `boolean true`; load from `x` → `def`; implicit cast `def` to `boolean false` → `boolean false`; boolean and `boolean true` and `boolean false` → `boolean false`; implicit cast `boolean false` → `def`; store `def` to `y`



## Boolean Or [boolean-or-operator]

Use the `boolean or operator '||'` to OR together two `boolean` type values where if either one of the `boolean` type values is `true` the resultant `boolean` type value is `true` and `false` otherwise.

**Errors**

* If either evaluated value is a value other than a `boolean` type value or a value that is castable to a `boolean` type value.

**Truth**

|     |     |     |
| --- | --- | --- |
|  | true | false |
| true | true | true |
| false | true | false |

**Grammar:**

```text
boolean_and: expression '||' expression;
```

**Examples**

* Boolean or with the `boolean` type.

    ```painless
    boolean x = false;     <1>
    boolean y = x || true; <2>
    y = false;             <3>
    y = y || x;            <4>
    ```

    1. declare `boolean x`; store `boolean false` to `x`
    2. declare `boolean y`; load from `x` → `boolean false`; boolean or `boolean false` and `boolean true` → `boolean true`; store `boolean true` to `y`
    3. store `boolean false` to `y`
    4. load from `y` → `boolean false @0`; load from `x` → `boolean false @1`; boolean or `boolean false @0` and `boolean false @1` → `boolean false`; store `boolean false` to `y`

* Boolean or with the `def` type.

    ```painless
    def x = false;     <1>
    def y = x || true; <2>
    y = false;         <3>
    y = y || x;        <4>
    ```

    1. declare `def x`; implicit cast `boolean false` to `def` → `def`; store `def` to `x`
    2. declare `def y`; load from `x` → `def`; implicit cast `def` to `boolean false` → `boolean true`; boolean or `boolean false` and `boolean true` → `boolean true`; implicit cast `boolean true` to `def` → `def`; store `def` to `y`
    3. implicit cast `boolean false` to `def` → `def`; store `def` to `y`;
    4. load from `y` → `def`; implicit cast `def` to `boolean false @0` → `boolean false @0`; load from `x` → `def`; implicit cast `def` to `boolean false @1` → `boolean false @1`; boolean or `boolean false @0` and `boolean false @1` → `boolean false`; implicit cast `boolean false` → `def`; store `def` to `y`



