---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-casting.html
products:
  - id: painless
---

# Casting [painless-casting]

A cast converts the value of an original type to the equivalent value of a target type. An implicit cast infers the target type and automatically occurs during certain [operations](/reference/scripting-languages/painless/painless-operators.md). An explicit cast specifies the target type and forcefully occurs as its own operation. Use the `cast operator '()'` to specify an explicit cast.

Refer to the [cast table](#allowed-casts) for a quick reference on all allowed casts.

**Errors**

* If during a cast there exists no equivalent value for the target type.
* If an implicit cast is given, but an explicit cast is required.

**Grammar**

```text
cast: '(' TYPE ')' expression
```

**Examples**

* Valid casts.

    ```painless
    int i = (int)5L;         <1>
    Map m = new HashMap();   <2>
    HashMap hm = (HashMap)m; <3>
    ```

    1. declare `int i`; explicit cast `long 5` to `int 5` → `int 5`; store `int 5` to `i`
    2. declare `Map m`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `m`
    3. declare `HashMap hm`; load from `m` → `Map reference`; explicit cast `Map reference` to `HashMap reference` → `HashMap reference`; store `HashMap reference` to `hm`


## Numeric Type Casting [numeric-type-casting]

A [numeric type](/reference/scripting-languages/painless/painless-types.md#primitive-types) cast converts the value of an original numeric type to the equivalent value of a target numeric type. A cast between two numeric type values results in data loss when the value of the original numeric type is larger than the target numeric type can accommodate. A cast between an integer type value and a floating point type value can result in precision loss.

The allowed casts for values of each numeric type are shown as a row in the following table:

|     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- |
|  | byte | short | char | int | long | float | double |
| byte |  | implicit | implicit | implicit | implicit | implicit | implicit |
| short | explicit |  | explicit | implicit | implicit | implicit | implicit |
| char | explicit | explicit |  | implicit | implicit | implicit | implicit |
| int | explicit | explicit | explicit |  | implicit | implicit | implicit |
| long | explicit | explicit | explicit | explicit |  | implicit | implicit |
| float | explicit | explicit | explicit | explicit | explicit |  | implicit |
| double | explicit | explicit | explicit | explicit | explicit | explicit |  |

**Examples**

* Valid numeric type casts.

    ```painless
    int a = 1;            <1>
    long b = a;           <2>
    short c = (short)b;   <3>
    double e = (double)a; <4>
    ```

    1. declare `int a`; store `int 1` to `a`
    2. declare `long b`; load from `a` → `int 1`; implicit cast `int 1` to `long 1` → `long 1`; store `long 1` to `b`
    3. declare `short c`; load from `b` → `long 1`; explicit cast `long 1` to `short 1` → `short 1`; store `short 1` value to `c`
    4. declare `double e`; load from `a` → `int 1`; explicit cast `int 1` to `double 1.0`; store `double 1.0` to `e`; (note the explicit cast is extraneous since an implicit cast is valid)

* Invalid numeric type casts resulting in errors.

    ```painless
    int a = 1.0; // error <1>
    int b = 2;            <2>
    byte c = b;  // error <3>
    ```

    1. declare `int i`; **error** → cannot implicit cast `double 1.0` to `int 1`; (note an explicit cast is valid)
    2. declare `int b`; store `int 2` to `b`
    3. declare byte `c`; load from `b` → `int 2`; **error** → cannot implicit cast `int 2` to `byte 2`; (note an explicit cast is valid)



## Reference Type Casting [reference-type-casting]

A [reference type](/reference/scripting-languages/painless/painless-types.md#reference-types) cast converts the value of an original reference type to the equivalent value of a target reference type. An implicit cast between two reference type values is allowed when the original reference type is a descendant of the target type. An explicit cast between two reference type values is allowed when the original type is a descendant of the target type or the target type is a descendant of the original type.

**Examples**

* Valid reference type casts.

    ```painless
    List x;                        <1>
    ArrayList y = new ArrayList(); <2>
    x = y;                         <3>
    y = (ArrayList)x;              <4>
    x = (List)y;                   <5>
    ```

    1. declare `List x`; store default value `null` to `x`
    2. declare `ArrayList y`; allocate `ArrayList` instance → `ArrayList reference`; store `ArrayList reference` to `y`;
    3. load from `y` → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `x`; (note `ArrayList` is a descendant of `List`)
    4. load from `x` → `List reference`; explicit cast `List reference` to `ArrayList reference` → `ArrayList reference`; store `ArrayList reference` to `y`;
    5. load from `y` → `ArrayList reference`; explicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `x`; (note the explicit cast is extraneous, and an implicit cast is valid)

* Invalid reference type casts resulting in errors.

    ```painless
    List x = new ArrayList();          <1>
    ArrayList y = x;          // error <2>
    Map m = (Map)x;           // error <3>
    ```

    1. declare `List x`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `x`
    2. declare `ArrayList y`; load from `x` → `List reference`; **error** → cannot implicit cast `List reference` to `ArrayList reference`; (note an explicit cast is valid since `ArrayList` is a descendant of `List`)
    3. declare `ArrayList y`; load from `x` → `List reference`; **error** → cannot explicit cast `List reference` to `Map reference`; (note no cast is valid since neither `List` nor `Map` is a descendant of the other)



## Dynamic Type Casting [dynamic-type-casting]

A [dynamic (`def`) type](/reference/scripting-languages/painless/painless-types.md#dynamic-types) cast converts the value of an original `def` type to the equivalent value of any target type or converts the value of any original type to the equivalent value of a target `def` type.

An implicit cast from any original type value to a `def` type value is always allowed. An explicit cast from any original type value to a `def` type value is always allowed but never necessary.

An implicit or explicit cast from an original `def` type value to any target type value is allowed if and only if the cast is normally allowed based on the current type value the `def` type value represents.

**Examples**

* Valid dynamic type casts with any original type to a target `def` type.

    ```painless
    def d0 = 3;               <1>
    d0 = new ArrayList();     <2>
    Object o = new HashMap(); <3>
    def d1 = o;               <4>
    int i = d1.size();        <5>
    ```

    1. declare `def d0`; implicit cast `int 3` to `def`; store `int 3` to `d0`
    2. allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def`; store `def` to `d0`
    3. declare `Object o`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Object reference` → `Object reference`; store `Object reference` to `o`
    4. declare `def d1`; load from `o` → `Object reference`; implicit cast `Object reference` to `def` → `def`; store `def` to `d1`
    5. declare `int i`; load from `d1` → `def`; implicit cast `def` to `HashMap reference` → HashMap reference`; call `size` on `HashMap reference` → `int 0`; store `int 0` to `i`; (note `def` was implicit cast to `HashMap reference` since `HashMap` is the child-most descendant type value that the `def` type value represents)

* Valid dynamic type casts with an original `def` type to any target type.

    ```painless
    def d = 1.0;         <1>
    int i = (int)d;      <2>
    d = 1;               <3>
    float f = d;         <4>
    d = new ArrayList(); <5>
    List l = d;          <6>
    ```

    1. declare `def d`; implicit cast `double 1.0` to `def` → `def`; store `def` to `d`
    2. declare `int i`; load from `d` → `def`; implicit cast `def` to `double 1.0` → `double 1.0`; explicit cast `double 1.0` to `int 1` → `int 1`; store `int 1` to `i`; (note the explicit cast is necessary since a `double` type value is not converted to an `int` type value implicitly)
    3. store `int 1` to `d`; (note the switch in the type `d` represents from `double` to `int`)
    4. declare `float i`; load from `d` → `def`; implicit cast `def` to `int 1` → `int 1`; implicit cast `int 1` to `float 1.0` → `float 1.0`; store `float 1.0` to `f`
    5. allocate `ArrayList` instance → `ArrayList reference`; store `ArrayList reference` to `d`; (note the switch in the type `d` represents from `int` to `ArrayList`)
    6. declare `List l`; load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `l`

* Invalid dynamic type casts resulting in errors.

    ```painless
    def d = 1;                  <1>
    short s = d;       // error <2>
    d = new HashMap();          <3>
    List l = d;        // error <4>
    ```

    1. declare `def d`; implicit cast `int 1` to `def` → `def`; store `def` to `d`
    2. declare `short s`; load from `d` → `def`; implicit cast `def` to `int 1` → `int 1`; **error** → cannot implicit cast `int 1` to `short 1`; (note an explicit cast is valid)
    3. allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def`; store `def` to `d`
    4. declare `List l`; load from `d` → `def`; implicit cast `def` to `HashMap reference`; **error** → cannot implicit cast `HashMap reference` to `List reference`; (note no cast is valid since neither `HashMap` nor `List` is a descendant of the other)



## String to Character Casting [string-character-casting]

Use the cast operator to convert a [`String` type](/reference/scripting-languages/painless/painless-types.md#string-type) value into a [`char` type](/reference/scripting-languages/painless/painless-types.md#primitive-types) value.

**Errors**

* If the `String` type value isn’t one character in length.
* If the `String` type value is `null`.

**Examples**

* Casting string literals into `char` type values.

    ```painless
    char c = (char)"C"; <1>
    c = (char)'c';      <2>
    ```

    1. declare `char c`; explicit cast `String "C"` to `char C` → `char C`; store `char C` to `c`
    2. explicit cast `String 'c'` to `char c` → `char c`; store `char c` to `c`

* Casting a `String` reference into a `char` type value.

    ```painless
    String s = "s";   <1>
    char c = (char)s; <2>
    ```

    1. declare `String s`; store `String "s"` to `s`;
    2. declare `char c` load from `s` → `String "s"`; explicit cast `String "s"` to `char s` → `char s`; store `char s` to `c`



## Character to String Casting [character-string-casting]

Use the cast operator to convert a [`char` type](/reference/scripting-languages/painless/painless-types.md#primitive-types) value into a [`String` type](/reference/scripting-languages/painless/painless-types.md#string-type) value.

**Examples**

* Casting a `String` reference into a `char` type value.

    ```painless
    char c = 65;          <1>
    String s = (String)c; <2>
    ```

    1. declare `char c`; store `char 65` to `c`;
    2. declare `String s` load from `c` → `char A`; explicit cast `char A` to `String "A"` → `String "A"`; store `String "A"` to `s`



## Boxing and Unboxing [boxing-unboxing]

Boxing is a special type of cast used to convert a primitive type to its corresponding reference type. Unboxing is the reverse used to convert a reference type to its corresponding primitive type.

Implicit boxing/unboxing occurs during the following operations:

* Conversions between a `def` type and a primitive type are implicitly boxed/unboxed as necessary, though this is referred to as an implicit cast throughout the documentation.
* Method/function call arguments are implicitly boxed/unboxed as necessary.
* A primitive type value is implicitly boxed when a reference type method is called on it.

Explicit boxing/unboxing is not allowed. Use the reference type API to explicitly convert a primitive type value to its respective reference type value and vice versa.

**Errors**

* If an explicit cast is made to box/unbox a primitive type.

**Examples**

* Uses of implicit boxing/unboxing.

    ```painless
    List l = new ArrayList();       <1>
    l.add(1);                       <2>
    Integer I = Integer.valueOf(0); <3>
    int i = l.get(i);               <4>
    ```

    1. declare `List l`; allocate `ArrayList` instance → `ArrayList reference`; store `ArrayList reference` to `l`;
    2. load from `l` → `List reference`; implicit cast `int 1` to `def` → `def`; call `add` on `List reference` with arguments (`def`); (note internally `int 1` is boxed to `Integer 1` to store as a `def` type value)
    3. declare `Integer I`; call `valueOf` on `Integer` with arguments of (`int 0`) → `Integer 0`; store `Integer 0` to `I`;
    4. declare `int i`; load from `I` → `Integer 0`; unbox `Integer 0` → `int 0`; load from `l` → `List reference`; call `get` on `List reference` with arguments (`int 0`) → `def`; implicit cast `def` to `int 1` → `int 1`; store `int 1` to `i`; (note internally `int 1` is unboxed from `Integer 1` when loaded from a `def` type value)

* Uses of invalid boxing/unboxing resulting in errors.

    ```painless
    Integer x = 1;                   // error <1>
    Integer y = (Integer)1;          // error <2>
    int a = Integer.valueOf(1);      // error <3>
    int b = (int)Integer.valueOf(1); // error <4>
    ```

    1. declare `Integer x`; **error** → cannot implicit box `int 1` to `Integer 1` during assignment
    2. declare `Integer y`; **error** → cannot explicit box `int 1` to `Integer 1` during assignment
    3. declare `int a`; call `valueOf` on `Integer` with arguments of (`int 1`) → `Integer 1`; **error** → cannot implicit unbox `Integer 1` to `int 1` during assignment
    4. declare `int a`; call `valueOf` on `Integer` with arguments of (`int 1`) → `Integer 1`; **error** → cannot explicit unbox `Integer 1` to `int 1` during assignment



## Promotion [promotion]

Promotion is when a single value is implicitly cast to a certain type or multiple values are implicitly cast to the same type as required for evaluation by certain operations. Each operation that requires promotion has a promotion table that shows all required implicit casts based on the type(s) of value(s). A value promoted to a `def` type at compile-time is promoted again at run-time based on the type the `def` value represents.

**Errors**

* If a specific operation cannot find an allowed promotion type for the type(s) of value(s) given.

**Examples**

* Uses of promotion.

    ```painless
    double d = 2 + 2.0; <1>
    def x = 1;          <2>
    float f = x + 2.0F; <3>
    ```

    1. declare `double d`; promote `int 2` and `double 2.0 @0`: result `double`; implicit cast `int 2` to `double 2.0 @1` → `double 2.0 @1`; add `double 2.0 @1` and `double 2.0 @0` → `double 4.0`; store `double 4.0` to `d`
    2. declare `def x`; implicit cast `int 1` to `def` → `def`; store `def` to `x`;
    3. declare `float f`; load from `x` → `def`; implicit cast `def` to `int 1` → `int 1`; promote `int 1` and `float 2.0`: result `float`; implicit cast `int 1` to `float 1.0` → `float `1.0`; add `float 1.0` and `float 2.0` → `float 3.0`; store `float 3.0` to `f`; (note this example illustrates promotion done at run-time as promotion done at compile-time would have resolved to a `def` type value)



## Allowed Casts [allowed-casts]

The following tables show all allowed casts. Read the tables row by row, where the original type is shown in the first column, and each subsequent column indicates whether a cast to the specified target type is implicit (I), explicit (E), boxed/unboxed for methods only (A), a reference type cast (@), or is not allowed (-). See [reference type casting](#reference-type-casting) for allowed reference type casts.

**Primitive/Reference Types**

|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | O | N | T | b | y | s | c | i | j | f | d | B | Y | S | C | I | J | F | D | R | def |
| Object    ( O ) |  | @ | @ | - | - | - | - | - | - | - | - | @ | @ | @ | @ | @ | @ | @ | @ | @ | I |
| Number    ( N ) | I |  | - | - | - | - | - | - | - | - | - | - | @ | @ | - | @ | @ | @ | @ | @ | I |
| String    ( T ) | I | - |  | - | - | - | - | - | - | - | - | - | - | - | E | - | - | - | - | - | I |
| boolean   ( b ) | A | - | - |  | - | - | - | - | - | - | - | A | - | - | - | - | - | - | - | - | I |
| byte      ( y ) | A | A | - | - |  | I | E | I | I | I | I | - | A | A | - | A | A | A | A | - | I |
| short     ( s ) | A | A | - | - | E |  | E | I | I | I | I | - | - | A | - | A | A | A | A | - | I |
| char      ( c ) | A | - | E | - | E | E |  | I | I | I | I | - | - | - | A | A | A | A | A | - | I |
| int       ( i ) | A | A | - | - | E | E | E |  | I | I | I | - | - | - | - | A | A | A | A | - | I |
| long      ( j ) | A | A | - | - | E | E | E | E |  | I | I | - | - | - | - | - | A | A | A | - | I |
| float     ( f ) | A | A | - | - | E | E | E | E | E |  | I | - | - | - | - | - | - | A | A | - | I |
| double    ( d ) | A | A | - | - | E | E | E | E | E | E |  | - | - | - | - | - | - | - | A | - | I |
| Boolean   ( B ) | A | - | - | A | - | - | - | - | - | - | - |  | - | - | - | - | - | - | - | @ | I |
| Byte      ( Y ) | A | I | - | - | A | A | - | A | A | A | A | - |  | A | - | A | A | A | A | @ | I |
| Short     ( S ) | A | I | - | - | - | A | - | A | A | A | A | - | - |  | - | A | A | A | A | @ | I |
| Character ( C ) | A | - | - | - | - | - | A | A | A | A | A | - | - | - |  | A | A | A | A | @ | I |
| Integer   ( I ) | A | - | - | - | - | - | - | A | A | A | A | - | - | - | - |  | A | A | A | @ | I |
| Long      ( J ) | A | - | - | - | - | - | - | - | A | A | A | - | - | - | - | - |  | A | A | @ | I |
| Float     ( F ) | A | - | - | - | - | - | - | - | - | A | A | - | - | - | - | - | - |  | A | @ | I |
| Double    ( D ) | A | - | - | - | - | - | - | - | - | - | A | - | - | - | - | - | - | - |  | @ | I |
| Reference ( R ) | I | @ | @ | - | - | - | - | - | - | - | - | @ | @ | @ | @ | @ | @ | @ | @ | @ | I |

**`def` Type**

|     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |     |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  | O | N | T | b | y | s | c | i | j | f | d | B | Y | S | C | I | J | F | D | R |
| def as String | I | - | I | - | - | - | E | - | - | - | - | - | - | - | E | - | - | - | - | @ |
| def as boolean/Boolean | I | - | - | I | - | - | - | - | - | - | - | I | - | - | - | - | - | - | - | @ |
| def as byte/Byte | I | - | - | - | I | I | E | I | I | I | I | - | I | I | E | I | I | I | I | @ |
| def as short/Short | I | - | - | - | E | I | E | I | I | I | I | - | E | I | E | I | I | I | I | @ |
| def as char/Character | I | - | - | - | E | E | I | I | I | I | I | - | E | E | I | I | I | I | I | @ |
| def as int/Integer | I | - | - | - | E | E | E | I | I | I | I | - | E | E | E | I | I | I | I | @ |
| def as long/Long | I | - | - | - | E | E | E | E | I | I | I | - | E | E | E | E | I | I | I | @ |
| def as float/Float | I | - | - | - | E | E | E | E | E | I | I | - | E | E | E | E | E | I | I | @ |
| def as double/Double | I | - | - | - | E | E | E | E | E | E | I | - | E | E | E | E | E | E | I | @ |
| def as Reference | @ | @ | @ | - | - | - | - | - | - | - | - | @ | @ | @ | @ | @ | @ | @ | @ | @ |


