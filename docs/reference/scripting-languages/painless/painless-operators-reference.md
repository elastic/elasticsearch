---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators-reference.html
products:
  - id: painless
---

# Operators: Reference [painless-operators-reference]

## Method Call [method-call-operator]

Use the `method call operator '()'` to call a member method on a [reference type](/reference/scripting-languages/painless/painless-types.md#reference-types) value. Implicit [boxing/unboxing](/reference/scripting-languages/painless/painless-casting.md#boxing-unboxing) is evaluated as necessary per argument during the method call. When a method call is made on a target `def` type value, the parameters and return type value are considered to also be of the `def` type and are evaluated at run-time.

An overloaded method is one that shares the same name with two or more methods. A method is overloaded based on arity where the same name is re-used for multiple methods as long as the number of parameters differs.

**Errors**

* If the reference type value is `null`.
* If the member method name doesn’t exist for a given reference type value.
* If the number of arguments passed in is different from the number of specified parameters.
* If the arguments cannot be implicitly cast or implicitly boxed/unboxed to the correct type values for the parameters.

**Grammar**

```text
method_call: '.' ID arguments;
arguments: '(' (expression (',' expression)*)? ')';
```

**Examples**

* Method calls on different reference types.

    ```painless
    Map m = new HashMap();                         <1>
    m.put(1, 2);                                   <2>
    int z = m.get(1);                              <3>
    def d = new ArrayList();                       <4>
    d.add(1);                                      <5>
    int i = Integer.parseInt(d.get(0).toString()); <6>
    ```

    1. declare `Map m`; allocate `HashMap` instance → `HashMap reference`; store `HashMap reference` to `m`
    2. load from `m` → `Map reference`; implicit cast `int 1` to `def` → `def`; implicit cast `int 2` to `def` → `def`; call `put` on `Map reference` with arguments (`int 1`, `int 2`)
    3. declare `int z`; load from `m` → `Map reference`; call `get` on `Map reference` with arguments (`int 1`) → `def`; implicit cast `def` to `int 2` → `int 2`; store `int 2` to `z`
    4. declare `def d`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList` to `def` → `def`; store `def` to `d`
    5. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference` call `add` on `ArrayList reference` with arguments (`int 1`);
    6. declare `int i`; load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference` call `get` on `ArrayList reference` with arguments (`int 1`) → `def`; implicit cast `def` to `Integer 1 reference` → `Integer 1 reference`; call `toString` on `Integer 1 reference` → `String '1'`; call `parseInt` on `Integer` with arguments (`String '1'`) → `int 1`; store `int 1` in `i`;



## Field Access [field-access-operator]

Use the `field access operator '.'` to store a value to or load a value from a [reference type](/reference/scripting-languages/painless/painless-types.md#reference-types) member field.

**Errors**

* If the reference type value is `null`.
* If the member field name doesn’t exist for a given reference type value.

**Grammar**

```text
field_access: '.' ID;
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

* Field access with the `Example` type.

    ```painless
    Example example = new Example(); <1>
    example.x = 1;                   <2>
    example.y = example.x;           <3>
    example.z = new ArrayList();     <4>
    example.z.add(1);                <5>
    example.x = example.z.get(0);    <6>
    ```

    1. declare `Example example`; allocate `Example` instance → `Example reference`; store `Example reference` to `example`
    2. load from `example` → `Example reference`; store `int 1` to `x` of `Example reference`
    3. load from `example` → `Example reference @0`; load from `example` → `Example reference @1`; load from `x` of `Example reference @1` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` to `y` of `Example reference @0`; (note `Example reference @0` and `Example reference @1` are the same)
    4. load from `example` → `Example reference`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `z` of `Example reference`
    5. load from `example` → `Example reference`; load from `z` of `Example reference` → `List reference`; call `add` on `List reference` with arguments (`int 1`)
    6. load from `example` → `Example reference @0`; load from `example` → `Example reference @1`; load from `z` of `Example reference @1` → `List reference`; call `get` on `List reference` with arguments (`int 0`) → `int 1`; store `int 1` in `x` of `List reference @0`; (note `Example reference @0` and `Example reference @1` are the same)



## Null Safe [null-safe-operator]

Use the `null safe operator '?.'` instead of the method call operator or field access operator to ensure a reference type value is `non-null` before a method call or field access. A `null` value will be returned if the reference type value is `null`, otherwise the method call or field access is evaluated.

**Errors**

* If the method call return type value or the field access type value is not a reference type value and is not implicitly castable to a reference type value.

**Grammar**

```text
null_safe: null_safe_method_call
         | null_safe_field_access
         ;

null_safe_method_call: '?.' ID arguments;
arguments: '(' (expression (',' expression)*)? ')';

null_safe_field_access: '?.' ID;
```

**Examples**

The examples use the following reference type definition:

```painless
name:
  Example

non-static member methods:
  * List factory()

non-static member fields:
  * List x
```

* Null safe without a `null` value.

    ```painless
    Example example = new Example(); <1>
    List x = example?.factory();     <2>
    ```

    1. declare `Example example`; allocate `Example` instance → `Example reference`; store `Example reference` to `example`
    2. declare `List x`; load from `example` → `Example reference`; null safe call `factory` on `Example reference` → `List reference`; store `List reference` to `x`;

* Null safe with a `null` value;

    ```painless
    Example example = null; <1>
    List x = example?.x;    <2>
    ```

    1. declare `Example example`; store `null` to `example`
    2. declare `List x`; load from `example` → `Example reference`; null safe access `x` on `Example reference` → `null`; store `null` to `x`; (note the **null safe operator** returned `null` because `example` is `null`)



## List Initialization [list-initialization-operator]

Use the `list initialization operator '[]'` to allocate an `List` type instance to the heap with a set of pre-defined values. Each value used to initialize the `List` type instance is cast to a `def` type value upon insertion into the `List` type instance using the `add` method. The order of the specified values is maintained.

**Grammar**

```text
list_initialization: '[' expression (',' expression)* ']'
                   | '[' ']';
```

**Examples**

* List initialization of an empty `List` type value.

    ```painless
    List empty = []; <1>
    ```

    1. declare `List empty`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `empty`

* List initialization with static values.

    ```painless
    List list = [1, 2, 3]; <1>
    ```

    1. declare `List list`; allocate `ArrayList` instance → `ArrayList reference`; call `add` on `ArrayList reference` with arguments(`int 1`); call `add` on `ArrayList reference` with arguments(`int 2`); call `add` on `ArrayList reference` with arguments(`int 3`); implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `list`

* List initialization with non-static values.

    ```painless
    int i = 1;                  <1>
    long l = 2L;                <2>
    float f = 3.0F;             <3>
    double d = 4.0;             <4>
    String s = "5";             <5>
    List list = [i, l, f*d, s]; <6>
    ```

    1. declare `int i`; store `int 1` to `i`
    2. declare `long l`; store `long 2` to `l`
    3. declare `float f`; store `float 3.0` to `f`
    4. declare `double d`; store `double 4.0` to `d`
    5. declare `String s`; store `String "5"` to `s`
    6. declare `List list`; allocate `ArrayList` instance → `ArrayList reference`; load from `i` → `int 1`; call `add` on `ArrayList reference` with arguments(`int 1`); load from `l` → `long 2`; call `add` on `ArrayList reference` with arguments(`long 2`); load from `f` → `float 3.0`; load from `d` → `double 4.0`; promote `float 3.0` and `double 4.0`: result `double`; implicit cast `float 3.0` to `double 3.0` → `double 3.0`; multiply `double 3.0` and `double 4.0` → `double 12.0`; call `add` on `ArrayList reference` with arguments(`double 12.0`); load from `s` → `String "5"`; call `add` on `ArrayList reference` with arguments(`String "5"`); implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `list`



## List Access [list-access-operator]

Use the `list access operator '[]'` as a shortcut for a `set` method call or `get` method call made on a `List` type value.

**Errors**

* If a value other than a `List` type value is accessed.
* If a non-integer type value is used as an index for a `set` method call or `get` method call.

**Grammar**

```text
list_access: '[' expression ']'
```

**Examples**

* List access with the `List` type.

    ```painless
    List list = new ArrayList(); <1>
    list.add(1);                 <2>
    list.add(2);                 <3>
    list.add(3);                 <4>
    list[0] = 2;                 <5>
    list[1] = 5;                 <6>
    int x = list[0] + list[1];   <7>
    int y = 1;                   <8>
    int z = list[y];             <9>
    ```

    1. declare `List list`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `list`
    2. load from `list` → `List reference`; call `add` on `List reference` with arguments(`int 1`)
    3. load from `list` → `List reference`; call `add` on `List reference` with arguments(`int 2`)
    4. load from `list` → `List reference`; call `add` on `List reference` with arguments(`int 3`)
    5. load from `list` → `List reference`; call `set` on `List reference` with arguments(`int 0`, `int 2`)
    6. load from `list` → `List reference`; call `set` on `List reference` with arguments(`int 1`, `int 5`)
    7. declare `int x`; load from `list` → `List reference`; call `get` on `List reference` with arguments(`int 0`) → `def`; implicit cast `def` to `int 2` → `int 2`; load from `list` → `List reference`; call `get` on `List reference` with arguments(`int 1`) → `def`; implicit cast `def` to `int 5` → `int 5`; add `int 2` and `int 5` → `int 7`; store `int 7` to `x`
    8. declare `int y`; store `int 1` int `y`
    9. declare `int z`; load from `list` → `List reference`; load from `y` → `int 1`; call `get` on `List reference` with arguments(`int 1`) → `def`; implicit cast `def` to `int 5` → `int 5`; store `int 5` to `z`

* List access with the `def` type.

    ```painless
    def d = new ArrayList(); <1>
    d.add(1);                <2>
    d.add(2);                <3>
    d.add(3);                <4>
    d[0] = 2;                <5>
    d[1] = 5;                <6>
    def x = d[0] + d[1];     <7>
    def y = 1;               <8>
    def z = d[y];            <9>
    ```

    1. declare `List d`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def`; store `def` to `d`
    2. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `add` on `ArrayList reference` with arguments(`int 1`)
    3. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `add` on `ArrayList reference` with arguments(`int 2`)
    4. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `add` on `ArrayList reference` with arguments(`int 3`)
    5. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `set` on `ArrayList reference` with arguments(`int 0`, `int 2`)
    6. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `set` on `ArrayList reference` with arguments(`int 1`, `int 5`)
    7. declare `def x`; load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `get` on `ArrayList reference` with arguments(`int 0`) → `def`; implicit cast `def` to `int 2` → `int 2`; load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `get` on `ArrayList reference` with arguments(`int 1`) → `def`; implicit cast `def` to `int 2` → `int 2`; add `int 2` and `int 5` → `int 7`; store `int 7` to `x`
    8. declare `int y`; store `int 1` int `y`
    9. declare `int z`; load from `d` → `ArrayList reference`; load from `y` → `def`; implicit cast `def` to `int 1` → `int 1`; call `get` on `ArrayList reference` with arguments(`int 1`) → `def`; store `def` to `z`



## Map Initialization [map-initialization-operator]

Use the `map initialization operator '[:]'` to allocate a `Map` type instance to the heap with a set of pre-defined values. Each pair of values used to initialize the `Map` type instance are cast to `def` type values upon insertion into the `Map` type instance using the `put` method.

**Grammar**

```text
map_initialization: '[' key_pair (',' key_pair)* ']'
                  | '[' ':' ']';
key_pair: expression ':' expression
```

**Examples**

* Map initialization of an empty `Map` type value.

    ```painless
    Map empty = [:]; <1>
    ```

    1. declare `Map empty`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `empty`

* Map initialization with static values.

    ```painless
    Map map = [1:2, 3:4, 5:6]; <1>
    ```

    1. declare `Map map`; allocate `HashMap` instance → `HashMap reference`; call `put` on `HashMap reference` with arguments(`int 1`, `int 2`); call `put` on `HashMap reference` with arguments(`int 3`, `int 4`); call `put` on `HashMap reference` with arguments(`int 5`, `int 6`); implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `map`

* Map initialization with non-static values.

    ```painless
    byte b = 0;                  <1>
    int i = 1;                   <2>
    long l = 2L;                 <3>
    float f = 3.0F;              <4>
    double d = 4.0;              <5>
    String s = "5";              <6>
    Map map = [b:i, l:f*d, d:s]; <7>
    ```

    1. declare `byte b`; store `byte 0` to `b`
    2. declare `int i`; store `int 1` to `i`
    3. declare `long l`; store `long 2` to `l`
    4. declare `float f`; store `float 3.0` to `f`
    5. declare `double d`; store `double 4.0` to `d`
    6. declare `String s`; store `String "5"` to `s`
    7. declare `Map map`; allocate `HashMap` instance → `HashMap reference`; load from `b` → `byte 0`; load from `i` → `int 1`; call `put` on `HashMap reference` with arguments(`byte 0`, `int 1`); load from `l` → `long 2`; load from `f` → `float 3.0`; load from `d` → `double 4.0`; promote `float 3.0` and `double 4.0`: result `double`; implicit cast `float 3.0` to `double 3.0` → `double 3.0`; multiply `double 3.0` and `double 4.0` → `double 12.0`; call `put` on `HashMap reference` with arguments(`long 2`, `double 12.0`); load from `d` → `double 4.0`; load from `s` → `String "5"`; call `put` on `HashMap reference` with arguments(`double 4.0`, `String "5"`); implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `map`



## Map Access [map-access-operator]

Use the `map access operator '[]'` as a shortcut for a `put` method call or `get` method call made on a `Map` type value.

**Errors**

* If a value other than a `Map` type value is accessed.

**Grammar**

```text
map_access: '[' expression ']'
```

**Examples**

* Map access with the `Map` type.

    ```painless
    Map map = new HashMap();               <1>
    map['value2'] = 2;                     <2>
    map['value5'] = 5;                     <3>
    int x = map['value2'] + map['value5']; <4>
    String y = 'value5';                   <5>
    int z = x[z];                          <6>
    ```

    1. declare `Map map`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `map`
    2. load from `map` → `Map reference`; call `put` on `Map reference` with arguments(`String 'value2'`, `int 2`)
    3. load from `map` → `Map reference`; call `put` on `Map reference` with arguments(`String 'value5'`, `int 5`)
    4. declare `int x`; load from `map` → `Map reference`; call `get` on `Map reference` with arguments(`String 'value2'`) → `def`; implicit cast `def` to `int 2` → `int 2`; load from `map` → `Map reference`; call `get` on `Map reference` with arguments(`String 'value5'`) → `def`; implicit cast `def` to `int 5` → `int 5`; add `int 2` and `int 5` → `int 7`; store `int 7` to `x`
    5. declare `String y`; store `String 'value5'` to `y`
    6. declare `int z`; load from `map` → `Map reference`; load from `y` → `String 'value5'`; call `get` on `Map reference` with arguments(`String 'value5'`) → `def`; implicit cast `def` to `int 5` → `int 5`; store `int 5` to `z`

* Map access with the `def` type.

    ```painless
    def d = new HashMap();             <1>
    d['value2'] = 2;                   <2>
    d['value5'] = 5;                   <3>
    int x = d['value2'] + d['value5']; <4>
    String y = 'value5';               <5>
    def z = d[y];                      <6>
    ```

    1. declare `def d`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def`; store `def` to `d`
    2. load from `d` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; call `put` on `HashMap reference` with arguments(`String 'value2'`, `int 2`)
    3. load from `d` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; call `put` on `HashMap reference` with arguments(`String 'value5'`, `int 5`)
    4. declare `int x`; load from `d` → `def`; implicit cast `def` to `HashMap reference` → `HashMap reference`; call `get` on `HashMap reference` with arguments(`String 'value2'`) → `def`; implicit cast `def` to `int 2` → `int 2`; load from `d` → `def`; call `get` on `HashMap reference` with arguments(`String 'value5'`) → `def`; implicit cast `def` to `int 5` → `int 5`; add `int 2` and `int 5` → `int 7`; store `int 7` to `x`
    5. declare `String y`; store `String 'value5'` to `y`
    6. declare `def z`; load from `d` → `def`; load from `y` → `String 'value5'`; call `get` on `HashMap reference` with arguments(`String 'value5'`) → `def`; store `def` to `z`



## New Instance [new-instance-operator]

Use the `new instance operator 'new ()'` to allocate a [reference type](/reference/scripting-languages/painless/painless-types.md#reference-types) instance to the heap and call a specified constructor. Implicit [boxing/unboxing](/reference/scripting-languages/painless/painless-casting.md#boxing-unboxing) is evaluated as necessary per argument during the constructor call.

An overloaded constructor is one that shares the same name with two or more constructors. A constructor is overloaded based on arity where the same reference type name is re-used for multiple constructors as long as the number of parameters differs.

**Errors**

* If the reference type name doesn’t exist for instance allocation.
* If the number of arguments passed in is different from the number of specified parameters.
* If the arguments cannot be implicitly cast or implicitly boxed/unboxed to the correct type values for the parameters.

**Grammar**

```text
new_instance: 'new' TYPE '(' (expression (',' expression)*)? ')';
```

**Examples**

* Allocation of new instances with different types.

```painless
Map m = new HashMap();   <1>
def d = new ArrayList(); <2>
def e = new HashMap(m);  <3>
```

1. declare `Map m`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `m`;
2. declare `def d`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def`; store `def` to `d`;
3. declare `def e`; load from `m` → `Map reference`; allocate `HashMap` instance with arguments (`Map reference`) → `HashMap reference`; implicit cast `HashMap reference` to `def` → `def`; store `def` to `e`;



## String Concatenation [string-concatenation-operator]

Use the `string concatenation operator '+'` to concatenate two values together where at least one of the values is a [`String` type](/reference/scripting-languages/painless/painless-types.md#string-type).

**Grammar**

```text
concatenate: expression '+' expression;
```

**Examples**

* String concatenation with different primitive types.

    ```painless
    String x = "con";     <1>
    String y = x + "cat"; <2>
    String z = 4 + 5 + x; <3>
    ```

    1. declare `String x`; store `String "con"` to `x`;
    2. declare `String y`; load from `x` → `String "con"`; concat `String "con"` and `String "cat"` → `String "concat"`; store `String "concat"` to `y`
    3. declare `String z`; add `int 4` and `int 5` → `int 9`; concat `int 9` and `String "9concat"`; store `String "9concat"` to `z`; (note the addition is done prior to the concatenation due to precedence and associativity of the specific operations)

* String concatenation with the `def` type.

    ```painless
    def d = 2;             <1>
    d = "con" + d + "cat"; <2>
    ```

    1. declare `def`; implicit cast `int 2` to `def` → `def`; store `def` in `d`;
    2. concat `String "con"` and `int 2` → `String "con2"`; concat `String "con2"` and `String "cat"` → `String "con2cat"` implicit cast `String "con2cat"` to `def` → `def`; store `def` to `d`; (note the switch in type of `d` from `int` to `String`)



## Elvis [elvis-operator]

An elvis consists of two expressions. The first expression is evaluated with to check for a `null` value. If the first expression evaluates to `null` then the second expression is evaluated and its value used. If the first expression evaluates to `non-null` then the resultant value of the first expression is used. Use the `elvis operator '?:'` as a shortcut for the conditional operator.

**Errors**

* If the first expression or second expression cannot produce a `null` value.

**Grammar**

```text
elvis: expression '?:' expression;
```

**Examples**

* Elvis with different reference types.

    ```painless
    List x = new ArrayList();      <1>
    List y = x ?: new ArrayList(); <2>
    y = null;                      <3>
    List z = y ?: new ArrayList(); <4>
    ```

    1. declare `List x`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `x`;
    2. declare `List y`; load `x` → `List reference`; `List reference` equals `null` → `false`; evaluate 1st expression: `List reference` → `List reference`; store `List reference` to `y`
    3. store `null` to `y`;
    4. declare `List z`; load `y` → `List reference`; `List reference` equals `null` → `true`; evaluate 2nd expression: allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `z`;



