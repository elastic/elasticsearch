---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-types.html
---

# Types [painless-types]

A type is a classification of data used to define the properties of a value. These properties specify what data a value represents and the rules for how a value is evaluated during an [operation](/reference/scripting-languages/painless/painless-operators.md). Each type belongs to one of the following categories: [primitive](#primitive-types), [reference](#reference-types), or [dynamic](#dynamic-types).

## Primitive Types [primitive-types]

A primitive type represents basic data built natively into the JVM and is allocated to non-heap memory. Declare a primitive type [variable](/reference/scripting-languages/painless/painless-variables.md) or access a primitive type member field (from a reference type instance), and assign it a primitive type value for evaluation during later operations. The default value for a newly-declared primitive type variable is listed as part of the definitions below. A primitive type value is copied during an assignment or as an argument for a method/function call.

A primitive type has a corresponding reference type (also known as a boxed type). Use the [field access operator](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) or [method call operator](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator) on a primitive type value to force evaluation as its corresponding reference type value.

The following primitive types are available. The corresponding reference type is listed in parentheses. For example, `Byte` is the reference type for the `byte` primitive type:

::::{dropdown} Available primitive types
:name: available-primitive-types

`byte` (`Byte`)
:   8-bit, signed, two’s complement integer. Range: [`-128`, `127`]. Default: `0`.

`short` (`Short`)
:   16-bit, signed, two’s complement integer. Range: [`-32768`, `32767`]. Default: `0`.

`char` (`Character`)
:   16-bit, unsigned, Unicode character. Range: [`0`, `65535`]. Default: `0` or `\u0000`.

`int` (`Integer`)
:   32-bit, signed, two’s complement integer. Range: [`-2^31`, `2^31-1`]. Default: `0`.

`long` (`Long`)
:   64-bit, signed, two’s complement integer. Range: [`-2^63`, `2^63-1`]. Default: `0`.

`float (`Float`)`
:   32-bit, signed, single-precision, IEEE 754 floating point number. Default `0.0`.

`double` (`Double`)
:   64-bit, signed, double-precision, IEEE 754 floating point number. Default: `0.0`.

`boolean` (`Boolean`)
:   logical quantity with two possible values of `true` and `false`. Default: `false`.

::::


**Examples**

* Primitive types used in declaration, declaration and assignment.

    ```painless
    int i = 1;        <1>
    double d;         <2>
    boolean b = true; <3>
    ```

    1. declare `int i`; store `int 1` to `i`
    2. declare `double d`; store default `double 0.0` to `d`
    3. declare `boolean b`; store `boolean true` to `b`

* Method call on a primitive type using the corresponding reference type.

    ```painless
    int i = 1;    <1>
    i.toString(); <2>
    ```

    1. declare `int i`; store `int 1` to `i`
    2. load from `i` → `int 1`; box `int 1` → `Integer 1 reference`; call `toString` on `Integer 1 reference` → `String '1'`



## Reference Types [reference-types]

A reference type is a named construct (object), potentially representing multiple pieces of data (member fields) and logic to manipulate that data (member methods), defined as part of the application programming interface (API) for scripts.

A reference type instance is a single set of data for one reference type object allocated to the heap. Use the [new instance operator](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator) to allocate a reference type instance. Use a reference type instance to load from, store to, and manipulate complex data.

A reference type value refers to a reference type instance, and multiple reference type values may refer to the same reference type instance. A change to a reference type instance will affect all reference type values referring to that specific instance.

Declare a reference type [variable](/reference/scripting-languages/painless/painless-variables.md) or access a reference type member field (from a reference type instance), and assign it a reference type value for evaluation during later operations. The default value for a newly-declared reference type variable is `null`. A reference type value is shallow-copied during an assignment or as an argument for a method/function call. Assign `null` to a reference type variable to indicate the reference type value refers to no reference type instance. The JVM will garbage collect a reference type instance when it is no longer referred to by any reference type values. Pass `null` as an argument to a method/function call to indicate the argument refers to no reference type instance.

A reference type object defines zero-to-many of each of the following:

static member field
:   A static member field is a named and typed piece of data. Each reference type **object** contains one set of data representative of its static member fields. Use the [field access operator](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) in correspondence with the reference type object name to access a static member field for loading and storing to a specific reference type **object**. No reference type instance allocation is necessary to use a static member field.

non-static member field
:   A non-static member field is a named and typed piece of data. Each reference type **instance** contains one set of data representative of its reference type object’s non-static member fields. Use the [field access operator](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) for loading and storing to a non-static member field of a specific reference type **instance**. An allocated reference type instance is required to use a non-static member field.

static member method
:   A static member method is a [function](/reference/scripting-languages/painless/painless-functions.md) called on a reference type **object**. Use the [method call operator](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator) in correspondence with the reference type object name to call a static member method. No reference type instance allocation is necessary to use a static member method.

non-static member method
:   A non-static member method is a [function](/reference/scripting-languages/painless/painless-functions.md) called on a reference type **instance**. A non-static member method called on a reference type instance can load from and store to non-static member fields of that specific reference type instance. Use the [method call operator](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator) in correspondence with a specific reference type instance to call a non-static member method. An allocated reference type instance is required to use a non-static member method.

constructor
:   A constructor is a special type of [function](/reference/scripting-languages/painless/painless-functions.md) used to allocate a reference type **instance** defined by a specific reference type **object**. Use the [new instance operator](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator) to allocate a reference type instance.

A reference type object follows a basic inheritance model. Consider types A and B. Type A is considered to be a parent of B, and B a child of A, if B inherits (is able to access as its own) all of A’s non-static members. Type B is considered a descendant of A if there exists a recursive parent-child relationship from B to A with none to many types in between. In this case, B inherits all of A’s non-static members along with all of the non-static members of the types in between. Type B is also considered to be a type A in both relationships.

**Examples**

* Reference types evaluated in several different operations.

    ```painless
    List l = new ArrayList(); <1>
    l.add(1);                 <2>
    int i = l.get(0) + 2;     <3>
    ```

    1. declare `List l`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `l`
    2. load from `l` → `List reference`; implicit cast `int 1` to `def` → `def` call `add` on `List reference` with arguments (`def`)
    3. declare `int i`; load from `l` → `List reference`; call `get` on `List reference` with arguments (`int 0`) → `def`; implicit cast `def` to `int 1` → `int 1`; add `int 1` and `int 2` → `int 3`; store `int 3` to `i`

* Sharing a reference type instance.

    ```painless
    List l0 = new ArrayList();     <1>
    List l1 = l0;                  <2>
    l0.add(1);                     <3>
    l1.add(2);                     <4>
    int i = l1.get(0) + l0.get(1); <5>
    ```

    1. declare `List l0`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `l0`
    2. declare `List l1`; load from `l0` → `List reference`; store `List reference` to `l1` (note `l0` and `l1` refer to the same instance known as a shallow-copy)
    3. load from `l0` → `List reference`; implicit cast `int 1` to `def` → `def` call `add` on `List reference` with arguments (`def`)
    4. load from `l1` → `List reference`; implicit cast `int 2` to `def` → `def` call `add` on `List reference` with arguments (`def`)
    5. declare `int i`; load from `l0` → `List reference`; call `get` on `List reference` with arguments (`int 0`) → `def @0`; implicit cast `def @0` to `int 1` → `int 1`; load from `l1` → `List reference`; call `get` on `List reference` with arguments (`int 1`) → `def @1`; implicit cast `def @1` to `int 2` → `int 2`; add `int 1` and `int 2` → `int 3`; store `int 3` to `i`;

* Using the static members of a reference type.

    ```painless
    int i = Integer.MAX_VALUE;       <1>
    long l = Long.parseLong("123L"); <2>
    ```

    1. declare `int i`; load from `MAX_VALUE` on `Integer` → `int 2147483647`; store `int 2147483647` to `i`
    2. declare `long l`; call `parseLong` on `Long` with arguments (`long 123`) → `long 123`; store `long 123` to `l`



## Dynamic Types [dynamic-types]

A dynamic type value can represent the value of any primitive type or reference type using a single type name `def`. A `def` type value mimics the behavior of whatever value it represents at run-time and will always represent the child-most descendant type value of any type value when evaluated during operations.

Declare a `def` type [variable](/reference/scripting-languages/painless/painless-variables.md) or access a `def` type member field (from a reference type instance), and assign it any type of value for evaluation during later operations. The default value for a newly-declared `def` type variable is `null`. A `def` type variable or method/function parameter can change the type it represents during the compilation and evaluation of a script.

Using the `def` type can have a slight impact on performance. Use only primitive types and reference types directly when performance is critical.

**Errors**

* If a `def` type value represents an inappropriate type for evaluation of an operation at run-time.

**Examples**

* General uses of the `def` type.

    ```painless
    def dp = 1;               <1>
    def dr = new ArrayList(); <2>
    dr = dp;                  <3>
    ```

    1. declare `def dp`; implicit cast `int 1` to `def` → `def`; store `def` to `dp`
    2. declare `def dr`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `def` → `def`; store `def` to `dr`
    3. load from `dp` → `def`; store `def` to `dr`; (note the switch in the type `dr` represents from `ArrayList` to `int`)

* A `def` type value representing the child-most descendant of a value.

    ```painless
    Object l = new ArrayList(); <1>
    def d = l;                  <2>
    d.ensureCapacity(10);       <3>
    ```

    1. declare `Object l`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `Object reference` → `Object reference`; store `Object reference` to `l`
    2. declare `def d`; load from `l` → `Object reference`; implicit cast `Object reference` to `def` → `def`; store `def` to `d`;
    3. load from `d` → `def`; implicit cast `def` to `ArrayList reference` → `ArrayList reference`; call `ensureCapacity` on `ArrayList reference` with arguments (`int 10`); (note `def` was implicit cast to `ArrayList reference` since ArrayList` is the child-most descendant type value that the `def` type value represents)



## String Type [string-type]

The `String` type is a specialized reference type that does not require explicit allocation. Use a [string literal](/reference/scripting-languages/painless/painless-literals.md#string-literals) to directly evaluate a `String` type value. While not required, the [new instance operator](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator) can allocate `String` type instances.

**Examples**

* General use of the `String` type.

    ```painless
    String r = "some text";             <1>
    String s = 'some text';             <2>
    String t = new String("some text"); <3>
    String u;                           <4>
    ```

    1. declare `String r`; store `String "some text"` to `r`
    2. declare `String s`; store `String 'some text'` to `s`
    3. declare `String t`; allocate `String` instance with arguments (`String "some text"`) → `String "some text"`; store `String "some text"` to `t`
    4. declare `String u`; store default `null` to `u`



## void Type [void-type]

The `void` type represents the concept of a lack of type. Use the `void` type to indicate a function returns no value.

**Examples**

* Use of the `void` type in a function.

    ```painless
    void addToList(List l, def d) {
        l.add(d);
    }
    ```



## Array Type [array-type]

An array type is a specialized reference type where an array type instance contains a series of values allocated to the heap. Each value in an array type instance is defined as an element. All elements in an array type instance are of the same type (element type) specified as part of declaration. Each element is assigned an index within the range `[0, length)` where length is the total number of elements allocated for an array type instance.

Use the [new array operator](/reference/scripting-languages/painless/painless-operators-array.md#new-array-operator) or the [array initialization operator](/reference/scripting-languages/painless/painless-operators-array.md#array-initialization-operator) to allocate an array type instance. Declare an array type [variable](/reference/scripting-languages/painless/painless-variables.md) or access an array type member field (from a reference type instance), and assign it an array type value for evaluation during later operations. The default value for a newly-declared array type variable is `null`. An array type value is shallow-copied during an assignment or as an argument for a method/function call. Assign `null` to an array type variable to indicate the array type value refers to no array type instance. The JVM will garbage collect an array type instance when it is no longer referred to by any array type values. Pass `null` as an argument to a method/function call to indicate the argument refers to no array type instance.

Use the [array length operator](/reference/scripting-languages/painless/painless-operators-array.md#array-length-operator) to retrieve the length of an array type value as an `int` type value. Use the [array access operator](/reference/scripting-languages/painless/painless-operators-array.md#array-access-operator) to load from and store to an individual element within an array type instance.

When an array type instance is allocated with multiple dimensions using the range `[2, d]` where `d >= 2`, each element within each dimension in the range `[1, d-1]` is also an array type. The element type of each dimension, `n`, is an array type with the number of dimensions equal to `d-n`. For example, consider `int[][][]` with 3 dimensions. Each element in the 3rd dimension, `d-3`, is the primitive type `int`. Each element in the 2nd dimension, `d-2`, is the array type `int[]`. And each element in the 1st dimension, `d-1` is the array type `int[][]`.

**Examples**

* General use of single-dimensional arrays.

    ```painless
    int[] x;                   <1>
    float[] y = new float[10]; <2>
    def z = new float[5];      <3>
    y[9] = 1.0F;               <4>
    z[0] = y[9];               <5>
    ```

    1. declare `int[] x`; store default `null` to `x`
    2. declare `float[] y`; allocate `1-d float array` instance with `length [10]` → `1-d float array reference`; store `1-d float array reference` to `y`
    3. declare `def z`; allocate `1-d float array` instance with `length [5]` → `1-d float array reference`; implicit cast `1-d float array reference` to `def` → `def`; store `def` to `z`
    4. load from `y` → `1-d float array reference`; store `float 1.0` to `index [9]` of `1-d float array reference`
    5. load from `y` → `1-d float array reference @0`; load from `index [9]` of `1-d float array reference @0` → `float 1.0`; load from `z` → `def`; implicit cast `def` to `1-d float array reference @1` → `1-d float array reference @1`; store `float 1.0` to `index [0]` of `1-d float array reference @1`

* General use of a multi-dimensional array.

    ```painless
    int[][][] ia3 = new int[2][3][4]; <1>
    ia3[1][2][3] = 99;                <2>
    int i = ia3[1][2][3];             <3>
    ```

    1. declare `int[][][] ia`; allocate `3-d int array` instance with length `[2, 3, 4]` → `3-d int array reference`; store `3-d int array reference` to `ia3`
    2. load from `ia3` → `3-d int array reference`; store `int 99` to `index [1, 2, 3]` of `3-d int array reference`
    3. declare `int i`; load from `ia3` → `3-d int array reference`; load from `index [1, 2, 3]` of `3-d int array reference` → `int 99`; store `int 99` to `i`
