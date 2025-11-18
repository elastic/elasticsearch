---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators-array.html
products:
  - id: painless
---

# Operators: Array [painless-operators-array]

## Array Initialization [array-initialization-operator]

Use the `array initialization operator '[] {}'` to allocate a single-dimensional [array type](/reference/scripting-languages/painless/painless-types.md#array-type) instance to the heap with a set of pre-defined elements. Each value used to initialize an element in the array type instance is cast to the specified element type value upon insertion. The order of specified values is maintained.

**Errors**

* If a value is not castable to the specified type value.

**Grammar**

```text
array_initialization: 'new' TYPE '[' ']' '{' expression_list '}'
                    | 'new' TYPE '[' ']' '{' '}';
expression_list: expression (',' expression);
```

**Example:**

* Array initialization with static values.

    ```painless
    int[] x = new int[] {1, 2, 3}; <1>
    ```

    1. declare `int[] x`; allocate `1-d int array` instance with `length [3]` → `1-d int array reference`; store `int 1` to `index [0]` of `1-d int array reference`; store `int 2` to `index [1]` of `1-d int array reference`; store `int 3` to `index [2]` of `1-d int array reference`; store `1-d int array reference` to `x`;

* Array initialization with non-static values.

    ```painless
    int i = 1;      <1>
    long l = 2L;    <2>
    float f = 3.0F; <3>
    double d = 4.0; <4>
    String s = "5"; <5>
    def array = new def[] {i, l, f*d, s}; <6>
    ```

    1. declare `int i`; store `int 1` to `i`
    2. declare `long l`; store `long 2` to `l`
    3. declare `float f`; store `float 3.0` to `f`
    4. declare `double d`; store `double 4.0` to `d`
    5. declare `String s`; store `String "5"` to `s`
    6. declare `def array`; allocate `1-d def array` instance with `length [4]` → `1-d def array reference`; load from `i` → `int 1`; implicit cast `int 1` to `def` → `def`; store `def` to `index [0]` of `1-d def array reference`; load from `l` → `long 2`; implicit cast `long 2` to `def` → `def`; store `def` to `index [1]` of `1-d def array reference`; load from `f` → `float 3.0`; load from `d` → `double 4.0`; promote `float 3.0` and `double 4.0`: result `double`; implicit cast `float 3.0` to `double 3.0` → `double 3.0`; multiply `double 3.0` and `double 4.0` → `double 12.0`; implicit cast `double 12.0` to `def` → `def`; store `def` to `index [2]` of `1-d def array reference`; load from `s` → `String "5"`; implicit cast `String "5"` to `def` → `def`; store `def` to `index [3]` of `1-d def array reference`; implicit cast `1-d int array reference` to `def` → `def`; store `def` to `array`



## Array Access [array-access-operator]

Use the `array access operator '[]'` to store a value to or load a value from an [array type](/reference/scripting-languages/painless/painless-types.md#array-type) value. Each element of an array type value is accessed with an `int` type value to specify the index to store/load. The range of elements within an array that are accessible is `[0, size)` where size is the number of elements specified at the time of allocation. Use a negative `int` type value as an index to access an element in reverse from the end of an array type value within a range of `[-size, -1]`.

**Errors**

* If a value other than an `int` type value or a value that is castable to an `int` type value is provided as an index.
* If an element is accessed outside of the valid ranges.

**Grammar**

```text
brace_access: '[' expression ']'
```

**Examples**

* Array access with a single-dimensional array.

    ```painless
    int[] x = new int[2]; <1>
    x[0] = 2;             <2>
    x[1] = 5;             <3>
    int y = x[0] + x[1];  <4>
    int z = 1;            <5>
    int i = x[z];         <6>
    ```

    1. declare `int[] x`; allocate `1-d int array` instance with `length [2]` → `1-d int array reference`; store `1-d int array reference` to `x`
    2. load from `x` → `1-d int array reference`; store `int 2` to `index [0]` of `1-d int array reference`;
    3. load from `x` → `1-d int array reference`; store `int 5` to `index [1]` of `1-d int array reference`;
    4. declare `int y`; load from `x` → `1-d int array reference`; load from `index [0]` of `1-d int array reference` → `int 2`; load from `x` → `1-d int array reference`; load from `index [1]` of `1-d int array reference` → `int 5`; add `int 2` and `int 5` → `int 7`; store `int 7` to `y`
    5. declare `int z`; store `int 1` to `z`;
    6. declare `int i`; load from `x` → `1-d int array reference`; load from `z` → `int 1`; load from `index [1]` of `1-d int array reference` → `int 5`; store `int 5` to `i`;

* Array access with the `def` type.

    ```painless
    def d = new int[2];  <1>
    d[0] = 2;            <2>
    d[1] = 5;            <3>
    def x = d[0] + d[1]; <4>
    def y = 1;           <5>
    def z = d[y];        <6>
    ```

    1. declare `def d`; allocate `1-d int array` instance with `length [2]` → `1-d int array reference`; implicit cast `1-d int array reference` to `def` → `def`; store `def` to `d`
    2. load from `d` → `def` implicit cast `def` to `1-d int array reference` → `1-d int array reference`; store `int 2` to `index [0]` of `1-d int array reference`;
    3. load from `d` → `def` implicit cast `def` to `1-d int array reference` → `1-d int array reference`; store `int 5` to `index [1]` of `1-d int array reference`;
    4. declare `int x`; load from `d` → `def` implicit cast `def` to `1-d int array reference` → `1-d int array reference`; load from `index [0]` of `1-d int array reference` → `int 2`; load from `d` → `def` implicit cast `def` to `1-d int array reference` → `1-d int array reference`; load from `index [1]` of `1-d int array reference` → `int 5`; add `int 2` and `int 5` → `int 7`; implicit cast `int 7` to `def` → `def`; store `def` to `x`
    5. declare `def y`; implicit cast `int 1` to `def` → `def`; store `def` to `y`;
    6. declare `int i`; load from `d` → `def` implicit cast `def` to `1-d int array reference` → `1-d int array reference`; load from `y` → `def`; implicit cast `def` to `int 1` → `int 1`; load from `index [1]` of `1-d int array reference` → `int 5`; implicit cast `int 5` to `def`; store `def` to `z`;

* Array access with a multi-dimensional array.

    ```painless
    int[][][] ia3 = new int[2][3][4]; <1>
    ia3[1][2][3] = 99;                <2>
    int i = ia3[1][2][3];             <3>
    ```

    1. declare `int[][][] ia`; allocate `3-d int array` instance with length `[2, 3, 4]` → `3-d int array reference`; store `3-d int array reference` to `ia3`
    2. load from `ia3` → `3-d int array reference`; store `int 99` to `index [1, 2, 3]` of `3-d int array reference`
    3. declare `int i`; load from `ia3` → `3-d int array reference`; load from `index [1, 2, 3]` of `3-d int array reference` → `int 99`; store `int 99` to `i`



## Array Length [array-length-operator]

An array type value contains a read-only member field named `length`. The `length` field stores the size of the array as an `int` type value where size is the number of elements specified at the time of allocation. Use the [field access operator](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) to load the field `length` from an array type value.

**Examples**

* Access the `length` field.

    ```painless
    int[] x = new int[10]; <1>
    int l = x.length;      <2>
    ```

    1. declare `int[] x`; allocate `1-d int array` instance with `length [2]` → `1-d int array reference`; store `1-d int array reference` to `x`
    2. declare `int l`; load `x` → `1-d int array reference`; load `length` from `1-d int array reference` → `int 10`; store `int 10` to `l`;



## New Array [new-array-operator]

Use the `new array operator 'new []'` to allocate an array type instance to the heap. Specify the element type following the `new` token. Specify each dimension with the `[` and `]` tokens following the element type name. The size of each dimension is specified by an `int` type value in between each set of `[` and `]` tokens.

**Errors**

* If a value other than an `int` type value or a value that is castable to an `int` type value is specified for a dimension’s size.

**Grammar**

```text
new_array: 'new' TYPE ('[' expression ']')+;
```

**Examples**

* Allocation of different array types.

    ```painless
    int[] x = new int[5];    <1>
    x = new int[10];         <2>
    int y = 2;               <3>
    def z = new def[y][y*2]; <4>
    ```

    1. declare `int[] x`; allocate `1-d int array` instance with `length [5]` → `1-d int array reference`; store `1-d int array reference` to `x`
    2. allocate `1-d int array` instance with `length [10]` → `1-d int array reference`; store `1-d int array reference` to `x`
    3. declare `int y`; store `int 2` to `y`;
    4. declare `def z`; load from `y` → `int 2 @0`; load from `y` → `int 2 @1`; multiply `int 2 @1` by `int 2 @2` → `int 4`; allocate `2-d int array` instance with length `[2, 4]` → `2-d int array reference`; implicit cast `2-d int array reference` to `def` → `def`; store `def` to `z`;



