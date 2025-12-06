---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-variables.html
products:
  - id: painless
---

# Variables [painless-variables]

A variable loads and stores a value for evaluation during [operations](/reference/scripting-languages/painless/painless-operators.md).

## Declaration [variable-declaration]

Declare a variable before use with the format of [type](/reference/scripting-languages/painless/painless-types.md) followed by [identifier](/reference/scripting-languages/painless/painless-identifiers.md). Declare an [array type](/reference/scripting-languages/painless/painless-types.md#array-type) variable using an opening `[` token and a closing `]` token for each dimension directly after the identifier. Specify a comma-separated list of identifiers following the type to declare multiple variables in a single statement. Use an [assignment operator](#variable-assignment) combined with a declaration to immediately assign a value to a variable. A variable not immediately assigned a value will have a default value assigned implicitly based on the type.

**Errors**

* If a variable is used prior to or without declaration.

**Grammar**

```text
declaration : type ID assignment? (',' ID assignment?)*;
type: ID ('.' ID)* ('[' ']')*;
assignment: '=' expression;
```

**Examples**

* Different variations of variable declaration.

    ```painless
    int x;           <1>
    List y;          <2>
    int x, y = 5, z; <3>
    def d;           <4>
    int i = 10;      <5>
    float[] f;       <6>
    Map[][] m;       <7>
    ```

    1. declare `int x`; store default `null` to `x`
    2. declare `List y`; store default `null` to `y`
    3. declare `int x`; store default `int 0` to `x`; declare `int y`; store `int 5` to `y`; declare `int z`; store default `int 0` to `z`;
    4. declare `def d`; store default `null` to `d`
    5. declare `int i`; store `int 10` to `i`
    6. declare `float[] f`; store default `null` to `f`
    7. declare `Map[][] m`; store default `null` to `m`



## Assignment [variable-assignment]

Use the `assignment operator '='` to store a value in a variable for use in subsequent operations. Any operation that produces a value can be assigned to any variable as long as the [types](/reference/scripting-languages/painless/painless-types.md) are the same or the resultant type can be [implicitly cast](/reference/scripting-languages/painless/painless-casting.md) to the variable type.

**Errors**

* If the type of value is unable to match the type of variable.

**Grammar**

```text
assignment: ID '=' expression
```

**Examples**

* Variable assignment with an integer literal.

    ```painless
    int i;  <1>
    i = 10; <2>
    ```

    1. declare `int i`; store default `int 0` to `i`
    2. store `int 10` to `i`

* Declaration combined with immediate assignment.

    ```painless
    int i = 10;     <1>
    double j = 2.0; <2>
    ```

    1. declare `int i`; store `int 10` to `i`
    2. declare `double j`; store `double 2.0` to `j`

* Assignment of one variable to another using primitive type values.

    ```painless
    int i = 10; <1>
    int j = i;  <2>
    ```

    1. declare `int i`; store `int 10` to `i`
    2. declare `int j`; load from `i` → `int 10`; store `int 10` to `j`

* Assignment with reference types using the [new instance operator](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator).

    ```painless
    ArrayList l = new ArrayList(); <1>
    Map m = new HashMap();         <2>
    ```

    1. declare `ArrayList l`; allocate `ArrayList` instance → `ArrayList reference`; store `ArrayList reference` to `l`
    2. declare `Map m`; allocate `HashMap` instance → `HashMap reference`; implicit cast `HashMap reference` to `Map reference` → `Map reference`; store `Map reference` to `m`

* Assignment of one variable to another using reference type values.

    ```painless
    List l = new ArrayList(); <1>
    List k = l;               <2>
    List m;                   <3>
    m = k;                    <4>
    ```

    1. declare `List l`; allocate `ArrayList` instance → `ArrayList reference`; implicit cast `ArrayList reference` to `List reference` → `List reference`; store `List reference` to `l`
    2. declare `List k`; load from `l` → `List reference`; store `List reference` to `k`; (note `l` and `k` refer to the same instance known as a shallow-copy)
    3. declare `List m`; store default `null` to `m`
    4. load from `k` → `List reference`; store `List reference` to `m`; (note `l`, `k`, and `m` refer to the same instance)

* Assignment with array type variables using the [new array operator](/reference/scripting-languages/painless/painless-operators-array.md#new-array-operator).

    ```painless
    int[] ia1;                   <1>
    ia1 = new int[2];            <2>
    ia1[0] = 1;                  <3>
    int[] ib1 = ia1;             <4>
    int[][] ic2 = new int[2][5]; <5>
    ic2[1][3] = 2;               <6>
    ic2[0] = ia1;                <7>
    ```

    1. declare `int[] ia1`; store default `null` to `ia1`
    2. allocate `1-d int array` instance with `length [2]` → `1-d int array reference`; store `1-d int array reference` to `ia1`
    3. load from `ia1` → `1-d int array reference`; store `int 1` to `index [0]` of `1-d int array reference`
    4. declare `int[] ib1`; load from `ia1` → `1-d int array reference`; store `1-d int array reference` to `ib1`; (note `ia1` and `ib1` refer to the same instance known as a shallow copy)
    5. declare `int[][] ic2`; allocate `2-d int array` instance with `length [2, 5]` → `2-d int array reference`; store `2-d int array reference` to `ic2`
    6. load from `ic2` → `2-d int array reference`; store `int 2` to `index [1, 3]` of `2-d int array reference`
    7. load from `ia1` → `1-d int array reference`; load from `ic2` → `2-d int array reference`; store `1-d int array reference` to `index [0]` of `2-d int array reference`; (note `ia1`, `ib1`, and `index [0]` of `ia2` refer to the same instance)



