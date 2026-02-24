---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Operators [painless-operators]

Operators are the fundamental building blocks for data manipulation in Painless scripts. They enable calculations, comparisons, logical operations, and data access across all {{es}} scripting [contexts](/reference/scripting-languages/painless/painless-contexts.md).

An **operator** performs a specific action to evaluate values in a script. An **expression** combines one or more operators to produce a result. **Precedence** determines evaluation order when multiple operators are present, while **associativity** controls evaluation direction for operators with equal precedence.

Painless operators use Java-like syntax with {{es}} specific enhancements such as null-safe navigation and specialized data structure access.

## Operator categories

Painless organizes operators into five functional categories based on their purpose.

```mermaid
graph TB
    A["Painless Operators"]

    B["General"]
    C["Numeric"]
    D["Boolean"]
    E["Reference"]
    F["Array"]

    B1["Control expression flow and<br/>value assignment"]
    C1["Mathematical operations and<br/>bit manipulation"]
    D1["Boolean logic and<br/>conditional evaluation"]
    E1["Object interaction and<br/>safe data access"]
    F1["Array manipulation and<br/>element access"]

    B2["Precedence ( )<br/>Function Call ( )<br/>Cast ( )<br/>Conditional ? :<br/>Elvis ?:<br/>Assignment =<br/>Compound Assignment $="]
    C2["Post/Pre Increment ++<br/>Post/Pre Decrement --<br/>Unary +/-<br/>Bitwise Not ~<br/>Multiplication *<br/>Division /<br/>Remainder %<br/>Addition +<br/>Subtraction -<br/>Shift <<, >>, >>><br/>Bitwise And &<br/>Bitwise Xor ^<br/>Bitwise Or |"]
    D2["Boolean Not !<br/>Comparison >, >=, <, <=<br/>Instanceof instanceof<br/>Equality ==, !=<br/>Identity ===, !==<br/>Boolean Xor ^<br/>Boolean And &&<br/>Boolean Or ||"]
    E2["Method Call . ( )<br/>Field Access .<br/>Null Safe ?.<br/>New Instance new ( )<br/>String Concatenation +<br/>List/Map Init [ ], [ : ]<br/>List/Map Access [ ]"]
    F2["Array Init [ ] { }<br/>Array Access [ ]<br/>Array Length .length<br/>New Array new [ ]"]

    A --> B & C & D & E & F
    B --> B1
    C --> C1
    D --> D1
    E --> E1
    F --> F1
    B1 --> B2
    C1 --> C2
    D1 --> D2
    E1 --> E2
    F1 --> F2
    
    classDef rootNode fill:#0B64DD,stroke:#101C3F,stroke-width:2px,color:#fff
    classDef categoryBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#343741
    classDef descBox fill:#48EFCF,stroke:#343741,stroke-width:2px,color:#343741
    classDef exampleBox fill:#f5f7fa,stroke:#343741,stroke-width:2px,color:#343741

    class A rootNode
    class B,C,D,E,F categoryBox
    class B1,C1,D1,E1,F1 descBox
    class B2,C2,D2,E2,F2 exampleBox
```

### General operators

Control the fundamental flow and structure of expressions in Painless scripts. These operators manage how expressions are evaluated, values are assigned, and conditional logic is run. 

* [Precedence](/reference/scripting-languages/painless/painless-operators-general.md#precedence-operator) `()`: Controls evaluation order by overriding default precedence rules  
* [Function call](/reference/scripting-languages/painless/painless-operators-general.md#function-call-operator) `()`: Executes user-defined functions with arguments   
* [Cast](/reference/scripting-languages/painless/painless-operators-general.md#cast-operator) `()` : Forces explicit type conversion between compatible types  
* [Conditional](/reference/scripting-languages/painless/painless-operators-general.md#conditional-operator) `? :` : Provides inline if-else logic for expressions  
* [Elvis](/reference/scripting-languages/painless/painless-operators-reference.md#elvis-operator) `?:` : Returns first non-null value for null coalescing  
* [Assignment](/reference/scripting-languages/painless/painless-operators-general.md#assignment-operator) `=` : Stores values in variables or fields  
* [Compound assignment](/reference/scripting-languages/painless/painless-operators-general.md#compound-assignment-operator) `$=` : Combines binary operations with assignment (`+=`, `-=`, and so on)

### Numeric operators

Perform mathematical calculations and bit-level manipulations on numeric values. These operators handle arithmetic, bitwise operations, and value modifications essential for numerical computations.

* [Increment](/reference/scripting-languages/painless/painless-operators-numeric.md#post-increment-operator)/[Decrement](/reference/scripting-languages/painless/painless-operators-numeric.md#post-decrement-operator) (`++`. `--`) : Increases or decreases values by one (pre/post variants)  
* [Unary](/reference/scripting-languages/painless/painless-operators-numeric.md#unary-positive-operator) (`+`, `-`) : preserves or negates numeric values  
* [Bitwise not](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-not-operator) `~` : inverts all bits in integer values  
* [Multiplication](/reference/scripting-languages/painless/painless-operators-numeric.md#multiplication-operator)/[Division](/reference/scripting-languages/painless/painless-operators-numeric.md#division-operator)/[Remainder](/reference/scripting-languages/painless/painless-operators-numeric.md#remainder-operator) `*` `/` `%` : Basic arithmetic operations  
* [Addition](/reference/scripting-languages/painless/painless-operators-numeric.md#addition-operator)/[Subtraction](/reference/scripting-languages/painless/painless-operators-numeric.md#subtraction-operator) `+` `-` : Basic arithmetic operations  
* [Shift](/reference/scripting-languages/painless/painless-operators-numeric.md#left-shift-operator) (`<<`, `>>`, `>>>`) : Shifts bits left or right within integer values  
* [Bitwise and](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-and-operator) `&` : Performs AND operations on corresponding bits  
* [Bitwise xor](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-xor-operator) `^` : Performs XOR operations on corresponding bits  
* [Bitwise or](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-or-operator) `|`: Performs OR operations on corresponding bits

### Boolean operators

Handle logical evaluation, comparisons, and conditional expressions. These operators are fundamental for creating filters, conditional logic, and boolean expressions in scripts

* [Boolean not](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-not-operator) `!`:Inverts boolean values (true becomes false)  
* [Comparison](/reference/scripting-languages/painless/painless-operators-boolean.md#greater-than-operator) `>` `>=` `<` `<=` : Compares numeric values for ordering  
* [Instanceof](/reference/scripting-languages/painless/painless-operators-boolean.md#instanceof-operator) `instanceof`: Checks if an object is an instance of a specific type  
* [Equality](/reference/scripting-languages/painless/painless-operators-boolean.md#equality-equals-operator) `==` `!=` : Compares values for equality (calls equals() method)  
* [Identity](/reference/scripting-languages/painless/painless-operators-boolean.md#identity-equals-operator) `===` `!==` : Compares object references for same instance  
* [Boolean xor](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-xor-operator) `^`: Returns true if exactly one operand is true  
* [Boolean and](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-and-operator) `&&`: Returns true only if both operands are true  
* [Boolean or](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-or-operator) `||`: Returns true if at least one operand is true

### Reference operators

Enable interaction with objects, method calls, and data structure manipulation. These operators are essential for working with documents, collections, and complex data types in {{es}} contexts.

* [Method call](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator) `. ()`: Invokes methods on objects with optional arguments  
* [Field access](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) `.`: Accesses object properties and member fields  
* [Null safe](/reference/scripting-languages/painless/painless-operators-reference.md#null-safe-operator) `?.`: Safely accesses fields/methods without null pointer exceptions  
* [List/Map initialization](/reference/scripting-languages/painless/painless-operators-reference.md#list-initialization-operator) `[] [:]`: Creates new List or Map collections with initial values  
* [List/Map access](/reference/scripting-languages/painless/painless-operators-reference.md#list-access-operator) `[]`: Retrieves or sets elements in collections by key/index  
* [New instance](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator) `new ()`: Creates new object instances with constructor arguments  
* [String concatenation](/reference/scripting-languages/painless/painless-operators-reference.md#string-concatenation-operator) `+` : Joins strings and converts other types to strings

### Array operators

Provide specialized functionality for array creation, element access, and array property retrieval. These operators are essential when working with multi-value fields and array data structures.

* [Array initialization](/reference/scripting-languages/painless/painless-operators-array.md#array-initialization-operator) `[] {}`\`: Creates arrays with predefined values  
* [Array access](/reference/scripting-languages/painless/painless-operators-array.md#array-access-operator) `[]`: Retrieves or sets array elements by index position  
* [Array length](/reference/scripting-languages/painless/painless-operators-array.md#array-length-operator) `.`: Returns the number of elements in an array  
* [New array](/reference/scripting-languages/painless/painless-operators-array.md#new-array-operator) `new []`: Creates arrays with specified dimensions and sizes

## Complete operator reference

| **Operator** | **Category** | **Symbol(s)** | **Precedence** | **Associativity** |
| --- | --- | --- | --- | --- |
| [Precedence](/reference/scripting-languages/painless/painless-operators-general.md#precedence-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | () | 0 | left → right |
| [Method call](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | . () | 1 | left → right |
| [Field access](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | . | 1 | left → right |
| [Null safe](/reference/scripting-languages/painless/painless-operators-reference.md#null-safe-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | ?. | 1 | left → right |
| [Function call](/reference/scripting-languages/painless/painless-operators-general.md#function-call-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | () | 1 | left → right |
| [Array initialization](/reference/scripting-languages/painless/painless-operators-array.md#array-initialization-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | [] {} | 1 | left → right |
| [Array access](/reference/scripting-languages/painless/painless-operators-array.md#array-access-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | [] | 1 | left → right |
| [Array length](/reference/scripting-languages/painless/painless-operators-array.md#array-length-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | . | 1 | left → right |
| [List initialization](/reference/scripting-languages/painless/painless-operators-reference.md#list-initialization-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [] | 1 | left → right |
| [List access](/reference/scripting-languages/painless/painless-operators-reference.md#list-access-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [] | 1 | left → right |
| [Map initialization](/reference/scripting-languages/painless/painless-operators-reference.md#map-initialization-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [:] | 1 | left → right |
| [Map access](/reference/scripting-languages/painless/painless-operators-reference.md#map-access-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [] | 1 | left → right |
| [Post increment](/reference/scripting-languages/painless/painless-operators-numeric.md#post-increment-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ++ | 1 | left → right |
| [Post decrement](/reference/scripting-languages/painless/painless-operators-numeric.md#post-decrement-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) |  —  | 1 | left → right |
| [Pre increment](/reference/scripting-languages/painless/painless-operators-numeric.md#pre-increment-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ++ | 2 | right → left |
| [Pre decrement](/reference/scripting-languages/painless/painless-operators-numeric.md#pre-decrement-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) |  —  | 2 | right → left |
| [Unary positive](/reference/scripting-languages/painless/painless-operators-numeric.md#unary-positive-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | + | 2 | right → left |
| [Unary negative](/reference/scripting-languages/painless/painless-operators-numeric.md#unary-negative-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | - | 2 | right → left |
| [Boolean not](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-not-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | ! | 2 | right → left |
| [Bitwise not](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-not-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ~ | 2 | right → left |
| [Cast](/reference/scripting-languages/painless/painless-operators-general.md#cast-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | () | 3 | right → left |
| [New instance](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | new () | 3 | right → left |
| [New array](/reference/scripting-languages/painless/painless-operators-array.md#new-array-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | new [] | 3 | right → left |
| [Multiplication](/reference/scripting-languages/painless/painless-operators-numeric.md#multiplication-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | * | 4 | left → right |
| [Division](/reference/scripting-languages/painless/painless-operators-numeric.md#division-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | / | 4 | left → right |
| [Remainder](/reference/scripting-languages/painless/painless-operators-numeric.md#remainder-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | % | 4 | left → right |
| [String concatenation](/reference/scripting-languages/painless/painless-operators-reference.md#string-concatenation-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | + | 5 | left → right |
| [Addition](/reference/scripting-languages/painless/painless-operators-numeric.md#addition-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | + | 5 | left → right |
| [Subtraction](/reference/scripting-languages/painless/painless-operators-numeric.md#subtraction-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | - | 5 | left → right |
| [Left shift](/reference/scripting-languages/painless/painless-operators-numeric.md#left-shift-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | << | 6 | left → right |
| [Right shift](/reference/scripting-languages/painless/painless-operators-numeric.md#right-shift-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | >> | 6 | left → right |
| [Unsigned right shift](/reference/scripting-languages/painless/painless-operators-numeric.md#unsigned-right-shift-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | >>> | 6 | left → right |
| [Greater than](/reference/scripting-languages/painless/painless-operators-boolean.md#greater-than-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | > | 7 | left → right |
| [Greater than Or Equal](/reference/scripting-languages/painless/painless-operators-boolean.md#greater-than-or-equal-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | >= | 7 | left → right |
| [Less than](/reference/scripting-languages/painless/painless-operators-boolean.md#less-than-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | < | 7 | left → right |
| [Less than Or Equal](/reference/scripting-languages/painless/painless-operators-boolean.md#less-than-or-equal-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | <= | 7 | left → right |
| [Instanceof](/reference/scripting-languages/painless/painless-operators-boolean.md#instanceof-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | instanceof | 8 | left → right |
| [Equality equals](/reference/scripting-languages/painless/painless-operators-boolean.md#equality-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | == | 9 | left → right |
| [Equality not equals](/reference/scripting-languages/painless/painless-operators-boolean.md#equality-not-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | != | 9 | left → right |
| [Identity equals](/reference/scripting-languages/painless/painless-operators-boolean.md#identity-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | === | 9 | left → right |
| [Identity not equals](/reference/scripting-languages/painless/painless-operators-boolean.md#identity-not-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | !== | 9 | left → right |
| [Bitwise and](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-and-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | & | 10 | left → right |
| [Boolean xor](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-xor-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | ^ | 11 | left → right |
| [Bitwise xor](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-xor-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ^ | 11 | left → right |
| [Bitwise or](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-or-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | &#124; | 12 | left → right |
| [Boolean and](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-and-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | && | 13 | left → right |
| [Boolean or](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-or-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | &#124;&#124; | 14 | left → right |
| [Conditional](/reference/scripting-languages/painless/painless-operators-general.md#conditional-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | ? : | 15 | right → left |
| [Elvis](/reference/scripting-languages/painless/painless-operators-reference.md#elvis-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | ?: | 16 | right → left |
| [Assignment](/reference/scripting-languages/painless/painless-operators-general.md#assignment-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | = | 17 | right → left |
| [Compound assignment](/reference/scripting-languages/painless/painless-operators-general.md#compound-assignment-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | $= | 17 | right → left |

