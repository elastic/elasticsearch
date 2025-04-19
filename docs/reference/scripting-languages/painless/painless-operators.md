---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-operators.html
---

# Operators [painless-operators]

An operator is the most basic action that can be taken to evaluate values in a script. An expression is one-to-many consecutive operations. Precedence is the order in which an operator will be evaluated relative to another operator. Associativity is the direction within an expression in which a specific operator is evaluated. The following table lists all available operators:

| **Operator** | **Category** | **Symbol(s)** | **Precedence** | **Associativity** |
| --- | --- | --- | --- | --- |
| [Precedence](/reference/scripting-languages/painless/painless-operators-general.md#precedence-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | () | 0 | left → right |
| [Method Call](/reference/scripting-languages/painless/painless-operators-reference.md#method-call-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | . () | 1 | left → right |
| [Field Access](/reference/scripting-languages/painless/painless-operators-reference.md#field-access-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | . | 1 | left → right |
| [Null Safe](/reference/scripting-languages/painless/painless-operators-reference.md#null-safe-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | ?. | 1 | left → right |
| [Function Call](/reference/scripting-languages/painless/painless-operators-general.md#function-call-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | () | 1 | left → right |
| [Array Initialization](/reference/scripting-languages/painless/painless-operators-array.md#array-initialization-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | [] {} | 1 | left → right |
| [Array Access](/reference/scripting-languages/painless/painless-operators-array.md#array-access-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | [] | 1 | left → right |
| [Array Length](/reference/scripting-languages/painless/painless-operators-array.md#array-length-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | . | 1 | left → right |
| [List Initialization](/reference/scripting-languages/painless/painless-operators-reference.md#list-initialization-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [] | 1 | left → right |
| [List Access](/reference/scripting-languages/painless/painless-operators-reference.md#list-access-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [] | 1 | left → right |
| [Map Initialization](/reference/scripting-languages/painless/painless-operators-reference.md#map-initialization-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [:] | 1 | left → right |
| [Map Access](/reference/scripting-languages/painless/painless-operators-reference.md#map-access-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | [] | 1 | left → right |
| [Post Increment](/reference/scripting-languages/painless/painless-operators-numeric.md#post-increment-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ++ | 1 | left → right |
| [Post Decrement](/reference/scripting-languages/painless/painless-operators-numeric.md#post-decrement-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) |  —  | 1 | left → right |
| [Pre Increment](/reference/scripting-languages/painless/painless-operators-numeric.md#pre-increment-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ++ | 2 | right → left |
| [Pre Decrement](/reference/scripting-languages/painless/painless-operators-numeric.md#pre-decrement-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) |  —  | 2 | right → left |
| [Unary Positive](/reference/scripting-languages/painless/painless-operators-numeric.md#unary-positive-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | + | 2 | right → left |
| [Unary Negative](/reference/scripting-languages/painless/painless-operators-numeric.md#unary-negative-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | - | 2 | right → left |
| [Boolean Not](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-not-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | ! | 2 | right → left |
| [Bitwise Not](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-not-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ~ | 2 | right → left |
| [Cast](/reference/scripting-languages/painless/painless-operators-general.md#cast-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | () | 3 | right → left |
| [New Instance](/reference/scripting-languages/painless/painless-operators-reference.md#new-instance-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | new () | 3 | right → left |
| [New Array](/reference/scripting-languages/painless/painless-operators-array.md#new-array-operator) | [Array](/reference/scripting-languages/painless/painless-operators-array.md) | new [] | 3 | right → left |
| [Multiplication](/reference/scripting-languages/painless/painless-operators-numeric.md#multiplication-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | * | 4 | left → right |
| [Division](/reference/scripting-languages/painless/painless-operators-numeric.md#division-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | / | 4 | left → right |
| [Remainder](/reference/scripting-languages/painless/painless-operators-numeric.md#remainder-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | % | 4 | left → right |
| [String Concatenation](/reference/scripting-languages/painless/painless-operators-reference.md#string-concatenation-operator) | [Reference](/reference/scripting-languages/painless/painless-operators-reference.md) | + | 5 | left → right |
| [Addition](/reference/scripting-languages/painless/painless-operators-numeric.md#addition-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | + | 5 | left → right |
| [Subtraction](/reference/scripting-languages/painless/painless-operators-numeric.md#subtraction-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | - | 5 | left → right |
| [Left Shift](/reference/scripting-languages/painless/painless-operators-numeric.md#left-shift-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | << | 6 | left → right |
| [Right Shift](/reference/scripting-languages/painless/painless-operators-numeric.md#right-shift-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | >> | 6 | left → right |
| [Unsigned Right Shift](/reference/scripting-languages/painless/painless-operators-numeric.md#unsigned-right-shift-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | >>> | 6 | left → right |
| [Greater Than](/reference/scripting-languages/painless/painless-operators-boolean.md#greater-than-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | > | 7 | left → right |
| [Greater Than Or Equal](/reference/scripting-languages/painless/painless-operators-boolean.md#greater-than-or-equal-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | >= | 7 | left → right |
| [Less Than](/reference/scripting-languages/painless/painless-operators-boolean.md#less-than-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | < | 7 | left → right |
| [Less Than Or Equal](/reference/scripting-languages/painless/painless-operators-boolean.md#less-than-or-equal-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | <= | 7 | left → right |
| [Instanceof](/reference/scripting-languages/painless/painless-operators-boolean.md#instanceof-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | instanceof | 8 | left → right |
| [Equality Equals](/reference/scripting-languages/painless/painless-operators-boolean.md#equality-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | == | 9 | left → right |
| [Equality Not Equals](/reference/scripting-languages/painless/painless-operators-boolean.md#equality-not-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | != | 9 | left → right |
| [Identity Equals](/reference/scripting-languages/painless/painless-operators-boolean.md#identity-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | === | 9 | left → right |
| [Identity Not Equals](/reference/scripting-languages/painless/painless-operators-boolean.md#identity-not-equals-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | !== | 9 | left → right |
| [Bitwise And](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-and-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | & | 10 | left → right |
| [Boolean Xor](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-xor-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | ^ | 11 | left → right |
| [Bitwise Xor](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-xor-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | ^ | 11 | left → right |
| [Bitwise Or](/reference/scripting-languages/painless/painless-operators-numeric.md#bitwise-or-operator) | [Numeric](/reference/scripting-languages/painless/painless-operators-numeric.md) | &#124; | 12 | left → right |
| [Boolean And](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-and-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | && | 13 | left → right |
| [Boolean Or](/reference/scripting-languages/painless/painless-operators-boolean.md#boolean-or-operator) | [Boolean](/reference/scripting-languages/painless/painless-operators-boolean.md) | &#124;&#124; | 14 | left → right |
| [Conditional](/reference/scripting-languages/painless/painless-operators-general.md#conditional-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | ? : | 15 | right → left |
| [Elvis](/reference/scripting-languages/painless/painless-operators-reference.md#elvis-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | ?: | 16 | right → left |
| [Assignment](/reference/scripting-languages/painless/painless-operators-general.md#assignment-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | = | 17 | right → left |
| [Compound Assignment](/reference/scripting-languages/painless/painless-operators-general.md#compound-assignment-operator) | [General](/reference/scripting-languages/painless/painless-operators-general.md) | $= | 17 | right → left |

