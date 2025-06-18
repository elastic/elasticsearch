---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-literals.html
products:
  - id: painless
---

# Literals [painless-literals]

Use a literal to specify a value directly in an [operation](/reference/scripting-languages/painless/painless-operators.md).

## Integers [integer-literals]

Use an integer literal to specify an integer type value in decimal, octal, or hex notation of a [primitive type](/reference/scripting-languages/painless/painless-types.md#primitive-types) `int`, `long`, `float`, or `double`. Use the following single letter designations to specify the primitive type: `l` or `L` for `long`, `f` or `F` for `float`, and `d` or `D` for `double`. If not specified, the type defaults to `int`. Use `0` as a prefix to specify an integer literal as octal, and use `0x` or `0X` as a prefix to specify an integer literal as hex.

**Grammar**

```text
INTEGER: '-'? ( '0' | [1-9] [0-9]* ) [lLfFdD]?;
OCTAL:   '-'? '0' [0-7]+ [lL]?;
HEX:     '-'? '0' [xX] [0-9a-fA-F]+ [lL]?;
```

**Examples**

* Integer literals.

    ```painless
    0     <1>
    0D    <2>
    1234L <3>
    -90f  <4>
    -022  <5>
    0xF2A <6>
    ```

    1. `int 0`
    2. `double 0.0`
    3. `long 1234`
    4. `float -90.0`
    5. `int -18` in octal
    6. `int 3882` in hex



## Floats [float-literals]

Use a floating point literal to specify a floating point type value of a [primitive type](/reference/scripting-languages/painless/painless-types.md#primitive-types) `float` or `double`. Use the following single letter designations to specify the primitive type: `f` or `F` for `float` and `d` or `D` for `double`. If not specified, the type defaults to `double`.

**Grammar**

```text
DECIMAL: '-'? ( '0' | [1-9] [0-9]* ) (DOT [0-9]+)? EXPONENT? [fFdD]?;
EXPONENT: ( [eE] [+\-]? [0-9]+ );
```

**Examples**

* Floating point literals.

    ```painless
    0.0      <1>
    1E6      <2>
    0.977777 <3>
    -126.34  <4>
    89.9F    <5>
    ```

    1. `double 0.0`
    2. `double 1000000.0` in exponent notation
    3. `double 0.977777`
    4. `double -126.34`
    5. `float 89.9`



## Strings [string-literals]

Use a string literal to specify a [`String` type](/reference/scripting-languages/painless/painless-types.md#string-type) value with either single-quotes or double-quotes. Use a `\"` token to include a double-quote as part of a double-quoted string literal. Use a `\'` token to include a single-quote as part of a single-quoted string literal. Use a `\\` token to include a backslash as part of any string literal.

**Grammar**

```text
STRING: ( '"'  ( '\\"'  | '\\\\' | ~[\\"] )*? '"'  )
      | ( '\'' ( '\\\'' | '\\\\' | ~[\\'] )*? '\'' );
```

**Examples**

* String literals using single-quotes.

    ```painless
    'single-quoted string literal'
    '\'single-quoted with escaped single-quotes\' and backslash \\'
    'single-quoted with non-escaped "double-quotes"'
    ```

* String literals using double-quotes.

    ```painless
    "double-quoted string literal"
    "\"double-quoted with escaped double-quotes\" and backslash: \\"
    "double-quoted with non-escaped 'single-quotes'"
    ```



## Characters [character-literals]

Character literals are not specified directly. Instead, use the [cast operator](/reference/scripting-languages/painless/painless-casting.md#string-character-casting) to convert a `String` type value into a `char` type value.


