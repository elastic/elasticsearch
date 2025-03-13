## `REPLACE` [esql-replace]

**Syntax**

:::{image} ../../../../../images/replace.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression.

`regex`
:   Regular expression.

`newString`
:   Replacement string.

**Description**

The function substitutes in the string `str` any match of the regular expression `regex` with the replacement string `newStr`.

**Supported types**

| string | regex | newString | result |
| --- | --- | --- | --- |
| keyword | keyword | keyword | keyword |
| keyword | keyword | text | keyword |
| keyword | text | keyword | keyword |
| keyword | text | text | keyword |
| text | keyword | keyword | keyword |
| text | keyword | text | keyword |
| text | text | keyword | keyword |
| text | text | text | keyword |

**Example**

This example replaces any occurrence of the word "World" with the word "Universe":

```esql
ROW str = "Hello World"
| EVAL str = REPLACE(str, "World", "Universe")
| KEEP str
```

| str:keyword |
| --- |
| Hello Universe |


