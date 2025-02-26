## `SPLIT` [esql-split]

**Syntax**

:::{image} ../../../../../images/split.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression. If `null`, the function returns `null`.

`delim`
:   Delimiter. Only single byte delimiters are currently supported.

**Description**

Split a single valued string into multiple strings.

**Supported types**

| string | delim | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Example**

```esql
ROW words="foo;bar;baz;qux;quux;corge"
| EVAL word = SPLIT(words, ";")
```

| words:keyword | word:keyword |
| --- | --- |
| foo;bar;baz;qux;quux;corge | [foo,bar,baz,qux,quux,corge] |


