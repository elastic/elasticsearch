## `MV_CONCAT` [esql-mv_concat]

**Syntax**

:::{image} ../../../../../images/mv_concat.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   Multivalue expression.

`delim`
:   Delimiter.

**Description**

Converts a multivalued string expression into a single valued column containing the concatenation of all values separated by a delimiter.

**Supported types**

| string | delim | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Examples**

```esql
ROW a=["foo", "zoo", "bar"]
| EVAL j = MV_CONCAT(a, ", ")
```

| a:keyword | j:keyword |
| --- | --- |
| ["foo", "zoo", "bar"] | "foo, zoo, bar" |

To concat non-string columns, call [`TO_STRING`](../../esql-functions-operators.md#esql-to_string) first:

```esql
ROW a=[10, 9, 8]
| EVAL j = MV_CONCAT(TO_STRING(a), ", ")
```

| a:integer | j:keyword |
| --- | --- |
| [10, 9, 8] | "10, 9, 8" |


