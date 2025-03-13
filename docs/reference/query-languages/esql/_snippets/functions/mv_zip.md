## `MV_ZIP` [esql-mv_zip]

**Syntax**

:::{image} ../../../../../images/mv_zip.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string1`
:   Multivalue expression.

`string2`
:   Multivalue expression.

`delim`
:   Delimiter. Optional; if omitted, `,` is used as a default delimiter.

**Description**

Combines the values from two multivalued fields with a delimiter that joins them together.

**Supported types**

| string1 | string2 | delim | result |
| --- | --- | --- | --- |
| keyword | keyword | keyword | keyword |
| keyword | keyword | text | keyword |
| keyword | keyword |  | keyword |
| keyword | text | keyword | keyword |
| keyword | text | text | keyword |
| keyword | text |  | keyword |
| text | keyword | keyword | keyword |
| text | keyword | text | keyword |
| text | keyword |  | keyword |
| text | text | keyword | keyword |
| text | text | text | keyword |
| text | text |  | keyword |

**Example**

```esql
ROW a = ["x", "y", "z"], b = ["1", "2"]
| EVAL c = mv_zip(a, b, "-")
| KEEP a, b, c
```

| a:keyword | b:keyword | c:keyword |
| --- | --- | --- |
| [x, y, z] | [1 ,2] | [x-1, y-2, z] |
