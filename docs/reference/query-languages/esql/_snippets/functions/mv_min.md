## `MV_MIN` [esql-mv_min]

**Syntax**

:::{image} ../../../../../images/mv_min.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalued expression into a single valued column containing the minimum value.

**Supported types**

| field | result |
| --- | --- |
| boolean | boolean |
| date | date |
| date_nanos | date_nanos |
| double | double |
| integer | integer |
| ip | ip |
| keyword | keyword |
| long | long |
| text | keyword |
| unsigned_long | unsigned_long |
| version | version |

**Examples**

```esql
ROW a=[2, 1]
| EVAL min_a = MV_MIN(a)
```

| a:integer | min_a:integer |
| --- | --- |
| [2, 1] | 1 |

It can be used by any column type, including `keyword` columns. In that case, it picks the first string, comparing their utf-8 representation byte by byte:

```esql
ROW a=["foo", "bar"]
| EVAL min_a = MV_MIN(a)
```

| a:keyword | min_a:keyword |
| --- | --- |
| ["foo", "bar"] | "bar" |


