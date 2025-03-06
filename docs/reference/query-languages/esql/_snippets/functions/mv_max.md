## `MV_MAX` [esql-mv_max]

**Syntax**

:::{image} ../../../../../images/mv_max.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalued expression into a single valued column containing the maximum value.

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
ROW a=[3, 5, 1]
| EVAL max_a = MV_MAX(a)
```

| a:integer | max_a:integer |
| --- | --- |
| [3, 5, 1] | 5 |

It can be used by any column type, including `keyword` columns. In that case it picks the last string, comparing their utf-8 representation byte by byte:

```esql
ROW a=["foo", "zoo", "bar"]
| EVAL max_a = MV_MAX(a)
```

| a:keyword | max_a:keyword |
| --- | --- |
| ["foo", "zoo", "bar"] | "zoo" |


