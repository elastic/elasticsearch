## `MV_SORT` [esql-mv_sort]

**Syntax**

:::{image} ../../../../../images/mv_sort.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression. If `null`, the function returns `null`.

`order`
:   Sort order. The valid options are ASC and DESC, the default is ASC.

**Description**

Sorts a multivalued field in lexicographical order.

**Supported types**

| field | order | result |
| --- | --- | --- |
| boolean | keyword | boolean |
| date | keyword | date |
| date_nanos | keyword | date_nanos |
| double | keyword | double |
| integer | keyword | integer |
| ip | keyword | ip |
| keyword | keyword | keyword |
| long | keyword | long |
| text | keyword | keyword |
| version | keyword | version |

**Example**

```esql
ROW a = [4, 2, -3, 2]
| EVAL sa = mv_sort(a), sd = mv_sort(a, "DESC")
```

| a:integer | sa:integer | sd:integer |
| --- | --- | --- |
| [4, 2, -3, 2] | [-3, 2, 2, 4] | [4, 2, 2, -3] |


