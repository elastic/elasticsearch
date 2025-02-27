## `MV_SLICE` [esql-mv_slice]

**Syntax**

:::{image} ../../../../../images/mv_slice.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression. If `null`, the function returns `null`.

`start`
:   Start position. If `null`, the function returns `null`. The start argument can be negative. An index of -1 is used to specify the last value in the list.

`end`
:   End position(included). Optional; if omitted, the position at `start` is returned. The end argument can be negative. An index of -1 is used to specify the last value in the list.

**Description**

Returns a subset of the multivalued field using the start and end index values. This is most useful when reading from a function that emits multivalued columns in a known order like [`SPLIT`](../../esql-functions-operators.md#esql-split) or [`MV_SORT`](../../esql-functions-operators.md#esql-mv_sort).

The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md) are read from underlying storage is not guaranteed. It is **frequently** ascending, but don’t rely on that.

**Supported types**

| field | start | end | result |
| --- | --- | --- | --- |
| boolean | integer | integer | boolean |
| cartesian_point | integer | integer | cartesian_point |
| cartesian_shape | integer | integer | cartesian_shape |
| date | integer | integer | date |
| date_nanos | integer | integer | date_nanos |
| double | integer | integer | double |
| geo_point | integer | integer | geo_point |
| geo_shape | integer | integer | geo_shape |
| integer | integer | integer | integer |
| ip | integer | integer | ip |
| keyword | integer | integer | keyword |
| long | integer | integer | long |
| text | integer | integer | keyword |
| unsigned_long | integer | integer | unsigned_long |
| version | integer | integer | version |

**Examples**

```esql
row a = [1, 2, 2, 3]
| eval a1 = mv_slice(a, 1), a2 = mv_slice(a, 2, 3)
```

| a:integer | a1:integer | a2:integer |
| --- | --- | --- |
| [1, 2, 2, 3] | 2 | [2, 3] |

```esql
row a = [1, 2, 2, 3]
| eval a1 = mv_slice(a, -2), a2 = mv_slice(a, -3, -1)
```

| a:integer | a1:integer | a2:integer |
| --- | --- | --- |
| [1, 2, 2, 3] | 2 | [2, 2, 3] |


