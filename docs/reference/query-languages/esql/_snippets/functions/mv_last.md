## `MV_LAST` [esql-mv_last]

**Syntax**

:::{image} ../../../../../images/mv_last.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalue expression into a single valued column containing the last value. This is most useful when reading from a function that emits multivalued columns in a known order like [`SPLIT`](../../esql-functions-operators.md#esql-split).

The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md) are read from underlying storage is not guaranteed. It is **frequently** ascending, but don’t rely on that. If you need the maximum value use [`MV_MAX`](../../esql-functions-operators.md#esql-mv_max) instead of `MV_LAST`. `MV_MAX` has optimizations for sorted values so there isn’t a performance benefit to `MV_LAST`.

**Supported types**

| field | result |
| --- | --- |
| boolean | boolean |
| cartesian_point | cartesian_point |
| cartesian_shape | cartesian_shape |
| date | date |
| date_nanos | date_nanos |
| double | double |
| geo_point | geo_point |
| geo_shape | geo_shape |
| integer | integer |
| ip | ip |
| keyword | keyword |
| long | long |
| text | keyword |
| unsigned_long | unsigned_long |
| version | version |

**Example**

```esql
ROW a="foo;bar;baz"
| EVAL last_a = MV_LAST(SPLIT(a, ";"))
```

| a:keyword | last_a:keyword |
| --- | --- |
| foo;bar;baz | "baz" |


