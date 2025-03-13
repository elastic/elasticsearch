## `MV_FIRST` [esql-mv_first]

**Syntax**

:::{image} ../../../../../images/mv_first.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalued expression into a single valued column containing the first value. This is most useful when reading from a function that emits multivalued columns in a known order like [`SPLIT`](../../esql-functions-operators.md#esql-split).

The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md) are read from underlying storage is not guaranteed. It is **frequently** ascending, but don’t rely on that. If you need the minimum value use [`MV_MIN`](../../esql-functions-operators.md#esql-mv_min) instead of `MV_FIRST`. `MV_MIN` has optimizations for sorted values so there isn’t a performance benefit to `MV_FIRST`.

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
| EVAL first_a = MV_FIRST(SPLIT(a, ";"))
```

| a:keyword | first_a:keyword |
| --- | --- |
| foo;bar;baz | "foo" |


