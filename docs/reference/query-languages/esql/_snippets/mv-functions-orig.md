## {{esql}} multivalue functions [esql-mv-functions]

{{esql}} supports these multivalue functions:

:::{include} lists/mv-functions.md
:::


## `MV_APPEND` [esql-mv_append]

**Syntax**

:::{image} ../../../../images/mv_append.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Concatenates values of two multi-value fields.

**Supported types**

| field1 | field2 | result |
| --- | --- | --- |
| boolean | boolean | boolean |
| cartesian_point | cartesian_point | cartesian_point |
| cartesian_shape | cartesian_shape | cartesian_shape |
| date | date | date |
| date_nanos | date_nanos | date_nanos |
| double | double | double |
| geo_point | geo_point | geo_point |
| geo_shape | geo_shape | geo_shape |
| integer | integer | integer |
| ip | ip | ip |
| keyword | keyword | keyword |
| keyword | text | keyword |
| long | long | long |
| text | keyword | keyword |
| text | text | keyword |
| unsigned_long | unsigned_long | unsigned_long |
| version | version | version |


## `MV_AVG` [esql-mv_avg]

**Syntax**

:::{image} ../../../../images/mv_avg.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the average of all of the values.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | double |
| long | double |
| unsigned_long | double |

**Example**

```esql
ROW a=[3, 5, 1, 6]
| EVAL avg_a = MV_AVG(a)
```

| a:integer | avg_a:double |
| --- | --- |
| [3, 5, 1, 6] | 3.75 |


## `MV_CONCAT` [esql-mv_concat]

**Syntax**

:::{image} ../../../../images/mv_concat.svg
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

To concat non-string columns, call [`TO_STRING`](../esql-functions-operators.md#esql-to_string) first:

```esql
ROW a=[10, 9, 8]
| EVAL j = MV_CONCAT(TO_STRING(a), ", ")
```

| a:integer | j:keyword |
| --- | --- |
| [10, 9, 8] | "10, 9, 8" |


## `MV_COUNT` [esql-mv_count]

**Syntax**

:::{image} ../../../../images/mv_count.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalued expression into a single valued column containing a count of the number of values.

**Supported types**

| field | result |
| --- | --- |
| boolean | integer |
| cartesian_point | integer |
| cartesian_shape | integer |
| date | integer |
| date_nanos | integer |
| double | integer |
| geo_point | integer |
| geo_shape | integer |
| integer | integer |
| ip | integer |
| keyword | integer |
| long | integer |
| text | integer |
| unsigned_long | integer |
| version | integer |

**Example**

```esql
ROW a=["foo", "zoo", "bar"]
| EVAL count_a = MV_COUNT(a)
```

| a:keyword | count_a:integer |
| --- | --- |
| ["foo", "zoo", "bar"] | 3 |


## `MV_DEDUPE` [esql-mv_dedupe]

**Syntax**

:::{image} ../../../../images/mv_dedupe.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Remove duplicate values from a multivalued field.

::::{note}
`MV_DEDUPE` may, but won’t always, sort the values in the column.
::::


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
ROW a=["foo", "foo", "bar", "foo"]
| EVAL dedupe_a = MV_DEDUPE(a)
```

| a:keyword | dedupe_a:keyword |
| --- | --- |
| ["foo", "foo", "bar", "foo"] | ["foo", "bar"] |


## `MV_FIRST` [esql-mv_first]

**Syntax**

:::{image} ../../../../images/mv_first.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalued expression into a single valued column containing the first value. This is most useful when reading from a function that emits multivalued columns in a known order like [`SPLIT`](../esql-functions-operators.md#esql-split).

The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md) are read from underlying storage is not guaranteed. It is **frequently** ascending, but don’t rely on that. If you need the minimum value use [`MV_MIN`](../esql-functions-operators.md#esql-mv_min) instead of `MV_FIRST`. `MV_MIN` has optimizations for sorted values so there isn’t a performance benefit to `MV_FIRST`.

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


## `MV_LAST` [esql-mv_last]

**Syntax**

:::{image} ../../../../images/mv_last.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Multivalue expression.

**Description**

Converts a multivalue expression into a single valued column containing the last value. This is most useful when reading from a function that emits multivalued columns in a known order like [`SPLIT`](../esql-functions-operators.md#esql-split).

The order that [multivalued fields](/reference/query-languages/esql/esql-multivalued-fields.md) are read from underlying storage is not guaranteed. It is **frequently** ascending, but don’t rely on that. If you need the maximum value use [`MV_MAX`](../esql-functions-operators.md#esql-mv_max) instead of `MV_LAST`. `MV_MAX` has optimizations for sorted values so there isn’t a performance benefit to `MV_LAST`.

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


## `MV_MAX` [esql-mv_max]

**Syntax**

:::{image} ../../../../images/mv_max.svg
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


## `MV_MEDIAN` [esql-mv_median]

**Syntax**

:::{image} ../../../../images/mv_median.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the median value.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Examples**

```esql
ROW a=[3, 5, 1]
| EVAL median_a = MV_MEDIAN(a)
```

| a:integer | median_a:integer |
| --- | --- |
| [3, 5, 1] | 3 |

If the row has an even number of values for a column, the result will be the average of the middle two entries. If the column is not floating point, the average rounds **down**:

```esql
ROW a=[3, 7, 1, 6]
| EVAL median_a = MV_MEDIAN(a)
```

| a:integer | median_a:integer |
| --- | --- |
| [3, 7, 1, 6] | 4 |


## `MV_MEDIAN_ABSOLUTE_DEVIATION` [esql-mv_median_absolute_deviation]

**Syntax**

:::{image} ../../../../images/mv_median_absolute_deviation.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the median absolute deviation.  It is calculated as the median of each data point’s deviation from the median of the entire sample. That is, for a random variable `X`, the median absolute deviation is `median(|median(X) - X|)`.

::::{note}
If the field has an even number of values, the medians will be calculated as the average of the middle two values. If the value is not a floating point number, the averages are rounded towards 0.
::::


**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW values = [0, 2, 5, 6]
| EVAL median_absolute_deviation = MV_MEDIAN_ABSOLUTE_DEVIATION(values), median = MV_MEDIAN(values)
```

| values:integer | median_absolute_deviation:integer | median:integer |
| --- | --- | --- |
| [0, 2, 5, 6] | 2 | 3 |


## `MV_MIN` [esql-mv_min]

**Syntax**

:::{image} ../../../../images/mv_min.svg
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


## `MV_PERCENTILE` [esql-mv_percentile]

**Syntax**

:::{image} ../../../../images/mv_percentile.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

`percentile`
:   The percentile to calculate. Must be a number between 0 and 100. Numbers out of range will return a null instead.

**Description**

Converts a multivalued field into a single valued field containing the value at which a certain percentage of observed values occur.

**Supported types**

| number | percentile | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | integer |
| integer | integer | integer |
| integer | long | integer |
| long | double | long |
| long | integer | long |
| long | long | long |

**Example**

```esql
ROW values = [5, 5, 10, 12, 5000]
| EVAL p50 = MV_PERCENTILE(values, 50), median = MV_MEDIAN(values)
```

| values:integer | p50:integer | median:integer |
| --- | --- | --- |
| [5, 5, 10, 12, 5000] | 10 | 10 |


## `MV_PSERIES_WEIGHTED_SUM` [esql-mv_pseries_weighted_sum]

**Syntax**

:::{image} ../../../../images/mv_pseries_weighted_sum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

`p`
:   It is a constant number that represents the *p* parameter in the P-Series. It impacts every element’s contribution to the weighted sum.

**Description**

Converts a multivalued expression into a single-valued column by multiplying every element on the input list by its corresponding term in P-Series and computing the sum.

**Supported types**

| number | p | result |
| --- | --- | --- |
| double | double | double |

**Example**

```esql
ROW a = [70.0, 45.0, 21.0, 21.0, 21.0]
| EVAL sum = MV_PSERIES_WEIGHTED_SUM(a, 1.5)
| KEEP sum
```

| sum:double |
| --- |
| 94.45465156212452 |


## `MV_SLICE` [esql-mv_slice]

**Syntax**

:::{image} ../../../../images/mv_slice.svg
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

Returns a subset of the multivalued field using the start and end index values. This is most useful when reading from a function that emits multivalued columns in a known order like [`SPLIT`](../esql-functions-operators.md#esql-split) or [`MV_SORT`](../esql-functions-operators.md#esql-mv_sort).

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


## `MV_SORT` [esql-mv_sort]

**Syntax**

:::{image} ../../../../images/mv_sort.svg
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


## `MV_SUM` [esql-mv_sum]

**Syntax**

:::{image} ../../../../images/mv_sum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

**Description**

Converts a multivalued field into a single valued field containing the sum of all of the values.

**Supported types**

| number | result |
| --- | --- |
| double | double |
| integer | integer |
| long | long |
| unsigned_long | unsigned_long |

**Example**

```esql
ROW a=[3, 5, 6]
| EVAL sum_a = MV_SUM(a)
```

| a:integer | sum_a:integer |
| --- | --- |
| [3, 5, 6] | 14 |


## `MV_ZIP` [esql-mv_zip]

**Syntax**

:::{image} ../../../../images/mv_zip.svg
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
