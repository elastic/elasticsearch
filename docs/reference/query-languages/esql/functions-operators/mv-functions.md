---
applies_to:
  stack: ga
  serverless: ga
navigation_title: "Multivalue functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-mv-functions
---

# {{esql}} multivalue functions [esql-mv-functions]

{{esql}} fields can contain more than one value. Multivalue functions let you manipulate, filter, and reduce those values within a query without needing to normalize the data first.

{{esql}} supports these multivalue functions, grouped by category:

* [Manipulation functions](#manipulation-functions)
* [Transformation functions](#transformation-functions)
* [Filter and predicate functions](#filter-and-predicate-functions)
* [Reduction functions](#reduction-functions)

## Manipulation functions
Functions to add, remove, combine, or reorder multi-value inputs. All these functions
return multi-values.

* [`MV_APPEND`](./mv-functions/mv_append.md): Adds a value or multi-value to the end of a multi-value.
* [`MV_DIFFERENCE`](./mv-functions/mv_difference.md) {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview`: Removes a value or multi-value from a multi-value.
* [`MV_DEDUPE`](./mv-functions/mv_dedupe.md): Removes duplicate values from a multi-value.
* [`MV_SORT`](./mv-functions/mv_sort.md): Sorts the values in a multi-value.
* [`MV_INTERSECTION`](./mv-functions/mv_intersection.md) {applies_to}`stack: preview 9.3` {applies_to}`serverless: preview`: Keeps the values present in both multi-values.
* [`MV_UNION`](./mv-functions/mv_union.md) {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview`: Keeps all the unique values present in either multi-value.
* [`MV_SLICE`](./mv-functions/mv_slice.md): Keeps the values in a multi-value between the given start and end indexes.

## Transformation functions
Functions that map a multi-value to a new multi-value.

* [`MV_ZIP`](./mv-functions/mv_zip.md): Combines values from two multi-values at each position using a delimiter.

## Filter and predicate functions
Functions that return a boolean value based on the properties of a multi-value.
These provide optimized shorthand expressions for common operations.

:::{note}
`null` is interpreted as an empty set. To reject "unknown" or absent values, check for `null` before calling the function.
```esql
WHERE field2 IS NOT null AND MV_CONTAINS(field1, field2)
```
:::

* [`MV_CONTAINS`](./mv-functions/mv_contains.md) {applies_to}`stack: preview 9.2` {applies_to}`serverless: preview`: Tests if a multi-value contains _all_ of the provided values.
* [`MV_INTERSECTS`](./mv-functions/mv_intersects.md) {applies_to}`stack: preview 9.3` {applies_to}`serverless: preview`: Tests if a multi-value contains _any_ of the provided values.

## Reduction functions
Functions that reduce a multi-value to a single value.

### General functions

* [`MV_COUNT`](./mv-functions/mv_count.md): Counts the number of values in a multi-value.

### Selection functions
Functions that reduce a multi-value to a single value by keeping one of the existing values.

* [`MV_FIRST`](./mv-functions/mv_first.md): returns the first value in a multi-value.
* [`MV_LAST`](./mv-functions/mv_last.md): returns the last value in a multi-value.
* [`MV_MIN`](./mv-functions/mv_min.md): returns the smallest value in a multi-value.
* [`MV_MAX`](./mv-functions/mv_max.md): returns the largest value in a multi-value.

### Aggregation functions
Functions that reduce a multi-value to a single value by aggregating the values.

#### Numeric aggregation functions
Functions that calculate a single value from a numeric multi-value.
(double, integer, long, etc.)

* [`MV_AVG`](./mv-functions/mv_avg.md): calculates the average of all values in a multi-value.
* [`MV_SUM`](./mv-functions/mv_sum.md): calculates the sum of all values in a multi-value.
* [`MV_MEDIAN`](./mv-functions/mv_median.md): calculates the median of all values in a multi-value.
* [`MV_MEDIAN_ABSOLUTE_DEVIATION`](./mv-functions/mv_median_absolute_deviation.md): calculates the median absolute deviation of all values in a multi-value.
* [`MV_PERCENTILE`](./mv-functions/mv_percentile.md): calculates the percentile of all values in a multi-value.
* [`MV_PSERIES_WEIGHTED_SUM`](./mv-functions/mv_pseries_weighted_sum.md): calculates the weighted sum of all values in a multi-value.

#### String aggregation functions
Functions that calculate a single value from a string multi-value. (text, keyword)

* [`MV_CONCAT`](./mv-functions/mv_concat.md): concatenates all values into a single string.
