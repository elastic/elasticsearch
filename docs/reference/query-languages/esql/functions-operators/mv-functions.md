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

* [`MV_APPEND`](./mv-functions/mv_append.md)

  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_append.md
  :::
* [`MV_DIFFERENCE`](./mv-functions/mv_difference.md) {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview`
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_difference.md
  :::
* [`MV_DEDUPE`](./mv-functions/mv_dedupe.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_dedupe.md
  :::
* [`MV_SORT`](./mv-functions/mv_sort.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_sort.md
  :::
* [`MV_INTERSECTION`](./mv-functions/mv_intersection.md) {applies_to}`stack: preview 9.3` {applies_to}`serverless: preview`
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_intersection.md
  :::
* [`MV_UNION`](./mv-functions/mv_union.md) {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview`
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_union.md
  :::
* [`MV_SLICE`](./mv-functions/mv_slice.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_slice.md
  :::

## Transformation functions
Functions that map a multi-value to a new multi-value.

* [`MV_ZIP`](./mv-functions/mv_zip.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_zip.md
  :::

## Filter and predicate functions
Functions that return a boolean value based on the properties of a multi-value.
These provide optimized shorthand expressions for common operations.

* [`MV_CONTAINS`](./mv-functions/mv_contains.md) {applies_to}`stack: preview 9.2` {applies_to}`serverless: preview`
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_contains.md
  :::
* [`MV_INTERSECTS`](./mv-functions/mv_intersects.md) {applies_to}`stack: preview 9.3` {applies_to}`serverless: preview`
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_intersects.md
  :::

:::{note}
`null` is interpreted as an empty set. To reject "unknown" or absent values, check for `null` before calling the function.
```esql
WHERE field2 IS NOT null AND MV_CONTAINS(field1, field2)
```
:::

## Reduction functions
Functions that reduce a multi-value to a single value.

### General functions

* [`MV_COUNT`](./mv-functions/mv_count.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_count.md
  :::

### Selection functions
Functions that reduce a multi-value to a single value by keeping one of the existing values.

* [`MV_FIRST`](./mv-functions/mv_first.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_first.md
  :::
* [`MV_LAST`](./mv-functions/mv_last.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_last.md
  :::
* [`MV_MIN`](./mv-functions/mv_min.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_min.md
  :::
* [`MV_MAX`](./mv-functions/mv_max.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_max.md
  :::

### Aggregation functions
Functions that reduce a multi-value to a single value by aggregating the values.

#### Numeric aggregation functions
Functions that calculate a single value from a numeric multi-value.
(double, integer, long, etc.)

* [`MV_AVG`](./mv-functions/mv_avg.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_avg.md
  :::
* [`MV_SUM`](./mv-functions/mv_sum.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_sum.md
  :::
* [`MV_MEDIAN`](./mv-functions/mv_median.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_median.md
  :::
* [`MV_MEDIAN_ABSOLUTE_DEVIATION`](./mv-functions/mv_median_absolute_deviation.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_median_absolute_deviation.md
  :::
* [`MV_PERCENTILE`](./mv-functions/mv_percentile.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_percentile.md
  :::
* [`MV_PSERIES_WEIGHTED_SUM`](./mv-functions/mv_pseries_weighted_sum.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_pseries_weighted_sum.md
  :::

#### String aggregation functions
Functions that calculate a single value from a string multi-value. (text, keyword)

* [`MV_CONCAT`](./mv-functions/mv_concat.md)
  :::{include} ../_snippets/generated/x-pack-esql/functions/briefSummary/mv_concat.md
  :::
