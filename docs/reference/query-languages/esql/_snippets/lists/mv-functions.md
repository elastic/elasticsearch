# Manipulation
Functions to add, remove, combine, or reorder multi-value fields.

* [`MV_APPEND`](../../functions-operators/mv-functions/mv_append.md) - Adds values to the end of a multi-value field.
* [`MV_DEDUPE`](../../functions-operators/mv-functions/mv_dedupe.md) - Removes duplicate values from a multi-value field.
* [`MV_DIFFERENCE`](../../functions-operators/mv-functions/mv_difference.md) {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview` - Returns the values that appear in the first field, except those that appear in the second.
* [`MV_INTERSECTION`](../../functions-operators/mv-functions/mv_intersection.md) {applies_to}`stack: preview 9.3` {applies_to}`serverless: preview` - Returns the values that appear in both input fields.
* [`MV_SORT`](../../functions-operators/mv-functions/mv_sort.md) - sorts the values in a multi-value field.
* [`MV_SLICE`](../../functions-operators/mv-functions/mv_slice.md) - extracts a subset of values from a multi-value field.
* [`MV_UNION`](../../functions-operators/mv-functions/mv_union.md) {applies_to}`stack: preview 9.4` {applies_to}`serverless: preview` - takes the unique values of values present either of the two fields.

# Transformation
Functions that map a multi-value field to a new multi-value field.

* [`MV_ZIP`](../../functions-operators/mv-functions/mv_zip.md) - Combines the values from two multivalued fields with a delimiter that joins them together.

# Filtering and predicates
Functions that return a boolean value based on the properties of a multi-value
field. Providing optimized, shorthand expressions for common operations.

* [`MV_CONTAINS`](../../functions-operators/mv-functions/mv_contains.md) {applies_to}`stack: preview 9.2` {applies_to}`serverless: preview` - Tests if a multi-value field contains _all_ of the provided values.
* [`MV_INTERSECTS`](../../functions-operators/mv-functions/mv_intersects.md) {applies_to}`stack: preview 9.3` {applies_to}`serverless: preview` - Tests if a multi-value field contains _any_ of the provided values.

# Reduce
Functions that reduce a multi-value field to a single value.

## General

* [`MV_COUNT`](../../functions-operators/mv-functions/mv_count.md) - Counts the number of values in a multi-value field.

## Selection
Functions that reduce a multi-value field to a single value by returning one of
the values.

* [`MV_FIRST`](../../functions-operators/mv-functions/mv_first.md) - returns the first value in a multi-value field.
* [`MV_LAST`](../../functions-operators/mv-functions/mv_last.md) - returns the last value in a multi-value field.
* [`MV_MIN`](../../functions-operators/mv-functions/mv_min.md) - returns the smallest value in a multi-value field.
* [`MV_MAX`](../../functions-operators/mv-functions/mv_max.md) - returns the largest value in a multi-value field.

## Aggregation
Functions that reduce a multi-value field to a single value by aggregating the
values.

### Numeric Aggregations
Functions that calculate a single value from a numeric multi-value field.
(double, integer, long, etc.)

* [`MV_AVG`](../../functions-operators/mv-functions/mv_avg.md) - calculates the average of all values in a multi-value field.
* [`MV_SUM`](../../functions-operators/mv-functions/mv_sum.md) - calculates the sum of all values in a multi-value field.
* [`MV_MEDIAN`](../../functions-operators/mv-functions/mv_median.md) - calculates the median of all values in a multi-value field.
* [`MV_MEDIAN_ABSOLUTE_DEVIATION`](../../functions-operators/mv-functions/mv_median_absolute_deviation.md) - calculates the median absolute deviation of all values in a multi-value field.
* [`MV_PERCENTILE`](../../functions-operators/mv-functions/mv_percentile.md) - calculates the percentile of all values in a multi-value field.
* [`MV_PSERIES_WEIGHTED_SUM`](../../functions-operators/mv-functions/mv_pseries_weighted_sum.md) - calculates the weighted sum of all values in a multi-value field.

### String Aggregations
Functions that calculate a single value from a string multi-value field. (text,
keyword)

* [`MV_CONCAT`](../../functions-operators/mv-functions/mv_concat.md) - concatenates all values into a single string.
