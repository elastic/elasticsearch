```yaml {applies_to}
stack: ga 9.7+
serverless: ga
```

The `FILLNULL` processing command replaces null values in one or more columns with a value you choose, or with a per-type default.

## Syntax

```esql
FILLNULL fill_value ON columns
```

## Parameters

`fill_value`
:   The value used to replace nulls. It can be one of:

    - A literal number, string, or boolean (or a [query parameter](/reference/query-languages/esql/esql-rest.md#esql-rest-params) that resolves to one).
    - `DEFAULT`, to fill each targeted column with a default for its type.
    - `NULL`, to leave nulls unchanged. This is mainly useful with a query parameter that might be null, so that a null parameter is an explicit no-op rather than an error.

`columns`
:   A comma-separated list of column names and wildcard patterns to fill, for example `gender`, `a, b`, or `*_name`. Use `*` to target every column.

## Description

The `FILLNULL` processing command replaces null values in the targeted columns with `fill_value`. Non-null values are never changed, and each column keeps its type; only its nulls are replaced.

A literal `fill_value` is applied to every targeted column whose type it fits. A string is coerced where {{esql}} allows it, so a string value can fill `datetime`, `date_nanos`, `ip`, `version`, and `boolean` columns. If the value cannot be applied to a column, for example because its type does not match or it is out of range, that column is left unchanged and a warning lists the skipped columns.

`DEFAULT` fills each targeted column with a default for its type: `0` for numeric types, an empty string for `keyword` and `text`, and `false` for `boolean`. Types that have no default, such as dates, `ip`, `version`, spatial types, ranges, histograms, counter fields, and `aggregate_metric_double`, are left unchanged, with a warning. To fill one of those, provide an explicit value instead.

Targeting works the same for named columns, wildcard patterns, and `*`: a column whose type the value does not fit is skipped with a warning rather than failing the query. Listing `*` together with a name or pattern still targets every column.

When `ON *` runs after a union of sources, such as a multi-index or subquery [`FROM`](/reference/query-languages/esql/commands/from.md) or a [`FORK`](/reference/query-languages/esql/commands/fork.md), a metadata column such as `_index` that is null on some rows is left alone. Name it explicitly to fill it.

## Examples

Replace the nulls in a single column with a value, leaving the rows that already have a value untouched:

:::{include} ../../generated/x-pack-esql/commands/examples/fillnull.csv-spec/onColumnForDocs.md
:::

Fill every column with the default for its type using `DEFAULT ON *`:

:::{include} ../../generated/x-pack-esql/commands/examples/fillnull.csv-spec/defaultAllForDocs.md
:::
