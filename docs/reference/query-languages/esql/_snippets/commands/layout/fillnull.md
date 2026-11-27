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
    - `NULL`, to leave nulls unchanged (an explicit no-op). Write the `NULL` keyword in the query itself: a [query parameter](/reference/query-languages/esql/esql-rest.md#esql-rest-params) that resolves to null is rejected with an error, so the keyword is how you request a no-op, for example when the fill value is only conditionally present.

`columns`
:   A comma-separated list of column names and wildcard patterns to fill, for example `gender`, `a, b`, or `*_name`. Use `*` to target every column.

    A bare `*` targets only the query's own columns; it never targets a column whose name starts with an underscore, such as the metadata columns `_index` and `_score`, whatever the column holds and wherever it comes from. That includes a column that a union or a [`FORK`](/reference/query-languages/esql/commands/fork.md) passes on, and one that your own query makes with such a name. This exclusion applies only to bare `*`: to target such a column, name it, or use a wildcard pattern that matches it, such as `_ind*` for `_index`.

## Description

The `FILLNULL` processing command replaces null values in the targeted columns with `fill_value`. Non-null values are never changed, and each column keeps its type; only its nulls are replaced.

A literal `fill_value` is applied to every targeted column whose type it fits. A string is coerced where {{esql}} allows it, so a string value can fill `datetime`, `date_nanos`, `ip`, `version`, and `boolean` columns. If the value cannot be applied to a column, for example because its type does not match or it is out of range, that column is left unchanged and a warning lists the skipped columns. A column with no concrete type — for example one produced by `ROW a = null` — is treated as `null`-typed and is left unchanged even with an explicit value; cast it to a concrete type (for example `EVAL a = a::keyword`) before filling it.

`DEFAULT` fills each targeted column with a default for its type: `0` for numeric types, an empty string for `keyword` and `text`, and `false` for `boolean`. Other types have no default and are left unchanged, with a warning. Some of them can still be filled with an explicit value even though they have no default: `datetime`, `date_nanos`, `ip`, and `version` accept a string. The rest — spatial types such as `geo_point`, ranges, histograms, counter fields, and `aggregate_metric_double` — cannot be filled at all, neither by `DEFAULT` nor by an explicit value.

Targeting works the same for named columns, wildcard patterns, and `*`, and listing `*` together with a name or pattern still targets every column. A column that resolves but whose type the value does not fit is skipped with a warning rather than failing the query. A target that does not resolve is different: an unknown column name, or a wildcard pattern that matches no column, fails the query with a verification error, even when `*` is listed alongside it. This assumes the default handling of unmapped fields; if [`SET unmapped_fields`](/reference/query-languages/esql/esql-unmapped-fields.md) is set to `LOAD` or `LOAD_ALL`, a name that is absent from the mapping but loaded at the source resolves as an unmapped column instead of failing, and `FILLNULL` can fill it. That resolution only holds while the column is still in `FILLNULL`'s input: a name that an earlier command dropped — for example a [`STATS`](/reference/query-languages/esql/commands/stats-by.md) that does not output it — still fails as an unknown column.

The columns filled by all the `FILLNULL` commands in a single query share a limit (`esql.query.max_fillnull_fields`, 1,000 by default), and each `FORK` branch and view reference that runs one counts toward that query's total. An `IN` subquery runs as a query of its own with its own budget, so its columns are not added to the outer query's total. A query that would exceed its limit is rejected rather than filling a subset, so with wide data prefer naming the columns you need, or narrow the input with [`KEEP`](/reference/query-languages/esql/commands/keep.md) or [`DROP`](/reference/query-languages/esql/commands/drop.md) before `FILLNULL ... ON *`.

## Examples

Replace the nulls in a single column with a value, leaving the rows that already have a value untouched:

:::{include} ../../generated/x-pack-esql/commands/examples/fillnull.csv-spec/onColumnForDocs.md
:::

Fill every column with the default for its type using `DEFAULT ON *`:

:::{include} ../../generated/x-pack-esql/commands/examples/fillnull.csv-spec/defaultAllForDocs.md
:::
