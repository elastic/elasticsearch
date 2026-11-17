```yaml {applies_to}
stack: ga 9.7+
serverless: ga
```

The `DESCRIBE` command returns one row for each column of its input, with what the column is and where it comes from.

## Syntax

```esql
DESCRIBE
```

## Description

`DESCRIBE` takes no arguments. It describes the columns the query has at that point, after views, renames and computations are resolved, and reads no data. Its output is a table like any other, so the rest of the query can filter, sort or aggregate it.

`DESCRIBE` returns the following columns:

`name`
:   The name of the column.

`type`
:   The {{esql}} type of the column.

`role`
:   How the field the column reads is meant to be used: `time`, `dimension`, `metric`, `attribute`, `numeric`, `text`, `semantic`, `vector`, `metadata` or `other`. `dimension` and `metric` come from time series mappings. The others follow from the field type, and `time` is the `@timestamp` field. `mixed` means the indices disagree. `null` for a computed column.

`field`
:   The field or dataset column the column reads, or `null` if it is computed.

`origin`
:   Where the column comes from:
    * `mapped`: a mapped field, under its own name.
    * `renamed`: a mapped field or dataset column under another name.
    * `computed`: the result of an expression or an aggregation.
    * `dataset`: a column of a dataset.
    * `metadata`: a metadata field, such as `_index`.
    * `unmapped`: a field that no index maps.
    * `unsupported`: a field of a type that {{esql}} does not support.

`metric_type`
:   The `time_series_metric` of the field, such as `gauge` or `counter`. `mixed` means the indices disagree.

`unit`
:   The unit of the column, from the `unit` in the field's `meta`. A computed column has the unit that follows from the computation when there is one, for example `bytes/s` for the rate of a field in `bytes`. When the indices or the branches of a query disagree, the column lists every unit.

`description`
:   The `description` in the field's `meta`. When the indices or the branches of a query disagree, the column lists every description. The listed units and descriptions are collected independently, so their positions don't match.

## Examples

Describe the fields of an index:

:::{include} ../../generated/x-pack-esql/commands/examples/describe.csv-spec/describeRoles.md
:::

Follow a column back to the field it reads:

:::{include} ../../generated/x-pack-esql/commands/examples/describe.csv-spec/describeLineage.md
:::

Summarize the fields of an index by role:

:::{include} ../../generated/x-pack-esql/commands/examples/describe.csv-spec/describeRolesSummary.md
:::
