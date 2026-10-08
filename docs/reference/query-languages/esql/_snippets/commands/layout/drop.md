```yaml {applies_to}
serverless: ga
stack: ga
```

The `DROP` processing command removes one or more columns.

## Syntax

```esql
DROP columns
```

## Parameters

`columns`
:   A comma-separated list of columns to remove. Supports wildcards.

:::::{note}
A wildcard pattern that matches no columns causes the query to fail with an error such as `No matches found for pattern [columns*]`. The exception is when [`SET unmapped_fields`](/reference/query-languages/esql/directives/set.md#esql-unmapped_fields) is set to `"nullify"` or `"load"`: in that case, a pattern that matches no columns is ignored.
:::::

## Examples

The following examples show how to remove columns by name and by pattern.

### Drop a column by name

:::{include} ../../generated/x-pack-esql/commands/examples/drop.csv-spec/height.md
:::

### Drop columns matching a wildcard pattern

Rather than specify each column by name, you can use wildcards to drop all
columns with a name that matches a pattern:

:::{include} ../../generated/x-pack-esql/commands/examples/drop.csv-spec/heightWithWildcard.md
:::
