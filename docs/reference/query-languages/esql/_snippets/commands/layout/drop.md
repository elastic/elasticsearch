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

## Examples

The following examples show how to remove columns by name and by pattern.

### Drop a column by name

:::{include} ../examples/drop.csv-spec/height.md
:::

### Drop columns matching a wildcard pattern

Rather than specify each column by name, you can use wildcards to drop all
columns with a name that matches a pattern:

:::{include} ../examples/drop.csv-spec/heightWithWildcard.md
:::
