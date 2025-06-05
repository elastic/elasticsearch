## `DROP` [esql-drop]

The `DROP` processing command removes one or more columns.

**Syntax**

```esql
DROP columns
```

**Parameters**

`columns`
:   A comma-separated list of columns to remove. Supports wildcards.

**Examples**

:::{include} ../examples/drop.csv-spec/height.md
:::
Rather than specify each column by name, you can use wildcards to drop all
columns with a name that matches a pattern:

:::{include} ../examples/drop.csv-spec/heightWithWildcard.md
:::
