## `RENAME` [esql-rename]

The `RENAME` processing command renames one or more columns.

**Syntax**

```esql
RENAME old_name1 AS new_name1[, ..., old_nameN AS new_nameN]
```

**Parameters**

`old_nameX`
:   The name of a column you want to rename.

`new_nameX`
:   The new name of the column. If it conflicts with an existing column name,
    the existing column is dropped. If multiple columns are renamed to the same
    name, all but the rightmost column with the same new name are dropped.

**Description**

The `RENAME` processing command renames one or more columns. If a column with
the new name already exists, it will be replaced by the new column.

A `RENAME` with multiple column renames is equivalent to multiple sequential `RENAME` commands.

**Examples**

:::{include} ../examples/docs.csv-spec/rename.md
:::

Multiple columns can be renamed with a single `RENAME` command:

:::{include} ../examples/docs.csv-spec/renameMultipleColumns.md
:::

With multiple `RENAME` commands:

:::{include} ../examples/docs.csv-spec/renameMultipleColumnsDifferentCommands.md
:::
