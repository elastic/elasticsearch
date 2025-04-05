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
:   The new name of the column. If it conflicts with an existing column name, the existing column is dropped. If multiple columns are renamed to the same name, all but the rightmost column with the same new name are dropped.

**Description**

The `RENAME` processing command renames one or more columns. If a column with the new name already exists, it will be replaced by the new column.

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, still_hired
| RENAME  still_hired AS employed
```

Multiple columns can be renamed with a single `RENAME` command:

```esql
FROM employees
| KEEP first_name, last_name
| RENAME first_name AS fn, last_name AS ln
```


