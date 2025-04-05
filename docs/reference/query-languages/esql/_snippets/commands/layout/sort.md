## `SORT` [esql-sort]

The `SORT` processing command sorts a table on one or more columns.

**Syntax**

```esql
SORT column1 [ASC/DESC][NULLS FIRST/NULLS LAST][, ..., columnN [ASC/DESC][NULLS FIRST/NULLS LAST]]
```

**Parameters**

`columnX`
:   The column to sort on.

**Description**

The `SORT` processing command sorts a table on one or more columns.

The default sort order is ascending. Use `ASC` or `DESC` to specify an explicit sort order.

Two rows with the same sort key are considered equal. You can provide additional sort expressions to act as tie breakers.

Sorting on multivalued columns uses the lowest value when sorting ascending and the highest value when sorting descending.

By default, `null` values are treated as being larger than any other value. With an ascending sort order, `null` values are sorted last, and with a descending sort order, `null` values are sorted first. You can change that by providing `NULLS FIRST` or `NULLS LAST`.

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height
```

Explicitly sorting in ascending order with `ASC`:

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height DESC
```

Providing additional sort expressions to act as tie breakers:

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT height DESC, first_name ASC
```

Sorting `null` values first using `NULLS FIRST`:

```esql
FROM employees
| KEEP first_name, last_name, height
| SORT first_name ASC NULLS FIRST
```


