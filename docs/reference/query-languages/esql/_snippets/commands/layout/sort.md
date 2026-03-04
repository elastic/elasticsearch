```yaml {applies_to}
serverless: ga
stack: ga
```

The `SORT` processing command sorts a table on one or more columns.

## Syntax

```esql
SORT column1 [ASC/DESC][NULLS FIRST/NULLS LAST][, ..., columnN [ASC/DESC][NULLS FIRST/NULLS LAST]]
```

## Parameters

`columnX`
:   The column to sort on.

## Description

The `SORT` processing command sorts a table on one or more columns.

The default sort order is ascending. Use `ASC` or `DESC` to specify an explicit
sort order.

Two rows with the same sort key are considered equal. You can provide additional
sort expressions to act as tie breakers.

Sorting on multivalued columns uses the lowest value when sorting ascending and
the highest value when sorting descending.

By default, `null` values are treated as being larger than any other value. With
an ascending sort order, `null` values are sorted last, and with a descending
sort order, `null` values are sorted first. You can change that by providing
`NULLS FIRST` or `NULLS LAST`.

## Examples

The following examples show how to control sort order, tie-breaking, and null placement.

### Sort in default ascending order

:::{include} ../examples/docs.csv-spec/sort.md
:::

### Sort in descending order with DESC

:::{include} ../examples/docs.csv-spec/sortDesc.md
:::

### Break ties with additional sort expressions

:::{include} ../examples/docs.csv-spec/sortTie.md
:::

### Control null placement with NULLS FIRST or NULLS LAST

:::{include} ../examples/docs.csv-spec/sortNullsFirst.md
:::
