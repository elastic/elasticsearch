```yaml {applies_to}
serverless: ga
stack: ga
```

The `SORT` processing command sorts a table on one or more expressions.

## Syntax

```esql
SORT expression1 [ASC/DESC][NULLS FIRST/NULLS LAST][, ..., expressionN [ASC/DESC][NULLS FIRST/NULLS LAST]]
```

## Parameters

`expressionX`
:   The expression to sort on. Can be a column name, a
    [function](/reference/query-languages/esql/esql-functions-operators.md#esql-functions) (for example,
    `length(field)`, `DATE_EXTRACT("year", date)`), or an arithmetic expression (for example, `salary * 2`).
    The expression is evaluated per row and the result is used for ordering.

## Description

The `SORT` processing command sorts a table on one or more expressions. You can sort by any
expression, not only column names—for example, `length(first_name)` or `DATE_EXTRACT("year", hire_date)`.

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

The following examples show how to control sort order, tie-breaking, null placement, and sorting by expressions.

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

### Sort by expression

You can sort by any expression, not just column names. The following example sorts rows by the
length of the `first_name` field in descending order:

```esql
FROM employees
| KEEP first_name, last_name
| SORT length(first_name) DESC
| LIMIT 5
```
