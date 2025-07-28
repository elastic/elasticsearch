## `WHERE` [esql-where]

The `WHERE` processing command produces a table that contains all the rows from
the input table for which the provided condition evaluates to `true`.

::::{tip}
In case of value exclusions, fields with `null` values will be excluded from search results.
In this context a `null` means either there is an explicit `null` value in the document or
there is no value at all. For example: `WHERE field != "value"` will be interpreted as
`WHERE field != "value" AND field IS NOT NULL`.
::::


**Syntax**

```esql
WHERE expression
```

**Parameters**

`expression`
:   A boolean expression.

**Examples**

:::{include} ../examples/docs.csv-spec/where.md
:::
Which, if `still_hired` is a boolean field, can be simplified to:

:::{include} ../examples/docs.csv-spec/whereBoolean.md
:::
Use date math to retrieve data from a specific time range. For example, to
retrieve the last hour of logs:

:::{include} ../examples/date.csv-spec/docsNowWhere.md
:::
`WHERE` supports various [functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).
For example the [`LENGTH`](/reference/query-languages/esql/functions-operators/string-functions.md#esql-length) function:

:::{include} ../examples/docs.csv-spec/whereFunction.md
:::

For a complete list of all functions, refer to [Functions overview](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).

### NULL Predicates

For NULL comparison, use the `IS NULL` and `IS NOT NULL` predicates.

:::{include} ../../operators/examples/is_null.md
:::

:::{include} ../../operators/examples/is_not_null.md
:::

### Matching text

For matching text, you can use [full text search functions](/reference/query-languages/esql/functions-operators/search-functions.md) like `MATCH`.

Use [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) to perform a
[match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) on a specified field.

Match can be used on text fields, as well as other field types like boolean, dates, and numeric types.

:::{include} ../../functions/examples/match.md
:::

::::{tip}
You can also use the shorthand [match operator](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator) `:` instead of `MATCH`.

::::

### LIKE and RLIKE

Use `LIKE` to filter data based on string patterns using wildcards. `LIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

The following wildcard characters are supported:

* `*` matches zero or more characters.
* `?` matches one character.

:::{include} ../../operators/types/like.md
:::

:::{include} ../../operators/examples/like.md
:::

:::{include} ../../operators/detailedDescription/like.md
:::

Use `RLIKE` to filter data based on string patterns using using [regular expressions](/reference/query-languages/query-dsl/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

:::{include} ../../operators/types/rlike.md
:::

:::{include} ../../operators/examples/rlike.md
:::

:::{include} ../../operators/detailedDescription/rlike.md
:::

### IN

The `IN` operator allows testing whether a field or expression equals an element
in a list of literals, fields or expressions:

:::{include} ../../operators/examples/in.md
:::

For a complete list of all operators, refer to [Operators](/reference/query-languages/esql/esql-functions-operators.md#esql-operators-overview).
