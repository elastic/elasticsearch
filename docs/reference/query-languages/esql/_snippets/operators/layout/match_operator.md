### Match operator (`:`) [esql-match-operator]
```{applies_to}
stack: preview =9.0, ga 9.1+
```

Use the match operator (`:`) to perform full-text search and filter rows that match a given query string.

**Syntax**

:::{image} /reference/query-languages/esql/images/generated/x-pack-esql/operators/match_operator.svg
:alt: Embedded
:class: text-center
:::

The match operator performs a [match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) on the specified field or expression. Returns true if the provided query matches the row.

The match operator is equivalent to the [match function](../../../functions-operators/search-functions/match.md), which is the standard function for performing full-text search in ES|QL.

{applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`
The match operator can also search expressions that are not backed by an index,
such as computed columns produced by `EVAL` or `STATS`.
When the target is not an indexed field, the search evaluates by scanning values
row by row, which may be slower on large datasets.

For using the function syntax, or adding [match query parameters](/reference/query-languages/query-dsl/query-dsl-match-query.md#match-field-params), you can use the [match function](../../../functions-operators/search-functions/match.md).

:::{tip}
Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
:::

:::{include} ../../generated/x-pack-esql/operators/types/match_operator.md
:::

:::{include} ../../generated/x-pack-esql/operators/examples/match_operator.md
:::
