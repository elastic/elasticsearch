### Match operator (`:`) [esql-match-operator]

The only search operator is match (`:`).

**Syntax**

:::{image} ../../../images/operators/match_operator.svg
:alt: Embedded
:class: text-center
:::


::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The match operator performs a [match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) on the specified field. Returns true if the provided query matches the row.

The match operator is equivalent to the [match function](../../../functions-operators/search-functions.md#esql-match).

For using the function syntax, or adding [match query parameters](/reference/query-languages/query-dsl/query-dsl-match-query.md#match-field-params), you can use the [match function](../../../functions-operators/search-functions.md#esql-match).


:::{include} ../types/match_operator.md
:::

:::{include} ../examples/match_operator.md
:::
