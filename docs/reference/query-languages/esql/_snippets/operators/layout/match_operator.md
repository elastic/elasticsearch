### Match operator (`:`) [esql-match-operator]
```{applies_to}
stack: preview 9.0, ga 9.1
```

The only search operator is match (`:`).

**Syntax**

:::{image} ../../../images/operators/match_operator.svg
:alt: Embedded
:class: text-center
:::

The match operator performs a [match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) on the specified field. Returns true if the provided query matches the row.

The match operator is equivalent to the [match function](../../../functions-operators/search-functions.md#esql-match).

For using the function syntax, or adding [match query parameters](/reference/query-languages/query-dsl/query-dsl-match-query.md#match-field-params), you can use the [match function](../../../functions-operators/search-functions.md#esql-match).


:::{include} ../types/match_operator.md
:::

:::{include} ../examples/match_operator.md
:::
