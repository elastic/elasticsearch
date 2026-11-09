### `IN` [esql-in-operator]
```{applies_to}
stack: ga
serverless: ga
```

:::{image} /reference/query-languages/esql/images/generated/x-pack-esql/operators/in.svg
:alt: Embedded
:class: text-center
:::

The `IN` operator allows testing whether a field or expression equals an element in a list of literals, fields or expressions.

You can also use a subquery with `IN` or `NOT IN` to compare a field or a tuple of fields against the results of another query, in [`WHERE`](/reference/query-languages/esql/commands/where.md), [`EVAL`](/reference/query-languages/esql/commands/eval.md), and the per-aggregate `WHERE` of [`STATS`](/reference/query-languages/esql/commands/stats-by.md) and [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md). To learn more, refer to [Use subqueries with IN and NOT IN](/reference/query-languages/esql/esql-in-subquery.md).

:::{include} ../../generated/x-pack-esql/operators/examples/in.md
:::
