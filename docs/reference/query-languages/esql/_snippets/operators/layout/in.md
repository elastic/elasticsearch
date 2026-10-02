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

You can also use a subquery with `IN` or `NOT IN` in a [`WHERE`](/reference/query-languages/esql/commands/where.md) command to filter rows against the results of another query. To learn more, refer to [Filter rows with IN subqueries](/reference/query-languages/esql/esql-in-subquery.md).

:::{include} ../../generated/x-pack-esql/operators/examples/in.md
:::
