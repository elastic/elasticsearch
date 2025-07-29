### `RLIKE` [esql-rlike]

:::{image} ../../../images/operators/rlike.svg
:alt: Embedded
:class: text-center
:::

Use `RLIKE` to filter data based on string patterns using using [regular expressions](/reference/query-languages/query-dsl/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

:::{include} ../types/rlike.md
:::

:::{include} ../examples/rlike.md
:::

:::{include} ../detailedDescription/rlike.md
:::
