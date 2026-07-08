### `RLIKE` [esql-rlike]

:::{image} /reference/query-languages/esql/images/generated/x-pack-esql/operators/rlike.svg
:alt: Embedded
:class: text-center
:::

Use `RLIKE` to filter data based on string patterns using [regular expressions](/reference/query-languages/query-dsl/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

:::{include} ../../generated/x-pack-esql/operators/types/rlike.md
:::

:::{include} ../../generated/x-pack-esql/operators/examples/rlike.md
:::

:::{include} ../../generated/x-pack-esql/operators/detailedDescription/rlike.md
:::
