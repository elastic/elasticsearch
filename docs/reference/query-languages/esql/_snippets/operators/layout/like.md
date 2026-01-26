### `LIKE` [esql-like]

:::{image} ../../../images/operators/like.svg
:alt: Embedded
:class: text-center
:::

Use `LIKE` to filter data based on string patterns using wildcards. `LIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

The following wildcard characters are supported:

* `*` matches zero or more characters.
* `?` matches one character.


:::{include} ../types/like.md
:::

:::{include} ../examples/like.md
:::

:::{include} ../detailedDescription/like.md
:::
