## `MV_PSERIES_WEIGHTED_SUM` [esql-mv_pseries_weighted_sum]

**Syntax**

:::{image} ../../../../../images/mv_pseries_weighted_sum.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Multivalue expression.

`p`
:   It is a constant number that represents the *p* parameter in the P-Series. It impacts every element’s contribution to the weighted sum.

**Description**

Converts a multivalued expression into a single-valued column by multiplying every element on the input list by its corresponding term in P-Series and computing the sum.

**Supported types**

| number | p | result |
| --- | --- | --- |
| double | double | double |

**Example**

```esql
ROW a = [70.0, 45.0, 21.0, 21.0, 21.0]
| EVAL sum = MV_PSERIES_WEIGHTED_SUM(a, 1.5)
| KEEP sum
```

| sum:double |
| --- |
| 94.45465156212452 |


