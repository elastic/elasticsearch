## `WEIGHTED_AVG` [esql-weighted_avg]

**Syntax**

:::{image} ../../../../../images/weighted_avg.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   A numeric value.

`weight`
:   A numeric weight.

**Description**

The weighted average of a numeric expression.

**Supported types**

| number | weight | result |
| --- | --- | --- |
| double | double | double |
| double | integer | double |
| double | long | double |
| integer | double | double |
| integer | integer | double |
| integer | long | double |
| long | double | double |
| long | integer | double |
| long | long | double |

**Example**

```esql
FROM employees
| STATS w_avg = WEIGHTED_AVG(salary, height) by languages
| EVAL w_avg = ROUND(w_avg)
| KEEP w_avg, languages
| SORT languages
```

| w_avg:double | languages:integer |
| --- | --- |
| 51464.0 | 1 |
| 48477.0 | 2 |
| 52379.0 | 3 |
| 47990.0 | 4 |
| 42119.0 | 5 |
| 52142.0 | null |
