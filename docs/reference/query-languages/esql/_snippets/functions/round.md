## `ROUND` [esql-round]

**Syntax**

:::{image} ../../../../../images/round.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   The numeric value to round. If `null`, the function returns `null`.

`decimals`
:   The number of decimal places to round to. Defaults to 0. If `null`, the function returns `null`.

**Description**

Rounds a number to the specified number of decimal places. Defaults to 0, which returns the nearest integer. If the precision is a negative number, rounds to the number of digits left of the decimal point.

**Supported types**

| number | decimals | result |
| --- | --- | --- |
| double | integer | double |
| double | long | double |
| double |  | double |
| integer | integer | integer |
| integer | long | integer |
| integer |  | integer |
| long | integer | long |
| long | long | long |
| long |  | long |
| unsigned_long | integer | unsigned_long |
| unsigned_long | long | unsigned_long |
| unsigned_long |  | unsigned_long |

**Example**

```esql
FROM employees
| KEEP first_name, last_name, height
| EVAL height_ft = ROUND(height * 3.281, 1)
```

| first_name:keyword | last_name:keyword | height:double | height_ft:double |
| --- | --- | --- | --- |
| Arumugam | Ossenbruggen | 2.1 | 6.9 |
| Kwee | Schusler | 2.1 | 6.9 |
| Saniya | Kalloufi | 2.1 | 6.9 |


