## `TO_DATEPERIOD` [esql-to_dateperiod]

**Syntax**

:::{image} ../../../../../images/to_dateperiod.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input is a valid constant date period expression.

**Description**

Converts an input value into a `date_period` value.

**Supported types**

| field | result |
| --- | --- |
| date_period | date_period |
| keyword | date_period |
| text | date_period |

**Example**

```esql
row x = "2024-01-01"::datetime | eval y = x + "3 DAYS"::date_period, z = x - to_dateperiod("3 days");
```

| x:datetime | y:datetime | z:datetime |
| --- | --- | --- |
| 2024-01-01 | 2024-01-04 | 2023-12-29 |


