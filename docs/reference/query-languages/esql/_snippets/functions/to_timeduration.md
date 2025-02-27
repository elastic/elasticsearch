## `TO_TIMEDURATION` [esql-to_timeduration]

**Syntax**

:::{image} ../../../../../images/to_timeduration.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input is a valid constant time duration expression.

**Description**

Converts an input value into a `time_duration` value.

**Supported types**

| field | result |
| --- | --- |
| keyword | time_duration |
| text | time_duration |
| time_duration | time_duration |

**Example**

```esql
row x = "2024-01-01"::datetime | eval y = x + "3 hours"::time_duration, z = x - to_timeduration("3 hours");
```

| x:datetime | y:datetime | z:datetime |
| --- | --- | --- |
| 2024-01-01 | 2024-01-01T03:00:00.000Z | 2023-12-31T21:00:00.000Z |


