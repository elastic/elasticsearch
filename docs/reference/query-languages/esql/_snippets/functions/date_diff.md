## `DATE_DIFF` [esql-date_diff]

**Syntax**

:::{image} ../../../../../images/date_diff.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`unit`
:   Time difference unit

`startTimestamp`
:   A string representing a start timestamp

`endTimestamp`
:   A string representing an end timestamp

**Description**

Subtracts the `startTimestamp` from the `endTimestamp` and returns the difference in multiples of `unit`. If `startTimestamp` is later than the `endTimestamp`, negative values are returned.

| Datetime difference units |
| --- |
| **unit** | **abbreviations** |
| year | years, yy, yyyy |
| quarter | quarters, qq, q |
| month | months, mm, m |
| dayofyear | dy, y |
| day | days, dd, d |
| week | weeks, wk, ww |
| weekday | weekdays, dw |
| hour | hours, hh |
| minute | minutes, mi, n |
| second | seconds, ss, s |
| millisecond | milliseconds, ms |
| microsecond | microseconds, mcs |
| nanosecond | nanoseconds, ns |

Note that while there is an overlap between the function’s supported units and {{esql}}'s supported time span literals, these sets are distinct and not interchangeable. Similarly, the supported abbreviations are conveniently shared with implementations of this function in other established products and not necessarily common with the date-time nomenclature used by {{es}}.

**Supported types**

| unit | startTimestamp | endTimestamp | result |
| --- | --- | --- | --- |
| keyword | date | date | integer |
| keyword | date | date_nanos | integer |
| keyword | date_nanos | date | integer |
| keyword | date_nanos | date_nanos | integer |
| text | date | date | integer |
| text | date | date_nanos | integer |
| text | date_nanos | date | integer |
| text | date_nanos | date_nanos | integer |

**Examples**

```esql
ROW date1 = TO_DATETIME("2023-12-02T11:00:00.000Z"), date2 = TO_DATETIME("2023-12-02T11:00:00.001Z")
| EVAL dd_ms = DATE_DIFF("microseconds", date1, date2)
```

| date1:date | date2:date | dd_ms:integer |
| --- | --- | --- |
| 2023-12-02T11:00:00.000Z | 2023-12-02T11:00:00.001Z | 1000 |

When subtracting in calendar units - like year, month a.s.o. - only the fully elapsed units are counted. To avoid this and obtain also remainders, simply switch to the next smaller unit and do the date math accordingly.

```esql
ROW end_23=TO_DATETIME("2023-12-31T23:59:59.999Z"),
  start_24=TO_DATETIME("2024-01-01T00:00:00.000Z"),
    end_24=TO_DATETIME("2024-12-31T23:59:59.999")
| EVAL end23_to_start24=DATE_DIFF("year", end_23, start_24)
| EVAL end23_to_end24=DATE_DIFF("year", end_23, end_24)
| EVAL start_to_end_24=DATE_DIFF("year", start_24, end_24)
```

| end_23:date | start_24:date | end_24:date | end23_to_start24:integer | end23_to_end24:integer | start_to_end_24:integer |
| --- | --- | --- | --- | --- | --- |
| 2023-12-31T23:59:59.999Z | 2024-01-01T00:00:00.000Z | 2024-12-31T23:59:59.999Z | 0 | 1 | 0 |


