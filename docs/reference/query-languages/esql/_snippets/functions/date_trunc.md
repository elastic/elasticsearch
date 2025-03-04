## `DATE_TRUNC` [esql-date_trunc]

**Syntax**

:::{image} ../../../../../images/date_trunc.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`interval`
:   Interval; expressed using the timespan literal syntax.

`date`
:   Date expression

**Description**

Rounds down a date to the closest interval.

**Supported types**

| interval | date | result |
| --- | --- | --- |
| date_period | date | date |
| date_period | date_nanos | date_nanos |
| time_duration | date | date |
| time_duration | date_nanos | date_nanos |

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, hire_date
| EVAL year_hired = DATE_TRUNC(1 year, hire_date)
```

| first_name:keyword | last_name:keyword | hire_date:date | year_hired:date |
| --- | --- | --- | --- |
| Alejandro | McAlpine | 1991-06-26T00:00:00.000Z | 1991-01-01T00:00:00.000Z |
| Amabile | Gomatam | 1992-11-18T00:00:00.000Z | 1992-01-01T00:00:00.000Z |
| Anneke | Preusig | 1989-06-02T00:00:00.000Z | 1989-01-01T00:00:00.000Z |

Combine `DATE_TRUNC` with [`STATS`](/reference/query-languages/esql/esql-commands.md#esql-stats-by) to create date histograms. For example, the number of hires per year:

```esql
FROM employees
| EVAL year = DATE_TRUNC(1 year, hire_date)
| STATS hires = COUNT(emp_no) BY year
| SORT year
```

| hires:long | year:date |
| --- | --- |
| 11 | 1985-01-01T00:00:00.000Z |
| 11 | 1986-01-01T00:00:00.000Z |
| 15 | 1987-01-01T00:00:00.000Z |
| 9 | 1988-01-01T00:00:00.000Z |
| 13 | 1989-01-01T00:00:00.000Z |
| 12 | 1990-01-01T00:00:00.000Z |
| 6 | 1991-01-01T00:00:00.000Z |
| 8 | 1992-01-01T00:00:00.000Z |
| 3 | 1993-01-01T00:00:00.000Z |
| 4 | 1994-01-01T00:00:00.000Z |
| 5 | 1995-01-01T00:00:00.000Z |
| 1 | 1996-01-01T00:00:00.000Z |
| 1 | 1997-01-01T00:00:00.000Z |
| 1 | 1999-01-01T00:00:00.000Z |

Or an hourly error rate:

```esql
FROM sample_data
| EVAL error = CASE(message LIKE "*error*", 1, 0)
| EVAL hour = DATE_TRUNC(1 hour, @timestamp)
| STATS error_rate = AVG(error) by hour
| SORT hour
```

| error_rate:double | hour:date |
| --- | --- |
| 0.0 | 2023-10-23T12:00:00.000Z |
| 0.6 | 2023-10-23T13:00:00.000Z |


