---
navigation_title: "Time spans"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-time-spans.html
---

# {{esql}} time spans [esql-time-spans]

Time spans represent intervals between two datetime values. There are currently two supported types of time spans:

* `DATE_PERIOD` specifies intervals in years, quarters, months, weeks and days
* `TIME_DURATION` specifies intervals in hours, minutes, seconds and milliseconds

A time span requires two elements: an integer value and a temporal unit.

Time spans work with grouping functions such as [BUCKET](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-bucket),
scalar functions such as [DATE_TRUNC](/reference/query-languages/esql/functions-operators/date-time-functions.md#esql-date_trunc)
and arithmetic operators such as [`+`](/reference/query-languages/esql/functions-operators/operators.md#esql-add)
and [`-`](/reference/query-languages/esql/functions-operators/operators.md#esql-sub).
Convert strings to time spans using [TO_DATEPERIOD](/reference/query-languages/esql/functions-operators/type-conversion-functions.md#esql-to_dateperiod),
[TO_TIMEDURATION](/reference/query-languages/esql/functions-operators/type-conversion-functions.md#esql-to_timeduration),
or the [cast operators](/reference/query-languages/esql/functions-operators/operators.md#esql-cast-operator) `::DATE_PERIOD`, `::TIME_DURATION`.


## Examples of using time spans in {{esql}} [esql-time-spans-examples]

With `BUCKET`:

```esql
FROM employees
| WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
| STATS hires_per_week = COUNT(*) BY week = BUCKET(hire_date, 1 week)
| SORT week
```

| hires_per_week:long | week:date |
| --- | --- |
| 2 | 1985-02-18T00:00:00.000Z |
| 1 | 1985-05-13T00:00:00.000Z |
| 1 | 1985-07-08T00:00:00.000Z |
| 1 | 1985-09-16T00:00:00.000Z |
| 2 | 1985-10-14T00:00:00.000Z |
| 4 | 1985-11-18T00:00:00.000Z |

With `DATE_TRUNC`:

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

With `+` and/or `-`:

```esql
FROM sample_data
| WHERE @timestamp > NOW() - 1 hour
```

| @timestamp:date | client_ip:ip | event_duration:long | message:keyword |
| --- | --- | --- | --- |

When a time span is provided as a named parameter in string format, `TO_DATEPERIOD`, `::DATE_PERIOD`, `TO_TIMEDURATION` or `::TIME_DURATION` can be used to convert to its corresponding time span value for arithmetic operations like `+` and/or `-`.

```esql
POST /_query
{
   "query": """
   FROM employees
   | EVAL x = hire_date + ?timespan::DATE_PERIOD, y = hire_date - TO_DATEPERIOD(?timespan)
   """,
   "params": [{"timespan" : "1 day"}]
}
```

When a time span is provided as a named parameter in string format, it can be automatically converted to its corresponding time span value in grouping functions and scalar functions, like `BUCKET` and `DATE_TRUNC`.

```esql
POST /_query
{
   "query": """
   FROM employees
   | WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
   | STATS hires_per_week = COUNT(*) BY week = BUCKET(hire_date, ?timespan)
   | SORT week
   """,
   "params": [{"timespan" : "1 week"}]
}
```

```esql
POST /_query
{
   "query": """
   FROM employees
   | KEEP first_name, last_name, hire_date
   | EVAL year_hired = DATE_TRUNC(?timespan, hire_date)
   """,
   "params": [{"timespan" : "1 year"}]
}
```


## Supported temporal units [esql-time-spans-table]

| Temporal Units | Valid Abbreviations |
| --- | --- |
| year | y, yr, years |
| quarter | q, quarters |
| month | mo, months |
| week | w, weeks |
| day | d, days |
| hour | h, hours |
| minute | min, minutes |
| second | s, sec, seconds |
| millisecond | ms, milliseconds |

