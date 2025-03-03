## {{esql}} grouping functions [esql-group-functions]


The [`STATS`](/reference/query-languages/esql/esql-commands.md#esql-stats-by) command supports these grouping functions:

:::{include} lists/grouping-functions.md
:::


## `BUCKET` [esql-bucket]

**Syntax**

:::{image} ../../../../images/bucket.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Numeric or date expression from which to derive buckets.

`buckets`
:   Target number of buckets, or desired bucket size if `from` and `to` parameters are omitted.

`from`
:   Start of the range. Can be a number, a date or a date expressed as a string.

`to`
:   End of the range. Can be a number, a date or a date expressed as a string.

**Description**

Creates groups of values - buckets - out of a datetime or numeric input. The size of the buckets can either be provided directly, or chosen based on a recommended count and values range.

**Supported types**

| field | buckets | from | to | result |
| --- | --- | --- | --- | --- |
| date | date_period |  |  | date |
| date | integer | date | date | date |
| date | integer | date | keyword | date |
| date | integer | date | text | date |
| date | integer | keyword | date | date |
| date | integer | keyword | keyword | date |
| date | integer | keyword | text | date |
| date | integer | text | date | date |
| date | integer | text | keyword | date |
| date | integer | text | text | date |
| date | time_duration |  |  | date |
| date_nanos | date_period |  |  | date_nanos |
| date_nanos | integer | date | date | date_nanos |
| date_nanos | integer | date | keyword | date_nanos |
| date_nanos | integer | date | text | date_nanos |
| date_nanos | integer | keyword | date | date_nanos |
| date_nanos | integer | keyword | keyword | date_nanos |
| date_nanos | integer | keyword | text | date_nanos |
| date_nanos | integer | text | date | date_nanos |
| date_nanos | integer | text | keyword | date_nanos |
| date_nanos | integer | text | text | date_nanos |
| date_nanos | time_duration |  |  | date_nanos |
| double | double |  |  | double |
| double | integer | double | double | double |
| double | integer | double | integer | double |
| double | integer | double | long | double |
| double | integer | integer | double | double |
| double | integer | integer | integer | double |
| double | integer | integer | long | double |
| double | integer | long | double | double |
| double | integer | long | integer | double |
| double | integer | long | long | double |
| double | integer |  |  | double |
| double | long |  |  | double |
| integer | double |  |  | double |
| integer | integer | double | double | double |
| integer | integer | double | integer | double |
| integer | integer | double | long | double |
| integer | integer | integer | double | double |
| integer | integer | integer | integer | double |
| integer | integer | integer | long | double |
| integer | integer | long | double | double |
| integer | integer | long | integer | double |
| integer | integer | long | long | double |
| integer | integer |  |  | double |
| integer | long |  |  | double |
| long | double |  |  | double |
| long | integer | double | double | double |
| long | integer | double | integer | double |
| long | integer | double | long | double |
| long | integer | integer | double | double |
| long | integer | integer | integer | double |
| long | integer | integer | long | double |
| long | integer | long | double | double |
| long | integer | long | integer | double |
| long | integer | long | long | double |
| long | integer |  |  | double |
| long | long |  |  | double |

**Examples**

`BUCKET` can work in two modes: one in which the size of the bucket is computed based on a buckets count recommendation (four parameters) and a range, and another in which the bucket size is provided directly (two parameters).

Using a target number of buckets, a start of a range, and an end of a range, `BUCKET` picks an appropriate bucket size to generate the target number of buckets or fewer. For example, asking for at most 20 buckets over a year results in monthly buckets:

```esql
FROM employees
| WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
| STATS hire_date = MV_SORT(VALUES(hire_date)) BY month = BUCKET(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
| SORT hire_date
```

| hire_date:date | month:date |
| --- | --- |
| [1985-02-18T00:00:00.000Z, 1985-02-24T00:00:00.000Z] | 1985-02-01T00:00:00.000Z |
| 1985-05-13T00:00:00.000Z | 1985-05-01T00:00:00.000Z |
| 1985-07-09T00:00:00.000Z | 1985-07-01T00:00:00.000Z |
| 1985-09-17T00:00:00.000Z | 1985-09-01T00:00:00.000Z |
| [1985-10-14T00:00:00.000Z, 1985-10-20T00:00:00.000Z] | 1985-10-01T00:00:00.000Z |
| [1985-11-19T00:00:00.000Z, 1985-11-20T00:00:00.000Z, 1985-11-21T00:00:00.000Z] | 1985-11-01T00:00:00.000Z |

The goal isn’t to provide **exactly** the target number of buckets, it’s to pick a range that people are comfortable with that provides at most the target number of buckets.

Combine `BUCKET` with an [aggregation](../esql-functions-operators.md#esql-agg-functions) to create a histogram:

```esql
FROM employees
| WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
| STATS hires_per_month = COUNT(*) BY month = BUCKET(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
| SORT month
```

| hires_per_month:long | month:date |
| --- | --- |
| 2 | 1985-02-01T00:00:00.000Z |
| 1 | 1985-05-01T00:00:00.000Z |
| 1 | 1985-07-01T00:00:00.000Z |
| 1 | 1985-09-01T00:00:00.000Z |
| 2 | 1985-10-01T00:00:00.000Z |
| 4 | 1985-11-01T00:00:00.000Z |

::::{note}
`BUCKET` does not create buckets that don’t match any documents. That’s why this example is missing `1985-03-01` and other dates.
::::


Asking for more buckets can result in a smaller range. For example, asking for at most 100 buckets in a year results in weekly buckets:

```esql
FROM employees
| WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
| STATS hires_per_week = COUNT(*) BY week = BUCKET(hire_date, 100, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
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

::::{note}
`BUCKET` does not filter any rows. It only uses the provided range to pick a good bucket size. For rows with a value outside of the range, it returns a bucket value that corresponds to a bucket outside the range. Combine`BUCKET` with [`WHERE`](/reference/query-languages/esql/esql-commands.md#esql-where) to filter rows.
::::


If the desired bucket size is known in advance, simply provide it as the second argument, leaving the range out:

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

::::{note}
When providing the bucket size as the second parameter, it must be a time duration or date period.
::::


`BUCKET` can also operate on numeric fields. For example, to create a salary histogram:

```esql
FROM employees
| STATS COUNT(*) by bs = BUCKET(salary, 20, 25324, 74999)
| SORT bs
```

| COUNT(*):long | bs:double |
| --- | --- |
| 9 | 25000.0 |
| 9 | 30000.0 |
| 18 | 35000.0 |
| 11 | 40000.0 |
| 11 | 45000.0 |
| 10 | 50000.0 |
| 7 | 55000.0 |
| 9 | 60000.0 |
| 8 | 65000.0 |
| 8 | 70000.0 |

Unlike the earlier example that intentionally filters on a date range, you rarely want to filter on a numeric range. You have to find the `min` and `max` separately. {{esql}} doesn’t yet have an easy way to do that automatically.

The range can be omitted if the desired bucket size is known in advance. Simply provide it as the second argument:

```esql
FROM employees
| WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
| STATS c = COUNT(1) BY b = BUCKET(salary, 5000.)
| SORT b
```

| c:long | b:double |
| --- | --- |
| 1 | 25000.0 |
| 1 | 30000.0 |
| 1 | 40000.0 |
| 2 | 45000.0 |
| 2 | 50000.0 |
| 1 | 55000.0 |
| 1 | 60000.0 |
| 1 | 65000.0 |
| 1 | 70000.0 |

Create hourly buckets for the last 24 hours, and calculate the number of events per hour:

```esql
FROM sample_data
| WHERE @timestamp >= NOW() - 1 day and @timestamp < NOW()
| STATS COUNT(*) BY bucket = BUCKET(@timestamp, 25, NOW() - 1 day, NOW())
```

| COUNT(*):long | bucket:date |
| --- | --- |

Create monthly buckets for the year 1985, and calculate the average salary by hiring month

```esql
FROM employees
| WHERE hire_date >= "1985-01-01T00:00:00Z" AND hire_date < "1986-01-01T00:00:00Z"
| STATS AVG(salary) BY bucket = BUCKET(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
| SORT bucket
```

| AVG(salary):double | bucket:date |
| --- | --- |
| 46305.0 | 1985-02-01T00:00:00.000Z |
| 44817.0 | 1985-05-01T00:00:00.000Z |
| 62405.0 | 1985-07-01T00:00:00.000Z |
| 49095.0 | 1985-09-01T00:00:00.000Z |
| 51532.0 | 1985-10-01T00:00:00.000Z |
| 54539.75 | 1985-11-01T00:00:00.000Z |

`BUCKET` may be used in both the aggregating and grouping part of the [STATS …​ BY …​](/reference/query-languages/esql/esql-commands.md#esql-stats-by) command provided that in the aggregating part the function is referenced by an alias defined in the grouping part, or that it is invoked with the exact same expression:

```esql
FROM employees
| STATS s1 = b1 + 1, s2 = BUCKET(salary / 1000 + 999, 50.) + 2 BY b1 = BUCKET(salary / 100 + 99, 50.), b2 = BUCKET(salary / 1000 + 999, 50.)
| SORT b1, b2
| KEEP s1, b1, s2, b2
```

| s1:double | b1:double | s2:double | b2:double |
| --- | --- | --- | --- |
| 351.0 | 350.0 | 1002.0 | 1000.0 |
| 401.0 | 400.0 | 1002.0 | 1000.0 |
| 451.0 | 450.0 | 1002.0 | 1000.0 |
| 501.0 | 500.0 | 1002.0 | 1000.0 |
| 551.0 | 550.0 | 1002.0 | 1000.0 |
| 601.0 | 600.0 | 1002.0 | 1000.0 |
| 601.0 | 600.0 | 1052.0 | 1050.0 |
| 651.0 | 650.0 | 1052.0 | 1050.0 |
| 701.0 | 700.0 | 1052.0 | 1050.0 |
| 751.0 | 750.0 | 1052.0 | 1050.0 |
| 801.0 | 800.0 | 1052.0 | 1050.0 |

Sometimes you need to change the start value of each bucket by a given duration (similar to date histogram aggregation’s [`offset`](/reference/data-analysis/aggregations/search-aggregations-bucket-histogram-aggregation.md) parameter). To do so, you will need to take into account how the language handles expressions within the `STATS` command: if these contain functions or arithmetic operators, a virtual `EVAL` is inserted before and/or after the `STATS` command. Consequently, a double compensation is needed to adjust the bucketed date value before the aggregation and then again after. For instance, inserting a negative offset of `1 hour` to buckets of `1 year` looks like this:

```esql
FROM employees
| STATS dates = MV_SORT(VALUES(birth_date)) BY b = BUCKET(birth_date + 1 HOUR, 1 YEAR) - 1 HOUR
| EVAL d_count = MV_COUNT(dates)
| SORT d_count, b
| LIMIT 3
```

| dates:date | b:date | d_count:integer |
| --- | --- | --- |
| 1965-01-03T00:00:00.000Z | 1964-12-31T23:00:00.000Z | 1 |
| [1955-01-21T00:00:00.000Z, 1955-08-20T00:00:00.000Z, 1955-08-28T00:00:00.000Z, 1955-10-04T00:00:00.000Z] | 1954-12-31T23:00:00.000Z | 4 |
| [1957-04-04T00:00:00.000Z, 1957-05-23T00:00:00.000Z, 1957-05-25T00:00:00.000Z, 1957-12-03T00:00:00.000Z] | 1956-12-31T23:00:00.000Z | 4 |


## `CATEGORIZE` [esql-categorize]

::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


**Syntax**

:::{image} ../../../../images/categorize.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Expression to categorize

**Description**

Groups text messages into categories of similarly formatted text values.

`CATEGORIZE` has the following limitations:

* can’t be used within other expressions
* can’t be used with multiple groupings
* can’t be used or referenced within aggregate functions

**Supported types**

| field | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

This example categorizes server logs messages into categories and aggregates their counts.

```esql
FROM sample_data
| STATS count=COUNT() BY category=CATEGORIZE(message)
```

| count:long | category:keyword |
| --- | --- |
| 3 | .**?Connected.+?to.**? |
| 3 | .**?Connection.+?error.**? |
| 1 | .**?Disconnected.**? |
