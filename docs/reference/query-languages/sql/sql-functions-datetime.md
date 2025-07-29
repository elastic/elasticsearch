---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-datetime.html
---

# Date/time and interval functions and operators [sql-functions-datetime]

Elasticsearch SQL offers a wide range of facilities for performing date/time manipulations.

## Intervals [sql-functions-datetime-interval]

A common requirement when dealing with date/time in general revolves around the notion of `interval`, a topic that is worth exploring in the context of {{es}} and Elasticsearch SQL.

{{es}} has comprehensive support for [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) both inside [index names](/reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) and [queries](/reference/elasticsearch/mapping-reference/mapping-date-format.md). Inside Elasticsearch SQL the former is supported as is by passing the expression in the table name, while the latter is supported through the standard SQL `INTERVAL`.

The table below shows the mapping between {{es}} and Elasticsearch SQL:

| **{{es}}** | **Elasticsearch SQL** |
| --- | --- |
| Index/Table datetime math |
| `<index-{now/M{YYYY.MM}}>` |
| Query date/time math |
| `1y` | `INTERVAL 1 YEAR` |
| `2M` | `INTERVAL 2 MONTH` |
| `3w` | `INTERVAL 21 DAY` |
| `4d` | `INTERVAL 4 DAY` |
| `5h` | `INTERVAL 5 HOUR` |
| `6m` | `INTERVAL 6 MINUTE` |
| `7s` | `INTERVAL 7 SECOND` |

`INTERVAL` allows either `YEAR` and `MONTH` to be mixed together *or* `DAY`, `HOUR`, `MINUTE` and `SECOND`.

::::{tip}
Elasticsearch SQL accepts also the plural for each time unit (e.g. both `YEAR` and `YEARS` are valid).
::::


Example of the possible combinations below:

| **Interval** | **Description** |
| --- | --- |
| `INTERVAL '1-2' YEAR TO MONTH` | 1 year and 2 months |
| `INTERVAL '3 4' DAYS TO HOURS` | 3 days and 4 hours |
| `INTERVAL '5 6:12' DAYS TO MINUTES` | 5 days, 6 hours and 12 minutes |
| `INTERVAL '3 4:56:01' DAY TO SECOND` | 3 days, 4 hours, 56 minutes and 1 second |
| `INTERVAL '2 3:45:01.23456789' DAY TO SECOND` | 2 days, 3 hours, 45 minutes, 1 second and 234567890 nanoseconds |
| `INTERVAL '123:45' HOUR TO MINUTES` | 123 hours and 45 minutes |
| `INTERVAL '65:43:21.0123' HOUR TO SECONDS` | 65 hours, 43 minutes, 21 seconds and 12300000 nanoseconds |
| `INTERVAL '45:01.23' MINUTES TO SECONDS` | 45 minutes, 1 second and 230000000 nanoseconds |


## Comparison [_comparison]

Date/time fields can be compared to [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) expressions with the equality (`=`) and `IN` operators:

```sql
SELECT hire_date FROM emp WHERE hire_date = '1987-03-01||+4y/y';

       hire_date
------------------------
1991-01-26T00:00:00.000Z
1991-10-22T00:00:00.000Z
1991-09-01T00:00:00.000Z
1991-06-26T00:00:00.000Z
1991-08-30T00:00:00.000Z
1991-12-01T00:00:00.000Z
```

```sql
SELECT hire_date FROM emp WHERE hire_date IN ('1987-03-01||+2y/M', '1987-03-01||+3y/M');

       hire_date
------------------------
1989-03-31T00:00:00.000Z
1990-03-02T00:00:00.000Z
```


## Operators [_operators]

Basic arithmetic operators (`+`, `-`, `*`) support date/time parameters as indicated below:

```sql
SELECT INTERVAL 1 DAY + INTERVAL 53 MINUTES AS result;

    result
---------------
+1 00:53:00
```

```sql
SELECT CAST('1969-05-13T12:34:56' AS DATETIME) + INTERVAL 49 YEARS AS result;

       result
--------------------
2018-05-13T12:34:56Z
```

```sql
SELECT - INTERVAL '49-1' YEAR TO MONTH result;

    result
---------------
-49-1
```

```sql
SELECT INTERVAL '1' DAY - INTERVAL '2' HOURS AS result;

    result
---------------
+0 22:00:00
```

```sql
SELECT CAST('2018-05-13T12:34:56' AS DATETIME) - INTERVAL '2-8' YEAR TO MONTH AS result;

       result
--------------------
2015-09-13T12:34:56Z
```

```sql
SELECT -2 * INTERVAL '3' YEARS AS result;

    result
---------------
-6-0
```


## Functions [_functions]

Functions that target date/time.


## `CURRENT_DATE/CURDATE` [sql-functions-current-date]

```sql
CURRENT_DATE
CURRENT_DATE()
CURDATE()
```

**Input**: *none*

**Output**: date

**Description**: Returns the date (no time part) when the current query reached the server. It can be used both as a keyword: `CURRENT_DATE` or as a function with no arguments: `CURRENT_DATE()`.

::::{note}
Unlike CURRENT_DATE, `CURDATE()` can only be used as a function with no arguments and not as a keyword.
::::


This method always returns the same value for its every occurrence within the same query.

```sql
SELECT CURRENT_DATE AS result;

         result
------------------------
2018-12-12
```

```sql
SELECT CURRENT_DATE() AS result;

         result
------------------------
2018-12-12
```

```sql
SELECT CURDATE() AS result;

         result
------------------------
2018-12-12
```

Typically, this function (as well as its twin [TODAY())](#sql-functions-today) function is used for relative date filtering:

```sql
SELECT first_name FROM emp WHERE hire_date > TODAY() - INTERVAL 35 YEARS ORDER BY first_name ASC LIMIT 5;

 first_name
------------
Alejandro
Amabile
Anoosh
Basil
Brendon
```


## `CURRENT_TIME/CURTIME` [sql-functions-current-time]

```sql
CURRENT_TIME
CURRENT_TIME([precision]) <1>
CURTIME
```

**Input**:

1. fractional digits; optional


**Output**: time

**Description**: Returns the time when the current query reached the server. As a function, `CURRENT_TIME()` accepts *precision* as an optional parameter for rounding the second fractional digits (nanoseconds). The default *precision* is 3, meaning a milliseconds precision current time will be returned.

This method always returns the same value for its every occurrence within the same query.

```sql
SELECT CURRENT_TIME AS result;

         result
------------------------
12:31:27.237Z
```

```sql
SELECT CURRENT_TIME() AS result;

         result
------------------------
12:31:27.237Z
```

```sql
SELECT CURTIME() AS result;

         result
------------------------
12:31:27.237Z
```

```sql
SELECT CURRENT_TIME(1) AS result;

         result
------------------------
12:31:27.2Z
```

Typically, this function is used for relative date/time filtering:

```sql
SELECT first_name FROM emp WHERE CAST(hire_date AS TIME) > CURRENT_TIME() - INTERVAL 20 MINUTES ORDER BY first_name ASC LIMIT 5;

  first_name
---------------
Alejandro
Amabile
Anneke
Anoosh
Arumugam
```

::::{important}
Currently, using a *precision* greater than 6 doesn’t make any difference to the output of the function as the maximum number of second fractional digits returned is 6.
::::



## `CURRENT_TIMESTAMP` [sql-functions-current-timestamp]

```sql
CURRENT_TIMESTAMP
CURRENT_TIMESTAMP([precision]) <1>
```

**Input**:

1. fractional digits; optional


**Output**: date/time

**Description**: Returns the date/time when the current query reached the server. As a function, `CURRENT_TIMESTAMP()` accepts *precision* as an optional parameter for rounding the second fractional digits (nanoseconds). The default *precision* is 3, meaning a milliseconds precision current date/time will be returned.

This method always returns the same value for its every occurrence within the same query.

```sql
SELECT CURRENT_TIMESTAMP AS result;

         result
------------------------
2018-12-12T14:48:52.448Z
```

```sql
SELECT CURRENT_TIMESTAMP() AS result;

         result
------------------------
2018-12-12T14:48:52.448Z
```

```sql
SELECT CURRENT_TIMESTAMP(1) AS result;

         result
------------------------
2018-12-12T14:48:52.4Z
```

Typically, this function (as well as its twin [NOW())](#sql-functions-now) function is used for relative date/time filtering:

```sql
SELECT first_name FROM emp WHERE hire_date > NOW() - INTERVAL 100 YEARS ORDER BY first_name ASC LIMIT 5;

  first_name
---------------
Alejandro
Amabile
Anneke
Anoosh
Arumugam
```

::::{important}
Currently, using a *precision* greater than 6 doesn’t make any difference to the output of the function as the maximum number of second fractional digits returned is 6.
::::



## `DATE_ADD/DATEADD/TIMESTAMP_ADD/TIMESTAMPADD` [sql-functions-datetime-add]

```sql
DATE_ADD(
    string_exp, <1>
    integer_exp, <2>
    datetime_exp) <3>
```

**Input**:

1. string expression denoting the date/time unit to add to the date/datetime. If `null`, the function returns `null`.
2. integer expression denoting how many times the above unit should be added to/from the date/datetime, if a negative value is used it results to a subtraction from the date/datetime. If `null`, the function returns `null`.
3. date/datetime expression. If `null`, the function returns `null`.


**Output**: datetime

**Description**: Add the given number of date/time units to a date/datetime. If the number of units is negative then it’s subtracted from the date/datetime.

::::{warning}
If the second argument is a long there is possibility of truncation since an integer value will be extracted and used from that long.
::::


| Datetime units to add/subtract |
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

```sql
SELECT DATE_ADD('years', 10, '2019-09-04T11:22:33.000Z'::datetime) AS "+10 years";

      +10 years
------------------------
2029-09-04T11:22:33.000Z
```

```sql
SELECT DATE_ADD('week', 10, '2019-09-04T11:22:33.000Z'::datetime) AS "+10 weeks";

      +10 weeks
------------------------
2019-11-13T11:22:33.000Z
```

```sql
SELECT DATE_ADD('seconds', -1234, '2019-09-04T11:22:33.000Z'::datetime) AS "-1234 seconds";

      -1234 seconds
------------------------
2019-09-04T11:01:59.000Z
```

```sql
SELECT DATE_ADD('qq', -417, '2019-09-04'::date) AS "-417 quarters";

      -417 quarters
------------------------
1915-06-04T00:00:00.000Z
```

```sql
SELECT DATE_ADD('minutes', 9235, '2019-09-04'::date) AS "+9235 minutes";

      +9235 minutes
------------------------
2019-09-10T09:55:00.000Z
```


## `DATE_DIFF/DATEDIFF/TIMESTAMP_DIFF/TIMESTAMPDIFF` [sql-functions-datetime-diff]

```sql
DATE_DIFF(
    string_exp, <1>
    datetime_exp, <2>
    datetime_exp) <3>
```

**Input**:

1. string expression denoting the date/time unit difference between the following two date/datetime expressions. If `null`, the function returns `null`.
2. start date/datetime expression. If `null`, the function returns `null`.
3. end date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Subtract the second argument from the third argument and return their difference in multiples of the unit specified in the first argument. If the second argument (start) is greater than the third argument (end), then negative values are returned.

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

```sql
SELECT DATE_DIFF('years', '2019-09-04T11:22:33.000Z'::datetime, '2032-09-04T22:33:11.000Z'::datetime) AS "diffInYears";

      diffInYears
------------------------
13
```

```sql
SELECT DATE_DIFF('week', '2019-09-04T11:22:33.000Z'::datetime, '2016-12-08T22:33:11.000Z'::datetime) AS "diffInWeeks";

      diffInWeeks
------------------------
-143
```

```sql
SELECT DATE_DIFF('seconds', '2019-09-04T11:22:33.123Z'::datetime, '2019-07-12T22:33:11.321Z'::datetime) AS "diffInSeconds";

      diffInSeconds
------------------------
-4625362
```

```sql
SELECT DATE_DIFF('qq', '2019-09-04'::date, '2025-04-25'::date) AS "diffInQuarters";

      diffInQuarters
------------------------
23
```

::::{note}
For `hour` and `minute`, `DATEDIFF` doesn’t do any rounding, but instead first truncates the more detailed time fields on the 2 dates to zero and then calculates the subtraction.
::::


```sql
SELECT DATEDIFF('hours', '2019-11-10T12:10:00.000Z'::datetime, '2019-11-10T23:59:59.999Z'::datetime) AS "diffInHours";

      diffInHours
------------------------
11
```

```sql
SELECT DATEDIFF('minute', '2019-11-10T12:10:00.000Z'::datetime, '2019-11-10T12:15:59.999Z'::datetime) AS "diffInMinutes";

      diffInMinutes
------------------------
5
```

```sql
SELECT DATE_DIFF('minutes', '2019-09-04'::date, '2015-08-17T22:33:11.567Z'::datetime) AS "diffInMinutes";

      diffInMinutes
------------------------
-2128407
```


## `DATE_FORMAT` [sql-functions-datetime-dateformat]

```sql
DATE_FORMAT(
    date_exp/datetime_exp/time_exp, <1>
    string_exp) <2>
```

**Input**:

1. date/datetime/time expression. If `null`, the function returns `null`.
2. format pattern. If `null` or an empty string, the function returns `null`.


**Output**: string

**Description**: Returns the date/datetime/time as a string using the format specified in the 2nd argument. The formatting pattern is one of the specifiers used in the [MySQL DATE_FORMAT() function](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format).

::::{note}
If the 1st argument is of type `time`, then pattern specified by the 2nd argument cannot contain date related units (e.g. *dd*, *MM*, *yyyy*, etc.). If it contains such units an error is returned. Ranges for month and day specifiers (%c, %D, %d, %e, %m) start at one, unlike MySQL, where they start at zero, due to the fact that MySQL permits the storing of incomplete dates such as *2014-00-00*. Elasticsearch in this case returns an error.
::::


```sql
SELECT DATE_FORMAT(CAST('2020-04-05' AS DATE), '%d/%m/%Y') AS "date";

      date
------------------
05/04/2020
```

```sql
SELECT DATE_FORMAT(CAST('2020-04-05T11:22:33.987654' AS DATETIME), '%d/%m/%Y %H:%i:%s.%f') AS "datetime";

      datetime
------------------
05/04/2020 11:22:33.987654
```

```sql
SELECT DATE_FORMAT(CAST('23:22:33.987' AS TIME), '%H %i %s.%f') AS "time";

      time
------------------
23 22 33.987000
```


## `DATE_PARSE` [sql-functions-datetime-dateparse]

```sql
DATE_PARSE(
    string_exp, <1>
    string_exp) <2>
```

**Input**:

1. date expression as a string. If `null` or an empty string, the function returns `null`.
2. parsing pattern. If `null` or an empty string, the function returns `null`.


**Output**: date

**Description**: Returns a date by parsing the 1st argument using the format specified in the 2nd argument. The parsing format pattern used is the one from [`java.time.format.DateTimeFormatter`](https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/time/format/DateTimeFormatter.html).

::::{note}
If the parsing pattern does not contain all valid date units (e.g. *HH:mm:ss*, *dd-MM HH:mm:ss*, etc.) an error is returned as the function needs to return a value of `date` type which will contain date part.
::::


```sql
SELECT DATE_PARSE('07/04/2020', 'dd/MM/yyyy') AS "date";

   date
-----------
2020-04-07
```

::::{note}
The resulting `date` will have the time zone specified by the user through the [`time_zone`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query)/[`timezone`](docs-content://explore-analyze/query-filter/languages/sql-jdbc.md#jdbc-cfg-timezone) REST/driver parameters with no conversion applied.

```sql
{
    "query" : "SELECT DATE_PARSE('07/04/2020', 'dd/MM/yyyy') AS \"date\"",
    "time_zone" : "Europe/Athens"
}

   date
------------
2020-04-07T00:00:00.000+03:00
```

::::



## `DATETIME_FORMAT` [sql-functions-datetime-datetimeformat]

```sql
DATETIME_FORMAT(
    date_exp/datetime_exp/time_exp, <1>
    string_exp) <2>
```

**Input**:

1. date/datetime/time expression. If `null`, the function returns `null`.
2. format pattern. If `null` or an empty string, the function returns `null`.


**Output**: string

**Description**: Returns the date/datetime/time as a string using the format specified in the 2nd argument. The formatting pattern used is the one from [`java.time.format.DateTimeFormatter`](https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/time/format/DateTimeFormatter.html).

::::{note}
If the 1st argument is of type `time`, then pattern specified by the 2nd argument cannot contain date related units (e.g. *dd*, *MM*, *yyyy*, etc.). If it contains such units an error is returned.
::::


```sql
SELECT DATETIME_FORMAT(CAST('2020-04-05' AS DATE), 'dd/MM/yyyy') AS "date";

      date
------------------
05/04/2020
```

```sql
SELECT DATETIME_FORMAT(CAST('2020-04-05T11:22:33.987654' AS DATETIME), 'dd/MM/yyyy HH:mm:ss.SS') AS "datetime";

      datetime
------------------
05/04/2020 11:22:33.98
```

```sql
SELECT DATETIME_FORMAT(CAST('11:22:33.987' AS TIME), 'HH mm ss.S') AS "time";

      time
------------------
11 22 33.9
```


## `DATETIME_PARSE` [sql-functions-datetime-datetimeparse]

```sql
DATETIME_PARSE(
    string_exp, <1>
    string_exp) <2>
```

**Input**:

1. datetime expression as a string. If `null` or an empty string, the function returns `null`.
2. parsing pattern. If `null` or an empty string, the function returns `null`.


**Output**: datetime

**Description**: Returns a datetime by parsing the 1st argument using the format specified in the 2nd argument. The parsing format pattern used is the one from [`java.time.format.DateTimeFormatter`](https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/time/format/DateTimeFormatter.html).

::::{note}
If the parsing pattern contains only date or only time units (e.g. *dd/MM/yyyy*, *HH:mm:ss*, etc.) an error is returned as the function needs to return a value of `datetime` type which must contain both.
::::


```sql
SELECT DATETIME_PARSE('07/04/2020 10:20:30.123', 'dd/MM/yyyy HH:mm:ss.SSS') AS "datetime";

      datetime
------------------------
2020-04-07T10:20:30.123Z
```

```sql
SELECT DATETIME_PARSE('10:20:30 07/04/2020 Europe/Berlin', 'HH:mm:ss dd/MM/yyyy VV') AS "datetime";

      datetime
------------------------
2020-04-07T08:20:30.000Z
```

::::{note}
If timezone is not specified in the datetime string expression and the parsing pattern, the resulting `datetime` will have the time zone specified by the user through the [`time_zone`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query)/[`timezone`](docs-content://explore-analyze/query-filter/languages/sql-jdbc.md#jdbc-cfg-timezone) REST/driver parameters with no conversion applied.

```sql
{
    "query" : "SELECT DATETIME_PARSE('10:20:30 07/04/2020', 'HH:mm:ss dd/MM/yyyy') AS \"datetime\"",
    "time_zone" : "Europe/Athens"
}

      datetime
-----------------------------
2020-04-07T10:20:30.000+03:00
```

::::



## `TIME_PARSE` [sql-functions-datetime-timeparse]

```sql
TIME_PARSE(
    string_exp, <1>
    string_exp) <2>
```

**Input**:

1. time expression as a string. If `null` or an empty string, the function returns `null`.
2. parsing pattern. If `null` or an empty string, the function returns `null`.


**Output**: time

**Description**: Returns a time by parsing the 1st argument using the format specified in the 2nd argument. The parsing format pattern used is the one from [`java.time.format.DateTimeFormatter`](https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/time/format/DateTimeFormatter.html).

::::{note}
If the parsing pattern contains only date units (e.g. *dd/MM/yyyy*) an error is returned as the function needs to return a value of `time` type which will contain only time.
::::


```sql
SELECT TIME_PARSE('10:20:30.123', 'HH:mm:ss.SSS') AS "time";

     time
---------------
10:20:30.123Z
```

```sql
SELECT TIME_PARSE('10:20:30-01:00', 'HH:mm:ssXXX') AS "time";

     time
---------------
11:20:30.000Z
```

::::{note}
If timezone is not specified in the time string expression and the parsing pattern, the resulting `time` will have the offset of the time zone specified by the user through the [`time_zone`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query)/[`timezone`](docs-content://explore-analyze/query-filter/languages/sql-jdbc.md#jdbc-cfg-timezone) REST/driver parameters at the Unix epoch date (`1970-01-01`) with no conversion applied.

```sql
{
    "query" : "SELECT DATETIME_PARSE('10:20:30', 'HH:mm:ss') AS \"time\"",
    "time_zone" : "Europe/Athens"
}

      time
------------------------------------
10:20:30.000+02:00
```

::::



## `DATE_PART/DATEPART` [sql-functions-datetime-part]

```sql
DATE_PART(
    string_exp, <1>
    datetime_exp) <2>
```

**Input**:

1. string expression denoting the unit to extract from the date/datetime. If `null`, the function returns `null`.
2. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the specified unit from a date/datetime. It’s similar to [`EXTRACT`](#sql-functions-datetime-extract) but with different names and aliases for the units and provides more options (e.g.: `TZOFFSET`).

| Datetime units to extract |
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
| tzoffset | tz |

```sql
SELECT DATE_PART('year', '2019-09-22T11:22:33.123Z'::datetime) AS "years";

   years
----------
2019
```

```sql
SELECT DATE_PART('mi', '2019-09-04T11:22:33.123Z'::datetime) AS mins;

   mins
-----------
22
```

```sql
SELECT DATE_PART('quarters', CAST('2019-09-24' AS DATE)) AS quarter;

   quarter
-------------
3
```

```sql
SELECT DATE_PART('month', CAST('2019-09-24' AS DATE)) AS month;

   month
-------------
9
```

::::{note}
For `week` and `weekday` the unit is extracted using the non-ISO calculation, which means that a given week is considered to start from Sunday, not Monday.
::::


```sql
SELECT DATE_PART('week', '2019-09-22T11:22:33.123Z'::datetime) AS week;

   week
----------
39
```

::::{note}
The `tzoffset` returns the total number of minutes (signed) that represent the time zone’s offset.
::::


```sql
SELECT DATE_PART('tzoffset', '2019-09-04T11:22:33.123+05:15'::datetime) AS tz_mins;

   tz_mins
--------------
315
```

```sql
SELECT DATE_PART('tzoffset', '2019-09-04T11:22:33.123-03:49'::datetime) AS tz_mins;

   tz_mins
--------------
-229
```


## `DATE_TRUNC/DATETRUNC` [sql-functions-datetime-trunc]

```sql
DATE_TRUNC(
    string_exp, <1>
    datetime_exp/interval_exp) <2>
```

**Input**:

1. string expression denoting the unit to which the date/datetime/interval should be truncated to. If `null`, the function returns `null`.
2. date/datetime/interval expression. If `null`, the function returns `null`.


**Output**: datetime/interval

**Description**: Truncate the date/datetime/interval to the specified unit by setting all fields that are less significant than the specified one to zero (or one, for day, day of week and month). If the first argument is `week` and the second argument is of `interval` type, an error is thrown since the `interval` data type doesn’t support a `week` time unit.

| Datetime truncation units |
| --- |
| **unit** | **abbreviations** |
| millennium | millennia |
| century | centuries |
| decade | decades |
| year | years, yy, yyyy |
| quarter | quarters, qq, q |
| month | months, mm, m |
| week | weeks, wk, ww |
| day | days, dd, d |
| hour | hours, hh |
| minute | minutes, mi, n |
| second | seconds, ss, s |
| millisecond | milliseconds, ms |
| microsecond | microseconds, mcs |
| nanosecond | nanoseconds, ns |

```sql
SELECT DATE_TRUNC('millennium', '2019-09-04T11:22:33.123Z'::datetime) AS millennium;

      millennium
------------------------
2000-01-01T00:00:00.000Z
```

```sql
SELECT DATETRUNC('week', '2019-08-24T11:22:33.123Z'::datetime) AS week;

      week
------------------------
2019-08-19T00:00:00.000Z
```

```sql
SELECT DATE_TRUNC('mi', '2019-09-04T11:22:33.123Z'::datetime) AS mins;

      mins
------------------------
2019-09-04T11:22:00.000Z
```

```sql
SELECT DATE_TRUNC('decade', CAST('2019-09-04' AS DATE)) AS decades;

      decades
------------------------
2010-01-01T00:00:00.000Z
```

```sql
SELECT DATETRUNC('quarters', CAST('2019-09-04' AS DATE)) AS quarter;

      quarter
------------------------
2019-07-01T00:00:00.000Z
```

```sql
SELECT DATE_TRUNC('centuries', INTERVAL '199-5' YEAR TO MONTH) AS centuries;

      centuries
------------------
 +100-0
```

```sql
SELECT DATE_TRUNC('hours', INTERVAL '17 22:13:12' DAY TO SECONDS) AS hour;

      hour
------------------
+17 22:00:00
```

```sql
SELECT DATE_TRUNC('days', INTERVAL '19 15:24:19' DAY TO SECONDS) AS day;

      day
------------------
+19 00:00:00
```


## `FORMAT` [sql-functions-datetime-format]

```sql
FORMAT(
    date_exp/datetime_exp/time_exp, <1>
    string_exp) <2>
```

**Input**:

1. date/datetime/time expression. If `null`, the function returns `null`.
2. format pattern. If `null` or an empty string, the function returns `null`.


**Output**: string

**Description**: Returns the date/datetime/time as a string using the [format](https://docs.microsoft.com/en-us/sql/t-sql/functions/format-transact-sql#arguments) specified in the 2nd argument. The formatting pattern used is the one from [Microsoft SQL Server Format Specification](https://docs.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings).

::::{note}
If the 1st argument is of type `time`, then pattern specified by the 2nd argument cannot contain date related units (e.g. *dd*, *MM*, *yyyy*, etc.). If it contains such units an error is returned.<br> Format specifier `F` will be working similar to format specifier `f`. It will return the fractional part of seconds, and the number of digits will be same as of the number of `Fs` provided as input (up to 9 digits). Result will contain `0` appended in the end to match with number of `F` provided. e.g.: for a time part `10:20:30.1234` and pattern `HH:mm:ss.FFFFFF`, the output string of the function would be: `10:20:30.123400`.<br> Format specifier `y` will return year-of-era instead of one/two low-order digits. eg.: For year `2009`, `y` will be returning `2009` instead of `9`. For year `43`, `y` format specifier will return `43`. - Special characters like `"` , `\` and `%` will be returned as it is without any change. eg.: formatting date `17-sep-2020` with `%M` will return `%9`
::::


```sql
SELECT FORMAT(CAST('2020-04-05' AS DATE), 'dd/MM/yyyy') AS "date";

      date
------------------
05/04/2020
```

```sql
SELECT FORMAT(CAST('2020-04-05T11:22:33.987654' AS DATETIME), 'dd/MM/yyyy HH:mm:ss.ff') AS "datetime";

      datetime
------------------
05/04/2020 11:22:33.98
```

```sql
SELECT FORMAT(CAST('11:22:33.987' AS TIME), 'HH mm ss.f') AS "time";

      time
------------------
11 22 33.9
```


## `TO_CHAR` [sql-functions-datetime-to_char]

```sql
TO_CHAR(
    date_exp/datetime_exp/time_exp, <1>
    string_exp) <2>
```

**Input**:

1. date/datetime/time expression. If `null`, the function returns `null`.
2. format pattern. If `null` or an empty string, the function returns `null`.


**Output**: string

**Description**: Returns the date/datetime/time as a string using the format specified in the 2nd argument. The formatting pattern conforms to [PostgreSQL Template Patterns for Date/Time Formatting](https://www.postgresql.org/docs/13/functions-formatting.html).

::::{note}
If the 1st argument is of type `time`, then the pattern specified by the 2nd argument cannot contain date related units (e.g. *dd*, *MM*, *YYYY*, etc.). If it contains such units an error is returned.<br> The result of the patterns `TZ` and `tz` (time zone abbreviations) in some cases differ from the results returned by the `TO_CHAR` in PostgreSQL. The reason is that the time zone abbreviations specified by the JDK are different from the ones specified by PostgreSQL. This function might show an actual time zone abbreviation instead of the generic `LMT` or empty string or offset returned by the PostgreSQL implementation. The summer/daylight markers might also differ between the two implementations (e.g. will show `HT` instead of `HST` for Hawaii).<br> The `FX`, `TM`, `SP` pattern modifiers are not supported and will show up as `FX`, `TM`, `SP` literals in the output.
::::


```sql
SELECT TO_CHAR(CAST('2020-04-05' AS DATE), 'DD/MM/YYYY') AS "date";

      date
------------------
05/04/2020
```

```sql
SELECT TO_CHAR(CAST('2020-04-05T11:22:33.987654' AS DATETIME), 'DD/MM/YYYY HH24:MI:SS.FF2') AS "datetime";

      datetime
------------------
05/04/2020 11:22:33.98
```

```sql
SELECT TO_CHAR(CAST('23:22:33.987' AS TIME), 'HH12 MI SS.FF1') AS "time";

      time
------------------
11 22 33.9
```


## `DAY_OF_MONTH/DOM/DAY` [sql-functions-datetime-day]

```sql
DAY_OF_MONTH(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the day of the month from a date/datetime.

```sql
SELECT DAY_OF_MONTH(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
19
```


## `DAY_OF_WEEK/DAYOFWEEK/DOW` [sql-functions-datetime-dow]

```sql
DAY_OF_WEEK(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the day of the week from a date/datetime. Sunday is `1`, Monday is `2`, etc.

```sql
SELECT DAY_OF_WEEK(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
2
```


## `DAY_OF_YEAR/DOY` [sql-functions-datetime-doy]

```sql
DAY_OF_YEAR(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the day of the year from a date/datetime.

```sql
SELECT DAY_OF_YEAR(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
50
```


## `DAY_NAME/DAYNAME` [sql-functions-datetime-dayname]

```sql
DAY_NAME(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Extract the day of the week from a date/datetime in text format (`Monday`, `Tuesday`…​).

```sql
SELECT DAY_NAME(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
Monday
```


## `HOUR_OF_DAY/HOUR` [sql-functions-datetime-hour]

```sql
HOUR_OF_DAY(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the hour of the day from a date/datetime.

```sql
SELECT HOUR_OF_DAY(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS hour;

     hour
---------------
10
```


## `ISO_DAY_OF_WEEK/ISODAYOFWEEK/ISODOW/IDOW` [sql-functions-datetime-isodow]

```sql
ISO_DAY_OF_WEEK(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the day of the week from a date/datetime, following the [ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_week_date). Monday is `1`, Tuesday is `2`, etc.

```sql
SELECT ISO_DAY_OF_WEEK(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
1
```


## `ISO_WEEK_OF_YEAR/ISOWEEKOFYEAR/ISOWEEK/IWOY/IW` [sql-functions-datetime-isoweek]

```sql
ISO_WEEK_OF_YEAR(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the week of the year from a date/datetime, following [ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_week_date). The first week of a year is the first week with a majority (4 or more) of its days in January.

```sql
SELECT ISO_WEEK_OF_YEAR(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS week;

     week
---------------
8
```


## `MINUTE_OF_DAY` [sql-functions-datetime-minuteofday]

```sql
MINUTE_OF_DAY(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the minute of the day from a date/datetime.

```sql
SELECT MINUTE_OF_DAY(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS minute;

    minute
---------------
623
```


## `MINUTE_OF_HOUR/MINUTE` [sql-functions-datetime-minute]

```sql
MINUTE_OF_HOUR(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the minute of the hour from a date/datetime.

```sql
SELECT MINUTE_OF_HOUR(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS minute;

    minute
---------------
23
```


## `MONTH_OF_YEAR/MONTH` [sql-functions-datetime-month]

```sql
MONTH(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the month of the year from a date/datetime.

```sql
SELECT MONTH_OF_YEAR(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS month;

     month
---------------
2
```


## `MONTH_NAME/MONTHNAME` [sql-functions-datetime-monthname]

```sql
MONTH_NAME(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: string

**Description**: Extract the month from a date/datetime in text format (`January`, `February`…​).

```sql
SELECT MONTH_NAME(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS month;

     month
---------------
February
```


## `NOW` [sql-functions-now]

```sql
NOW()
```

**Input**: *none*

**Output**: datetime

**Description**: This function offers the same functionality as [CURRENT_TIMESTAMP()](#sql-functions-current-timestamp) function: returns the datetime when the current query reached the server. This method always returns the same value for its every occurrence within the same query.

```sql
SELECT NOW() AS result;

         result
------------------------
2018-12-12T14:48:52.448Z
```

Typically, this function (as well as its twin [CURRENT_TIMESTAMP())](#sql-functions-current-timestamp) function is used for relative date/time filtering:

```sql
SELECT first_name FROM emp WHERE hire_date > NOW() - INTERVAL 100 YEARS ORDER BY first_name ASC LIMIT 5;

  first_name
---------------
Alejandro
Amabile
Anneke
Anoosh
Arumugam
```


## `SECOND_OF_MINUTE/SECOND` [sql-functions-datetime-second]

```sql
SECOND_OF_MINUTE(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the second of the minute from a date/datetime.

```sql
SELECT SECOND_OF_MINUTE(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS second;

    second
---------------
27
```


## `QUARTER` [sql-functions-datetime-quarter]

```sql
QUARTER(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the year quarter the date/datetime falls in.

```sql
SELECT QUARTER(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS quarter;

    quarter
---------------
1
```


## `TODAY` [sql-functions-today]

```sql
TODAY()
```

**Input**: *none*

**Output**: date

**Description**: This function offers the same functionality as [CURRENT_DATE()](#sql-functions-current-date) function: returns the date when the current query reached the server. This method always returns the same value for its every occurrence within the same query.

```sql
SELECT TODAY() AS result;

         result
------------------------
2018-12-12
```

Typically, this function (as well as its twin [CURRENT_TIMESTAMP())](#sql-functions-current-timestamp) function is used for relative date filtering:

```sql
SELECT first_name FROM emp WHERE hire_date > TODAY() - INTERVAL 35 YEARS ORDER BY first_name ASC LIMIT 5;

 first_name
------------
Alejandro
Amabile
Anoosh
Basil
Brendon
```


## `WEEK_OF_YEAR/WEEK` [sql-functions-datetime-week]

```sql
WEEK_OF_YEAR(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the week of the year from a date/datetime.

```sql
SELECT WEEK(CAST('1988-01-05T09:22:10Z' AS TIMESTAMP)) AS week, ISOWEEK(CAST('1988-01-05T09:22:10Z' AS TIMESTAMP)) AS isoweek;

      week     |   isoweek
---------------+---------------
2              |1
```


## `YEAR` [sql-functions-datetime-year]

```sql
YEAR(datetime_exp) <1>
```

**Input**:

1. date/datetime expression. If `null`, the function returns `null`.


**Output**: integer

**Description**: Extract the year from a date/datetime.

```sql
SELECT YEAR(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS year;

     year
---------------
2018
```


## `EXTRACT` [sql-functions-datetime-extract]

```sql
EXTRACT(
    datetime_function  <1>
    FROM datetime_exp) <2>
```

**Input**:

1. date/time function name
2. date/datetime expression


**Output**: integer

**Description**: Extract fields from a date/datetime by specifying the name of a `datetime` function. The following

```sql
SELECT EXTRACT(DAY_OF_YEAR FROM CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
50
```

is the equivalent to

```sql
SELECT DAY_OF_YEAR(CAST('2018-02-19T10:23:27Z' AS TIMESTAMP)) AS day;

      day
---------------
50
```


