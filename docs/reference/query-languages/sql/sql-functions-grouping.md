---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-grouping.html
---

# Grouping functions [sql-functions-grouping]

Functions for creating special *grouping*s (also known as *bucketing*); as such these need to be used as part of the [grouping](/reference/query-languages/sql/sql-syntax-select.md#sql-syntax-group-by).

## `HISTOGRAM` [sql-functions-grouping-histogram]

```sql
HISTOGRAM(
    numeric_exp,        <1>
    numeric_interval)   <2>

HISTOGRAM(
    date_exp,           <3>
    date_time_interval) <4>
```

**Input**:

1. numeric expression (typically a field). If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.
2. numeric interval. If `null`, the function returns `null`.
3. date/time expression (typically a field). If this field contains only `null` values, the function returns `null`. Otherwise, the function ignores `null` values in this field.
4. date/time [interval](/reference/query-languages/sql/sql-functions-datetime.md#sql-functions-datetime-interval). If `null`, the function returns `null`.


**Output**: non-empty buckets or groups of the given expression divided according to the given interval

**Description**: The histogram function takes all matching values and divides them into buckets with fixed size matching the given interval, using (roughly) the following formula:

```sql
bucket_key = Math.floor(value / interval) * interval
```

::::{note}
The histogram in SQL does **NOT** return empty buckets for missing intervals as the traditional [histogram](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md) and  [date histogram](/reference/aggregations/search-aggregations-bucket-datehistogram-aggregation.md). Such behavior does not fit conceptually in SQL which treats all missing values as `null`; as such the histogram places all missing values in the `null` group.
::::


`Histogram` can be applied on either numeric fields:

```sql
SELECT HISTOGRAM(salary, 5000) AS h FROM emp GROUP BY h;

       h
---------------
25000
30000
35000
40000
45000
50000
55000
60000
65000
70000
```

or date/time fields:

```sql
SELECT HISTOGRAM(birth_date, INTERVAL 1 YEAR) AS h, COUNT(*) AS c FROM emp GROUP BY h;


           h            |       c
------------------------+---------------
null                    |10
1952-01-01T00:00:00.000Z|8
1953-01-01T00:00:00.000Z|11
1954-01-01T00:00:00.000Z|8
1955-01-01T00:00:00.000Z|4
1956-01-01T00:00:00.000Z|5
1957-01-01T00:00:00.000Z|4
1958-01-01T00:00:00.000Z|7
1959-01-01T00:00:00.000Z|9
1960-01-01T00:00:00.000Z|8
1961-01-01T00:00:00.000Z|8
1962-01-01T00:00:00.000Z|6
1963-01-01T00:00:00.000Z|7
1964-01-01T00:00:00.000Z|4
1965-01-01T00:00:00.000Z|1
```

Expressions inside the histogram are also supported as long as the return type is numeric:

```sql
SELECT HISTOGRAM(salary % 100, 10) AS h, COUNT(*) AS c FROM emp GROUP BY h;

       h       |       c
---------------+---------------
0              |10
10             |15
20             |10
30             |14
40             |9
50             |9
60             |8
70             |13
80             |3
90             |9
```

Do note that histograms (and grouping functions in general) allow custom expressions but cannot have any functions applied to them in the `GROUP BY`. In other words, the following statement is **NOT** allowed:

```sql
SELECT MONTH(HISTOGRAM(birth_date), 2)) AS h, COUNT(*) as c FROM emp GROUP BY h ORDER BY h DESC;
```

as it requires two groupings (one for histogram followed by a second for applying the function on top of the histogram groups).

Instead one can rewrite the query to move the expression on the histogram *inside* of it:

```sql
SELECT HISTOGRAM(MONTH(birth_date), 2) AS h, COUNT(*) as c FROM emp GROUP BY h ORDER BY h DESC;

       h       |       c
---------------+---------------
12             |7
10             |17
8              |16
6              |16
4              |18
2              |10
0              |6
null           |10
```

::::{important}
When the histogram in SQL is applied on **DATE** type instead of **DATETIME**, the interval specified is truncated to the multiple of a day. E.g.: for `HISTOGRAM(CAST(birth_date AS DATE), INTERVAL '2 3:04' DAY TO MINUTE)` the interval actually used will be `INTERVAL '2' DAY`. If the interval specified is less than 1 day, e.g.: `HISTOGRAM(CAST(birth_date AS DATE), INTERVAL '20' HOUR)` then the interval used will be `INTERVAL '1' DAY`.
::::


::::{important}
All intervals specified for a date/time HISTOGRAM will use a [fixed interval](/reference/aggregations/search-aggregations-bucket-datehistogram-aggregation.md) in their `date_histogram` aggregation definition, with the notable exceptions of `INTERVAL '1' YEAR`, `INTERVAL '1' MONTH` and `INTERVAL '1' DAY`  where a calendar interval is used. The choice for a calendar interval was made for having a more intuitive result for YEAR, MONTH and DAY groupings. In the case of YEAR, for example, the calendar intervals consider a one year bucket as the one starting on January 1st that specific year, whereas a fixed interval one-year-bucket considers one year as a number of milliseconds (for example, `31536000000ms` corresponding to 365 days, 24 hours per day, 60 minutes per hour etc.). With fixed intervals, the day of February 5th, 2019 for example, belongs to a bucket that starts on December 20th, 2018 and {{es}} (and implicitly Elasticsearch SQL) would have returned the year 2018 for a date that’s actually in 2019. With calendar interval this behavior is more intuitive, having the day of February 5th, 2019 actually belonging to the 2019 year bucket.
::::


::::{important}
Histogram in SQL cannot be applied on **TIME** type. E.g.: `HISTOGRAM(CAST(birth_date AS TIME), INTERVAL '10' MINUTES)` is currently not supported.
::::
