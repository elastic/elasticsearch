---
navigation_title: "Range"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
---

# Range query [query-dsl-range-query]

Returns documents that contain terms within a provided range.

## Example request [range-query-ex-request]

The following search returns documents where the `age` field contains a term between `10` and `20`.

```console
GET /_search
{
  "query": {
    "range": {
      "age": {
        "gte": 10,
        "lte": 20,
        "boost": 2.0
      }
    }
  }
}
```


## Top-level parameters for `range` [range-query-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [range-query-field-params]

`gt`
:   (Optional) Greater than.

`gte`
:   (Optional) Greater than or equal to.

`lt`
:   (Optional) Less than.

`lte`
:   (Optional) Less than or equal to.

`format`
:   (Optional, string) Date format used to convert `date` values in the query.

    By default, {{es}} uses the [date `format`](/reference/elasticsearch/mapping-reference/mapping-date-format.md) provided in the `<field>`'s
    mapping. This value overrides that mapping format.

    For valid syntax, see [`format`](/reference/elasticsearch/mapping-reference/mapping-date-format.md).

::::{warning}
If a format or date value is incomplete, the range query replaces any missing components with default values. See [Missing date components](#missing-date-components).
::::



$$$querying-range-fields$$$

`relation`
:   (Optional, string) Indicates how the range query matches values for `range`
    fields. Valid values are:

    `INTERSECTS` (Default)
    :   Matches documents with a range field value that intersects the query’s range.

    `CONTAINS`
    :   Matches documents with a range field value that entirely contains the query’s range.

    `WITHIN`
    :   Matches documents with a range field value entirely within the query’s range.


`time_zone`
:   (Optional, string) [Coordinated Universal Time (UTC) offset](https://en.wikipedia.org/wiki/List_of_UTC_time_offsets) or [IANA time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
    used to convert `date` values in the query to UTC.

    Valid values are ISO 8601 UTC offsets, such as `+01:00` or -`08:00`, and IANA
    time zone IDs, such as `America/Los_Angeles`.

    For an example query using the `time_zone` parameter, see
    [Time zone in `range` queries](#range-query-time-zone).

::::{note}
The `time_zone` parameter does **not** affect the [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) value of `now`. `now` is always the current system time in UTC.

However, the `time_zone` parameter does convert dates calculated using `now` and [date math rounding](/reference/elasticsearch/rest-apis/common-options.md#date-math). For example, the `time_zone` parameter will convert a value of `now/d`.

::::



`boost`
:   (Optional, float) Floating point number used to decrease or increase the
    [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of a query. Defaults to `1.0`.

    You can use the `boost` parameter to adjust relevance scores for searches
    containing two or more queries.

    Boost values are relative to the default value of `1.0`. A boost value between `0`
    and `1.0` decreases the relevance score. A value greater than `1.0`
    increases the relevance score.



## Notes [range-query-notes]

### Using the `range` query with `text` and `keyword` fields [ranges-on-text-and-keyword]

Range queries on [`text`](/reference/elasticsearch/mapping-reference/text.md) or [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) fields will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.


### Using the `range` query with `date` fields [ranges-on-dates]

When the `<field>` parameter is a [`date`](/reference/elasticsearch/mapping-reference/date.md) field data type, you can use [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) with the following parameters:

* `gt`
* `gte`
* `lt`
* `lte`

For example, the following search returns documents where the `timestamp` field contains a date between today and yesterday.

```console
GET /_search
{
  "query": {
    "range": {
      "timestamp": {
        "gte": "now-1d/d",
        "lte": "now/d"
      }
    }
  }
}
```

#### Missing date components [missing-date-components]

For range queries and [date range](/reference/aggregations/search-aggregations-bucket-daterange-aggregation.md) aggregations, {{es}} replaces missing date components with the following values. Missing year components are not replaced.

```text
MONTH_OF_YEAR:    01
DAY_OF_MONTH:     01
HOUR_OF_DAY:      23
MINUTE_OF_HOUR:   59
SECOND_OF_MINUTE: 59
NANO_OF_SECOND:   999_999_999
```

For example, if the format is `yyyy-MM`, {{es}} converts a `gt` value of `2099-12` to `2099-12-01T23:59:59.999_999_999Z`. This date uses the provided year (`2099`) and month (`12`) but uses the default day (`01`), hour (`23`), minute (`59`), second (`59`), and nanosecond (`999_999_999`).


#### Numeric date range value [numeric-date]

When no date format is specified and the range query is targeting a date field, numeric values are interpreted representing milliseconds-since-the-epoch. If you want the value to represent a year, e.g. 2020, you need to pass it as a String value (e.g. "2020") that will be parsed according to the default format or the set format.


#### Date math and rounding [range-query-date-math-rounding]

{{es}} rounds [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) values in parameters as follows:

`gt`
:   Rounds up to the first millisecond not covered by the rounded date.

    For example, `2014-11-18||/M` rounds up to `2014-12-01T00:00:00.000`,
    excluding the entire month of November.


`gte`
:   Rounds down to the first millisecond.

    For example, `2014-11-18||/M` rounds down to `2014-11-01T00:00:00.000`,
    including the entire month.


`lt`
:   Rounds down to the last millisecond before the rounded value.

    For example, `2014-11-18||/M` rounds down to `2014-10-31T23:59:59.999`,
    excluding the entire month of November.


`lte`
:   Rounds up to the latest millisecond in the rounding interval.

    For example, `2014-11-18||/M` rounds up to `2014-11-30T23:59:59.999`,
    including the entire month.




### Example query using `time_zone` parameter [range-query-time-zone]

You can use the `time_zone` parameter to convert `date` values to UTC using a UTC offset. For example:

```console
GET /_search
{
  "query": {
    "range": {
      "timestamp": {
        "time_zone": "+01:00",        <1>
        "gte": "2020-01-01T00:00:00", <2>
        "lte": "now"                  <3>
      }
    }
  }
}
```
% TEST[continued]

1. Indicates that `date` values use a UTC offset of `+01:00`.
2. With a UTC offset of `+01:00`, {{es}} converts this date to `2019-12-31T23:00:00 UTC`.
3. The `time_zone` parameter does not affect the `now` value.
