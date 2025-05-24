---
navigation_title: "Auto-interval date histogram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-autodatehistogram-aggregation.html
---

# Auto-interval date histogram aggregation [search-aggregations-bucket-autodatehistogram-aggregation]


A multi-bucket aggregation similar to the [Date histogram](/reference/aggregations/search-aggregations-bucket-datehistogram-aggregation.md) except instead of providing an interval to use as the width of each bucket, a target number of buckets is provided indicating the number of buckets needed and the interval of the buckets is automatically chosen to best achieve that target. The number of buckets returned will always be less than or equal to this target number.

The buckets field is optional, and will default to 10 buckets if not specified.

Requesting a target of 10 buckets.

$$$autodatehistogram-aggregation-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "auto_date_histogram": {
        "field": "date",
        "buckets": 10
      }
    }
  }
}
```
% TEST[setup:sales]

## Keys [_keys]

Internally, a date is represented as a 64 bit number representing a timestamp in milliseconds-since-the-epoch. These timestamps are returned as the bucket `key`s. The `key_as_string` is the same timestamp converted to a formatted date string using the format specified with the `format` parameter:

::::{tip}
If no `format` is specified, then it will use the first date [format](/reference/elasticsearch/mapping-reference/mapping-date-format.md) specified in the field mapping.
::::


$$$autodatehistogram-aggregation-format-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "auto_date_histogram": {
        "field": "date",
        "buckets": 5,
        "format": "yyyy-MM-dd" <1>
      }
    }
  }
}
```
% TEST[setup:sales]

1. Supports expressive date [format pattern](/reference/aggregations/search-aggregations-bucket-daterange-aggregation.md#date-format-pattern)


Response:

```console-result
{
  ...
  "aggregations": {
    "sales_over_time": {
      "buckets": [
        {
          "key_as_string": "2015-01-01",
          "key": 1420070400000,
          "doc_count": 3
        },
        {
          "key_as_string": "2015-02-01",
          "key": 1422748800000,
          "doc_count": 2
        },
        {
          "key_as_string": "2015-03-01",
          "key": 1425168000000,
          "doc_count": 2
        }
      ],
      "interval": "1M"
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]


## Intervals [_intervals]

The interval of the returned buckets is selected based on the data collected by the aggregation so that the number of buckets returned is less than or equal to the number requested. The possible intervals returned are:

seconds
:   In multiples of 1, 5, 10 and 30

minutes
:   In multiples of 1, 5, 10 and 30

hours
:   In multiples of 1, 3 and 12

days
:   In multiples of 1, and 7

months
:   In multiples of 1, and 3

years
:   In multiples of 1, 5, 10, 20, 50 and 100

In the worst case, where the number of daily buckets are too many for the requested number of buckets, the number of buckets returned will be 1/7th of the number of buckets requested.


## Time Zone [_time_zone]

Date-times are stored in Elasticsearch in UTC. By default, all bucketing and rounding is also done in UTC. The `time_zone` parameter can be used to indicate that bucketing should use a different time zone.

Time zones may either be specified as an ISO 8601 UTC offset (e.g. `+01:00` or `-08:00`)  or as a timezone id, an identifier used in the TZ database like `America/Los_Angeles`.

Consider the following example:

$$$autodatehistogram-aggregation-timezone-example$$$

```console
PUT my-index-000001/_doc/1?refresh
{
  "date": "2015-10-01T00:30:00Z"
}

PUT my-index-000001/_doc/2?refresh
{
  "date": "2015-10-01T01:30:00Z"
}

PUT my-index-000001/_doc/3?refresh
{
  "date": "2015-10-01T02:30:00Z"
}

GET my-index-000001/_search?size=0
{
  "aggs": {
    "by_day": {
      "auto_date_histogram": {
        "field":     "date",
        "buckets" : 3
      }
    }
  }
}
```

UTC is used if no time zone is specified, three 1-hour buckets are returned starting at midnight UTC on 1 October 2015:

```console-result
{
  ...
  "aggregations": {
    "by_day": {
      "buckets": [
        {
          "key_as_string": "2015-10-01T00:00:00.000Z",
          "key": 1443657600000,
          "doc_count": 1
        },
        {
          "key_as_string": "2015-10-01T01:00:00.000Z",
          "key": 1443661200000,
          "doc_count": 1
        },
        {
          "key_as_string": "2015-10-01T02:00:00.000Z",
          "key": 1443664800000,
          "doc_count": 1
        }
      ],
      "interval": "1h"
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

If a `time_zone` of `-01:00` is specified, then midnight starts at one hour before midnight UTC:

```console
GET my-index-000001/_search?size=0
{
  "aggs": {
    "by_day": {
      "auto_date_histogram": {
        "field":     "date",
        "buckets" : 3,
        "time_zone": "-01:00"
      }
    }
  }
}
```
% TEST[continued]

Now three 1-hour buckets are still returned but the first bucket starts at 11:00pm on 30 September 2015 since that is the local time for the bucket in the specified time zone.

```console-result
{
  ...
  "aggregations": {
    "by_day": {
      "buckets": [
        {
          "key_as_string": "2015-09-30T23:00:00.000-01:00", <1>
          "key": 1443657600000,
          "doc_count": 1
        },
        {
          "key_as_string": "2015-10-01T00:00:00.000-01:00",
          "key": 1443661200000,
          "doc_count": 1
        },
        {
          "key_as_string": "2015-10-01T01:00:00.000-01:00",
          "key": 1443664800000,
          "doc_count": 1
        }
      ],
      "interval": "1h"
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

1. The `key_as_string` value represents midnight on each day in the specified time zone.


::::{warning}
When using time zones that follow DST (daylight savings time) changes, buckets close to the moment when those changes happen can have slightly different sizes than neighbouring buckets. For example, consider a DST start in the `CET` time zone: on 27 March 2016 at 2am, clocks were turned forward 1 hour to 3am local time. If the result of the aggregation was daily buckets, the bucket covering that day will only hold data for 23 hours instead of the usual 24 hours for other buckets. The same is true for shorter intervals like e.g. 12h. Here, we will have only a 11h bucket on the morning of 27 March when the DST shift happens.
::::



## Minimum Interval parameter [_minimum_interval_parameter]

The `minimum_interval` allows the caller to specify the minimum rounding interval that should be used. This can make the collection process more efficient, as the aggregation will not attempt to round at any interval lower than `minimum_interval`.

The accepted units for `minimum_interval` are:

* year
* month
* day
* hour
* minute
* second

$$$autodatehistogram-aggregation-minimum-interval-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sale_date": {
      "auto_date_histogram": {
        "field": "date",
        "buckets": 10,
        "minimum_interval": "minute"
      }
    }
  }
}
```
% TEST[setup:sales]


## Missing value [_missing_value]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

$$$autodatehistogram-aggregation-missing-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sale_date": {
      "auto_date_histogram": {
        "field": "date",
        "buckets": 10,
        "missing": "2000/01/01" <1>
      }
    }
  }
}
```
% TEST[setup:sales]

1. Documents without a value in the `publish_date` field will fall into the same bucket as documents that have the value `2000-01-01`.



