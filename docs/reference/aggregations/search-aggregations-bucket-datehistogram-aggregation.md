---
navigation_title: "Date histogram"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html
---

# Date histogram aggregation [search-aggregations-bucket-datehistogram-aggregation]


This multi-bucket aggregation is similar to the normal [histogram](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md), but it can only be used with date or date range values. Because dates are represented internally in Elasticsearch as long values, it is possible, but not as accurate, to use the normal `histogram` on dates as well. The main difference in the two APIs is that here the interval can be specified using date/time expressions. Time-based data requires special support because time-based intervals are not always a fixed length.

Like the histogram, values are rounded **down** into the closest bucket. For example, if the interval is a calendar day, `2020-01-03T07:00:01Z` is rounded to `2020-01-03T00:00:00Z`. Values are rounded as follows:

```java
bucket_key = Math.floor(value / interval) * interval
```

## Calendar and fixed intervals [calendar_and_fixed_intervals]

When configuring a date histogram aggregation, the interval can be specified in two manners: calendar-aware time intervals, and fixed time intervals.

Calendar-aware intervals understand that daylight savings changes the length of specific days, months have different amounts of days, and leap seconds can be tacked onto a particular year.

Fixed intervals are, by contrast, always multiples of SI units and do not change based on calendaring context.


## Calendar intervals [calendar_intervals]

Calendar-aware intervals are configured with the `calendar_interval` parameter. You can specify calendar intervals using the unit name, such as `month`, or as a single unit quantity, such as `1M`. For example, `day` and `1d` are equivalent. Multiple quantities, such as `2d`, are not supported.

The accepted calendar intervals are:

`minute`, `1m`
:   All minutes begin at 00 seconds. One minute is the interval between 00 seconds of the first minute and 00 seconds of the following minute in the specified time zone, compensating for any intervening leap seconds, so that the number of minutes and seconds past the hour is the same at the start and end.

`hour`, `1h`
:   All hours begin at 00 minutes and 00 seconds. One hour (1h) is the interval between 00:00 minutes of the first hour and 00:00 minutes of the following hour in the specified time zone, compensating for any intervening leap seconds, so that the number of minutes and seconds past the hour is the same at the start and end.

`day`, `1d`
:   All days begin at the earliest possible time, which is usually 00:00:00 (midnight). One day (1d) is the interval between the start of the day and the start of the following day in the specified time zone, compensating for any intervening time changes.

`week`, `1w`
:   One week is the interval between the start day_of_week:hour:minute:second and the same day of the week and time of the following week in the specified time zone.

`month`, `1M`
:   One month is the interval between the start day of the month and time of day and the same day of the month and time of the following month in the specified time zone, so that the day of the month and time of day are the same at the start and end. Note that the day may differ if an [`offset` is used that is longer than a month](#search-aggregations-bucket-datehistogram-offset-months).

`quarter`, `1q`
:   One quarter is the interval between the start day of the month and time of day and the same day of the month and time of day three months later, so that the day of the month and time of day are the same at the start and end.<br>

`year`, `1y`
:   One year is the interval between the start day of the month and time of day and the same day of the month and time of day the following year in the specified time zone, so that the date and time are the same at the start and end.<br>

### Calendar interval examples [calendar_interval_examples]

As an example, here is an aggregation requesting bucket intervals of a month in calendar time:

$$$datehistogram-aggregation-calendar-interval-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"
      }
    }
  }
}
```

If you attempt to use multiples of calendar units, the aggregation will fail because only singular calendar units are supported:

$$$datehistogram-aggregation-calendar-interval-multiples-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "2d"
      }
    }
  }
}
```

```js
{
  "error" : {
    "root_cause" : [...],
    "type" : "x_content_parse_exception",
    "reason" : "[1:82] [date_histogram] failed to parse field [calendar_interval]",
    "caused_by" : {
      "type" : "illegal_argument_exception",
      "reason" : "The supplied interval [2d] could not be parsed as a calendar interval.",
      "stack_trace" : "java.lang.IllegalArgumentException: The supplied interval [2d] could not be parsed as a calendar interval."
    }
  }
}
```



## Fixed intervals [fixed_intervals]

Fixed intervals are configured with the `fixed_interval` parameter.

In contrast to calendar-aware intervals, fixed intervals are a fixed number of SI units and never deviate, regardless of where they fall on the calendar. One second is always composed of `1000ms`. This allows fixed intervals to be specified in any multiple of the supported units.

However, it means fixed intervals cannot express other units such as months, since the duration of a month is not a fixed quantity. Attempting to specify a calendar interval like month or quarter will throw an exception.

The accepted units for fixed intervals are:

milliseconds (`ms`)
:   A single millisecond. This is a very, very small interval.

seconds (`s`)
:   Defined as 1000 milliseconds each.

minutes (`m`)
:   Defined as 60 seconds each (60,000 milliseconds). All minutes begin at 00 seconds.

hours (`h`)
:   Defined as 60 minutes each (3,600,000 milliseconds). All hours begin at 00 minutes and 00 seconds.

days (`d`)
:   Defined as 24 hours (86,400,000 milliseconds). All days begin at the earliest possible time, which is usually 00:00:00 (midnight).

### Fixed interval examples [fixed_interval_examples]

If we try to recreate the "month" `calendar_interval` from earlier, we can approximate that with 30 fixed days:

$$$datehistogram-aggregation-fixed-interval-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date",
        "fixed_interval": "30d"
      }
    }
  }
}
```

But if we try to use a calendar unit that is not supported, such as weeks, we’ll get an exception:

$$$datehistogram-aggregation-fixed-interval-unsupported-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date",
        "fixed_interval": "2w"
      }
    }
  }
}
```

```js
{
  "error" : {
    "root_cause" : [...],
    "type" : "x_content_parse_exception",
    "reason" : "[1:82] [date_histogram] failed to parse field [fixed_interval]",
    "caused_by" : {
      "type" : "illegal_argument_exception",
      "reason" : "failed to parse setting [date_histogram.fixedInterval] with value [2w] as a time value: unit is missing or unrecognized",
      "stack_trace" : "java.lang.IllegalArgumentException: failed to parse setting [date_histogram.fixedInterval] with value [2w] as a time value: unit is missing or unrecognized"
    }
  }
}
```



## Date histogram usage notes [datehistogram-aggregation-notes]

In all cases, when the specified end time does not exist, the actual end time is the closest available time after the specified end.

Widely distributed applications must also consider vagaries such as countries that start and stop daylight savings time at 12:01 A.M., so end up with one minute of Sunday followed by an additional 59 minutes of Saturday once a year, and countries that decide to move across the international date line. Situations like that can make irregular time zone offsets seem easy.

As always, rigorous testing, especially around time-change events, will ensure that your time interval specification is what you intend it to be.

::::{warning}
To avoid unexpected results, all connected servers and clients must sync to a reliable network time service.
::::


::::{note}
Fractional time values are not supported, but you can address this by shifting to another time unit (e.g., `1.5h` could instead be specified as `90m`).
::::


::::{note}
You can also specify time values using abbreviations supported by [time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units) parsing.
::::



## Keys [datehistogram-aggregation-keys]

Internally, a date is represented as a 64 bit number representing a timestamp in milliseconds-since-the-epoch (01/01/1970 midnight UTC). These timestamps are returned as the `key` name of the bucket. The `key_as_string` is the same timestamp converted to a formatted date string using the `format` parameter specification:

::::{tip}
If you don’t specify `format`, the first date [format](/reference/elasticsearch/mapping-reference/mapping-date-format.md) specified in the field mapping is used.
::::


$$$datehistogram-aggregation-format-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "1M",
        "format": "yyyy-MM-dd" <1>
      }
    }
  }
}
```

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
      ]
    }
  }
}
```


## Time zone [datehistogram-aggregation-time-zone]

{{es}} stores date-times in Coordinated Universal Time (UTC). By default, all bucketing and rounding is also done in UTC. Use the `time_zone` parameter to indicate that bucketing should use a different time zone.

When you specify a time zone, the following logic is used to determine the bucket the document belongs in:

```java
bucket_key = localToUtc(Math.floor(utcToLocal(value) / interval) * interval))
```

For example, if the interval is a calendar day and the time zone is `America/New_York`, then the date value `2020-01-03T01:00:01Z` is processed as follows:

1. Converted to EST: `2020-01-02T20:00:01`
2. Rounded down to the nearest interval: `2020-01-02T00:00:00`
3. Converted back to UTC: `2020-01-02T05:00:00:00Z`

When a `key_as_string` is generated for the bucket, the key value is stored in `America/New_York` time, so it’ll display as `"2020-01-02T00:00:00"`.

You can specify time zones as an ISO 8601 UTC offset, such as `+01:00` or `-08:00`, or as an IANA time zone ID, such as `America/Los_Angeles`.

Consider the following example:

$$$datehistogram-aggregation-timezone-example$$$

```console
PUT my-index-000001/_doc/1?refresh
{
  "date": "2015-10-01T00:30:00Z"
}

PUT my-index-000001/_doc/2?refresh
{
  "date": "2015-10-01T01:30:00Z"
}

GET my-index-000001/_search?size=0
{
  "aggs": {
    "by_day": {
      "date_histogram": {
        "field":     "date",
        "calendar_interval":  "day"
      }
    }
  }
}
```

If you don’t specify a time zone, UTC is used. This would result in both of these documents being placed into the same day bucket, which starts at midnight UTC on 1 October 2015:

```console-result
{
  ...
  "aggregations": {
    "by_day": {
      "buckets": [
        {
          "key_as_string": "2015-10-01T00:00:00.000Z",
          "key":           1443657600000,
          "doc_count":     2
        }
      ]
    }
  }
}
```

If you specify a `time_zone` of `-01:00`, midnight in that time zone is one hour before midnight UTC:

```console
GET my-index-000001/_search?size=0
{
  "aggs": {
    "by_day": {
      "date_histogram": {
        "field":     "date",
        "calendar_interval":  "day",
        "time_zone": "-01:00"
      }
    }
  }
}
```

Now the first document falls into the bucket for 30 September 2015, while the second document falls into the bucket for 1 October 2015:

```console-result
{
  ...
  "aggregations": {
    "by_day": {
      "buckets": [
        {
          "key_as_string": "2015-09-30T00:00:00.000-01:00", <1>
          "key": 1443574800000,
          "doc_count": 1
        },
        {
          "key_as_string": "2015-10-01T00:00:00.000-01:00", <1>
          "key": 1443661200000,
          "doc_count": 1
        }
      ]
    }
  }
}
```

1. The `key_as_string` value represents midnight on each day in the specified time zone.


::::{warning}
Many time zones shift their clocks for daylight savings time. Buckets close to the moment when those changes happen can have slightly different sizes than you would expect from the `calendar_interval` or `fixed_interval`. For example, consider a DST start in the `CET` time zone: on 27 March 2016 at 2am, clocks were turned forward 1 hour to 3am local time. If you use `day` as the `calendar_interval`, the bucket covering that day will only hold data for 23 hours instead of the usual 24 hours for other buckets. The same is true for shorter intervals, like a `fixed_interval` of `12h`, where you’ll have only a 11h bucket on the morning of 27 March when the DST shift happens.
::::



## Offset [search-aggregations-bucket-datehistogram-offset]

Use the `offset` parameter to change the start value of each bucket by the specified positive (`+`) or negative offset (`-`) duration, such as `1h` for an hour, or `1d` for a day. See [Time units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units) for more possible time duration options.

For example, when using an interval of `day`, each bucket runs from midnight to midnight. Setting the `offset` parameter to `+6h` changes each bucket to run from 6am to 6am:

$$$datehistogram-aggregation-offset-example$$$

```console
PUT my-index-000001/_doc/1?refresh
{
  "date": "2015-10-01T05:30:00Z"
}

PUT my-index-000001/_doc/2?refresh
{
  "date": "2015-10-01T06:30:00Z"
}

GET my-index-000001/_search?size=0
{
  "aggs": {
    "by_day": {
      "date_histogram": {
        "field":     "date",
        "calendar_interval":  "day",
        "offset":    "+6h"
      }
    }
  }
}
```

Instead of a single bucket starting at midnight, the above request groups the documents into buckets starting at 6am:

```console-result
{
  ...
  "aggregations": {
    "by_day": {
      "buckets": [
        {
          "key_as_string": "2015-09-30T06:00:00.000Z",
          "key": 1443592800000,
          "doc_count": 1
        },
        {
          "key_as_string": "2015-10-01T06:00:00.000Z",
          "key": 1443679200000,
          "doc_count": 1
        }
      ]
    }
  }
}
```

::::{note}
The start `offset` of each bucket is calculated after `time_zone` adjustments have been made.
::::


### Long offsets over calendar intervals [search-aggregations-bucket-datehistogram-offset-months]

It is typical to use offsets in units smaller than the `calendar_interval`. For example, using offsets in hours when the interval is days, or an offset of days when the interval is months. If the calendar interval is always of a standard length, or the `offset` is less than one unit of the calendar interval (for example less than `+24h` for `days` or less than `+28d` for months), then each bucket will have a repeating start. For example `+6h` for `days` will result in all buckets starting at 6am each day. However, `+30h` will also result in buckets starting at 6am, except when crossing days that change from standard to summer-savings time or vice-versa.

This situation is much more pronounced for months, where each month has a different length to at least one of its adjacent months. To demonstrate this, consider eight documents each with a date field on the 20th day of each of the eight months from January to August of 2022.

When querying for a date histogram over the calendar interval of months, the response will return one bucket per month, each with a single document. Each bucket will have a key named after the first day of the month, plus any offset. For example, the offset of `+19d` will result in buckets with names like `2022-01-20`.

$$$datehistogram-aggregation-offset-example-19d$$$

```console
"buckets": [
  { "key_as_string": "2022-01-20", "key": 1642636800000, "doc_count": 1 },
  { "key_as_string": "2022-02-20", "key": 1645315200000, "doc_count": 1 },
  { "key_as_string": "2022-03-20", "key": 1647734400000, "doc_count": 1 },
  { "key_as_string": "2022-04-20", "key": 1650412800000, "doc_count": 1 },
  { "key_as_string": "2022-05-20", "key": 1653004800000, "doc_count": 1 },
  { "key_as_string": "2022-06-20", "key": 1655683200000, "doc_count": 1 },
  { "key_as_string": "2022-07-20", "key": 1658275200000, "doc_count": 1 },
  { "key_as_string": "2022-08-20", "key": 1660953600000, "doc_count": 1 }
]
```

Increasing the offset to `+20d`, each document will appear in a bucket for the previous month, with all bucket keys ending with the same day of the month, as normal. However, further increasing to `+28d`, what used to be a February bucket has now become `"2022-03-01"`.

$$$datehistogram-aggregation-offset-example-28d$$$

```console
"buckets": [
  { "key_as_string": "2021-12-29", "key": 1640736000000, "doc_count": 1 },
  { "key_as_string": "2022-01-29", "key": 1643414400000, "doc_count": 1 },
  { "key_as_string": "2022-03-01", "key": 1646092800000, "doc_count": 1 },
  { "key_as_string": "2022-03-29", "key": 1648512000000, "doc_count": 1 },
  { "key_as_string": "2022-04-29", "key": 1651190400000, "doc_count": 1 },
  { "key_as_string": "2022-05-29", "key": 1653782400000, "doc_count": 1 },
  { "key_as_string": "2022-06-29", "key": 1656460800000, "doc_count": 1 },
  { "key_as_string": "2022-07-29", "key": 1659052800000, "doc_count": 1 }
]
```

If we continue to increase the offset, the 30-day months will also shift into the next month, so that 3 of the 8 buckets have different days than the other five. In fact if we keep going, we will find cases where two documents appear in the same month. Documents that were originally 30 days apart can be shifted into the same 31-day month bucket.

For example, for `+50d` we see:

$$$datehistogram-aggregation-offset-example-50d$$$

```console
"buckets": [
  { "key_as_string": "2022-01-20", "key": 1642636800000, "doc_count": 1 },
  { "key_as_string": "2022-02-20", "key": 1645315200000, "doc_count": 2 },
  { "key_as_string": "2022-04-20", "key": 1650412800000, "doc_count": 2 },
  { "key_as_string": "2022-06-20", "key": 1655683200000, "doc_count": 2 },
  { "key_as_string": "2022-08-20", "key": 1660953600000, "doc_count": 1 }
]
```

It is therefore always important when using `offset` with `calendar_interval` bucket sizes to understand the consequences of using offsets larger than the interval size.

More examples:

* If the goal is to, for example, have an annual histogram where each year starts on the 5th February, you could use `calendar_interval` of `year` and `offset` of `+33d`, and each year will be shifted identically, because the offset includes only January, which is the same length every year. However, if the goal is to have the year start on the 5th March instead, this technique will not work because the offset includes February, which changes length every four years.
* If you want a quarterly histogram starting on a date within the first month of the year, it will work, but as soon as you push the start date into the second month by having an offset longer than a month, the quarters will all start on different dates.



## Keyed response [date-histogram-keyed-response]

Setting the `keyed` flag to `true` associates a unique string key with each bucket and returns the ranges as a hash rather than an array:

$$$datehistogram-aggregation-keyed-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "1M",
        "format": "yyyy-MM-dd",
        "keyed": true
      }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "sales_over_time": {
      "buckets": {
        "2015-01-01": {
          "key_as_string": "2015-01-01",
          "key": 1420070400000,
          "doc_count": 3
        },
        "2015-02-01": {
          "key_as_string": "2015-02-01",
          "key": 1422748800000,
          "doc_count": 2
        },
        "2015-03-01": {
          "key_as_string": "2015-03-01",
          "key": 1425168000000,
          "doc_count": 2
        }
      }
    }
  }
}
```


## Scripts [date-histogram-scripts]

If the data in your documents doesn’t exactly match what you’d like to aggregate, use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) . For example, if the revenue for promoted sales should be recognized a day after the sale date:

$$$datehistogram-aggregation-runtime-field$$$

```console
POST /sales/_search?size=0
{
  "runtime_mappings": {
    "date.promoted_is_tomorrow": {
      "type": "date",
      "script": """
        long date = doc['date'].value.toInstant().toEpochMilli();
        if (doc['promoted'].value) {
          date += 86400;
        }
        emit(date);
      """
    }
  },
  "aggs": {
    "sales_over_time": {
      "date_histogram": {
        "field": "date.promoted_is_tomorrow",
        "calendar_interval": "1M"
      }
    }
  }
}
```


## Parameters [date-histogram-params]

You can control the order of the returned buckets using the `order` settings and filter the returned buckets based on a `min_doc_count` setting (by default all buckets between the first bucket that matches documents and the last one are returned). This histogram also supports the `extended_bounds` setting, which enables extending the bounds of the histogram beyond the data itself, and `hard_bounds` that limits the histogram to specified bounds. For more information, see [`Extended Bounds`](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md#search-aggregations-bucket-histogram-aggregation-extended-bounds) and [`Hard Bounds`](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md#search-aggregations-bucket-histogram-aggregation-hard-bounds).

### Missing value [date-histogram-missing-value]

The `missing` parameter defines how to treat documents that are missing a value. By default, they are ignored, but it is also possible to treat them as if they have a value.

$$$datehistogram-aggregation-missing-example$$$

```console
POST /sales/_search?size=0
{
  "aggs": {
    "sale_date": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "year",
        "missing": "2000/01/01" <1>
      }
    }
  }
}
```

1. Documents without a value in the `date` field will fall into the same bucket as documents that have the value `2000-01-01`.



### Order [date-histogram-order]

By default the returned buckets are sorted by their `key` ascending, but you can control the order using the `order` setting. This setting supports the same `order` functionality as [`Terms Aggregation`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-order).


### Using a script to aggregate by day of the week [date-histogram-aggregate-scripts]

When you need to aggregate the results by day of the week, run a `terms` aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) that returns the day of the week:

$$$datehistogram-aggregation-day-of-week-runtime-field$$$

```console
POST /sales/_search?size=0
{
  "runtime_mappings": {
    "date.day_of_week": {
      "type": "keyword",
      "script": "emit(doc['date'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ENGLISH))"
    }
  },
  "aggs": {
    "day_of_week": {
      "terms": { "field": "date.day_of_week" }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "day_of_week": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": "Sunday",
          "doc_count": 4
        },
        {
          "key": "Thursday",
          "doc_count": 3
        }
      ]
    }
  }
}
```

The response will contain all the buckets having the relative day of the week as key : 1 for Monday, 2 for Tuesday…​ 7 for Sunday.



