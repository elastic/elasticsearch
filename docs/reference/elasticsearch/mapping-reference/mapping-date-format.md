---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
---

# format [mapping-date-format]

In JSON documents, dates are represented as strings. Elasticsearch uses a set of preconfigured formats to recognize and parse these strings into a long value representing *milliseconds-since-the-epoch* in UTC.

Besides the [built-in formats](#built-in-date-formats), your own [custom formats](#custom-date-formats) can be specified using the familiar `yyyy/MM/dd` syntax:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "date": {
        "type":   "date",
        "format": "yyyy-MM-dd"
      }
    }
  }
}
```

Many APIs which support date values also support [date math](/reference/elasticsearch/rest-apis/common-options.md#date-math) expressions, such as `now-1m/d` — the current time, minus one month, rounded down to the nearest day.

## Custom date formats [custom-date-formats]

Completely customizable date formats are supported. The syntax for these is explained in [DateTimeFormatter docs](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html).

Note that while the built-in formats for week dates use the ISO definition of weekyears, custom formatters using the `Y`, `W`, or `w` field specifiers use the JDK locale definition of weekyears. This can result in different values between the built-in formats and custom formats for week dates.


## Built-in formats [built-in-date-formats]

Most of the below formats have a `strict` companion format, which means that year, month and day parts of the month must use respectively 4, 2 and 2 digits exactly, potentially prepending zeros. For instance a date like `5/11/1` would be considered invalid and would need to be rewritten to `2005/11/01` to be accepted by the date parser.

To use them, you need to prepend `strict_` to the name of the date format, for instance `strict_date_optional_time` instead of `date_optional_time`.

These strict date formats are especially useful when [date fields are dynamically mapped](docs-content://manage-data/data-store/mapping/dynamic-field-mapping.md#date-detection) in order to make sure to not accidentally map irrelevant strings as dates.

The following tables lists all the defaults ISO formats supported:

`epoch_millis`
:   A formatter for the number of milliseconds since the epoch. Note, that this timestamp is subject to the limits of a Java `Long.MIN_VALUE` and `Long.MAX_VALUE`.

`epoch_second`
:   A formatter for the number of seconds since the epoch. Note, that this timestamp is subject to the limits of a Java `Long.MIN_VALUE` and `Long. MAX_VALUE` divided by 1000 (the number of milliseconds in a second).

$$$strict-date-time$$$`date_optional_time` or `strict_date_optional_time`
:   A generic ISO datetime parser, where the date must include the year at a minimum, and the time (separated by `T`), is optional. Examples: `yyyy-MM-dd'T'HH:mm:ss.SSSZ` or  `yyyy-MM-dd`.

    ```
    NOTE: When using `date_optional_time`, the parsing is lenient and will attempt to parse
    numbers as a year (e.g. `292278994` will be parsed as a year). This can lead to unexpected results
    when paired with a numeric focused format like `epoch_second` and `epoch_millis`.
    It is recommended you use `strict_date_optional_time` when pairing with a numeric focused format.
    ```


$$$strict-date-time-nanos$$$`strict_date_optional_time_nanos`
:   A generic ISO datetime parser, where the date must include the year at a minimum, and the time (separated by `T`), is optional. The fraction of a second part has a nanosecond resolution. Examples: `yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ` or  `yyyy-MM-dd`.

`basic_date`
:   A basic formatter for a full date as four digit year, two digit month of year, and two digit day of month: `yyyyMMdd`.

`basic_date_time`
:   A basic formatter that combines a basic date and time, separated by a *T*: `yyyyMMdd'T'HHmmss.SSSZ`.

`basic_date_time_no_millis`
:   A basic formatter that combines a basic date and time without millis, separated by a *T*: `yyyyMMdd'T'HHmmssZ`.

`basic_ordinal_date`
:   A formatter for a full ordinal date, using a four digit year and three digit dayOfYear: `yyyyDDD`.

`basic_ordinal_date_time`
:   A formatter for a full ordinal date and time, using a four digit year and three digit dayOfYear: `yyyyDDD'T'HHmmss.SSSZ`.

`basic_ordinal_date_time_no_millis`
:   A formatter for a full ordinal date and time without millis, using a four digit year and three digit dayOfYear: `yyyyDDD'T'HHmmssZ`.

`basic_time`
:   A basic formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, three digit millis, and time zone offset: `HHmmss.SSSZ`.

`basic_time_no_millis`
:   A basic formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, and time zone offset: `HHmmssZ`.

`basic_t_time`
:   A basic formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, three digit millis, and time zone off set prefixed by *T*: `'T'HHmmss.SSSZ`.

`basic_t_time_no_millis`
:   A basic formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, and time zone offset prefixed by *T*: `'T'HHmmssZ`.

`basic_week_date` or `strict_basic_week_date`
:   A basic formatter for a full date as four digit weekyear, two digit week of weekyear, and one digit day of week: `xxxx'W'wwe`.

`basic_week_date_time` or `strict_basic_week_date_time`
:   A basic formatter that combines a basic weekyear date and time, separated by a *T*: `xxxx'W'wwe'T'HHmmss.SSSZ`.

`basic_week_date_time_no_millis` or `strict_basic_week_date_time_no_millis`
:   A basic formatter that combines a basic weekyear date and time without millis, separated by a *T*: `xxxx'W'wwe'T'HHmmssZ`.

`date` or `strict_date`
:   A formatter for a full date as four digit year, two digit month of year, and two digit day of month: `yyyy-MM-dd`.

`date_hour` or `strict_date_hour`
:   A formatter that combines a full date and two digit hour of day: `yyyy-MM-dd'T'HH`.

`date_hour_minute` or `strict_date_hour_minute`
:   A formatter that combines a full date, two digit hour of day, and two digit minute of hour: `yyyy-MM-dd'T'HH:mm`.

`date_hour_minute_second` or `strict_date_hour_minute_second`
:   A formatter that combines a full date, two digit hour of day, two digit minute of hour, and two digit second of minute: `yyyy-MM-dd'T'HH:mm:ss`.

`date_hour_minute_second_fraction` or `strict_date_hour_minute_second_fraction`
:   A formatter that combines a full date, two digit hour of day, two digit minute of hour, two digit second of minute, and three digit fraction of second: `yyyy-MM-dd'T'HH:mm:ss.SSS`.

`date_hour_minute_second_millis` or `strict_date_hour_minute_second_millis`
:   A formatter that combines a full date, two digit hour of day, two digit minute of hour, two digit second of minute, and three digit fraction of second: `yyyy-MM-dd'T'HH:mm:ss.SSS`.

`date_time` or `strict_date_time`
:   A formatter that combines a full date and time, separated by a *T*: `yyyy-MM-dd'T'HH:mm:ss.SSSZ`.

`date_time_no_millis` or `strict_date_time_no_millis`
:   A formatter that combines a full date and time without millis, separated by a *T*: `yyyy-MM-dd'T'HH:mm:ssZ`.

`hour` or `strict_hour`
:   A formatter for a two digit hour of day: `HH`

`hour_minute` or `strict_hour_minute`
:   A formatter for a two digit hour of day and two digit minute of hour: `HH:mm`.

`hour_minute_second` or `strict_hour_minute_second`
:   A formatter for a two digit hour of day, two digit minute of hour, and two digit second of minute: `HH:mm:ss`.

`hour_minute_second_fraction` or `strict_hour_minute_second_fraction`
:   A formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, and three digit fraction of second: `HH:mm:ss.SSS`.

`hour_minute_second_millis` or `strict_hour_minute_second_millis`
:   A formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, and three digit fraction of second: `HH:mm:ss.SSS`.

`ordinal_date` or `strict_ordinal_date`
:   A formatter for a full ordinal date, using a four digit year and three digit dayOfYear: `yyyy-DDD`.

`ordinal_date_time` or `strict_ordinal_date_time`
:   A formatter for a full ordinal date and time, using a four digit year and three digit dayOfYear: `yyyy-DDD'T'HH:mm:ss.SSSZ`.

`ordinal_date_time_no_millis` or `strict_ordinal_date_time_no_millis`
:   A formatter for a full ordinal date and time without millis, using a four digit year and three digit dayOfYear: `yyyy-DDD'T'HH:mm:ssZ`.

`time` or `strict_time`
:   A formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, three digit fraction of second, and time zone offset: `HH:mm:ss.SSSZ`.

`time_no_millis` or `strict_time_no_millis`
:   A formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, and time zone offset: `HH:mm:ssZ`.

`t_time` or `strict_t_time`
:   A formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, three digit fraction of second, and time zone offset prefixed by *T*: `'T'HH:mm:ss.SSSZ`.

`t_time_no_millis` or `strict_t_time_no_millis`
:   A formatter for a two digit hour of day, two digit minute of hour, two digit second of minute, and time zone offset prefixed by *T*: `'T'HH:mm:ssZ`.

`week_date` or `strict_week_date`
:   A formatter for a full date as four digit weekyear, two digit week of weekyear, and one digit day of week: `YYYY-'W'ww-e`. This uses the ISO week-date definition.

`week_date_time` or `strict_week_date_time`
:   A formatter that combines a full weekyear date and time, separated by a *T*: `YYYY-'W'ww-e'T'HH:mm:ss.SSSZ`. This uses the ISO week-date definition.

`week_date_time_no_millis` or `strict_week_date_time_no_millis`
:   A formatter that combines a full weekyear date and time without millis, separated by a *T*: `YYYY-'W'ww-e'T'HH:mm:ssZ`. This uses the ISO week-date definition.

`weekyear` or `strict_weekyear`
:   A formatter for a four digit weekyear: `YYYY`. This uses the ISO week-date definition.

`weekyear_week` or `strict_weekyear_week`
:   A formatter for a four digit weekyear and two digit week of weekyear: `YYYY-'W'ww`. This uses the ISO week-date definition.

`weekyear_week_day` or `strict_weekyear_week_day`
:   A formatter for a four digit weekyear, two digit week of weekyear, and one digit day of week: `YYYY-'W'ww-e`. This uses the ISO week-date definition.

`year` or `strict_year`
:   A formatter for a four digit year: `yyyy`.

`year_month` or `strict_year_month`
:   A formatter for a four digit year and two digit month of year: `yyyy-MM`.

`year_month_day` or `strict_year_month_day`
:   A formatter for a four digit year, two digit month of year, and two digit day of month: `yyyy-MM-dd`.


