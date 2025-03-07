## `DATE_EXTRACT` [esql-date_extract]

**Syntax**

:::{image} ../../../../../images/date_extract.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`datePart`
:   Part of the date to extract.  Can be: `aligned_day_of_week_in_month`, `aligned_day_of_week_in_year`, `aligned_week_of_month`, `aligned_week_of_year`, `ampm_of_day`, `clock_hour_of_ampm`, `clock_hour_of_day`, `day_of_month`, `day_of_week`, `day_of_year`, `epoch_day`, `era`, `hour_of_ampm`, `hour_of_day`, `instant_seconds`, `micro_of_day`, `micro_of_second`, `milli_of_day`, `milli_of_second`, `minute_of_day`, `minute_of_hour`, `month_of_year`, `nano_of_day`, `nano_of_second`, `offset_seconds`, `proleptic_month`, `second_of_day`, `second_of_minute`, `year`, or `year_of_era`. Refer to [java.time.temporal.ChronoField](https://docs.oracle.com/javase/8/docs/api/java/time/temporal/ChronoField.md) for a description of these values.  If `null`, the function returns `null`.

`date`
:   Date expression. If `null`, the function returns `null`.

**Description**

Extracts parts of a date, like year, month, day, hour.

**Supported types**

| datePart | date | result |
| --- | --- | --- |
| keyword | date | long |
| keyword | date_nanos | long |
| text | date | long |
| text | date_nanos | long |

**Examples**

```esql
ROW date = DATE_PARSE("yyyy-MM-dd", "2022-05-06")
| EVAL year = DATE_EXTRACT("year", date)
```

| date:date | year:long |
| --- | --- |
| 2022-05-06T00:00:00.000Z | 2022 |

Find all events that occurred outside of business hours (before 9 AM or after 5PM), on any given date:

```esql
FROM sample_data
| WHERE DATE_EXTRACT("hour_of_day", @timestamp) < 9 AND DATE_EXTRACT("hour_of_day", @timestamp) >= 17
```

| @timestamp:date | client_ip:ip | event_duration:long | message:keyword |
| --- | --- | --- | --- |


