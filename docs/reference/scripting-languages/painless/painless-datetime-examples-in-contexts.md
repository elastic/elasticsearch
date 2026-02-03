---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime examples in contexts [_datetime_examples_in_contexts]

Try out these Painless datetime examples that include real world contexts.

## Load the example data [_load_the_example_data]

Run the following curl commands to load the data necessary for the context examples into an {{es}} cluster:

1. Create [mappings](docs-content://manage-data/data-store/mapping.md) for the sample data.

    ```console
    PUT /messages
    {
      "mappings": {
        "properties": {
          "priority": {
            "type": "integer"
          },
          "datetime": {
            "type": "date"
          },
          "message": {
            "type": "text"
          }
        }
      }
    }
    ```

2. Load the sample data.

    ```console
    POST /_bulk
    { "index" : { "_index" : "messages", "_id" : "1" } }
    { "priority": 1, "datetime": "2019-07-17T12:13:14Z", "message": "m1" }
    { "index" : { "_index" : "messages", "_id" : "2" } }
    { "priority": 1, "datetime": "2019-07-24T01:14:59Z", "message": "m2" }
    { "index" : { "_index" : "messages", "_id" : "3" } }
    { "priority": 2, "datetime": "1983-10-14T00:36:42Z", "message": "m3" }
    { "index" : { "_index" : "messages", "_id" : "4" } }
    { "priority": 3, "datetime": "1983-10-10T02:15:15Z", "message": "m4" }
    { "index" : { "_index" : "messages", "_id" : "5" } }
    { "priority": 3, "datetime": "1983-10-10T17:18:19Z", "message": "m5" }
    { "index" : { "_index" : "messages", "_id" : "6" } }
    { "priority": 1, "datetime": "2019-08-03T17:19:31Z", "message": "m6" }
    { "index" : { "_index" : "messages", "_id" : "7" } }
    { "priority": 3, "datetime": "2019-08-04T17:20:00Z", "message": "m7" }
    { "index" : { "_index" : "messages", "_id" : "8" } }
    { "priority": 2, "datetime": "2019-08-04T18:01:01Z", "message": "m8" }
    { "index" : { "_index" : "messages", "_id" : "9" } }
    { "priority": 3, "datetime": "1983-10-10T19:00:45Z", "message": "m9" }
    { "index" : { "_index" : "messages", "_id" : "10" } }
    { "priority": 2, "datetime": "2019-07-23T23:39:54Z", "message": "m10" }
    ```
    % TEST[continued]


## Day-of-the-week bucket aggregation example [_day_of_the_week_bucket_aggregation_example]

The following example uses a [terms aggregation](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-script) as part of the [bucket script aggregation context](/reference/scripting-languages/painless/painless-bucket-script-agg-context.md) to display the number of messages from each day-of-the-week.

```console
GET /messages/_search?pretty=true
{
  "aggs": {
    "day-of-week-count": {
      "terms": {
        "script": "return doc[\"datetime\"].value.getDayOfWeekEnum();"
      }
    }
  }
}
```
% TEST[continued]

## Morning/evening bucket aggregation example [_morningevening_bucket_aggregation_example]

The following example uses a [terms aggregation](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-script) as part of the [bucket script aggregation context](/reference/scripting-languages/painless/painless-bucket-script-agg-context.md) to display the number of messages received in the morning versus the evening.

```console
GET /messages/_search?pretty=true
{
  "aggs": {
    "am-pm-count": {
      "terms": {
        "script": "return doc[\"datetime\"].value.getHour() < 12 ? \"AM\" : \"PM\";"
      }
    }
  }
}
```
% TEST[continued]

## Age of a message script field example [_age_of_a_message_script_field_example]

The following example uses a [script field](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields) as part of the [field context](/reference/scripting-languages/painless/painless-field-context.md) to display the elapsed time between "now" and when a message was received.

```console
GET /_search?pretty=true
{
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "message_age": {
      "script": {
        "source": "ZonedDateTime now = ZonedDateTime.ofInstant(Instant.ofEpochMilli(params[\"now\"]), ZoneId.of(\"Z\")); ZonedDateTime mdt = doc[\"datetime\"].value; String age; long years = mdt.until(now, ChronoUnit.YEARS); age = years + \"Y \"; mdt = mdt.plusYears(years); long months = mdt.until(now, ChronoUnit.MONTHS); age += months + \"M \"; mdt = mdt.plusMonths(months); long days = mdt.until(now, ChronoUnit.DAYS); age += days + \"D \"; mdt = mdt.plusDays(days); long hours = mdt.until(now, ChronoUnit.HOURS); age += hours + \"h \"; mdt = mdt.plusHours(hours); long minutes = mdt.until(now, ChronoUnit.MINUTES); age += minutes + \"m \"; mdt = mdt.plusMinutes(minutes); long seconds = mdt.until(now, ChronoUnit.SECONDS); age += hours + \"s\"; return age;",
        "params": {
          "now": 1574005645830
        }
      }
    }
  }
}
```
% TEST[continued]

The following shows the script broken into multiple lines:

```painless
ZonedDateTime now = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(params['now']), ZoneId.of('Z')); <1>
ZonedDateTime mdt = doc['datetime'].value; <2>

String age;

long years = mdt.until(now, ChronoUnit.YEARS); <3>
age = years + 'Y '; <4>
mdt = mdt.plusYears(years); <5>

long months = mdt.until(now, ChronoUnit.MONTHS);
age += months + 'M ';
mdt = mdt.plusMonths(months);

long days = mdt.until(now, ChronoUnit.DAYS);
age += days + 'D ';
mdt = mdt.plusDays(days);

long hours = mdt.until(now, ChronoUnit.HOURS);
age += hours + 'h ';
mdt = mdt.plusHours(hours);

long minutes = mdt.until(now, ChronoUnit.MINUTES);
age += minutes + 'm ';
mdt = mdt.plusMinutes(minutes);

long seconds = mdt.until(now, ChronoUnit.SECONDS);
age += hours + 's';

return age; <6>
```

1. Parse the datetime "now" as input from the user-defined params.
2. Store the datetime the message was received as a `ZonedDateTime`.
3. Find the difference in years between "now" and the datetime the message was received.
4. Add the difference in years later returned in the format `Y <years> ...` for the age of a message.
5. Add the years so only the remainder of the months, days, etc. remain as the difference between "now" and the datetime the message was received. Repeat this pattern until the desired granularity is reached (seconds in this example).
6. Return the age of the message in the format `Y <years> M <months> D <days> h <hours> m <minutes> s <seconds>`.

