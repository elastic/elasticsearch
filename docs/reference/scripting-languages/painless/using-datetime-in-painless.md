---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-datetime.html
---

# Using datetime in Painless [painless-datetime]

## Datetime API [_datetime_api]

Datetimes in Painless use the standard Java libraries and are available through the Painless [Shared API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html). Most of the classes from the following Java packages are available to use in Painless scripts:

* [java.time](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html)
* [java.time.chrono](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-chrono.html)
* [java.time.format](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-format.html)
* [java.time.temporal](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-temporal.html)
* [java.time.zone](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-zone.html)


## Datetime Representation [_datetime_representation]

Datetimes in Painless are most commonly represented as a numeric value, a string value, or a complex value.

numeric
:   a datetime representation as a number from a starting offset called an epoch; in Painless this is typically a [long](/reference/scripting-languages/painless/painless-types.md#primitive-types) as milliseconds since an epoch of 1970-01-01 00:00:00 Zulu Time

string
:   a datetime representation as a sequence of characters defined by a standard format or a custom format; in Painless this is typically a [String](/reference/scripting-languages/painless/painless-types.md#string-type) of the standard format [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)

complex
:   a datetime representation as a complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) that abstracts away internal details of how the datetime is stored and often provides utilities for modification and comparison; in Painless this is typically a [ZonedDateTime](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZonedDateTime)

Switching between different representations of datetimes is often necessary to achieve a script’s objective(s). A typical pattern in a script is to switch a numeric or string datetime to a complex datetime, modify or compare the complex datetime, and then switch it back to a numeric or string datetime for storage or to return a result.


## Datetime Parsing and Formatting [_datetime_parsing_and_formatting]

Datetime parsing is a switch from a string datetime to a complex datetime, and datetime formatting is a switch from a complex datetime to a string datetime.

A [DateTimeFormatter](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-format.html#painless-api-reference-shared-DateTimeFormatter) is a complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) that defines the allowed sequence of characters for a string datetime. Datetime parsing and formatting often require a DateTimeFormatter. For more information about how to use a DateTimeFormatter see the [Java documentation](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html).

### Datetime Parsing Examples [_datetime_parsing_examples]

* parse from milliseconds

    ```painless
    String milliSinceEpochString = "434931330000";
    long milliSinceEpoch = Long.parseLong(milliSinceEpochString);
    Instant instant = Instant.ofEpochMilli(milliSinceEpoch);
    ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
    ```

* parse from ISO 8601

    ```painless
    String datetime = '1983-10-13T22:15:30Z';
    ZonedDateTime zdt = ZonedDateTime.parse(datetime); <1>
    ```

    1. Note the parse method uses ISO 8601 by default.

* parse from RFC 1123

    ```painless
    String datetime = 'Thu, 13 Oct 1983 22:15:30 GMT';
    ZonedDateTime zdt = ZonedDateTime.parse(datetime,
            DateTimeFormatter.RFC_1123_DATE_TIME); <1>
    ```

    1. Note the use of a built-in DateTimeFormatter.

* parse from a custom format

    ```painless
    String datetime = 'custom y 1983 m 10 d 13 22:15:30 Z';
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(
            "'custom' 'y' yyyy 'm' MM 'd' dd HH:mm:ss VV");
    ZonedDateTime zdt = ZonedDateTime.parse(datetime, dtf); <1>
    ```

    1. Note the use of a custom DateTimeFormatter.



### Datetime Formatting Examples [_datetime_formatting_examples]

* format to ISO 8601

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    String datetime = zdt.format(DateTimeFormatter.ISO_INSTANT); <1>
    ```

    1. Note the use of a built-in DateTimeFormatter.

* format to a custom format

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(
            "'date:' yyyy/MM/dd 'time:' HH:mm:ss");
    String datetime = zdt.format(dtf); <1>
    ```

    1. Note the use of a custom DateTimeFormatter.




## Datetime Conversion [_datetime_conversion]

Datetime conversion is a switch from a numeric datetime to a complex datetime and vice versa.

### Datetime Conversion Examples [_datetime_conversion_examples]

* convert from milliseconds

    ```painless
    long milliSinceEpoch = 434931330000L;
    Instant instant = Instant.ofEpochMilli(milliSinceEpoch);
    ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
    ```

* convert to milliseconds

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    long milliSinceEpoch = zdt.toInstant().toEpochMilli();
    ```




## Datetime Pieces [_datetime_pieces]

Datetime representations often contain the data to extract individual datetime pieces such as year, hour, timezone, etc. Use individual pieces of a datetime to create a complex datetime, and use a complex datetime to extract individual pieces.

### Datetime Pieces Examples [_datetime_pieces_examples]

* create a complex datetime from pieces

    ```painless
    int year = 1983;
    int month = 10;
    int day = 13;
    int hour = 22;
    int minutes = 15;
    int seconds = 30;
    int nanos = 0;
    ZonedDateTime zdt = ZonedDateTime.of(
            year, month, day, hour, minutes, seconds, nanos, ZoneId.of('Z'));
    ```

* extract pieces from a complex datetime

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 100, ZoneId.of(tz));
    int year = zdt.getYear();
    int month = zdt.getMonthValue();
    int day = zdt.getDayOfMonth();
    int hour = zdt.getHour();
    int minutes = zdt.getMinute();
    int seconds = zdt.getSecond();
    int nanos = zdt.getNano();
    ```




## Datetime Modification [_datetime_modification]

Use either a numeric datetime or a complex datetime to do modification such as adding several seconds to a datetime or subtracting several days from a datetime. Use standard [numeric operators](/reference/scripting-languages/painless/painless-operators-numeric.md) to modify a numeric datetime. Use [methods](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZonedDateTime) (or fields) to modify a complex datetime. Note many complex datetimes are immutable so upon modification a new complex datetime is created that requires [assignment](/reference/scripting-languages/painless/painless-variables.md#variable-assignment) or immediate use.

### Datetime Modification Examples [_datetime_modification_examples]

* Subtract three seconds from a numeric datetime in milliseconds

    ```painless
    long milliSinceEpoch = 434931330000L;
    milliSinceEpoch = milliSinceEpoch - 1000L*3L;
    ```

* Add three days to a complex datetime

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime updatedZdt = zdt.plusDays(3);
    ```

* Subtract 125 minutes from a complex datetime

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime updatedZdt = zdt.minusMinutes(125);
    ```

* Set the year on a complex datetime

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime updatedZdt = zdt.withYear(1976);
    ```




## Datetime Difference (Elapsed Time) [_datetime_difference_elapsed_time]

Use either two numeric datetimes or two complex datetimes to calculate the difference (elapsed time) between two different datetimes. Use [subtraction](/reference/scripting-languages/painless/painless-operators-numeric.md#subtraction-operator) to calculate the difference between two numeric datetimes of the same time unit such as milliseconds. For complex datetimes there is often a method or another complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) available to calculate the difference. Use [ChronoUnit](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-temporal.html#painless-api-reference-shared-ChronoUnit) to calculate the difference between two complex datetimes if supported.

### Datetime Difference Examples [_datetime_difference_examples]

* Difference in milliseconds between two numeric datetimes

    ```painless
    long startTimestamp = 434931327000L;
    long endTimestamp = 434931330000L;
    long differenceInMillis = endTimestamp - startTimestamp;
    ```

* Difference in milliseconds between two complex datetimes

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 11000000, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 35, 0, ZoneId.of('Z'));
    long differenceInMillis = ChronoUnit.MILLIS.between(zdt1, zdt2);
    ```

* Difference in days between two complex datetimes

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 11000000, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));
    long differenceInDays = ChronoUnit.DAYS.between(zdt1, zdt2);
    ```




## Datetime Comparison [_datetime_comparison]

Use either two numeric datetimes or two complex datetimes to do a datetime comparison. Use standard [comparison operators](/reference/scripting-languages/painless/painless-operators-boolean.md) to compare two numeric datetimes of the same time unit such as milliseconds. For complex datetimes there is often a method or another complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) available to do the comparison.

### Datetime Comparison Examples [_datetime_comparison_examples]

* Greater than comparison of two numeric datetimes in milliseconds

    ```painless
    long timestamp1 = 434931327000L;
    long timestamp2 = 434931330000L;

    if (timestamp1 > timestamp2) {
       // handle condition
    }
    ```

* Equality comparison of two complex datetimes

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));

    if (zdt1.equals(zdt2)) {
        // handle condition
    }
    ```

* Less than comparison of two complex datetimes

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));

    if (zdt1.isBefore(zdt2)) {
        // handle condition
    }
    ```

* Greater than comparison of two complex datetimes

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));

    if (zdt1.isAfter(zdt2)) {
        // handle condition
    }
    ```




## Datetime Zone [_datetime_zone]

Both string datetimes and complex datetimes have a timezone with a default of `UTC`. Numeric datetimes do not have enough explicit information to have a timezone, so `UTC` is always assumed. Use [methods](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZonedDateTime) (or fields) in conjunction with a [ZoneId](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZoneId) to change the timezone for a complex datetime. Parse a string datetime into a complex datetime to change the timezone, and then format the complex datetime back into a desired string datetime. Note many complex datetimes are immutable so upon modification a new complex datetime is created that requires [assignment](/reference/scripting-languages/painless/painless-variables.md#variable-assignment) or immediate use.

### Datetime Zone Examples [_datetime_zone_examples]

* Modify the timezone for a complex datetime

    ```painless
    ZonedDateTime utc =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime pst = utc.withZoneSameInstant(ZoneId.of('America/Los_Angeles'));
    ```

* Modify the timezone for a string datetime

    ```painless
    String gmtString = 'Thu, 13 Oct 1983 22:15:30 GMT';
    ZonedDateTime gmtZdt = ZonedDateTime.parse(gmtString,
            DateTimeFormatter.RFC_1123_DATE_TIME); <1>
    ZonedDateTime pstZdt =
            gmtZdt.withZoneSameInstant(ZoneId.of('America/Los_Angeles'));
    String pstString = pstZdt.format(DateTimeFormatter.RFC_1123_DATE_TIME);
    ```

    1. Note the use of a built-in DateTimeFormatter.




## Datetime Input [_datetime_input]

There are several common ways datetimes are used as input for a script determined by the [Painless context](/reference/scripting-languages/painless/painless-contexts.md). Typically, datetime input will be accessed from parameters specified by the user, from an original source document, or from an indexed document.

### Datetime Input From User Parameters [_datetime_input_from_user_parameters]

Use the [params section](docs-content://explore-analyze/scripting/modules-scripting-using.md) during script specification to pass in a numeric datetime or string datetime as a script input. Access to user-defined parameters within a script is dependent on the Painless context, though, the parameters are most commonly accessible through an input called `params`.

**Examples**

* Parse a numeric datetime from user parameters to a complex datetime

    * Input:

        ```JSON
        ...
        "script": {
            ...
            "params": {
                "input_datetime": 434931327000
            }
        }
        ...
        ```

    * Script:

        ```painless
        long inputDateTime = params['input_datetime'];
        Instant instant = Instant.ofEpochMilli(inputDateTime);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
        ```

* Parse a string datetime from user parameters to a complex datetime

    * Input:

        ```JSON
        ...
        "script": {
            ...
            "params": {
                "input_datetime": "custom y 1983 m 10 d 13 22:15:30 Z"
            }
        }
        ...
        ```

    * Script:

        ```painless
        String datetime = params['input_datetime'];
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(
                "'custom' 'y' yyyy 'm' MM 'd' dd HH:mm:ss VV");
        ZonedDateTime zdt = ZonedDateTime.parse(datetime, dtf); <1>
        ```

        1. Note the use of a custom DateTimeFormatter.



### Datetime Input From a Source Document [_datetime_input_from_a_source_document]

Use an original [source](/reference/elasticsearch/mapping-reference/mapping-source-field.md) document as a script input to access a numeric datetime or string datetime for a specific field within that document. Access to an original source document within a script is dependent on the Painless context and is not always available. An original source document is most commonly accessible through an input called `ctx['_source']` or `params['_source']`.

**Examples**

* Parse a numeric datetime from a sourced document to a complex datetime

    * Input:

        ```JSON
        {
          ...
          "input_datetime": 434931327000
          ...
        }
        ```

    * Script:

        ```painless
        long inputDateTime = ctx['_source']['input_datetime']; <1>
        Instant instant = Instant.ofEpochMilli(inputDateTime);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
        ```

        1. Note access to `_source` is dependent on the Painless context.

* Parse a string datetime from a sourced document to a complex datetime

    * Input:

        ```JSON
        {
          ...
          "input_datetime": "1983-10-13T22:15:30Z"
          ...
        }
        ```

    * Script:

        ```painless
        String datetime = params['_source']['input_datetime']; <1>
        ZonedDateTime zdt = ZonedDateTime.parse(datetime); <2>
        ```

        1. Note access to `_source` is dependent on the Painless context.
        2. Note the parse method uses ISO 8601 by default.



### Datetime Input From an Indexed Document [_datetime_input_from_an_indexed_document]

Use an indexed document as a script input to access a complex datetime for a specific field within that document where the field is mapped as a [standard date](/reference/elasticsearch/mapping-reference/date.md) or a [nanosecond date](/reference/elasticsearch/mapping-reference/date_nanos.md). Numeric datetime fields mapped as [numeric](/reference/elasticsearch/mapping-reference/number.md) and string datetime fields mapped as [keyword](/reference/elasticsearch/mapping-reference/keyword.md) are accessible through an indexed document as well. Access to an indexed document within a script is dependent on the Painless context and is not always available. An indexed document is most commonly accessible through an input called `doc`.

**Examples**

* Format a complex datetime from an indexed document to a string datetime

    * Assumptions:

        * The field `input_datetime` exists in all indexes as part of the query
        * All indexed documents contain the field `input_datetime`

    * Mappings:

        ```JSON
        {
          "mappings": {
            ...
            "properties": {
              ...
              "input_datetime": {
                "type": "date"
              }
              ...
            }
            ...
          }
        }
        ```

    * Script:

        ```painless
        ZonedDateTime input = doc['input_datetime'].value;
        String output = input.format(DateTimeFormatter.ISO_INSTANT); <1>
        ```

        1. Note the use of a built-in DateTimeFormatter.

* Find the difference between two complex datetimes from an indexed document

    * Assumptions:

        * The fields `start` and `end` may **not** exist in all indexes as part of the query
        * The fields `start` and `end` may **not** have values in all indexed documents

    * Mappings:

        ```JSON
        {
          "mappings": {
            ...
            "properties": {
              ...
              "start": {
                "type": "date"
              },
              "end": {
                "type": "date"
              }
              ...
            }
            ...
          }
        }
        ```

    * Script:

        ```painless
        if (doc.containsKey('start') && doc.containsKey('end')) { <1>

            if (doc['start'].size() > 0 && doc['end'].size() > 0) { <2>

                ZonedDateTime start = doc['start'].value;
                ZonedDateTime end = doc['end'].value;
                long differenceInMillis = ChronoUnit.MILLIS.between(start, end);

                // handle difference in times
            } else {
                // handle fields without values
            }
        } else {
            // handle index with missing fields
        }
        ```

        1. When a query’s results span multiple indexes, some indexes may not contain a specific field. Use the `containsKey` method call on the `doc` input to ensure a field exists as part of the index for the current document.
        2. Some fields within a document may have no values. Use the `size` method call on a field within the `doc` input to ensure that field has at least one value for the current document.




## Datetime Now [_datetime_now]

Under most Painless contexts the current datetime, `now`, is not supported. There are two primary reasons for this. The first is that scripts are often run once per document, so each time the script is run a different `now` is returned. The second is that scripts are often run in a distributed fashion without a way to appropriately synchronize `now`. Instead, pass in a user-defined parameter with either a string datetime or numeric datetime for `now`. A numeric datetime is preferred as there is no need to parse it for comparison.

### Datetime Now Examples [_datetime_now_examples]

* Use a numeric datetime as `now`

    * Assumptions:

        * The field `input_datetime` exists in all indexes as part of the query
        * All indexed documents contain the field `input_datetime`

    * Mappings:

        ```JSON
        {
          "mappings": {
              ...
              "properties": {
                ...
                "input_datetime": {
                "type": "date"
              }
              ...
            }
            ...
          }
        }
        ```

    * Input:

        ```JSON
        ...
        "script": {
            ...
            "params": {
                "now": <generated numeric datetime in milliseconds since epoch>
            }
        }
        ...
        ```

    * Script:

        ```painless
        long now = params['now'];
        ZonedDateTime inputDateTime = doc['input_datetime'];
        long millisDateTime = inputDateTime.toInstant().toEpochMilli();
        long elapsedTime = now - millisDateTime;
        ```

* Use a string datetime as `now`

    * Assumptions:

        * The field `input_datetime` exists in all indexes as part of the query
        * All indexed documents contain the field `input_datetime`

    * Mappings:

        ```JSON
        {
          "mappings": {
            ...
            "properties": {
              ...
              "input_datetime": {
                "type": "date"
              }
              ...
            }
            ...
          }
        }
        ```

    * Input:

        ```JSON
        ...
        "script": {
            ...
            "params": {
                "now": "<generated string datetime in ISO-8601>"
            }
        }
        ...
        ```

    * Script:

        ```painless
        String nowString = params['now'];
        ZonedDateTime nowZdt = ZonedDateTime.parse(nowString); <1>
        long now = ZonedDateTime.toInstant().toEpochMilli();
        ZonedDateTime inputDateTime = doc['input_datetime'];
        long millisDateTime = zdt.toInstant().toEpochMilli();
        long elapsedTime = now - millisDateTime;
        ```

        1. Note this parses the same string datetime every time the script runs. Use a numeric datetime to avoid a significant performance hit.




## Datetime Examples in Contexts [_datetime_examples_in_contexts]

### Load the Example Data [_load_the_example_data]

Run the following curl commands to load the data necessary for the context examples into an Elasticsearch cluster:

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



### Day-of-the-Week Bucket Aggregation Example [_day_of_the_week_bucket_aggregation_example]

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


### Morning/Evening Bucket Aggregation Example [_morningevening_bucket_aggregation_example]

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


### Age of a Message Script Field Example [_age_of_a_message_script_field_example]

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




