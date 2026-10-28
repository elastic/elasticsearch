---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime Input [_datetime_input]

There are several common ways datetimes are used as input for a script determined by the [Painless context](/reference/scripting-languages/painless/painless-contexts.md). Typically, datetime input will be accessed from parameters specified by the user, from an original source document, or from an indexed document.

## Datetime input from user parameters [_datetime_input_from_user_parameters]

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

        1. This uses a custom `DateTimeFormatter`.



## Datetime input from a source document [_datetime_input_from_a_source_document]

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

        1. Access to `_source` is dependent on the Painless context.

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

        1. Access to `_source` is dependent on the Painless context.
        2. The parse method uses ISO 8601 by default.



## Datetime input from an indexed document [_datetime_input_from_an_indexed_document]

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

        1. This uses a built-in `DateTimeFormatter`.

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

