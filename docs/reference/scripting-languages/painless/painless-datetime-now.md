---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime now [_datetime_now]

Under most Painless contexts the current datetime, `now`, is not supported. There are two primary reasons for this. The first is that scripts are often run once per document, so each time the script is run a different `now` is returned. The second is that scripts are often run in a distributed fashion without a way to appropriately synchronize `now`. Instead, pass in a user-defined parameter with either a string datetime or numeric datetime for `now`. A numeric datetime is preferred as there is no need to parse it for comparison.

## Datetime now examples [_datetime_now_examples]

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
        % NOTCONSOLE

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
        % NOTCONSOLE

    * Script:

        ```painless
        long now = params['now'];
        ZonedDateTime inputDateTime = doc['input_datetime'];
        long millisDateTime = inputDateTime.toInstant().toEpochMilli();
        long elapsedTime = now - millisDateTime;
        ```
        % NOTCONSOLE

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
        % NOTCONSOLE

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
        % NOTCONSOLE

    * Script:

        ```painless
        String nowString = params['now'];
        ZonedDateTime nowZdt = ZonedDateTime.parse(nowString); <1>
        long now = ZonedDateTime.toInstant().toEpochMilli();
        ZonedDateTime inputDateTime = doc['input_datetime'];
        long millisDateTime = zdt.toInstant().toEpochMilli();
        long elapsedTime = now - millisDateTime;
        ```
        % NOTCONSOLE

        1. This parses the same string datetime every time the script runs. Use a numeric datetime to avoid a significant performance hit.


