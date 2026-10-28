---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime Modification [_datetime_modification]

Use either a numeric datetime or a complex datetime to do modifications such as adding several seconds to a datetime or subtracting several days from a datetime. Use standard [numeric operators](/reference/scripting-languages/painless/painless-operators-numeric.md) to modify a numeric datetime. Use [methods](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZonedDateTime) (or fields) to modify a complex datetime. Many complex datetimes are immutable, so upon modification a new complex datetime is created that requires [assignment](/reference/scripting-languages/painless/painless-variables.md#variable-assignment) or immediate use.

## Datetime modification examples [_datetime_modification_examples]

* Subtract three seconds from a numeric datetime in milliseconds:

    ```painless
    long milliSinceEpoch = 434931330000L;
    milliSinceEpoch = milliSinceEpoch - 1000L*3L;
    ```

* Add three days to a complex datetime:

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime updatedZdt = zdt.plusDays(3);
    ```

* Subtract 125 minutes from a complex datetime:

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime updatedZdt = zdt.minusMinutes(125);
    ```

* Set the year on a complex datetime:

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime updatedZdt = zdt.withYear(1976);
    ```
