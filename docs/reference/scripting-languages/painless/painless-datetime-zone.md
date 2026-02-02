---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime zone [_datetime_zone]

Both string datetimes and complex datetimes have a timezone with a default of `UTC`. Numeric datetimes do not have enough explicit information to have a timezone, so `UTC` is always assumed. Use [methods](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZonedDateTime) (or fields) in conjunction with a [ZoneId](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZoneId) to change the timezone for a complex datetime. Parse a string datetime into a complex datetime to change the timezone, and then format the complex datetime back into a desired string datetime. Many complex datetimes are immutable, so upon modification a new complex datetime is created that requires [assignment](/reference/scripting-languages/painless/painless-variables.md#variable-assignment) or immediate use.

## Datetime zone examples [_datetime_zone_examples]

* Modify the timezone for a complex datetime:

    ```painless
    ZonedDateTime utc =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime pst = utc.withZoneSameInstant(ZoneId.of('America/Los_Angeles'));
    ```

* Modify the timezone for a string datetime:

    ```painless
    String gmtString = 'Thu, 13 Oct 1983 22:15:30 GMT';
    ZonedDateTime gmtZdt = ZonedDateTime.parse(gmtString,
            DateTimeFormatter.RFC_1123_DATE_TIME); <1>
    ZonedDateTime pstZdt =
            gmtZdt.withZoneSameInstant(ZoneId.of('America/Los_Angeles'));
    String pstString = pstZdt.format(DateTimeFormatter.RFC_1123_DATE_TIME);
    ```

    1. This uses a built-in `DateTimeFormatter`.


