---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime parsing and formatting [_datetime_parsing_and_formatting]

Datetime parsing is a switch from a string datetime to a complex datetime, and datetime formatting is a switch from a complex datetime to a string datetime.

A [DateTimeFormatter](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-format.html#painless-api-reference-shared-DateTimeFormatter) is a complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) that defines the allowed sequence of characters for a string datetime. Datetime parsing and formatting often require a DateTimeFormatter. For more information about how to use a DateTimeFormatter see the [Java documentation](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html).

## Datetime parsing examples [_datetime_parsing_examples]

* Parse from milliseconds:

    ```painless
    String milliSinceEpochString = "434931330000";
    long milliSinceEpoch = Long.parseLong(milliSinceEpochString);
    Instant instant = Instant.ofEpochMilli(milliSinceEpoch);
    ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
    ```

* Parse from ISO 8601:

    ```painless
    String datetime = '1983-10-13T22:15:30Z';
    ZonedDateTime zdt = ZonedDateTime.parse(datetime); <1>
    ```

    1. The parse method uses ISO 8601 by default.

* Parse from RFC 1123:

    ```painless
    String datetime = 'Thu, 13 Oct 1983 22:15:30 GMT';
    ZonedDateTime zdt = ZonedDateTime.parse(datetime,
            DateTimeFormatter.RFC_1123_DATE_TIME); <1>
    ```

    1. This uses a built-in `DateTimeFormatter`.

* Parse from a custom format:

    ```painless
    String datetime = 'custom y 1983 m 10 d 13 22:15:30 Z';
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(
            "'custom' 'y' yyyy 'm' MM 'd' dd HH:mm:ss VV");
    ZonedDateTime zdt = ZonedDateTime.parse(datetime, dtf); <1>
    ```

    1. This uses a custom `DateTimeFormatter`.



## Datetime formatting examples [_datetime_formatting_examples]

* Format to ISO 8601:

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    String datetime = zdt.format(DateTimeFormatter.ISO_INSTANT); <1>
    ```

    1. This uses a built-in `DateTimeFormatter`.

* Format to a custom format:

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(
            "'date:' yyyy/MM/dd 'time:' HH:mm:ss");
    String datetime = zdt.format(dtf); <1>
    ```

    1. This uses a custom `DateTimeFormatter`.

