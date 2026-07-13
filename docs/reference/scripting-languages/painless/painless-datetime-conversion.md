---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime conversion [_datetime_conversion]

Datetime conversion is a switch from a numeric datetime to a complex datetime and the reverse.

## Datetime conversion examples [_datetime_conversion_examples]

* Convert a date from milliseconds:

    ```painless
    long milliSinceEpoch = 434931330000L;
    Instant instant = Instant.ofEpochMilli(milliSinceEpoch);
    ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
    ```

* Convert a date to milliseconds:

    ```painless
    ZonedDateTime zdt =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    long milliSinceEpoch = zdt.toInstant().toEpochMilli();
    ```


