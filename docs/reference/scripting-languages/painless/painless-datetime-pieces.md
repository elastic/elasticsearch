---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime Pieces [_datetime_pieces]

Datetime representations often contain the data to extract individual datetime pieces such as year, hour, timezone, and so on. Use individual pieces of a datetime to create a complex datetime, and use a complex datetime to extract individual pieces.

## Datetime Pieces Examples [_datetime_pieces_examples]

* Create a complex datetime from pieces:

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

* Extract pieces from a complex datetime:

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

