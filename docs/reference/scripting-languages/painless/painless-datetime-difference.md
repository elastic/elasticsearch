---
navigation_title: Datetime difference
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime difference (elapsed time) [_datetime_difference_elapsed_time]

Use either two numeric datetimes or two complex datetimes to calculate the difference (elapsed time) between two different datetimes. Use [subtraction](/reference/scripting-languages/painless/painless-operators-numeric.md#subtraction-operator) to calculate the difference between two numeric datetimes of the same time unit such as milliseconds. For complex datetimes there is often a method or another complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) available to calculate the difference. Use [ChronoUnit](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time-temporal.html#painless-api-reference-shared-ChronoUnit) to calculate the difference between two complex datetimes if supported.

## Datetime difference examples [_datetime_difference_examples]

* Calculate the difference in milliseconds between two numeric datetimes:

    ```painless
    long startTimestamp = 434931327000L;
    long endTimestamp = 434931330000L;
    long differenceInMillis = endTimestamp - startTimestamp;
    ```

* Calculate the difference in milliseconds between two complex datetimes:

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 11000000, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 35, 0, ZoneId.of('Z'));
    long differenceInMillis = ChronoUnit.MILLIS.between(zdt1, zdt2);
    ```

* Calculate the difference in days between two complex datetimes:

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 11000000, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));
    long differenceInDays = ChronoUnit.DAYS.between(zdt1, zdt2);
    ```


