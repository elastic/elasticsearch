---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime comparison [_datetime_comparison]

Use either two numeric datetimes or two complex datetimes to do a datetime comparison. Use standard [comparison operators](/reference/scripting-languages/painless/painless-operators-boolean.md) to compare two numeric datetimes of the same time unit such as milliseconds. For complex datetimes there is often a method or another complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) available to do the comparison.

## Datetime comparison examples [_datetime_comparison_examples]

* Perform a `greater than` comparison of two numeric datetimes in milliseconds:

    ```painless
    long timestamp1 = 434931327000L;
    long timestamp2 = 434931330000L;

    if (timestamp1 > timestamp2) {
       // handle condition
    }
    ```

* Perform an `equality` comparison of two complex datetimes:

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));

    if (zdt1.equals(zdt2)) {
        // handle condition
    }
    ```

* Perform a `less than` comparison of two complex datetimes:

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));

    if (zdt1.isBefore(zdt2)) {
        // handle condition
    }
    ```

* Perform a `greater than` comparison of two complex datetimes

    ```painless
    ZonedDateTime zdt1 =
            ZonedDateTime.of(1983, 10, 13, 22, 15, 30, 0, ZoneId.of('Z'));
    ZonedDateTime zdt2 =
            ZonedDateTime.of(1983, 10, 17, 22, 15, 35, 0, ZoneId.of('Z'));

    if (zdt1.isAfter(zdt2)) {
        // handle condition
    }
    ```
