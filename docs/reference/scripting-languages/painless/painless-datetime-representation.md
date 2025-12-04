---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Datetime representation [_datetime_representation]

Datetimes in Painless are most commonly represented as a numeric value, a string value, or a complex value.

numeric
:   A datetime representation as a number from a starting offset called an epoch; in Painless this is typically a [long](/reference/scripting-languages/painless/painless-types.md#primitive-types) as milliseconds since an epoch of `1970-01-01 00:00:00` Zulu Time.

string
:   A datetime representation as a sequence of characters defined by a standard format or a custom format; in Painless this is typically a [String](/reference/scripting-languages/painless/painless-types.md#string-type) of the standard format [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601).

complex
:   A datetime representation as a complex type ([object](/reference/scripting-languages/painless/painless-types.md#reference-types)) that abstracts away internal details of how the datetime is stored and often provides utilities for modification and comparison; in Painless this is typically a [ZonedDateTime](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared-java-time.html#painless-api-reference-shared-ZonedDateTime).

Switching between different representations of datetimes is often necessary to achieve a script’s objectives. A typical pattern in a script is to switch a numeric or string datetime to a complex datetime, modify or compare the complex datetime, and then switch it back to a numeric or string datetime for storage or to return a result.

