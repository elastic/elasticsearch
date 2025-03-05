## `TO_DATETIME` [esql-to_datetime]

**Syntax**

:::{image} ../../../../../images/to_datetime.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input value to a date value. A string will only be successfully converted if it’s respecting the format `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`. To convert dates in other formats, use [`DATE_PARSE`](../../esql-functions-operators.md#esql-date_parse).

::::{note}
Note that when converting from nanosecond resolution to millisecond resolution with this function, the nanosecond date is truncated, not rounded.
::::


**Supported types**

| field | result |
| --- | --- |
| date | date |
| date_nanos | date |
| double | date |
| integer | date |
| keyword | date |
| long | date |
| text | date |
| unsigned_long | date |

**Examples**

```esql
ROW string = ["1953-09-02T00:00:00.000Z", "1964-06-02T00:00:00.000Z", "1964-06-02 00:00:00"]
| EVAL datetime = TO_DATETIME(string)
```

| string:keyword | datetime:date |
| --- | --- |
| ["1953-09-02T00:00:00.000Z", "1964-06-02T00:00:00.000Z", "1964-06-02 00:00:00"] | [1953-09-02T00:00:00.000Z, 1964-06-02T00:00:00.000Z] |

Note that in this example, the last value in the source multi-valued field has not been converted. The reason being that if the date format is not respected, the conversion will result in a **null** value. When this happens a *Warning* header is added to the response. The header will provide information on the source of the failure:

`"Line 1:112: evaluation of [TO_DATETIME(string)] failed, treating result as null. "Only first 20 failures recorded."`

A following header will contain the failure reason and the offending value:

`"java.lang.IllegalArgumentException: failed to parse date field [1964-06-02 00:00:00] with format [yyyy-MM-dd'T'HH:mm:ss.SSS'Z']"`

If the input parameter is of a numeric type, its value will be interpreted as milliseconds since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time). For example:

```esql
ROW int = [0, 1]
| EVAL dt = TO_DATETIME(int)
```

| int:integer | dt:date |
| --- | --- |
| [0, 1] | [1970-01-01T00:00:00.000Z, 1970-01-01T00:00:00.001Z] |


