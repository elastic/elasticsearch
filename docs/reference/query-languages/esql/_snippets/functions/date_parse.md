## `DATE_PARSE` [esql-date_parse]

**Syntax**

:::{image} ../../../../../images/date_parse.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`datePattern`
:   The date format. Refer to the [`DateTimeFormatter` documentation](https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/time/format/DateTimeFormatter.md) for the syntax. If `null`, the function returns `null`.

`dateString`
:   Date expression as a string. If `null` or an empty string, the function returns `null`.

**Description**

Returns a date by parsing the second argument using the format specified in the first argument.

**Supported types**

| datePattern | dateString | result |
| --- | --- | --- |
| keyword | keyword | date |
| keyword | text | date |
| text | keyword | date |
| text | text | date |

**Example**

```esql
ROW date_string = "2022-05-06"
| EVAL date = DATE_PARSE("yyyy-MM-dd", date_string)
```

| date_string:keyword | date:date |
| --- | --- |
| 2022-05-06 | 2022-05-06T00:00:00.000Z |


