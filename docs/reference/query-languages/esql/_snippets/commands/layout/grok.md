## `GROK` [esql-grok]

`GROK` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).

**Syntax**

```esql
GROK input "pattern"
```

**Parameters**

`input`
:   The column that contains the string you want to structure. If the column has multiple values, `GROK` will process each value.

`pattern`
:   A grok pattern. If a field name conflicts with an existing column, the existing column is discarded. If a field name is used more than once, a multi-valued column will be created with one value per each occurrence of the field name.

**Description**

`GROK` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md). `GROK` matches the string against patterns, based on regular expressions, and extracts the specified patterns as columns.

Refer to [Process data with `GROK`](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-process-data-with-grok) for the syntax of grok patterns.

**Examples**

The following example parses a string that contains a timestamp, an IP address, an email address, and a number:

```esql
ROW a = "2023-01-23T12:15:00.000Z 127.0.0.1 some.email@foo.com 42"
| GROK a """%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num}"""
| KEEP date, ip, email, num
```

| date:keyword | ip:keyword | email:keyword | num:keyword |
| --- | --- | --- | --- |
| 2023-01-23T12:15:00.000Z | 127.0.0.1 | `some.email@foo.com` | 42 |

By default, `GROK` outputs keyword string columns. `int` and `float` types can be converted by appending `:type` to the semantics in the pattern. For example `{NUMBER:num:int}`:

```esql
ROW a = "2023-01-23T12:15:00.000Z 127.0.0.1 some.email@foo.com 42"
| GROK a """%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"""
| KEEP date, ip, email, num
```

| date:keyword | ip:keyword | email:keyword | num:integer |
| --- | --- | --- | --- |
| 2023-01-23T12:15:00.000Z | 127.0.0.1 | `some.email@foo.com` | 42 |

For other type conversions, use [Type conversion functions](/reference/query-languages/esql/functions-operators/type-conversion-functions.md):

```esql
ROW a = "2023-01-23T12:15:00.000Z 127.0.0.1 some.email@foo.com 42"
| GROK a """%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"""
| KEEP date, ip, email, num
| EVAL date = TO_DATETIME(date)
```

| ip:keyword | email:keyword | num:integer | date:date |
| --- | --- | --- | --- |
| 127.0.0.1 | `some.email@foo.com` | 42 | 2023-01-23T12:15:00.000Z |

If a field name is used more than once, `GROK` creates a multi-valued column:

```esql
FROM addresses
| KEEP city.name, zip_code
| GROK zip_code """%{WORD:zip_parts} %{WORD:zip_parts}"""
```

| city.name:keyword | zip_code:keyword | zip_parts:keyword |
| --- | --- | --- |
| Amsterdam | 1016 ED | ["1016", "ED"] |
| San Francisco | CA 94108 | ["CA", "94108"] |
| Tokyo | 100-7014 | null |


