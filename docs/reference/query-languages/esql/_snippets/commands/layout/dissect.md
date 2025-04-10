## `DISSECT` [esql-dissect]

`DISSECT` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).

**Syntax**

```esql
DISSECT input "pattern" [APPEND_SEPARATOR="<separator>"]
```

**Parameters**

`input`
:   The column that contains the string you want to structure.  If the column has
multiple values, `DISSECT` will process each value.

`pattern`
:   A [dissect pattern](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-dissect-patterns).
    If a field name conflicts with an existing column, the existing column is dropped.
    If a field name is used more than once, only the rightmost duplicate creates a column.

`<separator>`
:   A string used as the separator between appended values, when using the [append modifier](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-append-modifier).

**Description**

`DISSECT` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).
`DISSECT` matches the string against a delimiter-based pattern, and extracts the specified keys as columns.

Refer to [Process data with `DISSECT`](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-process-data-with-dissect) for the syntax of dissect patterns.

**Examples**

The following example parses a string that contains a timestamp, some text, and
an IP address:

:::{include} ../examples/docs.csv-spec/basicDissect.md
:::

By default, `DISSECT` outputs keyword string columns. To convert to another
type, use [Type conversion functions](/reference/query-languages/esql/functions-operators/type-conversion-functions.md):

:::{include} ../examples/docs.csv-spec/dissectWithToDatetime.md
:::
