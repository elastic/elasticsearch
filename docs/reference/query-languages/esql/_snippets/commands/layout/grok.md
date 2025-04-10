## `GROK` [esql-grok]

`GROK` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).

**Syntax**

```esql
GROK input "pattern"
```

**Parameters**

`input`
:   The column that contains the string you want to structure. If the column has
    multiple values, `GROK` will process each value.

`pattern`
:   A grok pattern. If a field name conflicts with an existing column, the existing column is discarded.
    If a field name is used more than once, a multi-valued column will be created with one value
    per each occurrence of the field name.

**Description**

`GROK` enables you to [extract structured data out of a string](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md).
`GROK` matches the string against patterns, based on regular expressions,
and extracts the specified patterns as columns.

Refer to [Process data with `GROK`](/reference/query-languages/esql/esql-process-data-with-dissect-grok.md#esql-process-data-with-grok) for the syntax of grok patterns.

**Examples**

The following example parses a string that contains a timestamp, an IP address,
an email address, and a number:

:::{include} ../examples/docs.csv-spec/basicGrok.md
:::

By default, `GROK` outputs keyword string columns. `int` and `float` types can
be converted by appending `:type` to the semantics in the pattern. For example
`{NUMBER:num:int}`:

:::{include} ../examples/docs.csv-spec/grokWithConversionSuffix.md
:::

For other type conversions, use [Type conversion functions](/reference/query-languages/esql/functions-operators/type-conversion-functions.md):

:::{include} ../examples/docs.csv-spec/grokWithToDatetime.md
:::

If a field name is used more than once, `GROK` creates a multi-valued column:

:::{include} ../examples/docs.csv-spec/grokWithDuplicateFieldNames.md
:::
