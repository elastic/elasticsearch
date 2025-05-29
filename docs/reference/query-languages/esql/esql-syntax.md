---
navigation_title: "Basic syntax"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-syntax.html
---

# Basic {{esql}} syntax [esql-syntax]

## Query structure [esql-basic-syntax]

An {{esql}} query is composed of a [source command](/reference/query-languages/esql/esql-commands.md) followed by an optional series of [processing commands](/reference/query-languages/esql/esql-commands.md), separated by a pipe character: `|`. For example:

```esql
source-command
| processing-command1
| processing-command2
```

The result of a query is the table produced by the final processing command.

For an overview of all supported commands, functions, and operators, refer to [Commands](/reference/query-languages/esql/esql-commands.md) and [Functions and operators](/reference/query-languages/esql/esql-functions-operators.md).

::::{note}
For readability, this documentation puts each processing command on a new line. However, you can write an {{esql}} query as a single line. The following query is identical to the previous one:

```esql
source-command | processing-command1 | processing-command2
```

::::



### Identifiers [esql-identifiers]

Identifiers need to be quoted with backticks (```) if:

* they don’t start with a letter, `_` or `@`
* any of the other characters is not a letter, number, or `_`

For example:

```esql
FROM index
| KEEP `1.field`
```

When referencing a function alias that itself uses a quoted identifier, the backticks of the quoted identifier need to be escaped with another backtick. For example:

```esql
FROM index
| STATS COUNT(`1.field`)
| EVAL my_count = `COUNT(``1.field``)`
```


### Literals [esql-literals]

{{esql}} currently supports numeric and string literals.


#### String literals [esql-string-literals]

A string literal is a sequence of unicode characters delimited by double quotes (`"`).

```esql
// Filter by a string value
FROM index
| WHERE first_name == "Georgi"
```

If the literal string itself contains quotes, these need to be escaped (`\\"`). {{esql}} also supports the triple-quotes (`"""`) delimiter, for convenience:

```esql
ROW name = """Indiana "Indy" Jones"""
```

The special characters CR, LF and TAB can be provided with the usual escaping: `\r`, `\n`, `\t`, respectively.


#### Numerical literals [esql-numeric-literals]

The numeric literals are accepted in decimal and in the scientific notation with the exponent marker (`e` or `E`), starting either with a digit, decimal point `.` or the negative sign `-`:

```sql
1969    -- integer notation
3.14    -- decimal notation
.1234   -- decimal notation starting with decimal point
4E5     -- scientific notation (with exponent marker)
1.2e-3  -- scientific notation with decimal point
-.1e2   -- scientific notation starting with the negative sign
```

The integer numeric literals are implicitly converted to the `integer`, `long` or the `double` type, whichever can first accommodate the literal’s value.

The floating point literals are implicitly converted the `double` type.

To obtain constant values of different types, use one of the numeric [conversion functions](/reference/query-languages/esql/functions-operators/type-conversion-functions.md).


### Comments [esql-comments]

{{esql}} uses C++ style comments:

* double slash `//` for single line comments
* `/*` and `*/` for block comments

```esql
// Query the employees index
FROM employees
| WHERE height > 2
```

```esql
FROM /* Query the employees index */ employees
| WHERE height > 2
```

```esql
FROM employees
/* Query the
 * employees
 * index */
| WHERE height > 2
```


### Timespan literals [esql-timespan-literals]

Datetime intervals and timespans can be expressed using timespan literals. Timespan literals are a combination of a number and a temporal unit. The supported temporal units are listed in [time span unit](/reference/query-languages/esql/esql-time-spans.md#esql-time-spans-table). More examples of the usages of time spans can be found in [Use time spans in ES|QL](/reference/query-languages/esql/esql-time-spans.md).

Timespan literals are not whitespace sensitive. These expressions are all valid:

* `1day`
* `1 day`
* `1       day`


### Function named parameters [esql-function-named-params]

Some functions like [match](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) use named parameters to provide additional options.

Named parameters allow specifying name value pairs, using the following syntax:

`{"option_name": option_value, "another_option_name": another_value}`

Valid value types are strings, numbers and booleans.

An example using [match](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match):

```console
POST /_query
{
"query": """
FROM library
| WHERE match(author, "Frank Herbert", {"minimum_should_match": 2, "operator": "AND"})
| LIMIT 5
"""
}
```

You can also use [query parameters](docs-content://explore-analyze/query-filter/languages/esql-rest.md#esql-rest-params) in function named parameters:

```console
POST /_query
{
"query": """
FROM library
| EVAL year = DATE_EXTRACT("year", release_date)
| WHERE page_count > ? AND match(author, ?, {"minimum_should_match": ?})
| LIMIT 5
""",
"params": [300, "Frank Herbert", 2]
}
```

