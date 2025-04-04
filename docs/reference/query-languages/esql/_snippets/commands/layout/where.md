## `WHERE` [esql-where]

The `WHERE` processing command produces a table that contains all the rows from
the input table for which the provided condition evaluates to `true`.

::::{tip}
In case of value exclusions, fields with `null` values will be excluded from search results.
In this context a `null` means either there is an explicit `null` value in the document or
there is no value at all. For example: `WHERE field != "value"` will be interpreted as
`WHERE field != "value" AND field IS NOT NULL`.
::::


**Syntax**

```esql
WHERE expression
```

**Parameters**

`expression`
:   A boolean expression.

**Examples**

```esql
FROM employees
| KEEP first_name, last_name, still_hired
| WHERE still_hired == true
```

Which, if `still_hired` is a boolean field, can be simplified to:

```esql
FROM employees
| KEEP first_name, last_name, still_hired
| WHERE still_hired
```

Use date math to retrieve data from a specific time range. For example, to retrieve the last hour of logs:

```esql
FROM sample_data
| WHERE @timestamp > NOW() - 1 hour
```

`WHERE` supports various [functions](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).
For example the [`LENGTH`](/reference/query-languages/esql/functions-operators/string-functions.md#esql-length) function:

```esql
FROM employees
| KEEP first_name, last_name, height
| WHERE LENGTH(first_name) < 4
```

For a complete list of all functions, refer to [Functions overview](/reference/query-languages/esql/esql-functions-operators.md#esql-functions).

For NULL comparison, use the `IS NULL` and `IS NOT NULL` predicates:

```esql
FROM employees
| WHERE birth_date IS NULL
| KEEP first_name, last_name
| SORT first_name
| LIMIT 3
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Basil | Tramer |
| Florian | Syrotiuk |
| Lucien | Rosenbaum |

```esql
FROM employees
| WHERE is_rehired IS NOT NULL
| STATS COUNT(emp_no)
```

| COUNT(emp_no):long |
| --- |
| 84 |

For matching text, you can use [full text search functions](/reference/query-languages/esql/functions-operators/search-functions.md) like `MATCH`.

Use [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) to perform a
[match query](/reference/query-languages/query-dsl/query-dsl-match-query.md) on a specified field.

Match can be used on text fields, as well as other field types like boolean, dates, and numeric types.

```esql
FROM books
| WHERE MATCH(author, "Faulkner")
| KEEP book_no, author
| SORT book_no
| LIMIT 5
```

| book_no:keyword | author:text |
| --- | --- |
| 2378 | [Carol Faulkner, Holly Byers Ochoa, Lucretia Mott] |
| 2713 | William Faulkner |
| 2847 | Colleen Faulkner |
| 2883 | William Faulkner |
| 3293 | Danny Faulkner |

::::{tip}
You can also use the shorthand [match operator](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator) `:` instead of `MATCH`.

::::


Use `LIKE` to filter data based on string patterns using wildcards. `LIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

The following wildcard characters are supported:

* `*` matches zero or more characters.
* `?` matches one character.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

```esql
FROM employees
| WHERE first_name LIKE """?b*"""
| KEEP first_name, last_name
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Ebbe | Callaway |
| Eberhardt | Terkki |

Matching the exact characters `*` and `.` will require escaping. The escape character is backslash `\`. Since also backslash is a special character in string literals, it will require further escaping.

```esql
ROW message = "foo * bar"
| WHERE message LIKE "foo \\* bar"
```

To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo * bar"
| WHERE message LIKE """foo \* bar"""
```

Use `RLIKE` to filter data based on string patterns using using [regular expressions](/reference/query-languages/query-dsl/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

```esql
FROM employees
| WHERE first_name RLIKE """.leja.*"""
| KEEP first_name, last_name
```

| first_name:keyword | last_name:keyword |
| --- | --- |
| Alejandro | McAlpine |

Matching special characters (eg. `.`, `*`, `(`…​) will require escaping. The escape character is backslash `\`. Since also backslash is a special character in string literals, it will require further escaping.

```esql
ROW message = "foo ( bar"
| WHERE message RLIKE "foo \\( bar"
```

To reduce the overhead of escaping, we suggest using triple quotes strings `"""`

```esql
ROW message = "foo ( bar"
| WHERE message RLIKE """foo \( bar"""
```

The `IN` operator allows testing whether a field or expression equals an element in a list of literals, fields or expressions:

```esql
ROW a = 1, b = 4, c = 3
| WHERE c-a IN (3, b / 2, a)
```

| a:integer | b:integer | c:integer |
| --- | --- | --- |
| 1 | 4 | 3 |

For a complete list of all operators, refer to [Operators](/reference/query-languages/esql/esql-functions-operators.md#esql-operators-overview).
