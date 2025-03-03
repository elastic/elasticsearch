## `LIKE` and `RLIKE` [esql-like-operators]

:::{include} ../lists/like-and-rlike-operators.md
:::

## `LIKE` [esql-like]

Use `LIKE` to filter data based on string patterns using wildcards. `LIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

The following wildcard characters are supported:

* `*` matches zero or more characters.
* `?` matches one character.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

**Examples**

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

## `RLIKE` [esql-rlike]

Use `RLIKE` to filter data based on string patterns using using [regular expressions](/reference/query-languages/regexp-syntax.md). `RLIKE` usually acts on a field placed on the left-hand side of the operator, but it can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern.

**Supported types**

| str | pattern | result |
| --- | --- | --- |
| keyword | keyword | boolean |
| text | keyword | boolean |

**Examples**

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
