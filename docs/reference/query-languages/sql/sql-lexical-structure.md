---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-lexical-structure.html
---

# Lexical structure [sql-lexical-structure]

This section covers the major lexical structure of SQL, which for the most part, is going to resemble that of ANSI SQL itself hence why low-levels details are not discussed in depth.

Elasticsearch SQL currently accepts only one *command* at a time. A command is a sequence of *tokens* terminated by the end of input stream.

A token can be a *key word*, an *identifier* (*quoted* or *unquoted*), a *literal* (or constant) or a special character symbol (typically a delimiter). Tokens are typically separated by whitespace (be it space, tab) though in some cases, where there is no ambiguity (typically due to a character symbol) this is not needed - however for readability purposes this should be avoided.

## Key Words [sql-syntax-keywords]

Take the following example:

```sql
SELECT * FROM table
```

This query has four tokens: `SELECT`, `*`, `FROM` and `table`. The first three, namely `SELECT`, `*` and `FROM` are *key words* meaning words that have a fixed meaning in SQL. The token `table` is an *identifier* meaning it identifies (by name) an entity inside SQL such as a table (in this case), a column, etc…​

As one can see, both key words and identifiers have the *same* lexical structure and thus one cannot know whether a token is one or the other without knowing the SQL language; the complete list of key words is available in the [reserved appendix](/reference/query-languages/sql/sql-syntax-reserved.md). Do note that key words are case-insensitive meaning the previous example can be written as:

```sql
select * fRoM table;
```

Identifiers however are not - as {{es}} is case sensitive, Elasticsearch SQL uses the received value verbatim.

To help differentiate between the two, through-out the documentation the SQL key words are upper-cased a convention we find increases readability and thus recommend to others.


## Identifiers [sql-syntax-identifiers]

Identifiers can be of two types: *quoted* and *unquoted*:

```sql
SELECT ip_address FROM "hosts-*"
```

This query has two identifiers, `ip_address` and `hosts-*` (an [index pattern](/reference/elasticsearch/rest-apis/api-conventions.md#api-multi-index)). As `ip_address` does not clash with any key words it can be used verbatim, `hosts-*` on the other hand cannot as it clashes with `-` (minus operation) and `*` hence the double quotes.

Another example:

```sql
SELECT "from" FROM "<logstash-{now/d}>"
```

The first identifier from needs to quoted as otherwise it clashes with the `FROM` key word (which is case insensitive as thus can be written as `from`) while the second identifier using {{es}} [Date math support in index and index alias names](/reference/elasticsearch/rest-apis/api-conventions.md#api-date-math-index-names) would have otherwise confuse the parser.

Hence why in general, **especially** when dealing with user input it is **highly** recommended to use quotes for identifiers. It adds minimal increase to your queries and in return offers clarity and disambiguation.


## Literals (Constants) [sql-syntax-literals]

Elasticsearch SQL supports two kind of *implicitly-typed* literals: strings and numbers.


#### String Literals [sql-syntax-string-literals]

A string literal is an arbitrary number of characters bounded by single quotes `'`: `'Giant Robot'`. To include a single quote in the string, escape it using another single quote: `'Captain EO''s Voyage'`.

::::{note}
An escaped single quote is **not** a double quote (`"`), but a single quote `'` *repeated* (`''`).
::::



#### Numeric Literals [_numeric_literals]

Numeric literals are accepted both in decimal and scientific notation with exponent marker (`e` or `E`), starting either with a digit or decimal point `.`:

```sql
1969    -- integer notation
3.14    -- decimal notation
.1234   -- decimal notation starting with decimal point
4E5     -- scientific notation (with exponent marker)
1.2e-3  -- scientific notation with decimal point
```

Numeric literals that contain a decimal point are always interpreted as being of type `double`. Those without are considered `integer` if they fit otherwise their type is `long` (or `BIGINT` in ANSI SQL types).


#### Generic Literals [sql-syntax-generic-literals]

When dealing with arbitrary type literal, one creates the object by casting, typically, the string representation to the desired type. This can be achieved through the dedicated [cast operator](/reference/query-languages/sql/sql-operators-cast.md) and [functions](/reference/query-languages/sql/sql-functions-type-conversion.md):

```sql
123::LONG                                   -- cast 123 to a LONG
CAST('1969-05-13T12:34:56' AS TIMESTAMP)    -- cast the given string to datetime
CONVERT('10.0.0.1', IP)                     -- cast '10.0.0.1' to an IP
```

Do note that Elasticsearch SQL provides functions that out of the box return popular literals (like `E()`) or provide dedicated parsing for certain strings.


## Single vs Double Quotes [sql-syntax-single-vs-double-quotes]

It is worth pointing out that in SQL, single quotes `'` and double quotes `"` have different meaning and **cannot** be used interchangeably. Single quotes are used to declare a [string literal](#sql-syntax-string-literals) while double quotes for [identifiers](#sql-syntax-identifiers).

To wit:

```sql
SELECT "first_name" <1>
  FROM "musicians"  <1>
 WHERE "last_name"  <1>
     = 'Carroll'    <2>
```

1. Double quotes `"` used for column and table identifiers
2. Single quotes `'` used for a string literal


::::{note}
To escape single or double quotes, one needs to use that specific quote one more time. For example, the literal `John's` can be escaped like `SELECT 'John''s' AS name`. The same goes for double quotes escaping - `SELECT 123 AS "test""number"` will display as a result a column with the name `test"number`.
::::



## Special characters [sql-syntax-special-chars]

A few characters that are not alphanumeric have a dedicated meaning different from that of an operator. For completeness these are specified below:

| **Char** | **Description** |
| --- | --- |
| `*` | The asterisk (or wildcard) is used in some contexts to denote all fields for a table. Can be also used as an argument to some aggregate functions. |
| `,` | Commas are used to enumerate the elements of a list. |
| `.` | Used in numeric constants or to separate identifiers qualifiers (catalog, table, column names, etc…​). |
| `()` | Parentheses are used for specific SQL commands, function declarations or to enforce precedence. |


## Operators [sql-syntax-operators]

Most operators in Elasticsearch SQL have the same precedence and are left-associative. As this is done at parsing time, parenthesis need to be used to enforce a different precedence.

The following table indicates the supported operators and their precedence (highest to lowest);

| **Operator/Element** | **Associativity** | **Description** |
| --- | --- | --- |
| `.` | left | qualifier separator |
| `::` | left | PostgreSQL-style type cast |
| `+ -` | right | unary plus and minus (numeric literal sign) |
| `* / %` | left | multiplication, division, modulo |
| `+ -` | left | addition, subtraction |
| `BETWEEN IN LIKE` |  | range containment, string matching |
| `< > <= >= = <=> <> !=` |  | comparison |
| `NOT` | right | logical negation |
| `AND` | left | logical conjunction |
| `OR` | left | logical disjunction |


## Comments [sql-syntax-comments]

Elasticsearch SQL allows comments which are sequence of characters ignored by the parsers.

Two styles are supported:

Single Line
:   Comments start with a double dash `--` and continue until the end of the line.

Multi line
:   Comments that start with `/*` and end with `*/` (also known as C-style).

```sql
-- single line comment
/* multi
   line
   comment
   that supports /* nested comments */
   */
```


