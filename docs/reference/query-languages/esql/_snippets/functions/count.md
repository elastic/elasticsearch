## `COUNT` [esql-count]

**Syntax**

:::{image} ../../../../../images/count.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Expression that outputs values to be counted. If omitted, equivalent to `COUNT(*)` (the number of rows).

**Description**

Returns the total number (count) of input values.

**Supported types**

| field | result |
| --- | --- |
| boolean | long |
| cartesian_point | long |
| date | long |
| double | long |
| geo_point | long |
| integer | long |
| ip | long |
| keyword | long |
| long | long |
| text | long |
| unsigned_long | long |
| version | long |

**Examples**

```esql
FROM employees
| STATS COUNT(height)
```

| COUNT(height):long |
| --- |
| 100 |

To count the number of rows, use `COUNT()` or `COUNT(*)`

```esql
FROM employees
| STATS count = COUNT(*) BY languages
| SORT languages DESC
```

| count:long | languages:integer |
| --- | --- |
| 10 | null |
| 21 | 5 |
| 18 | 4 |
| 17 | 3 |
| 19 | 2 |
| 15 | 1 |

The expression can use inline functions. This example splits a string into multiple values using the `SPLIT` function and counts the values

```esql
ROW words="foo;bar;baz;qux;quux;foo"
| STATS word_count = COUNT(SPLIT(words, ";"))
```

| word_count:long |
| --- |
| 6 |

To count the number of times an expression returns `TRUE` use a [`WHERE`](/reference/query-languages/esql/esql-commands.md#esql-where) command to remove rows that shouldn’t be included

```esql
ROW n=1
| WHERE n < 0
| STATS COUNT(n)
```

| COUNT(n):long |
| --- |
| 0 |

To count the same stream of data based on two different expressions use the pattern `COUNT(<expression> OR NULL)`. This builds on the three-valued logic ({{wikipedia}}/Three-valued_logic[3VL]) of the language: `TRUE OR NULL` is `TRUE`, but `FALSE OR NULL` is `NULL`, plus the way COUNT handles `NULL`s: `COUNT(TRUE)` and `COUNT(FALSE)` are both 1, but `COUNT(NULL)` is 0.

```esql
ROW n=1
| STATS COUNT(n > 0 OR NULL), COUNT(n < 0 OR NULL)
```

| COUNT(n > 0 OR NULL):long | COUNT(n < 0 OR NULL):long |
| --- | --- |
| 1 | 0 |


