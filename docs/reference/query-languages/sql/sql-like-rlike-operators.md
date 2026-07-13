---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-like-rlike-operators.html
---

# LIKE and RLIKE operators [sql-like-rlike-operators]

`LIKE` and `RLIKE` operators are commonly used to filter data based on string patterns. They usually act on a field placed on the left-hand side of the operator, but can also act on a constant (literal) expression. The right-hand side of the operator represents the pattern. Both can be used in the `WHERE` clause of the `SELECT` statement, but `LIKE` can also be used in other places, such as defining an [index pattern](/reference/query-languages/sql/sql-index-patterns.md) or across various [SHOW commands](/reference/query-languages/sql/sql-commands.md). This section covers only the `SELECT ... WHERE ...` usage.

::::{note}
One significant difference between `LIKE`/`RLIKE` and the [full-text search predicates](/reference/query-languages/sql/sql-functions-search.md) is that the former act on [exact fields](/reference/query-languages/sql/sql-data-types.md#sql-multi-field) while the latter also work on [analyzed](/reference/elasticsearch/mapping-reference/text.md) fields. If the field used with `LIKE`/`RLIKE` doesn’t have an exact not-normalized sub-field (of [keyword](/reference/elasticsearch/mapping-reference/keyword.md) type) Elasticsearch SQL will not be able to run the query. If the field is either exact or has an exact sub-field, it will use it as is, or it will automatically use the exact sub-field even if it wasn’t explicitly specified in the statement.
::::


## `LIKE` [sql-like-operator]

```sql
expression        <1>
LIKE constant_exp <2>
```

1. typically a field, or a constant expression
2. pattern


**Description**: The SQL `LIKE` operator is used to compare a value to similar values using wildcard operators. There are two wildcards used in conjunction with the `LIKE` operator:

* The percent sign (%)
* The underscore (_)

The percent sign represents zero, one or multiple characters. The underscore represents a single number or character. These symbols can be used in combinations.

::::{note}
No other characters have special meaning or act as wildcard. Characters often used as wildcards in other languages (`*` or `?`) are treated as normal characters.
::::


```sql
SELECT author, name FROM library WHERE name LIKE 'Dune%';

    author     |     name
---------------+---------------
Frank Herbert  |Dune
Frank Herbert  |Dune Messiah
```

There is, also, the possibility of using an escape character if one needs to match the wildcard characters themselves. This can be done by using the `ESCAPE [escape_character]` statement after the `LIKE ...` operator:

```
SELECT name, author FROM library WHERE name LIKE 'Dune/%' ESCAPE '/';
```
In the example above `/` is defined as an escape character which needs to be placed before the `%` or `_` characters if one needs to match those characters in the pattern specifically. By default, there is no escape character defined.

::::{important}
Even though `LIKE` is a valid option when searching or filtering in Elasticsearch SQL, full-text search predicates `MATCH` and `QUERY` are [faster and much more powerful and are the preferred alternative](#sql-like-prefer-full-text).
::::



## `RLIKE` [sql-rlike-operator]

```sql
expression         <1>
RLIKE constant_exp <2>
```

1. typically a field, or a constant expression
2. pattern


**Description**: This operator is similar to `LIKE`, but the user is not limited to search for a string based on a fixed pattern with the percent sign (`%`) and underscore (`_`); the pattern in this case is a regular expression which allows the construction of more flexible patterns.

For supported syntax, see [*Regular expression syntax*](/reference/query-languages/query-dsl/regexp-syntax.md).

```sql
SELECT author, name FROM library WHERE name RLIKE 'Child.* Dune';

    author     |      name
---------------+----------------
Frank Herbert  |Children of Dune
```

::::{important}
Even though `RLIKE` is a valid option when searching or filtering in Elasticsearch SQL, full-text search predicates `MATCH` and `QUERY` are [faster and much more powerful and are the preferred alternative](#sql-like-prefer-full-text).
::::



## Prefer full-text search predicates [sql-like-prefer-full-text]

When using `LIKE`/`RLIKE`, do consider using [full-text search predicates](/reference/query-languages/sql/sql-functions-search.md) which are faster, much more powerful and offer the option of sorting by relevancy (results can be returned based on how well they matched).

<!--
For example:

| **LIKE/RLIKE** | **QUERY/MATCH** |
| --- | --- |
| ``foo LIKE 'bar'`` | ``MATCH(foo, 'bar')`` |
| ``foo LIKE 'bar' AND tar LIKE 'goo'`` | ``MATCH('foo^2, tar^5', 'bar goo', 'operator=and')`` |
| ``foo LIKE 'barr'`` | ``QUERY('foo: bar~')`` |
| ``foo LIKE 'bar' AND tar LIKE 'goo'`` | ``QUERY('foo: bar AND tar: goo')`` |
| ``foo RLIKE 'ba.*'`` | ``MATCH(foo, 'ba', 'fuzziness=AUTO:1,5')`` |
| ``foo RLIKE 'b.{{1}}r'`` | ``MATCH(foo, 'br', 'fuzziness=1')`` |
-->
