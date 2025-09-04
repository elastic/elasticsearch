---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-syntax-select.html
---

# SELECT [sql-syntax-select]

```sql
SELECT [TOP [ count ] ] select_expr [, ...]
[ FROM table_name ]
[ WHERE condition ]
[ GROUP BY grouping_element [, ...] ]
[ HAVING condition]
[ ORDER BY expression [ ASC | DESC ] [, ...] ]
[ LIMIT [ count ] ]
[ PIVOT ( aggregation_expr FOR column IN ( value [ [ AS ] alias ] [, ...] ) ) ]
```

**Description**: Retrieves rows from zero or more tables.

The general execution of `SELECT` is as follows:

1. All elements in the `FROM` list are computed (each element can be base or alias table). Currently `FROM` supports exactly one table. Do note however that the table name can be a pattern (see [FROM Clause](#sql-syntax-from) below).
2. If the `WHERE` clause is specified, all rows that do not satisfy the condition are eliminated from the output. (See [WHERE Clause](#sql-syntax-where) below.)
3. If the `GROUP BY` clause is specified, or if there are aggregate function calls, the output is combined into groups of rows that match on one or more values, and the results of aggregate functions are computed. If the `HAVING` clause is present, it eliminates groups that do not satisfy the given condition. (See [GROUP BY Clause](#sql-syntax-group-by) and [HAVING Clause](#sql-syntax-having) below.)
4. The actual output rows are computed using the `SELECT` output expressions for each selected row or row group.
5. If the `ORDER BY` clause is specified, the returned rows are sorted in the specified order. If `ORDER BY` is not given, the rows are returned in whatever order the system finds fastest to produce. (See [ORDER BY Clause](#sql-syntax-order-by) below.)
6. If the `LIMIT` or `TOP` is specified (cannot use both in the same query), the `SELECT` statement only returns a subset of the result rows. (See [LIMIT Clause](#sql-syntax-limit) and [TOP clause](#sql-syntax-top) below.)

## `SELECT` List [sql-syntax-select-list]

`SELECT` list, namely the expressions between `SELECT` and `FROM`, represent the output rows of the `SELECT` statement.

As with a table, every output column of a `SELECT` has a name which can be either specified per column through the `AS` keyword :

```sql
SELECT 1 + 1 AS result;

    result
---------------
2
```

Note: `AS` is an optional keyword however it helps with the readability and in some case ambiguity of the query which is why it is recommended to specify it.

assigned by Elasticsearch SQL if no name is given:

```sql
SELECT 1 + 1;

    1 + 1
--------------
2
```

or if it’s a simple column reference, use its name as the column name:

```sql
SELECT emp_no FROM emp LIMIT 1;

    emp_no
---------------
10001
```


## Wildcard [sql-syntax-select-wildcard]

To select all the columns in the source, one can use `*`:

```sql
SELECT * FROM emp LIMIT 1;

     birth_date     |    emp_no     |  first_name   |    gender     |       hire_date        |   languages   |   last_name   |     name      |    salary
--------------------+---------------+---------------+---------------+------------------------+---------------+---------------+---------------+---------------
1953-09-02T00:00:00Z|10001          |Georgi         |M              |1986-06-26T00:00:00.000Z|2              |Facello        |Georgi Facello |57305
```

which essentially returns all(top-level fields, sub-fields, such as multi-fields are ignored] columns found.


## TOP [sql-syntax-top]

The `TOP` clause can be used before the [`SELECT` list](#sql-syntax-select-list) or the <<sql-syntax-select-wildcard, `wildcard`> to restrict (limit) the number of rows returned using the format:

```sql
SELECT TOP <count> <select list> ...
```

where

count
:   is a positive integer or zero indicating the maximum **possible** number of results being returned (as there might be fewer matches than the limit). If `0` is specified, no results are returned.

```sql
SELECT TOP 2 first_name, last_name, emp_no FROM emp;

  first_name   |   last_name   |    emp_no
---------------+---------------+---------------
Georgi         |Facello        |10001
Bezalel        |Simmel         |10002
```

::::{note}
[`TOP`](#sql-syntax-top) and [`LIMIT`](#sql-syntax-limit) cannot be used together in the same query and an error is returned otherwise.
::::



## FROM Clause [sql-syntax-from]

The `FROM` clause specifies one table for the `SELECT` and has the following syntax:

```sql
FROM table_name [ [ AS ] alias ]
```

where:

`table_name`
:   Represents the name (optionally qualified) of an existing table, either a concrete or base one (actual index) or alias.

If the table name contains special SQL characters (such as `.`,`-`,`*`,etc… ) use double quotes to escape them:

```sql
SELECT * FROM "emp" LIMIT 1;

     birth_date     |    emp_no     |  first_name   |    gender     |       hire_date        |   languages   |   last_name   |     name      |    salary
--------------------+---------------+---------------+---------------+------------------------+---------------+---------------+---------------+---------------
1953-09-02T00:00:00Z|10001          |Georgi         |M              |1986-06-26T00:00:00.000Z|2              |Facello        |Georgi Facello |57305
```

The name can be a [pattern](/reference/elasticsearch/rest-apis/api-conventions.md#api-multi-index) pointing to multiple indices (likely requiring quoting as mentioned above) with the restriction that **all** resolved concrete tables have **exact mapping**.

```sql
SELECT emp_no FROM "e*p" LIMIT 1;

    emp_no
---------------
10001
```

[preview] To run a [{{ccs}}](docs-content://solutions/search/cross-cluster-search.md), specify a cluster name using the `<remote_cluster>:<target>` syntax, where `<remote_cluster>` maps to a SQL catalog (cluster) and `<target>` to a table (index or data stream). The `<remote_cluster>` supports wildcards (`*`) and `<target>` can be an [index pattern](/reference/query-languages/sql/sql-index-patterns.md).

```sql
SELECT emp_no FROM "my*cluster:*emp" LIMIT 1;

    emp_no
---------------
10001
```

`alias`
:   A substitute name for the `FROM` item containing the alias. An alias is used for brevity or to eliminate ambiguity. When an alias is provided, it completely hides the actual name of the table and must be used in its place.

```sql
SELECT e.emp_no FROM emp AS e LIMIT 1;

    emp_no
-------------
10001
```


## WHERE Clause [sql-syntax-where]

The optional `WHERE` clause is used to filter rows from the query and has the following syntax:

```sql
WHERE condition
```

where:

`condition`
:   Represents an expression that evaluates to a `boolean`. Only the rows that match the condition (to `true`) are returned.

```sql
SELECT last_name FROM emp WHERE emp_no = 10001;

   last_name
---------------
Facello
```


## GROUP BY [sql-syntax-group-by]

The `GROUP BY` clause is used to divide the results into groups of rows on matching values from the designated columns. It has the following syntax:

```sql
GROUP BY grouping_element [, ...]
```

where:

`grouping_element`
:   Represents an expression on which rows are being grouped *on*. It can be a column name, alias or ordinal number of a column or an arbitrary expression of column values.

A common, group by column name:

```sql
SELECT gender AS g FROM emp GROUP BY gender;

       g
---------------
null
F
M
```

Grouping by output ordinal:

```sql
SELECT gender FROM emp GROUP BY 1;

    gender
---------------
null
F
M
```

Grouping by alias:

```sql
SELECT gender AS g FROM emp GROUP BY g;

       g
---------------
null
F
M
```

And grouping by column expression (typically used along-side an alias):

```sql
SELECT languages + 1 AS l FROM emp GROUP BY l;

       l
---------------
null
2
3
4
5
6
```

Or a mixture of the above:

```sql
SELECT gender g, languages l, COUNT(*) c FROM "emp" GROUP BY g, l ORDER BY languages ASC, gender DESC;

       g       |       l       |       c
---------------+---------------+---------------
M              |null           |7
F              |null           |3
M              |1              |9
F              |1              |4
null           |1              |2
M              |2              |11
F              |2              |5
null           |2              |3
M              |3              |11
F              |3              |6
M              |4              |11
F              |4              |6
null           |4              |1
M              |5              |8
F              |5              |9
null           |5              |4
```

When a `GROUP BY` clause is used in a `SELECT`, *all* output expressions must be either aggregate functions or expressions used for grouping or derivatives of (otherwise there would be more than one possible value to return for each ungrouped column).

To wit:

```sql
SELECT gender AS g, COUNT(*) AS c FROM emp GROUP BY gender;

       g       |       c
---------------+---------------
null           |10
F              |33
M              |57
```

Expressions over aggregates used in output:

```sql
SELECT gender AS g, ROUND((MIN(salary) / 100)) AS salary FROM emp GROUP BY gender;

       g       |    salary
---------------+---------------
null           |253
F              |259
M              |259
```

Multiple aggregates used:

```sql
SELECT gender AS g, KURTOSIS(salary) AS k, SKEWNESS(salary) AS s FROM emp GROUP BY gender;

       g       |        k         |         s
---------------+------------------+-------------------
null           |2.2215791166941923|-0.03373126000214023
F              |1.7873117044424276|0.05504995122217512
M              |2.280646181070106 |0.44302407229580243
```

::::{tip}
If custom bucketing is required, it can be achieved with the use of [`CASE`](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-case), as shown [here](/reference/query-languages/sql/sql-functions-conditional.md#sql-functions-conditional-case-groupby-custom-buckets).
::::


### Implicit Grouping [sql-syntax-group-by-implicit]

When an aggregation is used without an associated `GROUP BY`, an *implicit grouping* is applied, meaning all selected rows are considered to form a single default, or implicit group. As such, the query emits only a single row (as there is only a single group).

A common example is counting the number of records:

```sql
SELECT COUNT(*) AS count FROM emp;

     count
---------------
100
```

Of course, multiple aggregations can be applied:

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, AVG(salary) AS avg, COUNT(*) AS count FROM emp;

      min:i    |      max:i    |      avg:d    |     count:l
---------------+---------------+---------------+---------------
25324          |74999          |48248.55       |100
```



## HAVING [sql-syntax-having]

The `HAVING` clause can be used *only* along aggregate functions (and thus `GROUP BY`) to filter what groups are kept or not and has the following syntax:

```sql
HAVING condition
```

where:

`condition`
:   Represents an expression that evaluates to a `boolean`. Only groups that match the condition (to `true`) are returned.

Both `WHERE` and `HAVING` are used for filtering however there are several significant differences between them:

1. `WHERE` works on individual **rows**, `HAVING` works on the **groups** created by ``GROUP BY``
2. `WHERE` is evaluated **before** grouping, `HAVING` is evaluated **after** grouping

```sql
SELECT languages AS l, COUNT(*) AS c FROM emp GROUP BY l HAVING c BETWEEN 15 AND 20;

       l       |       c
---------------+---------------
1              |15
2              |19
3              |17
4              |18
```

Further more, one can use multiple aggregate expressions inside `HAVING` even ones that are not used in the output (`SELECT`):

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max, MAX(salary) - MIN(salary) AS diff FROM emp GROUP BY languages HAVING diff - max % min > 0 AND AVG(salary) > 30000;

      min      |      max      |     diff
---------------+---------------+---------------
28336          |74999          |46663
25976          |73717          |47741
29175          |73578          |44403
26436          |74970          |48534
27215          |74572          |47357
25324          |66817          |41493
```

### Implicit Grouping [sql-syntax-having-group-by-implicit]

As indicated above, it is possible to have a `HAVING` clause without a `GROUP BY`. In this case, the so-called [*implicit grouping*](#sql-syntax-group-by-implicit) is applied, meaning all selected rows are considered to form a single group and `HAVING` can be applied on any of the aggregate functions specified on this group. As such, the query emits only a single row (as there is only a single group) and `HAVING` condition returns either one row (the group) or zero if the condition fails.

In this example, `HAVING` matches:

```sql
SELECT MIN(salary) AS min, MAX(salary) AS max FROM emp HAVING min > 25000;

      min      |      max
---------------+---------------
25324          |74999
```



## ORDER BY [sql-syntax-order-by]

The `ORDER BY` clause is used to sort the results of `SELECT` by one or more expressions:

```sql
ORDER BY expression [ ASC | DESC ] [, ...]
```

where:

`expression`
:   Represents an input column, an output column or an ordinal number of the position (starting from one) of an output column. Additionally, ordering can be done based on the results *score*. The direction, if not specified, is by default `ASC` (ascending). Regardless of the ordering specified, null values are ordered last (at the end).

::::{important}
When used along-side, `GROUP BY` expression can point *only* to the columns used for grouping or aggregate functions.
::::


For example, the following query sorts by an arbitrary input field (`page_count`):

```sql
SELECT * FROM library ORDER BY page_count DESC LIMIT 5;

     author      |        name        |  page_count   |    release_date
-----------------+--------------------+---------------+--------------------
Peter F. Hamilton|Pandora's Star      |768            |2004-03-02T00:00:00Z
Vernor Vinge     |A Fire Upon the Deep|613            |1992-06-01T00:00:00Z
Frank Herbert    |Dune                |604            |1965-06-01T00:00:00Z
Alastair Reynolds|Revelation Space    |585            |2000-03-15T00:00:00Z
James S.A. Corey |Leviathan Wakes     |561            |2011-06-02T00:00:00Z
```


## Order By and Grouping [sql-syntax-order-by-grouping]

For queries that perform grouping, ordering can be applied either on the grouping columns (by default ascending) or on aggregate functions.

::::{note}
With `GROUP BY`, make sure the ordering targets the resulting group - applying it to individual elements inside the group will have no impact on the results since regardless of the order, values inside the group are aggregated.
::::


For example, to order groups simply indicate the grouping key:

```sql
SELECT gender AS g, COUNT(*) AS c FROM emp GROUP BY gender ORDER BY g DESC;

       g       |       c
---------------+---------------
M              |57
F              |33
null           |10
```

Multiple keys can be specified of course:

```sql
SELECT gender g, languages l, COUNT(*) c FROM "emp" GROUP BY g, l ORDER BY languages ASC, gender DESC;

       g       |       l       |       c
---------------+---------------+---------------
M              |null           |7
F              |null           |3
M              |1              |9
F              |1              |4
null           |1              |2
M              |2              |11
F              |2              |5
null           |2              |3
M              |3              |11
F              |3              |6
M              |4              |11
F              |4              |6
null           |4              |1
M              |5              |8
F              |5              |9
null           |5              |4
```

Further more, it is possible to order groups based on aggregations of their values:

```sql
SELECT gender AS g, MIN(salary) AS salary FROM emp GROUP BY gender ORDER BY salary DESC;

       g       |    salary
---------------+---------------
F              |25976
M              |25945
null           |25324
```

::::{important}
Ordering by aggregation is possible for up to **10000** entries for memory consumption reasons. In cases where the results pass this threshold, use [`LIMIT`](#sql-syntax-limit) or [`TOP`](#sql-syntax-top) to reduce the number of results.
::::



## Order By Score [sql-syntax-order-by-score]

When doing full-text queries in the `WHERE` clause, results can be returned based on their [score](https://www.elastic.co/guide/en/elasticsearch/guide/2.x/relevance-intro.html) or *relevance* to the given query.

::::{note}
When doing multiple text queries in the `WHERE` clause then, their scores will be combined using the same rules as {{es}}'s [bool query](/reference/query-languages/query-dsl/query-dsl-bool-query.md).
::::


To sort based on the `score`, use the special function `SCORE()`:

```sql
SELECT SCORE(), * FROM library WHERE MATCH(name, 'dune') ORDER BY SCORE() DESC;

    SCORE()    |    author     |       name        |  page_count   |    release_date
---------------+---------------+-------------------+---------------+--------------------
2.2886353      |Frank Herbert  |Dune               |604            |1965-06-01T00:00:00Z
1.8893257      |Frank Herbert  |Dune Messiah       |331            |1969-10-15T00:00:00Z
1.6086556      |Frank Herbert  |Children of Dune   |408            |1976-04-21T00:00:00Z
1.4005898      |Frank Herbert  |God Emperor of Dune|454            |1981-05-28T00:00:00Z
```

Note that you can return `SCORE()` by using a full-text search predicate in the `WHERE` clause. This is possible even if `SCORE()` is not used for sorting:

```sql
SELECT SCORE(), * FROM library WHERE MATCH(name, 'dune') ORDER BY page_count DESC;

    SCORE()    |    author     |       name        |  page_count   |    release_date
---------------+---------------+-------------------+---------------+--------------------
2.2886353      |Frank Herbert  |Dune               |604            |1965-06-01T00:00:00Z
1.4005898      |Frank Herbert  |God Emperor of Dune|454            |1981-05-28T00:00:00Z
1.6086556      |Frank Herbert  |Children of Dune   |408            |1976-04-21T00:00:00Z
1.8893257      |Frank Herbert  |Dune Messiah       |331            |1969-10-15T00:00:00Z
```

NOTE: Trying to return `score` from a non full-text query will return the same value for all results, as all are equally relevant.


## LIMIT [sql-syntax-limit]

The `LIMIT` clause restricts (limits) the number of rows returned using the format:

```sql
LIMIT ( <count> | ALL )
```

where

count
:   is a positive integer or zero indicating the maximum **possible** number of results being returned (as there might be fewer matches than the limit). If `0` is specified, no results are returned.

ALL
:   indicates there is no limit and thus all results are being returned.

```sql
SELECT first_name, last_name, emp_no FROM emp LIMIT 1;

  first_name   |   last_name   |    emp_no
---------------+---------------+---------------
Georgi         |Facello        |10001
```

::::{note}
[`TOP`](#sql-syntax-top) and [`LIMIT`](#sql-syntax-limit) cannot be used together in the same query and an error is returned otherwise.
::::



## PIVOT [sql-syntax-pivot]

The `PIVOT` clause performs a cross tabulation on the results of the query: it aggregates the results and rotates rows into columns. The rotation is done by turning unique values from one column in the expression - the pivoting column - into multiple columns in the output. The column values are aggregations on the remaining columns specified in the expression.

The clause can be broken down in three parts: the aggregation, the `FOR`- and the `IN`-subclause.

The `aggregation_expr` subclause specifies an expression containing an [aggregation function](/reference/query-languages/sql/sql-functions-aggs.md) to be applied on one of the source columns. Only one aggregation can be provided, currently.

The `FOR`-subclause specifies the pivoting column: the distinct values of this column will become the candidate set of values to be rotated.

The `IN`-subclause defines a filter: the intersection between the set provided here and the candidate set from the `FOR`-subclause will be rotated to become the headers of the columns appended to the end result. The filter can not be a subquery, one must provide here literal values, obtained in advance.

The pivoting operation will perform an implicit [GROUP BY](#sql-syntax-group-by) on all source columns not specified in the `PIVOT` clause, along with the values filtered through the `IN`-clause. Consider the following statement:

```sql
SELECT * FROM test_emp PIVOT (SUM(salary) FOR languages IN (1, 2)) LIMIT 5;

       birth_date    |    emp_no     |  first_name   |    gender     |     hire_date       |   last_name   |       name       |       1       |       2
---------------------+---------------+---------------+---------------+---------------------+---------------+------------------+---------------+---------------
null                 |10041          |Uri            |F              |1989-11-12 00:00:00.0|Lenart         |Uri Lenart        |56415          |null
null                 |10043          |Yishay         |M              |1990-10-20 00:00:00.0|Tzvieli        |Yishay Tzvieli    |34341          |null
null                 |10044          |Mingsen        |F              |1994-05-21 00:00:00.0|Casley         |Mingsen Casley    |39728          |null
1952-04-19 00:00:00.0|10009          |Sumant         |F              |1985-02-18 00:00:00.0|Peac           |Sumant Peac       |66174          |null
1953-01-07 00:00:00.0|10067          |Claudi         |M              |1987-03-04 00:00:00.0|Stavenow       |Claudi Stavenow   |null           |52044
```

The query execution could logically be broken down in the following steps:

1. a [GROUP BY](#sql-syntax-group-by) on the column in the `FOR`-clause: `languages`;
2. the resulting values are filtered through the set provided in the `IN`-clause;
3. the now filtered column is pivoted to form the headers of the two additional columns appended to the result: `1` and `2`;
4. a [GROUP BY](#sql-syntax-group-by) on all columns of the source table `test_emp`, except `salary` (part of the aggregation subclause) and `languages` (part of the `FOR`-clause);
5. the values in these appended columns are the `SUM` aggregations of `salary`, grouped by the respective language.

The table-value expression to cross-tabulate can also be the result of a subquery:

```sql
SELECT * FROM (SELECT languages, gender, salary FROM test_emp) PIVOT (AVG(salary) FOR gender IN ('F'));

   languages   |       'F'
---------------+------------------
null           |62140.666666666664
1              |47073.25
2              |50684.4
3              |53660.0
4              |49291.5
5              |46705.555555555555
```

The pivoted columns can be aliased (and quoting is required to accommodate white spaces), with or without a supporting `AS` token:

```sql
SELECT * FROM (SELECT languages, gender, salary FROM test_emp) PIVOT (AVG(salary) FOR gender IN ('M' AS "XY", 'F' "XX"));

   languages   |        XY       |        XX
---------------+-----------------+------------------
null           |48396.28571428572|62140.666666666664
1              |49767.22222222222|47073.25
2              |44103.90909090909|50684.4
3              |51741.90909090909|53660.0
4              |47058.90909090909|49291.5
5              |39052.875        |46705.555555555555
```

The resulting cross tabulation can further have the [ORDER BY](#sql-syntax-order-by) and [LIMIT](#sql-syntax-limit) clauses applied:

```sql
SELECT * FROM (SELECT languages, gender, salary FROM test_emp) PIVOT (AVG(salary) FOR gender IN ('F')) ORDER BY languages DESC LIMIT 4;
   languages   |       'F'
---------------+------------------
5              |46705.555555555555
4              |49291.5
3              |53660.0
2              |50684.4
```
