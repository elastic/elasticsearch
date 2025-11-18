---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-type-conversion.html
---

# Type conversion functions [sql-functions-type-conversion]

Functions for converting an expression of one data type to another.

## `CAST` [sql-functions-type-conversion-cast]

```sql
CAST(
    expression <1>
 AS data_type) <2>
```

1. Expression to cast. If `null`, the function returns `null`.
2. Target data type to cast to


**Description**: Casts the result of the given expression to the target [data type](/reference/query-languages/sql/sql-data-types.md). If the cast is not possible (for example because of target type is too narrow or because the value itself cannot be converted), the query fails.

```sql
SELECT CAST('123' AS INT) AS int;

      int
---------------
123
```

```sql
SELECT CAST(123 AS VARCHAR) AS string;

    string
---------------
123
```

```sql
SELECT YEAR(CAST('2018-05-19T11:23:45Z' AS TIMESTAMP)) AS year;

     year
---------------
2018
```

::::{important}
Both ANSI SQL and Elasticsearch SQL types are supported with the former taking precedence. This only affects `FLOAT` which due naming conflict, is interpreted as ANSI SQL and thus maps to `double` in {{es}} as oppose to `float`. To obtain an {{es}} `float`, perform casting to its SQL equivalent, `real` type.
::::



## `CONVERT` [sql-functions-type-conversion-convert]

```sql
CONVERT(
    expression, <1>
    data_type)  <2>
```

1. Expression to convert. If `null`, the function returns `null`.
2. Target data type to convert to


**Description**: Works exactly like [`CAST`](#sql-functions-type-conversion-cast) with slightly different syntax. Moreover, apart from the standard [data types](/reference/query-languages/sql/sql-data-types.md) it supports the corresponding [ODBC data types](https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/explicit-data-type-conversion-function?view=sql-server-2017).

```sql
SELECT CONVERT('123', SQL_INTEGER) AS int;

      int
---------------
123
```

```sql
SELECT CONVERT('123', INTEGER) AS int;

      int
---------------
123
```


