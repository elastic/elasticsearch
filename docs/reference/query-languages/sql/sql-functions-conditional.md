---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-conditional.html
---

# Conditional functions and expressions [sql-functions-conditional]

Functions that return one of their arguments by evaluating in an if-else manner.

## `CASE` [sql-functions-conditional-case]

```sql
CASE WHEN condition THEN result
    [WHEN ...]
    [ELSE default_result]
END
```

**Input**:

One or multiple *WHEN **condition** THEN **result** clauses are used and the expression can optionally have an *ELSE **default_result** clause. Every **condition** should be a boolean expression.

**Output**: one of the **result** expressions if the corresponding *WHEN **condition** evaluates to `true` or the **default_result** if all *WHEN **condition** clauses evaluate to `false`. If the optional *ELSE **default_result** clause is missing and all *WHEN **condition** clauses evaluate to `false` then `null` is returned.

**Description**: The CASE expression is a generic conditional expression which simulates if/else statements of other programming languages If the condition’s result is true, the value of the result expression that follows the condition will be the returned the subsequent when clauses will be skipped and not processed.

```sql
SELECT CASE WHEN 1 > 2 THEN 'elastic'
            WHEN 2 <= 3 THEN 'search'
       END AS "case";

    case
---------------
search
```

```sql
SELECT CASE WHEN 1 > 2 THEN 'elastic'
            WHEN 2 > 10 THEN 'search'
       END AS "case";

    case
---------------
null
```

```sql
SELECT CASE WHEN 1 > 2 THEN 'elastic'
            WHEN 2 > 10 THEN 'search'
            ELSE 'default'
       END AS "case";

    case
---------------
default
```

As a variant, a case expression can be expressed with a syntax similar to **switch-case** of other programming languages:

```sql
CASE expression
     WHEN value1 THEN result1
    [WHEN value2 THEN result2]
    [WHEN ...]
    [ELSE default_result]
END
```

In this case it’s transformed internally to:

```sql
CASE WHEN expression = value1 THEN result1
    [WHEN expression = value2 THEN result2]
    [WHEN ...]
    [ELSE default_result]
END
```

```sql
SELECT CASE 5
            WHEN 1 THEN 'elastic'
            WHEN 2 THEN 'search'
            WHEN 5 THEN 'elasticsearch'
       END AS "case";

    case
---------------
elasticsearch
```

```sql
SELECT CASE 5
            WHEN 1 THEN 'elastic'
            WHEN 2 THEN 'search'
            WHEN 3 THEN 'elasticsearch'
            ELSE 'default'
       END AS "case";

    case
---------------
default
```

::::{note}
All result expressions must be of compatible data types. More specifically all result expressions should have a compatible data type with the 1st *non-null* result expression. E.g.:

for the following query:

```sql
CASE WHEN a = 1 THEN null
     WHEN a > 2 THEN 10
     WHEN a > 5 THEN 'foo'
END
```

an error message would be returned, mentioning that **foo** is of data type **keyword**, which does not match the expected data type **integer** (based on result **10**).

::::


### Conditional bucketing [sql-functions-conditional-case-groupby-custom-buckets]

CASE can be used as a GROUP BY key in a query to facilitate custom bucketing and assign descriptive names to those buckets. If, for example, the values for a key are too many or, simply, ranges of those values are more interesting than every single value, CASE can create custom buckets as in the following example:

```sql
SELECT count(*) AS count,
  CASE WHEN NVL(languages, 0) = 0 THEN 'zero'
    WHEN languages = 1 THEN 'one'
    WHEN languages = 2 THEN 'bilingual'
    WHEN languages = 3 THEN 'trilingual'
    ELSE 'multilingual'
  END as lang_skills
FROM employees
GROUP BY lang_skills
ORDER BY lang_skills;
```

With this query, one can create normal grouping buckets for values *0, 1, 2, 3* with descriptive names, and every value *>= 4* falls into the *multilingual* bucket.



## `COALESCE` [sql-functions-conditional-coalesce]

```sql
COALESCE(
    expression, <1>
    expression, <2>
    ...)
```

**Input**:

1. 1st expression
2. 2nd expression


… 

**N**th expression

COALESCE can take an arbitrary number of arguments.

**Output**: one of the expressions or `null`

**Description**: Returns the first of its arguments that is not null. If all arguments are null, then it returns `null`.

```sql
SELECT COALESCE(null, 'elastic', 'search') AS "coalesce";

    coalesce
---------------
elastic
```

```sql
SELECT COALESCE(null, null, null, null) AS "coalesce";

    coalesce
---------------
null
```


## `GREATEST` [sql-functions-conditional-greatest]

```sql
GREATEST(
    expression, <1>
    expression, <2>
    ...)
```

**Input**:

1. 1st expression
2. 2nd expression


… 

**N**th expression

GREATEST can take an arbitrary number of arguments and all of them must be of the same data type.

**Output**: one of the expressions or `null`

**Description**: Returns the argument that has the largest value which is not null. If all arguments are null, then it returns `null`.

```sql
SELECT GREATEST(null, 1, 2) AS "greatest";

    greatest
---------------
2
```

```sql
SELECT GREATEST(null, null, null, null) AS "greatest";

    greatest
---------------
null
```


## `IFNULL` [sql-functions-conditional-ifnull]

```sql
IFNULL(
    expression, <1>
    expression) <2>
```

**Input**:

1. 1st expression
2. 2nd expression


**Output**: 2nd expression if 1st expression is null, otherwise 1st expression.

**Description**: Variant of [`COALESCE`](#sql-functions-conditional-coalesce) with only two arguments. Returns the first of its arguments that is not null. If all arguments are null, then it returns `null`.

```sql
SELECT IFNULL('elastic', null) AS "ifnull";

    ifnull
---------------
elastic
```

```sql
SELECT IFNULL(null, 'search') AS "ifnull";

    ifnull
---------------
search
```


## `IIF` [sql-functions-conditional-iif]

```sql
IIF(
    expression,   <1>
    expression,   <2>
    [expression]) <3>
```

**Input**:

1. boolean condition to check
2. return value if the boolean condition evaluates to `true`
3. return value if the boolean condition evaluates `false`; optional


**Output**: 2nd expression if 1st expression (condition) evaluates to `true`. If it evaluates to `false` return 3rd expression. If 3rd expression is not provided return `null`.

**Description**: Conditional function that implements the standard *IF <condition> THEN <result1> ELSE <result2>* logic of programming languages. If the 3rd expression is not provided and the condition evaluates to `false`, `null` is returned.

```sql
SELECT IIF(1 < 2, 'TRUE', 'FALSE') AS result1, IIF(1 > 2, 'TRUE', 'FALSE') AS result2;

    result1    |    result2
---------------+---------------
TRUE           |FALSE
```

```sql
SELECT IIF(1 < 2, 'TRUE') AS result1, IIF(1 > 2 , 'TRUE') AS result2;

    result1    |    result2
---------------+---------------
TRUE           |null
```

::::{tip}
**IIF** functions can be combined to implement more complex logic simulating the [`CASE`](#sql-functions-conditional-case) expression. E.g.:

```sql
IIF(a = 1, 'one', IIF(a = 2, 'two', IIF(a = 3, 'three', 'many')))
```

::::



## `ISNULL` [sql-functions-conditional-isnull]

```sql
ISNULL(
    expression, <1>
    expression) <2>
```

**Input**:

1. 1st expression
2. 2nd expression


**Output**: 2nd expression if 1st expression is null, otherwise 1st expression.

**Description**: Variant of [`COALESCE`](#sql-functions-conditional-coalesce) with only two arguments. Returns the first of its arguments that is not null. If all arguments are null, then it returns `null`.

```sql
SELECT ISNULL('elastic', null) AS "isnull";

    isnull
---------------
elastic
```

```sql
SELECT ISNULL(null, 'search') AS "isnull";

    isnull
---------------
search
```


## `LEAST` [sql-functions-conditional-least]

```sql
LEAST(
    expression, <1>
    expression, <2>
    ...)
```

**Input**:

1. 1st expression
2. 2nd expression


… 

**N**th expression

LEAST can take an arbitrary number of arguments and all of them must be of the same data type.

**Output**: one of the expressions or `null`

**Description**: Returns the argument that has the smallest value which is not null. If all arguments are null, then it returns `null`.

```sql
SELECT LEAST(null, 2, 1) AS "least";

    least
---------------
1
```

```sql
SELECT LEAST(null, null, null, null) AS "least";

    least
---------------
null
```


## `NULLIF` [sql-functions-conditional-nullif]

```sql
NULLIF(
    expression, <1>
    expression) <2>
```

**Input**:

1. 1st expression
2. 2nd expression


**Output**: `null` if the 2 expressions are equal, otherwise the 1st expression.

**Description**: Returns `null` when the two input expressions are equal and if not, it returns the 1st expression.

```sql
SELECT NULLIF('elastic', 'search') AS "nullif";
    nullif
---------------
elastic
```

```sql
SELECT NULLIF('elastic', 'elastic') AS "nullif";

    nullif:s
---------------
null
```


## `NVL` [sql-functions-conditional-nvl]

```sql
NVL(
    expression, <1>
    expression) <2>
```

**Input**:

1. 1st expression
2. 2nd expression


**Output**: 2nd expression if 1st expression is null, otherwise 1st expression.

**Description**: Variant of [`COALESCE`](#sql-functions-conditional-coalesce) with only two arguments. Returns the first of its arguments that is not null. If all arguments are null, then it returns `null`.

```sql
SELECT NVL('elastic', null) AS "nvl";

    nvl
---------------
elastic
```

```sql
SELECT NVL(null, 'search') AS "nvl";

    nvl
---------------
search
```
