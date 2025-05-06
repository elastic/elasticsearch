---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-operators.html
---

# Comparison operators [sql-operators]

Boolean operator for comparing against one or multiple expressions.

## `Equality (=)` [sql-operators-equality]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no = 10000 LIMIT 5;
```


## `Null safe Equality (<=>)` [sql-operators-null-safe-equality]

```sql
SELECT 'elastic' <=> null AS "equals";

    equals
---------------
false
```

```sql
SELECT null <=> null AS "equals";

    equals
---------------
true
```


## `Inequality (<> or !=)` [sql-operators-inequality]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no <> 10000 ORDER BY emp_no LIMIT 5;
```


## `Comparison (<, <=, >, >=)` [sql-operators-comparison]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no < 10003 ORDER BY emp_no LIMIT 5;
```


## `BETWEEN` [sql-operators-between]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no BETWEEN 9990 AND 10003 ORDER BY emp_no;
```


## `IS NULL/IS NOT NULL` [sql-operators-is-null]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no IS NOT NULL AND gender IS NULL;
```


## `IN (<value1>, <value2>, ...)` [sql-operators-in]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no IN (10000, 10001, 10002, 999) ORDER BY emp_no LIMIT 5;
```


