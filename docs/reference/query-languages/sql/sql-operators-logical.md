---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-operators-logical.html
---

# Logical operators [sql-operators-logical]

Boolean operator for evaluating one or two expressions.

## `AND` [sql-operators-and]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no > 10000 AND emp_no < 10005 ORDER BY emp_no LIMIT 5;
```


## `OR` [sql-operators-or]

```sql
SELECT last_name l FROM "test_emp" WHERE emp_no < 10003 OR emp_no = 10005 ORDER BY emp_no LIMIT 5;
```


## `NOT` [sql-operators-not]

```sql
SELECT last_name l FROM "test_emp" WHERE NOT emp_no = 10000 LIMIT 5;
```


