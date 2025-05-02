---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-syntax-show-columns.html
---

# SHOW COLUMNS [sql-syntax-show-columns]

```sql
SHOW COLUMNS
    [CATALOG identifier]? <1>
    [INCLUDE FROZEN]?     <2>
    [FROM | IN]
    [table_identifier |   <3>
     LIKE pattern]        <4>
```

1. Catalog (cluster) identifier. Supports wildcards (`*`).
2. Whether or not to include frozen indices.
3. Single table (index or data stream) identifier or double-quoted multi-target pattern.
4. SQL LIKE pattern matching table names.


See [index patterns](/reference/query-languages/sql/sql-index-patterns.md) for more information about patterns.

**Description**: List the columns in table and their data type (and other attributes).

```sql
SHOW COLUMNS IN emp;

       column       |     type      |    mapping
--------------------+---------------+---------------
birth_date          |TIMESTAMP      |datetime
dep                 |STRUCT         |nested
dep.dep_id          |VARCHAR        |keyword
dep.dep_name        |VARCHAR        |text
dep.dep_name.keyword|VARCHAR        |keyword
dep.from_date       |TIMESTAMP      |datetime
dep.to_date         |TIMESTAMP      |datetime
emp_no              |INTEGER        |integer
first_name          |VARCHAR        |text
first_name.keyword  |VARCHAR        |keyword
gender              |VARCHAR        |keyword
hire_date           |TIMESTAMP      |datetime
languages           |TINYINT        |byte
last_name           |VARCHAR        |text
last_name.keyword   |VARCHAR        |keyword
name                |VARCHAR        |keyword
salary              |INTEGER        |integer
```

