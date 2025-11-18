---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-syntax-show-tables.html
---

# SHOW TABLES [sql-syntax-show-tables]

```sql
SHOW TABLES
    [CATALOG [catalog_identifier | <1>
              LIKE pattern]]?      <2>
    [INCLUDE FROZEN]?              <3>
    [table_identifier |            <4>
     LIKE pattern]?                <5>
```

1. Catalog (cluster) identifier. Supports wildcards (`*`).
2. SQL LIKE pattern matching catalog names.
3. Whether or not to include frozen indices.
4. Single table (index or data stream) identifier or double-quoted multi-target pattern.
5. SQL LIKE pattern matching table names.


See [index patterns](/reference/query-languages/sql/sql-index-patterns.md) for more information about patterns.

**Description**: List the tables available to the current user and their type.

```sql
SHOW TABLES;

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
javaRestTest      |employees      |VIEW      |ALIAS
javaRestTest      |library        |TABLE     |INDEX
```

Match multiple indices by using {{es}} [multi-target syntax](/reference/elasticsearch/rest-apis/api-conventions.md#api-multi-index) notation:

```sql
SHOW TABLES "*,-l*";

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
javaRestTest      |employees      |VIEW      |ALIAS
```

One can also use the `LIKE` clause to restrict the list of names to the given pattern.

The pattern can be an exact match:

```sql
SHOW TABLES LIKE 'emp';

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
```

Multiple chars:

```sql
SHOW TABLES LIKE 'emp%';

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
javaRestTest      |employees      |VIEW      |ALIAS
```

A single char:

```sql
SHOW TABLES LIKE 'em_';

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
```

Or a mixture of single and multiple chars:

```sql
SHOW TABLES LIKE '%em_';

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
```

List tables within remote clusters whose names are matched by a wildcard:

```sql
SHOW TABLES CATALOG 'my_*' LIKE 'test_emp%';

     catalog     |     name      |     type      |     kind
-----------------+---------------+---------------+---------------
my_remote_cluster|test_emp       |TABLE          |INDEX
my_remote_cluster|test_emp_copy  |TABLE          |INDEX
```

