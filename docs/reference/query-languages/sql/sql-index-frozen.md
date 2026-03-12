---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-index-frozen.html
---

# Frozen indices [sql-index-frozen]

By default, Elasticsearch SQL doesn't search frozen indices. To search frozen indices, use one of the following features:

dedicated configuration parameter
:   Set to `true` properties `index_include_frozen` in the [SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query) or `index.include.frozen` in the drivers to include frozen indices.

dedicated keyword
:   Explicitly perform the inclusion through the dedicated `FROZEN` keyword in the `FROM` clause or `INCLUDE FROZEN` in the `SHOW` commands:

```sql
SHOW TABLES INCLUDE FROZEN;

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |archive        |TABLE     |FROZEN INDEX
javaRestTest      |emp            |TABLE     |INDEX
javaRestTest      |employees      |VIEW      |ALIAS
javaRestTest      |library        |TABLE     |INDEX
```

```sql
SELECT * FROM FROZEN archive LIMIT 1;

     author      |        name        |  page_count   |    release_date
-----------------+--------------------+---------------+--------------------
James S.A. Corey |Leviathan Wakes     |561            |2011-06-02T00:00:00Z
```

Unless enabled, frozen indices are completely ignored; it is as if they do not exist and as such, queries ran against them are likely to fail.

