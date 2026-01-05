---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-functions-system.html
---

# System functions [sql-functions-system]

These functions return metadata type of information about the system being queried.

## `DATABASE` [sql-functions-system-database]

```sql
DATABASE()
```

**Input**: *none*

**Output**: string

**Description**: Returns the name of the database being queried. In the case of Elasticsearch SQL, this is the name of the Elasticsearch cluster. This function should always return a non-null value.

```sql
SELECT DATABASE();

   DATABASE
---------------
elasticsearch
```


## `USER` [sql-functions-system-user]

```sql
USER()
```

**Input**: *none*

**Output**: string

**Description**: Returns the username of the authenticated user executing the query. This function can return `null` in case [security](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md) is disabled.

```sql
SELECT USER();

     USER
---------------
elastic
```


