---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-index-patterns.html
---

# Index patterns [sql-index-patterns]

Elasticsearch SQL supports two types of patterns for matching multiple indices or tables:


## {{es}} multi-target syntax [sql-index-patterns-multi]

The {{es}} notation for enumerating, including or excluding [multi-target syntax](/reference/elasticsearch/rest-apis/api-conventions.md#api-multi-index) is supported *as long* as it is quoted or escaped as a table identifier.

For example:

```sql
SHOW TABLES "*,-l*";

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
javaRestTest      |employees      |VIEW      |ALIAS
```

Notice the pattern is surrounded by double quotes `"`. It enumerated `*` meaning all indices however it excludes (due to `-`) all indices that start with `l`. This notation is very convenient and powerful as it allows both inclusion and exclusion, depending on the target naming convention.

The same kind of patterns can also be used to query multiple indices or tables.

For example:

```sql
SELECT emp_no FROM "e*p" LIMIT 1;

    emp_no
---------------
10001
```

::::{note}
There is the restriction that all resolved concrete tables have the exact same mapping.
::::


[preview] To run a [{{ccs}}](docs-content://solutions/search/cross-cluster-search.md), specify a cluster name using the `<remote_cluster>:<target>` syntax, where `<remote_cluster>` maps to a SQL catalog (cluster) and `<target>` to a table (index or data stream). The `<remote_cluster>` supports wildcards (`*`) and `<target>` can be an index pattern.

For example:

```sql
SELECT emp_no FROM "my*cluster:*emp" LIMIT 1;

    emp_no
---------------
10001
```


## SQL `LIKE` notation [sql-index-patterns-like]

The common `LIKE` statement (including escaping if needed) to match a wildcard pattern, based on one `_` or multiple `%` characters.

Using `SHOW TABLES` command again:

```sql
SHOW TABLES LIKE 'emp%';

 catalog       |     name      | type     |     kind
---------------+---------------+----------+---------------
javaRestTest      |emp            |TABLE     |INDEX
javaRestTest      |employees      |VIEW      |ALIAS
```

The pattern matches all tables that start with `emp`.

This command supports *escaping* as well, for example:

```sql
SHOW TABLES LIKE 'emp!%' ESCAPE '!';

 catalog       |     name      |     type      |     kind
---------------+---------------+---------------+---------------
```

Notice how now `emp%` does not match any tables because `%`, which means match zero or more characters, has been escaped by `!` and thus becomes an regular char. And since there is no table named `emp%`, an empty table is returned.

In a nutshell, the differences between the two type of patterns are:

| **Feature** | **Multi index** | **SQL `LIKE`** |
| --- | --- | --- |
| Type of quoting | `"` | `'` |
| Inclusion | Yes | Yes |
| Exclusion | Yes | No |
| Enumeration | Yes | No |
| One char pattern | No | `_` |
| Multi char pattern | `*` | `%` |
| Escaping | No | `ESCAPE` |

Which one to use, is up to you however try to stick to the same one across your queries for consistency.

::::{note}
As the query type of quoting between the two patterns is fairly similar (`"` vs `'`), Elasticsearch SQL *always* requires the keyword `LIKE` for SQL `LIKE` pattern.
::::


