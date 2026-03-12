---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-operators-cast.html
---

# Cast operators [sql-operators-cast]

## `Cast (::)` [sql-operators-cast-cast]

`::` provides an alternative syntax to the [`CAST`](/reference/query-languages/sql/sql-functions-type-conversion.md#sql-functions-type-conversion-cast) function.

```sql
SELECT '123'::long AS long;

      long
---------------
123
```


