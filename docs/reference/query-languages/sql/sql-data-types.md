---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html
---

# Data types [sql-data-types]

**Core types**

| **{{es}} type** | **Elasticsearch SQL type** | **SQL type** | **SQL precision** |
| --- | --- | --- | --- |
| [`null`](/reference/elasticsearch/mapping-reference/null-value.md) | `null` | NULL | 0 |
| [`boolean`](/reference/elasticsearch/mapping-reference/boolean.md) | `boolean` | BOOLEAN | 1 |
| [`byte`](/reference/elasticsearch/mapping-reference/number.md) | `byte` | TINYINT | 3 |
| [`short`](/reference/elasticsearch/mapping-reference/number.md) | `short` | SMALLINT | 5 |
| [`integer`](/reference/elasticsearch/mapping-reference/number.md) | `integer` | INTEGER | 10 |
| [`long`](/reference/elasticsearch/mapping-reference/number.md) | `long` | BIGINT | 19 |
| [`unsigned_long`](/reference/elasticsearch/mapping-reference/number.md) | `[preview] unsigned_long` | BIGINT | 20 |
| [`double`](/reference/elasticsearch/mapping-reference/number.md) | `double` | DOUBLE | 15 |
| [`float`](/reference/elasticsearch/mapping-reference/number.md) | `float` | REAL | 7 |
| [`half_float`](/reference/elasticsearch/mapping-reference/number.md) | `half_float` | FLOAT | 3 |
| [`scaled_float`](/reference/elasticsearch/mapping-reference/number.md) | `scaled_float` | DOUBLE | 15 |
| [keyword type family](/reference/elasticsearch/mapping-reference/keyword.md) | `keyword` | VARCHAR | 32,766 |
| [`text`](/reference/elasticsearch/mapping-reference/text.md) | `text` | VARCHAR | 2,147,483,647 |
| [`binary`](/reference/elasticsearch/mapping-reference/binary.md) | `binary` | VARBINARY | 2,147,483,647 |
| [`date`](/reference/elasticsearch/mapping-reference/date.md) | `datetime` | TIMESTAMP | 29 |
| [`ip`](/reference/elasticsearch/mapping-reference/ip.md) | `ip` | VARCHAR | 39 |
| [`version`](/reference/elasticsearch/mapping-reference/version.md) | `version` | VARCHAR | 32,766 |

**Complex types**

| **{{es}} type** | **Elasticsearch SQL type** | **SQL type** | **SQL precision** |
| --- | --- | --- | --- |
| [`object`](/reference/elasticsearch/mapping-reference/object.md) | `object` | STRUCT | 0 |
| [`nested`](/reference/elasticsearch/mapping-reference/nested.md) | `nested` | STRUCT | 0 |

**Unsupported types**

| **{{es}} type** | **Elasticsearch SQL type** | **SQL type** | **SQL precision** |
| --- | --- | --- | --- |
| *types not mentioned above* | `unsupported` | OTHER | 0 |

::::{note}
Most of {{es}} [data types](/reference/elasticsearch/mapping-reference/field-data-types.md) are available in Elasticsearch SQL, as indicated above. As one can see, all of {{es}} [data types](/reference/elasticsearch/mapping-reference/field-data-types.md) are mapped to the data type with the same name in Elasticsearch SQL, with the exception of **date** data type which is mapped to **datetime** in Elasticsearch SQL. This is to avoid confusion with the ANSI SQL types **DATE** (date only) and **TIME** (time only), which are also supported by Elasticsearch SQL in queries (with the use of [`CAST`](/reference/query-languages/sql/sql-functions-type-conversion.md#sql-functions-type-conversion-cast)/[`CONVERT`](/reference/query-languages/sql/sql-functions-type-conversion.md#sql-functions-type-conversion-convert)), but don’t correspond to an actual mapping in {{es}} (see the [`table`](#es-sql-only-types) below).
::::


Obviously, not all types in {{es}} have an equivalent in SQL and vice-versa hence why, Elasticsearch SQL uses the data type *particularities* of the former over the latter as ultimately {{es}} is the backing store.

In addition to the types above, Elasticsearch SQL also supports at *runtime* SQL-specific types that do not have an equivalent in {{es}}. Such types cannot be loaded from {{es}} (as it does not know about them) however can be used inside Elasticsearch SQL in queries or their results.

$$$es-sql-only-types$$$
The table below indicates these types:

| **SQL type** | **SQL precision** |
| --- | --- |
| `date` | 29 |
| `time` | 18 |
| `interval_year` | 7 |
| `interval_month` | 7 |
| `interval_day` | 23 |
| `interval_hour` | 23 |
| `interval_minute` | 23 |
| `interval_second` | 23 |
| `interval_year_to_month` | 7 |
| `interval_day_to_hour` | 23 |
| `interval_day_to_minute` | 23 |
| `interval_day_to_second` | 23 |
| `interval_hour_to_minute` | 23 |
| `interval_hour_to_second` | 23 |
| `interval_minute_to_second` | 23 |
| `geo_point` | 52 |
| `geo_shape` | 2,147,483,647 |
| `shape` | 2,147,483,647 |


## SQL and multi-fields [sql-multi-field]

A core concept in {{es}} is that of an `analyzed` field, that is a full-text value that is interpreted in order to be effectively indexed. These fields are of type [`text`](/reference/elasticsearch/mapping-reference/text.md) and are not used for sorting or aggregations as their actual value depends on the [`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md) used hence why {{es}} also offers the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) type for storing the *exact* value.

In most case, and the default actually, is to use both types for strings which {{es}} supports through [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md), that is the ability to index the same string in multiple ways; for example index it both as `text` for search but also as `keyword` for sorting and aggregations.

As SQL requires exact values, when encountering a `text` field Elasticsearch SQL will search for an exact multi-field that it can use for comparisons, sorting and aggregations. To do that, it will search for the first `keyword` that it can find that is *not* normalized and use that as the original field *exact* value.

Consider the following `string` mapping:

```js
{
  "first_name": {
    "type": "text",
    "fields": {
      "raw": {
        "type": "keyword"
      }
    }
  }
}
```

The following SQL query:

```sql
SELECT first_name FROM index WHERE first_name = 'John'
```

is identical to:

```sql
SELECT first_name FROM index WHERE first_name.raw = 'John'
```

as Elasticsearch SQL automatically *picks* up the `raw` multi-field from `raw` for exact matching.
