---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-translate.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# SQL Translate API [sql-translate]

The SQL Translate API accepts SQL in a JSON document and translates it into native {{es}} queries. For example:

```console
POST /_sql/translate
{
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 10
}
```
% TEST[setup:library]

Which returns:

```console-result
{
  "size": 10,
  "_source": false,
  "fields": [
    {
      "field": "author"
    },
    {
      "field": "name"
    },
    {
      "field": "page_count"
    },
    {
      "field": "release_date",
      "format": "strict_date_optional_time_nanos"
    }
  ],
  "sort": [
    {
      "page_count": {
        "order": "desc",
        "missing": "_first",
        "unmapped_type": "short"
      }
    }
  ],
  "track_total_hits": -1
}
```

Which is the request that SQL will run to provide the results. In this case, SQL will use the [scroll](/reference/elasticsearch/rest-apis/paginate-search-results.md#scroll-search-results) API. If the result contained an aggregation then SQL would use the normal [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search).

The request body accepts the same [parameters](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query) as the [SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-query), excluding `cursor`.

