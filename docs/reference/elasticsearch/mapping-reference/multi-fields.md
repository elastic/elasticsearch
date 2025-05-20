---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
---

# fields [multi-fields]

It is often useful to index the same field in different ways for different purposes. This is the purpose of *multi-fields*. For instance, a `string` field could be mapped as a `text` field for full-text search, and as a `keyword` field for sorting or aggregations:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "city": {
        "type": "text",
        "fields": {
          "raw": { <1>
            "type":  "keyword"
          }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "city": "New York"
}

PUT my-index-000001/_doc/2
{
  "city": "York"
}

GET my-index-000001/_search
{
  "query": {
    "match": {
      "city": "york" <2>
    }
  },
  "sort": {
    "city.raw": "asc" <3>
  },
  "aggs": {
    "Cities": {
      "terms": {
        "field": "city.raw" <3>
      }
    }
  }
}
```

1. The `city.raw` field is a `keyword` version of the `city` field.
2. The `city` field can be used for full text search.
3. The `city.raw` field can be used for sorting and aggregations


You can add multi-fields to an existing field using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).

::::{warning}
If an index (or data stream) contains documents when you add a multi-field, those documents will not have values for the new multi-field. You can populate the new multi-field with the [update by query API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query).
::::


A multi-field mapping is completely separate from the parent field’s mapping. A multi-field doesn’t inherit any mapping options from its parent field. Multi-fields don’t change the original `_source` field.

## Multi-fields with multiple analyzers [_multi_fields_with_multiple_analyzers]

Another use case of multi-fields is to analyze the same field in different ways for better relevance. For instance we could index a field with the [`standard` analyzer](/reference/text-analysis/analysis-standard-analyzer.md) which breaks text up into words, and again with the [`english` analyzer](/reference/text-analysis/analysis-lang-analyzer.md#english-analyzer) which stems words into their root form:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "text": { <1>
        "type": "text",
        "fields": {
          "english": { <2>
            "type":     "text",
            "analyzer": "english"
          }
        }
      }
    }
  }
}

PUT my-index-000001/_doc/1
{ "text": "quick brown fox" } <3>

PUT my-index-000001/_doc/2
{ "text": "quick brown foxes" } <3>

GET my-index-000001/_search
{
  "query": {
    "multi_match": {
      "query": "quick brown foxes",
      "fields": [ <4>
        "text",
        "text.english"
      ],
      "type": "most_fields" <4>
    }
  }
}
```

1. The `text` field uses the `standard` analyzer.
2. The `text.english` field uses the `english` analyzer.
3. Index two documents, one with `fox` and the other with `foxes`.
4. Query both the `text` and `text.english` fields and combine the scores.


The `text` field contains the term `fox` in the first document and `foxes` in the second document. The `text.english` field contains `fox` for both documents, because `foxes` is stemmed to `fox`.

The query string is also analyzed by the `standard` analyzer for the `text` field, and by the `english` analyzer for the `text.english` field. The stemmed field allows a query for `foxes` to also match the document containing just `fox`. This allows us to match as many documents as possible. By also querying the unstemmed `text` field, we improve the relevance score of the document which matches `foxes` exactly.


