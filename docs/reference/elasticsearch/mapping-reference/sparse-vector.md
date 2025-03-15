---
navigation_title: "Sparse vector"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sparse-vector.html
---

# Sparse vector field type [sparse-vector]


A `sparse_vector` field can index features and weights so that they can later be used to query documents in queries with a [`sparse_vector`](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md). This field can also be used with a legacy [`text_expansion`](/reference/query-languages/query-dsl/query-dsl-text-expansion-query.md) query.

`sparse_vector` is the field type that should be used with [ELSER mappings](docs-content://solutions/search/semantic-search/semantic-search-elser-ingest-pipelines.md#elser-mappings).

```console
PUT my-index
{
  "mappings": {
    "properties": {
      "text.tokens": {
        "type": "sparse_vector"
      }
    }
  }
}
```

See [semantic search with ELSER](docs-content://solutions/search/semantic-search/semantic-search-elser-ingest-pipelines.md) for a complete example on adding documents to a `sparse_vector` mapped field using ELSER.

## Parameters for `sparse_vector` fields [sparse-vectors-params]

The following parameters are accepted by `sparse_vector` fields:

[store](/reference/elasticsearch/mapping-reference/mapping-store.md)
:   Indicates whether the field value should be stored and retrievable independently of the [_source](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field. Accepted values: true or false (default). The field’s data is stored using term vectors, a disk-efficient structure compared to the original JSON input. The input map can be retrieved during a search request via the [`fields` parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param). To benefit from reduced disk usage, you must either:

    * Exclude the field from [_source](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#source-filtering).
    * Use [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source).



## Multi-value sparse vectors [index-multi-value-sparse-vectors]

When passing in arrays of values for sparse vectors the max value for similarly named features is selected.

The paper Adapting Learned Sparse Retrieval for Long Documents ([https://arxiv.org/pdf/2305.18494.pdf](https://arxiv.org/pdf/2305.18494.pdf)) discusses this in more detail. In summary, research findings support representation aggregation typically outperforming score aggregation.

For instances where you want to have overlapping feature names use should store them separately or use nested fields.

Below is an example of passing in a document with overlapping feature names. Consider that in this example two categories exist for positive sentiment and negative sentiment. However, for the purposes of retrieval we also want the overall impact rather than specific sentiment. In the example `impact` is stored as a multi-value sparse vector and only the max values of overlapping names are stored. More specifically the final `GET` query here returns a `_score` of ~1.2 (which is the `max(impact.delicious[0], impact.delicious[1])` and is approximate because we have a relative error of 0.4% as explained below)

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "text": {
        "type": "text",
        "analyzer": "standard"
      },
      "impact": {
        "type": "sparse_vector"
      },
      "positive": {
        "type": "sparse_vector"
      },
      "negative": {
        "type": "sparse_vector"
      }
    }
  }
}

POST my-index-000001/_doc
{
    "text": "I had some terribly delicious carrots.",
    "impact": [{"I": 0.55, "had": 0.4, "some": 0.28, "terribly": 0.01, "delicious": 1.2, "carrots": 0.8},
               {"I": 0.54, "had": 0.4, "some": 0.28, "terribly": 2.01, "delicious": 0.02, "carrots": 0.4}],
    "positive": {"I": 0.55, "had": 0.4, "some": 0.28, "terribly": 0.01, "delicious": 1.2, "carrots": 0.8},
    "negative": {"I": 0.54, "had": 0.4, "some": 0.28, "terribly": 2.01, "delicious": 0.02, "carrots": 0.4}
}

GET my-index-000001/_search
{
  "query": {
    "term": {
      "impact": {
         "value": "delicious"
      }
    }
  }
}
```

::::{note}
`sparse_vector` fields can not be included in indices that were **created** on {{es}} versions between 8.0 and 8.10
::::


::::{note}
`sparse_vector` fields only support strictly positive values. Negative values will be rejected.
::::


::::{note}
`sparse_vector` fields do not support [analyzers](docs-content://manage-data/data-store/text-analysis.md), querying, sorting or aggregating. They may only be used within specialized queries. The recommended query to use on these fields are [`sparse_vector`](/reference/query-languages/query-dsl/query-dsl-sparse-vector-query.md) queries. They may also be used within legacy [`text_expansion`](/reference/query-languages/query-dsl/query-dsl-text-expansion-query.md) queries.
::::


::::{note}
`sparse_vector` fields only preserve 9 significant bits for the precision, which translates to a relative error of about 0.4%.
::::



