---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-doc-count-field.html
---

# _doc_count field [mapping-doc-count-field]

Bucket aggregations always return a field named `doc_count` showing the number of documents that were aggregated and partitioned in each bucket. Computation of the value of `doc_count` is very simple. `doc_count` is incremented by 1 for every document collected in each bucket.

While this simple approach is effective when computing aggregations over individual documents, it fails to accurately represent documents that store pre-aggregated data (such as `histogram` or `aggregate_metric_double` fields), because one summary field may represent multiple documents.

To allow for correct computation of the number of documents when working with pre-aggregated data, we have introduced a metadata field type named `_doc_count`. `_doc_count` must always be a positive integer representing the number of documents aggregated in a single summary field.

When field `_doc_count` is added to a document, all bucket aggregations will respect its value and increment the bucket `doc_count` by the value of the field. If a document does not contain any `_doc_count` field, `_doc_count = 1` is implied by default.

::::{important}
* A `_doc_count` field can only store a single positive integer per document. Nested arrays are not allowed.
* If a document contains no `_doc_count` fields, aggregators will increment by 1, which is the default behavior.

::::


## Example [mapping-doc-count-field-example]

The following [create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) API request creates a new index with the following field mappings:

* `my_histogram`, a `histogram` field used to store percentile data
* `my_text`, a `keyword` field used to store a title for the histogram

```console
PUT my_index
{
  "mappings" : {
    "properties" : {
      "my_histogram" : {
        "type" : "histogram"
      },
      "my_text" : {
        "type" : "keyword"
      }
    }
  }
}
```

The following [index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) API requests store pre-aggregated data for two histograms: `histogram_1` and `histogram_2`.

```console
PUT my_index/_doc/1
{
  "my_text" : "histogram_1",
  "my_histogram" : {
      "values" : [0.1, 0.2, 0.3, 0.4, 0.5],
      "counts" : [3, 7, 23, 12, 6]
   },
  "_doc_count": 45 <1>
}

PUT my_index/_doc/2
{
  "my_text" : "histogram_2",
  "my_histogram" : {
      "values" : [0.1, 0.25, 0.35, 0.4, 0.45, 0.5],
      "counts" : [8, 17, 8, 7, 6, 2]
   },
  "_doc_count": 62 <1>
}
```

1. Field `_doc_count` must be a positive integer storing the number of documents aggregated to produce each histogram.


If we run the following [terms aggregation](/reference/data-analysis/aggregations/search-aggregations-bucket-terms-aggregation.md) on `my_index`:

```console
GET /_search
{
    "aggs" : {
        "histogram_titles" : {
            "terms" : { "field" : "my_text" }
        }
    }
}
```

We will get the following response:

```console-result
{
    ...
    "aggregations" : {
        "histogram_titles" : {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets" : [
                {
                    "key" : "histogram_2",
                    "doc_count" : 62
                },
                {
                    "key" : "histogram_1",
                    "doc_count" : 45
                }
            ]
        }
    }
}
```


