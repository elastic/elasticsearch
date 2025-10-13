---
navigation_title: "Multi Terms"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-multi-terms-aggregation.html
---

# Multi Terms aggregation [search-aggregations-bucket-multi-terms-aggregation]


A multi-bucket value source based aggregation where buckets are dynamically built - one per unique set of values. The multi terms aggregation is very similar to the [`terms aggregation`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-order), however in most cases it will be slower than the terms aggregation and will consume more memory. Therefore, if the same set of fields is constantly used, it would be more efficient to index a combined key for this fields as a separate field and use the terms aggregation on this field.

The multi_term aggregations are the most useful when you need to sort by a number of document or a metric aggregation on a composite key and get top N results. If sorting is not required and all values are expected to be retrieved using nested terms aggregation or [`composite aggregations`](/reference/aggregations/search-aggregations-bucket-composite-aggregation.md) will be a faster and more memory efficient solution.

Example:

$$$multi-terms-aggregation-example$$$

```console
GET /products/_search
{
  "aggs": {
    "genres_and_products": {
      "multi_terms": {
        "terms": [{
          "field": "genre" <1>
        }, {
          "field": "product"
        }]
      }
    }
  }
}
```

1. `multi_terms` aggregation can work with the same field types as a [`terms aggregation`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-order) and supports most of the terms aggregation parameters.


Response:

```console-result
{
  ...
  "aggregations" : {
    "genres_and_products" : {
      "doc_count_error_upper_bound" : 0,  <1>
      "sum_other_doc_count" : 0,          <2>
      "buckets" : [                       <3>
        {
          "key" : [                       <4>
            "rock",
            "Product A"
          ],
          "key_as_string" : "rock|Product A",
          "doc_count" : 2
        },
        {
          "key" : [
            "electronic",
            "Product B"
          ],
          "key_as_string" : "electronic|Product B",
          "doc_count" : 1
        },
        {
          "key" : [
            "jazz",
            "Product B"
          ],
          "key_as_string" : "jazz|Product B",
          "doc_count" : 1
        },
        {
          "key" : [
            "rock",
            "Product B"
          ],
          "key_as_string" : "rock|Product B",
          "doc_count" : 1
        }
      ]
    }
  }
}
```

1. an upper bound of the error on the document counts for each term, see <<search-aggregations-bucket-multi-terms-aggregation-approximate-counts,below>
2. when there are lots of unique terms, Elasticsearch only returns the top terms; this number is the sum of the document counts for all buckets that are not part of the response
3. the list of the top buckets.
4. the keys are arrays of values ordered the same ways as expression in the `terms` parameter of the aggregation


By default, the `multi_terms` aggregation will return the buckets for the top ten terms ordered by the `doc_count`. One can change this default behaviour by setting the `size` parameter.

## Aggregation Parameters [search-aggregations-bucket-multi-terms-aggregation-parameters]

The following parameters are supported. See [`terms aggregation`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-order) for more detailed explanation of these parameters.

size
:   Optional. Defines how many term buckets should be returned out of the overall terms list. Defaults to 10.

shard_size
:   Optional. The higher the requested `size` is, the more accurate the results will be, but also, the more expensive it will be to compute the final results. The default `shard_size` is `(size * 1.5 + 10)`.

show_term_doc_count_error
:   Optional. Calculates the doc count error on per term basis. Defaults to `false`

order
:   Optional. Specifies the order of the buckets. Defaults to the number of documents per bucket. The bucket terms value is used as a tiebreaker for buckets with the same document count.

min_doc_count
:   Optional. The minimal number of documents in a bucket for it to be returned. Defaults to 1.

shard_min_doc_count
:   Optional. The minimal number of documents in a bucket on each shard for it to be returned. Defaults to `min_doc_count`.

collect_mode
:   Optional. Specifies the strategy for data collection. The `depth_first` or `breadth_first` modes are supported. Defaults to `breadth_first`.


## Script [search-aggregations-bucket-multi-terms-aggregation-script]

Generating the terms using a script:

$$$multi-terms-aggregation-runtime-field-example$$$

```console
GET /products/_search
{
  "runtime_mappings": {
    "genre.length": {
      "type": "long",
      "script": "emit(doc['genre'].value.length())"
    }
  },
  "aggs": {
    "genres_and_products": {
      "multi_terms": {
        "terms": [
          {
            "field": "genre.length"
          },
          {
            "field": "product"
          }
        ]
      }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations" : {
    "genres_and_products" : {
      "doc_count_error_upper_bound" : 0,
      "sum_other_doc_count" : 0,
      "buckets" : [
        {
          "key" : [
            4,
            "Product A"
          ],
          "key_as_string" : "4|Product A",
          "doc_count" : 2
        },
        {
          "key" : [
            4,
            "Product B"
          ],
          "key_as_string" : "4|Product B",
          "doc_count" : 2
        },
        {
          "key" : [
            10,
            "Product B"
          ],
          "key_as_string" : "10|Product B",
          "doc_count" : 1
        }
      ]
    }
  }
}
```


## Missing value [_missing_value_3]

The `missing` parameter defines how documents that are missing a value should be treated. By default if any of the key components are missing the entire document will be ignored but it is also possible to treat them as if they had a value by using the `missing` parameter.

$$$multi-terms-aggregation-missing-example$$$

```console
GET /products/_search
{
  "aggs": {
    "genres_and_products": {
      "multi_terms": {
        "terms": [
          {
            "field": "genre"
          },
          {
            "field": "product",
            "missing": "Product Z"
          }
        ]
      }
    }
  }
}
```

Response:

```console-result
{
   ...
   "aggregations" : {
    "genres_and_products" : {
      "doc_count_error_upper_bound" : 0,
      "sum_other_doc_count" : 0,
      "buckets" : [
        {
          "key" : [
            "rock",
            "Product A"
          ],
          "key_as_string" : "rock|Product A",
          "doc_count" : 2
        },
        {
          "key" : [
            "electronic",
            "Product B"
          ],
          "key_as_string" : "electronic|Product B",
          "doc_count" : 1
        },
        {
          "key" : [
            "electronic",
            "Product Z"
          ],
          "key_as_string" : "electronic|Product Z",  <1>
          "doc_count" : 1
        },
        {
          "key" : [
            "jazz",
            "Product B"
          ],
          "key_as_string" : "jazz|Product B",
          "doc_count" : 1
        },
        {
          "key" : [
            "rock",
            "Product B"
          ],
          "key_as_string" : "rock|Product B",
          "doc_count" : 1
        }
      ]
    }
  }
}
```

1. Documents without a value in the `product` field will fall into the same bucket as documents that have the value `Product Z`.



## Mixing field types [_mixing_field_types]

::::{warning}
When aggregating on multiple indices the type of the aggregated field may not be the same in all indices. Some types are compatible with each other (`integer` and `long` or `float` and `double`) but when the types are a mix of decimal and non-decimal number the terms aggregation will promote the non-decimal numbers to decimal numbers. This can result in a loss of precision in the bucket values.
::::



## Sub aggregation and sorting examples [_sub_aggregation_and_sorting_examples]

As most bucket aggregations the `multi_term` supports sub aggregations and ordering the buckets by metrics sub-aggregation:

$$$multi-terms-aggregation-subaggregation-example$$$

```console
GET /products/_search
{
  "aggs": {
    "genres_and_products": {
      "multi_terms": {
        "terms": [
          {
            "field": "genre"
          },
          {
            "field": "product"
          }
        ],
        "order": {
          "total_quantity": "desc"
        }
      },
      "aggs": {
        "total_quantity": {
          "sum": {
            "field": "quantity"
          }
        }
      }
    }
  }
}
```

```console-result
{
  ...
  "aggregations" : {
    "genres_and_products" : {
      "doc_count_error_upper_bound" : 0,
      "sum_other_doc_count" : 0,
      "buckets" : [
        {
          "key" : [
            "jazz",
            "Product B"
          ],
          "key_as_string" : "jazz|Product B",
          "doc_count" : 1,
          "total_quantity" : {
            "value" : 10.0
          }
        },
        {
          "key" : [
            "rock",
            "Product A"
          ],
          "key_as_string" : "rock|Product A",
          "doc_count" : 2,
          "total_quantity" : {
            "value" : 9.0
          }
        },
        {
          "key" : [
            "electronic",
            "Product B"
          ],
          "key_as_string" : "electronic|Product B",
          "doc_count" : 1,
          "total_quantity" : {
            "value" : 3.0
          }
        },
        {
          "key" : [
            "rock",
            "Product B"
          ],
          "key_as_string" : "rock|Product B",
          "doc_count" : 1,
          "total_quantity" : {
            "value" : 1.0
          }
        }
      ]
    }
  }
}
```


