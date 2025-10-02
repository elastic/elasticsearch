---
navigation_title: "Categorize text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-categorize-text-aggregation.html
---

# Categorize text aggregation [search-aggregations-bucket-categorize-text-aggregation]


A multi-bucket aggregation that groups semi-structured text into buckets. Each `text` field is re-analyzed using a custom analyzer. The resulting tokens are then categorized creating buckets of similarly formatted text values. This aggregation works best with machine generated text like system logs. Only the first 100 analyzed tokens are used to categorize the text.

::::{note}
If you have considerable memory allocated to your JVM but are receiving circuit breaker exceptions from this aggregation, you may be attempting to categorize text that is poorly formatted for categorization. Consider adding `categorization_filters` or running under [sampler](/reference/aggregations/search-aggregations-bucket-sampler-aggregation.md), [diversified sampler](/reference/aggregations/search-aggregations-bucket-diversified-sampler-aggregation.md), or [random sampler](/reference/aggregations/search-aggregations-random-sampler-aggregation.md) to explore the created categories.
::::


::::{note}
The algorithm used for categorization was completely changed in version 8.3.0. As a result this aggregation will not work in a mixed version cluster where some nodes are on version 8.3.0 or higher and others are on a version older than 8.3.0. Upgrade all nodes in your cluster to the same version if you experience an error related to this change.
::::


## Parameters [bucket-categorize-text-agg-syntax]

`categorization_analyzer`
:   (Optional, object or string) The categorization analyzer specifies how the text is analyzed and tokenized before being categorized. The syntax is very similar to that used to define the `analyzer` in the [Analyze endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze). This property cannot be used at the same time as `categorization_filters`.

    The `categorization_analyzer` field can be specified either as a string or as an object. If it is a string it must refer to a [built-in analyzer](/reference/text-analysis/analyzer-reference.md) or one added by another plugin. If it is an object it has the following properties:

    **Properties of `categorization_analyzer`**:

    `char_filter`
    :   (array of strings or objects) One or more [character filters](/reference/text-analysis/character-filter-reference.md). In addition to the built-in character filters, other plugins can provide more character filters. This property is optional. If it is not specified, no character filters are applied prior to categorization. If you are customizing some other aspect of the analyzer and you need to achieve the equivalent of `categorization_filters` (which are not permitted when some other aspect of the analyzer is customized), add them here as [pattern replace character filters](/reference/text-analysis/analysis-pattern-replace-charfilter.md).

    `tokenizer`
    :   (string or object) The name or definition of the [tokenizer](/reference/text-analysis/tokenizer-reference.md) to use after character filters are applied. This property is compulsory if `categorization_analyzer` is specified as an object. Machine learning provides a tokenizer called `ml_standard` that tokenizes in a way that has been determined to produce good categorization results on a variety of log file formats for logs in English. If you want to use that tokenizer but change the character or token filters, specify `"tokenizer": "ml_standard"` in your `categorization_analyzer`. Additionally, the `ml_classic` tokenizer is available, which tokenizes in the same way as the non-customizable tokenizer in old versions of the product (before 6.2). `ml_classic` was the default categorization tokenizer in versions 6.2 to 7.13, so if you need categorization identical to the default for jobs created in these versions, specify `"tokenizer": "ml_classic"` in your `categorization_analyzer`.

    ::::{note}
    From {{es}} 8.10.0,  a new version number is used to track the configuration and state changes in the {{ml}} plugin. This new version number is decoupled from the product version and will increment independently.
    ::::


    `filter`
    :   (array of strings or objects) One or more [token filters](/reference/text-analysis/token-filter-reference.md). In addition to the built-in token filters, other plugins can provide more token filters. This property is optional. If it is not specified, no token filters are applied prior to categorization.


`categorization_filters`
:   (Optional, array of strings) This property expects an array of regular expressions. The expressions are used to filter out matching sequences from the categorization field values. You can use this functionality to fine tune the categorization by excluding sequences from consideration when categories are defined. For example, you can exclude SQL statements that appear in your log files. This property cannot be used at the same time as `categorization_analyzer`. If you only want to define simple regular expression filters that are applied prior to tokenization, setting this property is the easiest method. If you also want to customize the tokenizer or post-tokenization filtering, use the `categorization_analyzer` property instead and include the filters as `pattern_replace` character filters.

`field`
:   (Required, string) The semi-structured text field to categorize.

`max_matched_tokens`
:   (Optional, integer) This parameter does nothing now, but is permitted for compatibility with the original pre-8.3.0 implementation.

`max_unique_tokens`
:   (Optional, integer) This parameter does nothing now, but is permitted for compatibility with the original pre-8.3.0 implementation.

`min_doc_count`
:   (Optional, integer) The minimum number of documents for a bucket to be returned to the results.

`shard_min_doc_count`
:   (Optional, integer) The minimum number of documents for a bucket to be returned from the shard before merging.

`shard_size`
:   (Optional, integer) The number of categorization buckets to return from each shard before merging all the results.

`similarity_threshold`
:   (Optional, integer, default: `70`) The minimum percentage of token weight that must match for text to be added to the category bucket. Must be between 1 and 100. The larger the value the narrower the categories. Larger values will increase memory usage and create narrower categories.

`size`
:   (Optional, integer, default: `10`) The number of buckets to return.


## Response body [bucket-categorize-text-agg-response]

`key`
:   (string) Consists of the tokens (extracted by the `categorization_analyzer`) that are common to all values of the input field included in the category.

`doc_count`
:   (integer) Number of documents matching the category.

`max_matching_length`
:   (integer) Categories from short messages containing few tokens may also match categories containing many tokens derived from much longer messages. `max_matching_length` is an indication of the maximum length of messages that should be considered to belong to the category. When searching for messages that match the category, any messages longer than `max_matching_length` should be excluded. Use this field to prevent a search for members of a category of short messages from matching much longer ones.

`regex`
:   (string) A regular expression that will match all values of the input field included in the category. It is possible that the `regex` does not incorporate every term in `key`, if ordering varies between the values included in the category. However, in simple cases the `regex` will be the ordered terms concatenated into a regular expression that allows for arbitrary sections in between them. It is not recommended to use the `regex` as the primary mechanism for searching for the original documents that were categorized. Search using a regular expression is very slow. Instead the terms in the `key` field should be used to search for matching documents, as a terms search can use the inverted index and hence be much faster. However, there may be situations where it is useful to use the `regex` field to test whether a small set of messages that have not been indexed match the category, or to confirm that the terms in the `key` occur in the correct order in all the matched documents.


## Basic use [_basic_use]

::::{warning}
Re-analyzing *large* result sets will require a lot of time and memory. This aggregation should be used in conjunction with [Async search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-submit). Additionally, you may consider using the aggregation as a child of either the [sampler](/reference/aggregations/search-aggregations-bucket-sampler-aggregation.md) or [diversified sampler](/reference/aggregations/search-aggregations-bucket-diversified-sampler-aggregation.md) aggregation. This will typically improve speed and memory use.
::::


Example:

```console
POST log-messages/_search?filter_path=aggregations
{
  "aggs": {
    "categories": {
      "categorize_text": {
        "field": "message"
      }
    }
  }
}
```

Response:

```console-result
{
  "aggregations" : {
    "categories" : {
      "buckets" : [
        {
          "doc_count" : 3,
          "key" : "Node shutting down",
          "regex" : ".*?Node.+?shutting.+?down.*?",
          "max_matching_length" : 49
        },
        {
          "doc_count" : 1,
          "key" : "Node starting up",
          "regex" : ".*?Node.+?starting.+?up.*?",
          "max_matching_length" : 47
        },
        {
          "doc_count" : 1,
          "key" : "User foo_325 logging on",
          "regex" : ".*?User.+?foo_325.+?logging.+?on.*?",
          "max_matching_length" : 52
        },
        {
          "doc_count" : 1,
          "key" : "User foo_864 logged off",
          "regex" : ".*?User.+?foo_864.+?logged.+?off.*?",
          "max_matching_length" : 52
        }
      ]
    }
  }
}
```

Here is an example using `categorization_filters`

```console
POST log-messages/_search?filter_path=aggregations
{
  "aggs": {
    "categories": {
      "categorize_text": {
        "field": "message",
        "categorization_filters": ["\\w+\\_\\d{3}"] <1>
      }
    }
  }
}
```

1. The filters to apply to the analyzed tokens. It filters out tokens like `bar_123`.


Note how the `foo_<number>` tokens are not part of the category results

```console-result
{
  "aggregations" : {
    "categories" : {
      "buckets" : [
        {
          "doc_count" : 3,
          "key" : "Node shutting down",
          "regex" : ".*?Node.+?shutting.+?down.*?",
          "max_matching_length" : 49
        },
        {
          "doc_count" : 1,
          "key" : "Node starting up",
          "regex" : ".*?Node.+?starting.+?up.*?",
          "max_matching_length" : 47
        },
        {
          "doc_count" : 1,
          "key" : "User logged off",
          "regex" : ".*?User.+?logged.+?off.*?",
          "max_matching_length" : 52
        },
        {
          "doc_count" : 1,
          "key" : "User logging on",
          "regex" : ".*?User.+?logging.+?on.*?",
          "max_matching_length" : 52
        }
      ]
    }
  }
}
```

Here is an example using `categorization_filters`. The default analyzer uses the `ml_standard` tokenizer which is similar to a whitespace tokenizer but filters out tokens that could be interpreted as hexadecimal numbers. The default analyzer also uses the `first_line_with_letters` character filter, so that only the first meaningful line of multi-line messages is considered. But, it may be that a token is a known highly-variable token (formatted usernames, emails, etc.). In that case, it is good to supply custom `categorization_filters` to filter out those tokens for better categories. These filters may also reduce memory usage as fewer tokens are held in memory for the categories. (If there are sufficient examples of different usernames, emails, etc., then categories will form that naturally discard them as variables, but for small input data where only one example exists this won’t happen.)

```console
POST log-messages/_search?filter_path=aggregations
{
  "aggs": {
    "categories": {
      "categorize_text": {
        "field": "message",
        "categorization_filters": ["\\w+\\_\\d{3}"], <1>
        "similarity_threshold": 11 <2>
      }
    }
  }
}
```

1. The filters to apply to the analyzed tokens. It filters out tokens like `bar_123`.
2. Require 11% of token weight to match before adding a message to an existing category rather than creating a new one.


The resulting categories are now very broad, merging the log groups. (A `similarity_threshold` of 11% is generally too low. Settings over 50% are usually better.)

```console-result
{
  "aggregations" : {
    "categories" : {
      "buckets" : [
        {
          "doc_count" : 4,
          "key" : "Node",
          "regex" : ".*?Node.*?",
          "max_matching_length" : 49
        },
        {
          "doc_count" : 2,
          "key" : "User",
          "regex" : ".*?User.*?",
          "max_matching_length" : 52
        }
      ]
    }
  }
}
```

This aggregation can have both sub-aggregations and itself be a sub-aggregation. This allows gathering the top daily categories and the top sample doc as below.

```console
POST log-messages/_search?filter_path=aggregations
{
  "aggs": {
    "daily": {
      "date_histogram": {
        "field": "time",
        "fixed_interval": "1d"
      },
      "aggs": {
        "categories": {
          "categorize_text": {
            "field": "message",
            "categorization_filters": ["\\w+\\_\\d{3}"]
          },
          "aggs": {
            "hit": {
              "top_hits": {
                "size": 1,
                "sort": ["time"],
                "_source": "message"
              }
            }
          }
        }
      }
    }
  }
}
```

```console-result
{
  "aggregations" : {
    "daily" : {
      "buckets" : [
        {
          "key_as_string" : "2016-02-07T00:00:00.000Z",
          "key" : 1454803200000,
          "doc_count" : 3,
          "categories" : {
            "buckets" : [
              {
                "doc_count" : 2,
                "key" : "Node shutting down",
                "regex" : ".*?Node.+?shutting.+?down.*?",
                "max_matching_length" : 49,
                "hit" : {
                  "hits" : {
                    "total" : {
                      "value" : 2,
                      "relation" : "eq"
                    },
                    "max_score" : null,
                    "hits" : [
                      {
                        "_index" : "log-messages",
                        "_id" : "1",
                        "_score" : null,
                        "_source" : {
                          "message" : "2016-02-07T00:00:00+0000 Node 3 shutting down"
                        },
                        "sort" : [
                          1454803260000
                        ]
                      }
                    ]
                  }
                }
              },
              {
                "doc_count" : 1,
                "key" : "Node starting up",
                "regex" : ".*?Node.+?starting.+?up.*?",
                "max_matching_length" : 47,
                "hit" : {
                  "hits" : {
                    "total" : {
                      "value" : 1,
                      "relation" : "eq"
                    },
                    "max_score" : null,
                    "hits" : [
                      {
                        "_index" : "log-messages",
                        "_id" : "2",
                        "_score" : null,
                        "_source" : {
                          "message" : "2016-02-07T00:00:00+0000 Node 5 starting up"
                        },
                        "sort" : [
                          1454803320000
                        ]
                      }
                    ]
                  }
                }
              }
            ]
          }
        },
        {
          "key_as_string" : "2016-02-08T00:00:00.000Z",
          "key" : 1454889600000,
          "doc_count" : 3,
          "categories" : {
            "buckets" : [
              {
                "doc_count" : 1,
                "key" : "Node shutting down",
                "regex" : ".*?Node.+?shutting.+?down.*?",
                "max_matching_length" : 49,
                "hit" : {
                  "hits" : {
                    "total" : {
                      "value" : 1,
                      "relation" : "eq"
                    },
                    "max_score" : null,
                    "hits" : [
                      {
                        "_index" : "log-messages",
                        "_id" : "4",
                        "_score" : null,
                        "_source" : {
                          "message" : "2016-02-08T00:00:00+0000 Node 5 shutting down"
                        },
                        "sort" : [
                          1454889660000
                        ]
                      }
                    ]
                  }
                }
              },
              {
                "doc_count" : 1,
                "key" : "User logged off",
                "regex" : ".*?User.+?logged.+?off.*?",
                "max_matching_length" : 52,
                "hit" : {
                  "hits" : {
                    "total" : {
                      "value" : 1,
                      "relation" : "eq"
                    },
                    "max_score" : null,
                    "hits" : [
                      {
                        "_index" : "log-messages",
                        "_id" : "6",
                        "_score" : null,
                        "_source" : {
                          "message" : "2016-02-08T00:00:00+0000 User foo_864 logged off"
                        },
                        "sort" : [
                          1454889840000
                        ]
                      }
                    ]
                  }
                }
              },
              {
                "doc_count" : 1,
                "key" : "User logging on",
                "regex" : ".*?User.+?logging.+?on.*?",
                "max_matching_length" : 52,
                "hit" : {
                  "hits" : {
                    "total" : {
                      "value" : 1,
                      "relation" : "eq"
                    },
                    "max_score" : null,
                    "hits" : [
                      {
                        "_index" : "log-messages",
                        "_id" : "5",
                        "_score" : null,
                        "_source" : {
                          "message" : "2016-02-08T00:00:00+0000 User foo_325 logging on"
                        },
                        "sort" : [
                          1454889720000
                        ]
                      }
                    ]
                  }
                }
              }
            ]
          }
        }
      ]
    }
  }
}
```


