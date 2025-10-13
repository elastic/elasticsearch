---
navigation_title: "Terms"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html
---

# Terms aggregation [search-aggregations-bucket-terms-aggregation]


A multi-bucket value source based aggregation where buckets are dynamically built - one per unique value.

Example:

$$$terms-aggregation-example$$$

```console
GET /_search
{
  "aggs": {
    "genres": {
      "terms": { "field": "genre" }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "genres": {
      "doc_count_error_upper_bound": 0,   <1>
      "sum_other_doc_count": 0,           <2>
      "buckets": [                        <3>
        {
          "key": "electronic",
          "doc_count": 6
        },
        {
          "key": "rock",
          "doc_count": 3
        },
        {
          "key": "jazz",
          "doc_count": 2
        }
      ]
    }
  }
}
```

1. an upper bound of the error on the document counts for each term, see [below](#terms-agg-doc-count-error)
2. when there are lots of unique terms, Elasticsearch only returns the top terms; this number is the sum of the document counts for all buckets that are not part of the response
3. the list of the top buckets, the meaning of `top` being defined by the [order](#search-aggregations-bucket-terms-aggregation-order)


$$$search-aggregations-bucket-terms-aggregation-types$$$
The `field` can be [Keyword](/reference/elasticsearch/mapping-reference/keyword.md), [Numeric](/reference/elasticsearch/mapping-reference/number.md), [`ip`](/reference/elasticsearch/mapping-reference/ip.md), [`boolean`](/reference/elasticsearch/mapping-reference/boolean.md), or [`binary`](/reference/elasticsearch/mapping-reference/binary.md).

::::{note}
By default, you cannot run a `terms` aggregation on a `text` field. Use a `keyword` [sub-field](/reference/elasticsearch/mapping-reference/multi-fields.md) instead. Alternatively, you can enable [`fielddata`](/reference/elasticsearch/mapping-reference/text.md#fielddata-mapping-param) on the `text` field to create buckets for the field’s [analyzed](docs-content://manage-data/data-store/text-analysis.md) terms. Enabling `fielddata` can significantly increase memory usage.
::::


## Size [search-aggregations-bucket-terms-aggregation-size]

By default, the `terms` aggregation returns the top ten terms with the most documents. Use the `size` parameter to return more terms, up to the [search.max_buckets](/reference/elasticsearch/configuration-reference/search-settings.md#search-settings-max-buckets) limit.

If your data contains 100 or 1000 unique terms, you can increase the `size` of the `terms` aggregation to return them all. If you have more unique terms and you need them all, use the [composite aggregation](/reference/aggregations/search-aggregations-bucket-composite-aggregation.md) instead.

Larger values of `size` use more memory to compute and, push the whole aggregation close to the `max_buckets` limit. You’ll know you’ve gone too large if the request fails with a message about `max_buckets`.


## Shard size [search-aggregations-bucket-terms-aggregation-shard-size]

To get more accurate results, the `terms` agg fetches more than the top `size` terms from each shard. It fetches the top `shard_size` terms, which defaults to `size * 1.5 + 10`.

This is to handle the case when one term has many documents on one shard but is just below the `size` threshold on all other shards. If each shard only returned `size` terms, the aggregation would return an partial doc count for the term. So `terms` returns more terms in an attempt to catch the missing terms. This helps, but it’s still quite possible to return a partial doc count for a term. It just takes a term with more disparate per-shard doc counts.

You can increase `shard_size` to better account for these disparate doc counts and improve the accuracy of the selection of top terms. It is much cheaper to increase the `shard_size` than to increase the `size`. However, it still takes more bytes over the wire and waiting in memory on the coordinating node.

::::{important}
This guidance only applies if you’re using the `terms` aggregation’s default sort `order`. If you’re sorting by anything other than document count in descending order, see [Order](#search-aggregations-bucket-terms-aggregation-order).
::::


::::{note}
`shard_size` cannot be smaller than `size` (as it doesn’t make much sense). When it is, Elasticsearch will override it and reset it to be equal to `size`.
::::



## Document count error [terms-agg-doc-count-error]

Even with a larger `shard_size` value, `doc_count` values for a `terms` aggregation may be approximate. As a result, any sub-aggregations on the `terms` aggregation may also be approximate.

`sum_other_doc_count` is the number of documents that didn’t make it into the the top `size` terms. If this is greater than `0`, you can be sure that the `terms` agg had to throw away some buckets, either because they didn’t fit into `size` on the coordinating node or they didn’t fit into `shard_size` on the data node.


## Per bucket document count error [_per_bucket_document_count_error]

If you set the `show_term_doc_count_error` parameter to `true`, the `terms` aggregation will include `doc_count_error_upper_bound`, which is an upper bound to the error on the `doc_count` returned by each shard. It’s the sum of the size of the largest bucket on each shard that didn’t fit into `shard_size`.

In more concrete terms, imagine there is one bucket that is very large on one shard and just outside the `shard_size` on all the other shards. In that case, the `terms` agg will return the bucket because it is large, but it’ll be missing data from many documents on the shards where the term fell below the `shard_size` threshold. `doc_count_error_upper_bound` is the maximum number of those missing documents.

$$$terms-aggregation-doc-count-error-example$$$

```console
GET /_search
{
  "aggs": {
    "products": {
      "terms": {
        "field": "product",
        "size": 5,
        "show_term_doc_count_error": true
      }
    }
  }
}
```

These errors can only be calculated in this way when the terms are ordered by descending document count. When the aggregation is ordered by the terms values themselves (either ascending or descending) there is no error in the document count since if a shard does not return a particular term which appears in the results from another shard, it must not have that term in its index. When the aggregation is either sorted by a sub aggregation or in order of ascending document count, the error in the document counts cannot be determined and is given a value of -1 to indicate this.


## Order [search-aggregations-bucket-terms-aggregation-order]

By default, the `terms` aggregation orders terms by descending document `_count`.  This produces a bounded [document count](#terms-agg-doc-count-error) error that {{es}} can report.

You can use the `order` parameter to specify a different sort order, but we don’t recommend it.  It is extremely easy to create a terms ordering that will just return wrong results, and not obvious to see when you have done so. Change this only with caution.

::::{warning}
Especially avoid using `"order": { "_count": "asc" }`. If you need to find rare terms, use the [`rare_terms`](/reference/aggregations/search-aggregations-bucket-rare-terms-aggregation.md) aggregation instead. Due to the way the `terms` aggregation [gets terms from shards](#search-aggregations-bucket-terms-aggregation-shard-size), sorting by ascending doc count often produces inaccurate results.
::::


### Ordering by the term value [_ordering_by_the_term_value]

In this case, the buckets are ordered by the actual term values, such as lexicographic order for keywords or numerically for numbers. This sorting is safe in both ascending and descending directions, and produces accurate results.

Example of ordering the buckets alphabetically by their terms in an ascending manner:

$$$terms-aggregation-asc-example$$$

```console
GET /_search
{
  "aggs": {
    "genres": {
      "terms": {
        "field": "genre",
        "order": { "_key": "asc" }
      }
    }
  }
}
```


### Ordering by a sub aggregation [_ordering_by_a_sub_aggregation]

::::{warning}
Sorting by a sub aggregation generally produces incorrect ordering, due to the way the `terms` aggregation [gets results from shards](#search-aggregations-bucket-terms-aggregation-shard-size).
::::


There are two cases when sub-aggregation ordering is safe and returns correct results: sorting by a maximum in descending order, or sorting by a minimum in ascending order. These approaches work because they align with the behavior of sub aggregations. That is, if you’re looking for the largest maximum or the smallest minimum, the global answer (from combined shards) must be included in one of the local shard answers. Conversely, the smallest maximum and largest minimum wouldn’t be accurately computed.

Note also that in these cases, the ordering is correct but the doc counts and non-ordering sub aggregations may still have errors (and {{es}} does not calculate a bound for those errors).

Ordering the buckets by single value metrics sub-aggregation (identified by the aggregation name):

$$$terms-aggregation-subaggregation-example$$$

```console
GET /_search
{
  "aggs": {
    "genres": {
      "terms": {
        "field": "genre",
        "order": { "max_play_count": "desc" }
      },
      "aggs": {
        "max_play_count": { "max": { "field": "play_count" } }
      }
    }
  }
}
```

Ordering the buckets by multi value metrics sub-aggregation (identified by the aggregation name):

$$$terms-aggregation-multivalue-subaggregation-example$$$

```console
GET /_search
{
  "aggs": {
    "genres": {
      "terms": {
        "field": "genre",
        "order": { "playback_stats.max": "desc" }
      },
      "aggs": {
        "playback_stats": { "stats": { "field": "play_count" } }
      }
    }
  }
}
```

::::{admonition} Pipeline aggs cannot be used for sorting
:class: note

[Pipeline aggregations](/reference/aggregations/pipeline.md) are run during the reduce phase after all other aggregations have already completed. For this reason, they cannot be used for ordering.

::::


It is also possible to order the buckets based on a "deeper" aggregation in the hierarchy. This is supported as long as the aggregations path are of a single-bucket type, where the last aggregation in the path may either be a single-bucket one or a metrics one. If it’s a single-bucket type, the order will be defined by the number of docs in the bucket (i.e. `doc_count`), in case it’s a metrics one, the same rules as above apply (where the path must indicate the metric name to sort by in case of a multi-value metrics aggregation, and in case of a single-value metrics aggregation the sort will be applied on that value).

The path must be defined in the following form:

```ebnf
AGG_SEPARATOR       =  '>' ;
METRIC_SEPARATOR    =  '.' ;
AGG_NAME            =  <the name of the aggregation> ;
METRIC              =  <the name of the metric (in case of multi-value metrics aggregation)> ;
PATH                =  <AGG_NAME> [ <AGG_SEPARATOR>, <AGG_NAME> ]* [ <METRIC_SEPARATOR>, <METRIC> ] ;
```

$$$terms-aggregation-hierarchy-example$$$

```console
GET /_search
{
  "aggs": {
    "countries": {
      "terms": {
        "field": "artist.country",
        "order": { "rock>playback_stats.avg": "desc" }
      },
      "aggs": {
        "rock": {
          "filter": { "term": { "genre": "rock" } },
          "aggs": {
            "playback_stats": { "stats": { "field": "play_count" } }
          }
        }
      }
    }
  }
}
```

The above will sort the artist’s countries buckets based on the average play count among the rock songs.

Multiple criteria can be used to order the buckets by providing an array of order criteria such as the following:

$$$terms-aggregation-multicriteria-example$$$

```console
GET /_search
{
  "aggs": {
    "countries": {
      "terms": {
        "field": "artist.country",
        "order": [ { "rock>playback_stats.avg": "desc" }, { "_count": "desc" } ]
      },
      "aggs": {
        "rock": {
          "filter": { "term": { "genre": "rock" } },
          "aggs": {
            "playback_stats": { "stats": { "field": "play_count" } }
          }
        }
      }
    }
  }
}
```

The above will sort the artist’s countries buckets based on the average play count among the rock songs and then by their `doc_count` in descending order.

::::{note}
In the event that two buckets share the same values for all order criteria the bucket’s term value is used as a tie-breaker in ascending alphabetical order to prevent non-deterministic ordering of buckets.
::::



### Ordering by count ascending [_ordering_by_count_ascending]

Ordering terms by ascending document `_count` produces an unbounded error that {{es}} can’t accurately report. We therefore strongly recommend against using `"order": { "_count": "asc" }` as shown in the following example:

$$$terms-aggregation-count-example$$$

```console
GET /_search
{
  "aggs": {
    "genres": {
      "terms": {
        "field": "genre",
        "order": { "_count": "asc" }
      }
    }
  }
}
```



## Minimum document count [_minimum_document_count_4]

It is possible to only return terms that match more than a configured number of hits using the `min_doc_count` option:

$$$terms-aggregation-min-doc-count-example$$$

```console
GET /_search
{
  "aggs": {
    "tags": {
      "terms": {
        "field": "tags",
        "min_doc_count": 10
      }
    }
  }
}
```

The above aggregation would only return tags which have been found in 10 hits or more. Default value is `1`.

Terms are collected and ordered on a shard level and merged with the terms collected from other shards in a second step. However, the shard does not have the information about the global document count available. The decision if a term is added to a candidate list depends only on the order computed on the shard using local shard frequencies. The `min_doc_count` criterion is only applied after merging local terms statistics of all shards. In a way the decision to add the term as a candidate is made without being very *certain* about if the term will actually reach the required `min_doc_count`. This might cause many (globally) high frequent terms to be missing in the final result if low frequent terms populated the candidate lists. To avoid this, the `shard_size` parameter can be increased to allow more candidate terms on the shards. However, this increases memory consumption and network traffic.

### `shard_min_doc_count` [search-aggregations-bucket-terms-shard-min-doc-count]

The parameter `shard_min_doc_count` regulates the *certainty* a shard has if the term should actually be added to the candidate list or not with respect to the `min_doc_count`. Terms will only be considered if their local shard frequency within the set is higher than the `shard_min_doc_count`. If your dictionary contains many low frequent terms and you are not interested in those (for example misspellings), then you can set the `shard_min_doc_count` parameter to filter out candidate terms on a shard level that will with a reasonable certainty not reach the required `min_doc_count` even after merging the local counts. `shard_min_doc_count` is set to `0` per default and has no effect unless you explicitly set it.

::::{note}
Setting `min_doc_count`=`0` will also return buckets for terms that didn’t match any hit. However, some of the returned terms which have a document count of zero might only belong to deleted documents or documents from other types, so there is no warranty that a `match_all` query would find a positive document count for those terms.
::::


::::{warning}
When NOT sorting on `doc_count` descending, high values of `min_doc_count` may return a number of buckets which is less than `size` because not enough data was gathered from the shards. Missing buckets can be back by increasing `shard_size`. Setting `shard_min_doc_count` too high will cause terms to be filtered out on a shard level. This value should be set much lower than `min_doc_count/#shards`.
::::




## Script [search-aggregations-bucket-terms-aggregation-script]

Use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) if the data in your documents doesn’t exactly match what you’d like to aggregate. If, for example, "anthologies" need to be in a special category then you could run this:

$$$terms-aggregation-script-example$$$

```console
GET /_search
{
  "size": 0,
  "runtime_mappings": {
    "normalized_genre": {
      "type": "keyword",
      "script": """
        String genre = doc['genre'].value;
        if (doc['product'].value.startsWith('Anthology')) {
          emit(genre + ' anthology');
        } else {
          emit(genre);
        }
      """
    }
  },
  "aggs": {
    "genres": {
      "terms": {
        "field": "normalized_genre"
      }
    }
  }
}
```

Which will look like:

```console-result
{
  "aggregations": {
    "genres": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": "electronic",
          "doc_count": 4
        },
        {
          "key": "rock",
          "doc_count": 3
        },
        {
          "key": "electronic anthology",
          "doc_count": 2
        },
        {
          "key": "jazz",
          "doc_count": 2
        }
      ]
    }
  },
  ...
}
```

This is a little slower because the runtime field has to access two fields instead of one and because there are some optimizations that work on non-runtime `keyword` fields that we have to give up for for runtime `keyword` fields. If you need the speed, you can index the `normalized_genre` field.


## Filtering Values [_filtering_values_4]

It is possible to filter the values for which buckets will be created. This can be done using the `include` and `exclude` parameters which are based on regular expression strings or arrays of exact values. Additionally, `include` clauses can filter using `partition` expressions.

### Filtering Values with regular expressions [_filtering_values_with_regular_expressions_2]

$$$terms-aggregation-regex-example$$$

```console
GET /_search
{
  "aggs": {
    "tags": {
      "terms": {
        "field": "tags",
        "include": ".*sport.*",
        "exclude": "water_.*"
      }
    }
  }
}
```

In the above example, buckets will be created for all the tags that has the word `sport` in them, except those starting with `water_` (so the tag `water_sports` will not be aggregated). The `include` regular expression will determine what values are "allowed" to be aggregated, while the `exclude` determines the values that should not be aggregated. When both are defined, the `exclude` has precedence, meaning, the `include` is evaluated first and only then the `exclude`.

The syntax is the same as [regexp queries](/reference/query-languages/query-dsl/regexp-syntax.md).


### Filtering Values with exact values [_filtering_values_with_exact_values_2]

For matching based on exact values the `include` and `exclude` parameters can simply take an array of strings that represent the terms as they are found in the index:

$$$terms-aggregation-exact-example$$$

```console
GET /_search
{
  "aggs": {
    "JapaneseCars": {
      "terms": {
        "field": "make",
        "include": [ "mazda", "honda" ]
      }
    },
    "ActiveCarManufacturers": {
      "terms": {
        "field": "make",
        "exclude": [ "rover", "jensen" ]
      }
    }
  }
}
```


### Filtering Values with partitions [_filtering_values_with_partitions]

Sometimes there are too many unique terms to process in a single request/response pair so it can be useful to break the analysis up into multiple requests. This can be achieved by grouping the field’s values into a number of partitions at query-time and processing only one partition in each request. Consider this request which is looking for accounts that have not logged any access recently:

$$$terms-aggregation-partitions-example$$$

```console
GET /_search
{
   "size": 0,
   "aggs": {
      "expired_sessions": {
         "terms": {
            "field": "account_id",
            "include": {
               "partition": 0,
               "num_partitions": 20
            },
            "size": 10000,
            "order": {
               "last_access": "asc"
            }
         },
         "aggs": {
            "last_access": {
               "max": {
                  "field": "access_date"
               }
            }
         }
      }
   }
}
```

This request is finding the last logged access date for a subset of customer accounts because we might want to expire some customer accounts who haven’t been seen for a long while. The `num_partitions` setting has requested that the unique account_ids are organized evenly into twenty partitions (0 to 19). and the `partition` setting in this request filters to only consider account_ids falling into partition 0. Subsequent requests should ask for partitions 1 then 2 etc to complete the expired-account analysis.

Note that the `size` setting for the number of results returned needs to be tuned with the `num_partitions`. For this particular account-expiration example the process for balancing values for `size` and `num_partitions` would be as follows:

1. Use the `cardinality` aggregation to estimate the total number of unique account_id values
2. Pick a value for `num_partitions` to break the number from 1) up into more manageable chunks
3. Pick a `size` value for the number of responses we want from each partition
4. Run a test request

If we have a circuit-breaker error we are trying to do too much in one request and must increase `num_partitions`. If the request was successful but the last account ID in the date-sorted test response was still an account we might want to expire then we may be missing accounts of interest and have set our numbers too low. We must either

* increase the `size` parameter to return more results per partition (could be heavy on memory) or
* increase the `num_partitions` to consider less accounts per request (could increase overall processing time as we need to make more requests)

Ultimately this is a balancing act between managing the Elasticsearch resources required to process a single request and the volume of requests that the client application must issue to complete a task.

::::{warning}
Partitions cannot be used together with an `exclude` parameter.
::::




## Multi-field terms aggregation [_multi_field_terms_aggregation]

The `terms` aggregation does not support collecting terms from multiple fields in the same document. The reason is that the `terms` agg doesn’t collect the string term values themselves, but rather uses [global ordinals](#search-aggregations-bucket-terms-aggregation-execution-hint) to produce a list of all of the unique values in the field. Global ordinals results in an important performance boost which would not be possible across multiple fields.

There are three approaches that you can use to perform a `terms` agg across multiple fields:

[Script](#search-aggregations-bucket-terms-aggregation-script)
:   Use a script to retrieve terms from multiple fields. This disables the global ordinals optimization and will be slower than collecting terms from a single field, but it gives you the flexibility to implement this option at search time.

[`copy_to` field](/reference/elasticsearch/mapping-reference/copy-to.md)
:   If you know ahead of time that you want to collect the terms from two or more fields, then use `copy_to` in your mapping to create a new dedicated field at index time which contains the values from both fields. You can aggregate on this single field, which will benefit from the global ordinals optimization.

[`multi_terms` aggregation](/reference/aggregations/search-aggregations-bucket-multi-terms-aggregation.md)
:   Use multi_terms aggregation to combine terms from multiple fields into a compound key. This also disables the global ordinals and will be slower than collecting terms from a single field. It is faster but less flexible than using a script.


## Collect mode [search-aggregations-bucket-terms-aggregation-collect]

Deferring calculation of child aggregations

For fields with many unique terms and a small number of required results it can be more efficient to delay the calculation of child aggregations until the top parent-level aggs have been pruned. Ordinarily, all branches of the aggregation tree are expanded in one depth-first pass and only then any pruning occurs. In some scenarios this can be very wasteful and can hit memory constraints. An example problem scenario is querying a movie database for the 10 most popular actors and their 5 most common co-stars:

$$$terms-aggregation-collect-mode-example$$$

```console
GET /_search
{
  "aggs": {
    "actors": {
      "terms": {
        "field": "actors",
        "size": 10
      },
      "aggs": {
        "costars": {
          "terms": {
            "field": "actors",
            "size": 5
          }
        }
      }
    }
  }
}
```

Even though the number of actors may be comparatively small and we want only 50 result buckets there is a combinatorial explosion of buckets during calculation - a single actor can produce n² buckets where n is the number of actors. The sane option would be to first determine the 10 most popular actors and only then examine the top co-stars for these 10 actors. This alternative strategy is what we call the `breadth_first` collection mode as opposed to the `depth_first` mode.

::::{note}
The `breadth_first` is the default mode for fields with a cardinality bigger than the requested size or when the cardinality is unknown (numeric fields or scripts for instance). It is possible to override the default heuristic and to provide a collect mode directly in the request:
::::


$$$terms-aggregation-breadth-first-example$$$

```console
GET /_search
{
  "aggs": {
    "actors": {
      "terms": {
        "field": "actors",
        "size": 10,
        "collect_mode": "breadth_first" <1>
      },
      "aggs": {
        "costars": {
          "terms": {
            "field": "actors",
            "size": 5
          }
        }
      }
    }
  }
}
```

1. the possible values are `breadth_first` and `depth_first`


When using `breadth_first` mode the set of documents that fall into the uppermost buckets are cached for subsequent replay so there is a memory overhead in doing this which is linear with the number of matching documents. Note that the `order` parameter can still be used to refer to data from a child aggregation when using the `breadth_first` setting - the parent aggregation understands that this child aggregation will need to be called first before any of the other child aggregations.

::::{warning}
Nested aggregations such as `top_hits` which require access to score information under an aggregation that uses the `breadth_first` collection mode need to replay the query on the second pass but only for the documents belonging to the top buckets.
::::



## Execution hint [search-aggregations-bucket-terms-aggregation-execution-hint]

There are different mechanisms by which terms aggregations can be executed:

* by using field values directly in order to aggregate data per-bucket (`map`)
* by using global ordinals of the field and allocating one bucket per global ordinal (`global_ordinals`)

Elasticsearch tries to have sensible defaults so this is something that generally doesn’t need to be configured.

`global_ordinals` is the default option for `keyword` field, it uses global ordinals to allocates buckets dynamically so memory usage is linear to the number of values of the documents that are part of the aggregation scope.

`map` should only be considered when very few documents match a query. Otherwise the ordinals-based execution mode is significantly faster. By default, `map` is only used when running an aggregation on scripts, since they don’t have ordinals.

$$$terms-aggregation-execution-hint-example$$$

```console
GET /_search
{
  "aggs": {
    "tags": {
      "terms": {
        "field": "tags",
        "execution_hint": "map" <1>
      }
    }
  }
}
```

1. The possible values are `map`, `global_ordinals`


Please note that Elasticsearch will ignore this execution hint if it is not applicable and that there is no backward compatibility guarantee on these hints.


## Missing value [_missing_value_5]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

$$$terms-aggregation-missing-example$$$

```console
GET /_search
{
  "aggs": {
    "tags": {
      "terms": {
        "field": "tags",
        "missing": "N/A" <1>
      }
    }
  }
}
```

1. Documents without a value in the `tags` field will fall into the same bucket as documents that have the value `N/A`.



## Mixing field types [_mixing_field_types_2]

::::{warning}
When aggregating on multiple indices the type of the aggregated field may not be the same in all indices. Some types are compatible with each other (`integer` and `long` or `float` and `double`) but when the types are a mix of decimal and non-decimal number the terms aggregation will promote the non-decimal numbers to decimal numbers. This can result in a loss of precision in the bucket values.
::::



### Troubleshooting [search-aggregations-bucket-terms-aggregation-troubleshooting]

### Failed Trying to Format Bytes [_failed_trying_to_format_bytes]

When running a terms aggregation (or other aggregation, but in practice usually terms) over multiple indices, you may get an error that starts with "Failed trying to format bytes… ".  This is usually caused by two of the indices not having the same mapping type for the field being aggregated.

**Use an explicit `value_type`** Although it’s best to correct the mappings, you can work around this issue if the field is unmapped in one of the indices.  Setting the `value_type` parameter can resolve the issue by coercing the unmapped field into the correct type.

$$$terms-aggregation-value_type-example$$$

```console
GET /_search
{
  "aggs": {
    "ip_addresses": {
      "terms": {
        "field": "destination_ip",
        "missing": "0.0.0.0",
        "value_type": "ip"
      }
    }
  }
}
```
