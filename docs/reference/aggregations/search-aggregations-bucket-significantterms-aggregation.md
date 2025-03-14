---
navigation_title: "Significant terms"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html
---

# Significant terms aggregation [search-aggregations-bucket-significantterms-aggregation]


An aggregation that returns interesting or unusual occurrences of terms in a set.

* Suggesting "H5N1" when users search for "bird flu" in text
* Identifying the merchant that is the "common point of compromise" from the transaction history of credit card owners reporting loss
* Suggesting keywords relating to stock symbol $ATI for an automated news classifier
* Spotting the fraudulent doctor who is diagnosing more than their fair share of whiplash injuries
* Spotting the tire manufacturer who has a disproportionate number of blow-outs

In all these cases the terms being selected are not simply the most popular terms in a set. They are the terms that have undergone a significant change in popularity measured between a *foreground* and *background* set. If the term "H5N1" only exists in 5 documents in a 10 million document index and yet is found in 4 of the 100 documents that make up a user’s search results that is significant and probably very relevant to their search. 5/10,000,000 vs 4/100 is a big swing in frequency.

## Single-set analysis [_single_set_analysis]

In the simplest case, the *foreground* set of interest is the search results matched by a query and the *background* set used for statistical comparisons is the index or indices from which the results were gathered.

Example:

$$$significantterms-aggregation-example$$$

```console
GET /_search
{
  "query": {
    "terms": { "force": [ "British Transport Police" ] }
  },
  "aggregations": {
    "significant_crime_types": {
      "significant_terms": { "field": "crime_type" }
    }
  }
}
```

Response:

```console-result
{
  ...
  "aggregations": {
    "significant_crime_types": {
      "doc_count": 47347,
      "bg_count": 5064554,
      "buckets": [
        {
          "key": "Bicycle theft",
          "doc_count": 3640,
          "score": 0.371235374214817,
          "bg_count": 66799
        }
              ...
      ]
    }
  }
}
```

When querying an index of all crimes from all police forces, what these results show is that the British Transport Police force stand out as a force dealing with a disproportionately large number of bicycle thefts. Ordinarily, bicycle thefts represent only 1% of crimes (66799/5064554) but for the British Transport Police, who handle crime on railways and stations, 7% of crimes (3640/47347) is a bike theft. This is a significant seven-fold increase in frequency and so this anomaly was highlighted as the top crime type.

The problem with using a query to spot anomalies is it only gives us one subset to use for comparisons. To discover all the other police forces' anomalies we would have to repeat the query for each of the different forces.

This can be a tedious way to look for unusual patterns in an index.


## Multi-set analysis [_multi_set_analysis]

A simpler way to perform analysis across multiple categories is to use a parent-level aggregation to segment the data ready for analysis.

Example using a parent aggregation for segmentation:

$$$significantterms-aggregation-multiset--example$$$

```console
GET /_search
{
  "aggregations": {
    "forces": {
      "terms": { "field": "force" },
      "aggregations": {
        "significant_crime_types": {
          "significant_terms": { "field": "crime_type" }
        }
      }
    }
  }
}
```

Response:

```console-result
{
 ...
 "aggregations": {
    "forces": {
        "doc_count_error_upper_bound": 1375,
        "sum_other_doc_count": 7879845,
        "buckets": [
            {
                "key": "Metropolitan Police Service",
                "doc_count": 894038,
                "significant_crime_types": {
                    "doc_count": 894038,
                    "bg_count": 5064554,
                    "buckets": [
                        {
                            "key": "Robbery",
                            "doc_count": 27617,
                            "score": 0.0599,
                            "bg_count": 53182
                        }
                        ...
                    ]
                }
            },
            {
                "key": "British Transport Police",
                "doc_count": 47347,
                "significant_crime_types": {
                    "doc_count": 47347,
                    "bg_count": 5064554,
                    "buckets": [
                        {
                            "key": "Bicycle theft",
                            "doc_count": 3640,
                            "score": 0.371,
                            "bg_count": 66799
                        }
                        ...
                    ]
                }
            }
        ]
    }
  }
}
```

Now we have anomaly detection for each of the police forces using a single request.

We can use other forms of top-level aggregations to segment our data, for example segmenting by geographic area to identify unusual hot-spots of a particular crime type:

$$$significantterms-aggregation-hotspot-example$$$

```console
GET /_search
{
  "aggs": {
    "hotspots": {
      "geohash_grid": {
        "field": "location",
        "precision": 5
      },
      "aggs": {
        "significant_crime_types": {
          "significant_terms": { "field": "crime_type" }
        }
      }
    }
  }
}
```

This example uses the `geohash_grid` aggregation to create result buckets that represent geographic areas, and inside each bucket we can identify anomalous levels of a crime type in these tightly-focused areas e.g.

* Airports exhibit unusual numbers of weapon confiscations
* Universities show uplifts of bicycle thefts

At a higher geohash_grid zoom-level with larger coverage areas we would start to see where an entire police-force may be tackling an unusual volume of a particular crime type.

Obviously a time-based top-level segmentation would help identify current trends for each point in time where a simple `terms` aggregation would typically show the very popular "constants" that persist across all time slots.

::::{admonition} How are the scores calculated?
The numbers returned for scores are primarily intended for ranking different suggestions sensibly rather than something easily understood by end users. The scores are derived from the doc frequencies in *foreground* and *background* sets. In brief, a term is considered significant if there is a noticeable difference in the frequency in which a term appears in the subset and in the background. The way the terms are ranked can be configured, see "Parameters" section.

::::



## Use on free-text fields [_use_on_free_text_fields]

The significant_terms aggregation can be used effectively on tokenized free-text fields to suggest:

* keywords for refining end-user searches
* keywords for use in percolator queries

::::{warning}
Picking a free-text field as the subject of a significant terms analysis can be expensive! It will attempt to load every unique word into RAM. It is recommended to only use this on smaller indices.
::::


::::{admonition} Use the *"like this but not this"* pattern
You can spot mis-categorized content by first searching a structured field e.g. `category:adultMovie` and use significant_terms on the free-text "movie_description" field. Take the suggested words (I’ll leave them to your imagination) and then search for all movies NOT marked as category:adultMovie but containing these keywords. You now have a ranked list of badly-categorized movies that you should reclassify or at least remove from the "familyFriendly" category.

The significance score from each term can also provide a useful `boost` setting to sort matches. Using the `minimum_should_match` setting of the `terms` query with the keywords will help control the balance of precision/recall in the result set i.e a high setting would have a small number of relevant results packed full of keywords and a setting of "1" would produce a more exhaustive results set with all documents containing *any* keyword.

::::


::::{tip}
Free-text significant_terms are much more easily understood when viewed in context. Take the results of `significant_terms` suggestions from a free-text field and use them in a `terms` query on the same field with a `highlight` clause to present users with example snippets of documents. When the terms are presented unstemmed, highlighted, with the right case, in the right order and with some context, their significance/meaning is more readily apparent.

::::



## Custom background sets [_custom_background_sets]

Ordinarily, the foreground set of documents is "diffed" against a background set of all the documents in your index. However, sometimes it may prove useful to use a narrower background set as the basis for comparisons. For example, a query on documents relating to "Madrid" in an index with content from all over the world might reveal that "Spanish" was a significant term. This may be true but if you want some more focused terms you could use a `background_filter` on the term *spain* to establish a narrower set of documents as context. With this as a background "Spanish" would now be seen as commonplace and therefore not as significant as words like "capital" that relate more strongly with Madrid. Note that using a background filter will slow things down - each term’s background frequency must now be derived on-the-fly from filtering posting lists rather than reading the index’s pre-computed count for a term.


## Limitations [_limitations_8]

### Significant terms must be indexed values [_significant_terms_must_be_indexed_values]

Unlike the terms aggregation it is currently not possible to use script-generated terms for counting purposes. Because of the way the significant_terms aggregation must consider both *foreground* and *background* frequencies it would be prohibitively expensive to use a script on the entire index to obtain background frequencies for comparisons. Also DocValues are not supported as sources of term data for similar reasons.


### No analysis of floating point fields [_no_analysis_of_floating_point_fields]

Floating point fields are currently not supported as the subject of significant_terms analysis. While integer or long fields can be used to represent concepts like bank account numbers or category numbers which can be interesting to track, floating point fields are usually used to represent quantities of something. As such, individual floating point terms are not useful for this form of frequency analysis.


### Use as a parent aggregation [_use_as_a_parent_aggregation]

If there is the equivalent of a `match_all` query or no query criteria providing a subset of the index the significant_terms aggregation should not be used as the top-most aggregation - in this scenario the *foreground* set is exactly the same as the *background* set and so there is no difference in document frequencies to observe and from which to make sensible suggestions.

Another consideration is that the significant_terms aggregation produces many candidate results at shard level that are only later pruned on the reducing node once all statistics from all shards are merged. As a result, it can be inefficient and costly in terms of RAM to embed large child aggregations under a significant_terms aggregation that later discards many candidate terms. It is advisable in these cases to perform two searches - the first to provide a rationalized list of significant_terms and then add this shortlist of terms to a second query to go back and fetch the required child aggregations.


### Approximate counts [_approximate_counts]

The counts of how many documents contain a term provided in results are based on summing the samples returned from each shard and as such may be:

* low if certain shards did not provide figures for a given term in their top sample
* high when considering the background frequency as it may count occurrences found in deleted documents

Like most design decisions, this is the basis of a trade-off in which we have chosen to provide fast performance at the cost of some (typically small) inaccuracies. However, the `size` and `shard size` settings covered in the next section provide tools to help control the accuracy levels.



## Parameters [significantterms-aggregation-parameters]

### JLH score [_jlh_score]

The JLH score can be used as a significance score by adding the parameter

```js
	 "jlh": {
	 }
```

The scores are derived from the doc frequencies in *foreground* and *background* sets. The *absolute* change in popularity (foregroundPercent - backgroundPercent) would favor common terms whereas the *relative* change in popularity (foregroundPercent/ backgroundPercent) would favor rare terms. Rare vs common is essentially a precision vs recall balance and so the absolute and relative changes are multiplied to provide a sweet spot between precision and recall.


### Mutual information [_mutual_information]

Mutual information as described in "Information Retrieval", Manning et al., Chapter 13.5.1 can be used as significance score by adding the parameter

```js
	 "mutual_information": {
	      "include_negatives": true
	 }
```

Mutual information does not differentiate between terms that are descriptive for the subset or for documents outside the subset. The significant terms therefore can contain terms that appear more or less frequent in the subset than outside the subset. To filter out the terms that appear less often in the subset than in documents outside the subset, `include_negatives` can be set to `false`.

Per default, the assumption is that the documents in the bucket are also contained in the background. If instead you defined a custom background filter that represents a different set of documents that you want to compare to, set

```js
"background_is_superset": false
```


### Chi square [_chi_square]

Chi square as described in "Information Retrieval", Manning et al., Chapter 13.5.2 can be used as significance score by adding the parameter

```js
	 "chi_square": {
	 }
```

Chi square behaves like mutual information and can be configured with the same parameters `include_negatives` and `background_is_superset`.


### Google normalized distance [_google_normalized_distance]

Google normalized distance as described in ["The Google Similarity Distance", Cilibrasi and Vitanyi, 2007](https://arxiv.org/pdf/cs/0412098v3.pdf) can be used as significance score by adding the parameter

```js
	 "gnd": {
	 }
```

`gnd` also accepts the `background_is_superset` parameter.


### p-value score [p-value-score]

The p-value is the probability of obtaining test results at least as extreme as the results actually observed, under the assumption that the null hypothesis is correct. The p-value is calculated assuming that the foreground set and the background set are independent [Bernoulli trials](https://en.wikipedia.org/wiki/Bernoulli_trial), with the null hypothesis that the probabilities are the same.

#### Example usage [_example_usage]

This example calculates the p-value score for terms `user_agent.version` given the foreground set of "ended in failure" versus "NOT ended in failure".

`"background_is_superset": false` indicates that the background set does not contain the counts of the foreground set as they are filtered out.

`"normalize_above": 1000` facilitates returning consistent significance results at various scales. `1000` indicates that term counts greater than `1000` are scaled down by a factor of `1000/term_count`.

```console
GET /_search
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "event.outcome": "failure"
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": "2021-02-01",
              "lt": "2021-02-04"
            }
          }
        },
        {
          "term": {
            "service.name": {
              "value": "frontend-node"
            }
          }
        }
      ]
    }
  },
  "aggs": {
    "failure_p_value": {
      "significant_terms": {
        "field": "user_agent.version",
        "background_filter": {
          "bool": {
            "must_not": [
              {
                "term": {
                  "event.outcome": "failure"
                }
              }
            ],
            "filter": [
              {
                "range": {
                  "@timestamp": {
                    "gte": "2021-02-01",
                    "lt": "2021-02-04"
                  }
                }
              },
              {
                "term": {
                  "service.name": {
                    "value": "frontend-node"
                  }
                }
              }
            ]
          }
        },
        "p_value": {"background_is_superset": false, "normalize_above": 1000}
      }
    }
  }
}
```



### Percentage [_percentage]

A simple calculation of the number of documents in the foreground sample with a term divided by the number of documents in the background with the term. By default this produces a score greater than zero and less than one.

The benefit of this heuristic is that the scoring logic is simple to explain to anyone familiar with a "per capita" statistic. However, for fields with high cardinality there is a tendency for this heuristic to select the rarest terms such as typos that occur only once because they score 1/1 = 100%.

It would be hard for a seasoned boxer to win a championship if the prize was awarded purely on the basis of percentage of fights won - by these rules a newcomer with only one fight under their belt would be impossible to beat. Multiple observations are typically required to reinforce a view so it is recommended in these cases to set both `min_doc_count` and `shard_min_doc_count` to a higher value such as 10 in order to filter out the low-frequency terms that otherwise take precedence.

```js
	 "percentage": {
	 }
```


### Which one is best? [_which_one_is_best]

Roughly, `mutual_information` prefers high frequent terms even if they occur also frequently in the background. For example, in an analysis of natural language text this might lead to selection of stop words. `mutual_information` is unlikely to select very rare terms like misspellings. `gnd` prefers terms with a high co-occurrence and avoids selection of stopwords. It might be better suited for synonym detection. However, `gnd` has a tendency to select very rare terms that are, for example, a result of misspelling. `chi_square` and `jlh` are somewhat in-between.

It is hard to say which one of the different heuristics will be the best choice as it depends on what the significant terms are used for (see for example [Yang and Pedersen, "A Comparative Study on Feature Selection in Text Categorization", 1997](http://courses.ischool.berkeley.edu/i256/f06/papers/yang97comparative.pdf) for a study on using significant terms for feature selection for text classification).

If none of the above measures suits your usecase than another option is to implement a custom significance measure:


### Scripted [_scripted]

Customized scores can be implemented via a script:

```js
	    "script_heuristic": {
              "script": {
	        "lang": "painless",
	        "source": "params._subset_freq/(params._superset_freq - params._subset_freq + 1)"
	      }
            }
```

Scripts can be inline (as in above example), indexed or stored on disk. For details on the options, see [script documentation](docs-content://explore-analyze/scripting.md).

Available parameters in the script are

`_subset_freq`
:   Number of documents the term appears in the subset.

`_superset_freq`
:   Number of documents the term appears in the superset.

`_subset_size`
:   Number of documents in the subset.

`_superset_size`
:   Number of documents in the superset.


### Size & Shard Size [sig-terms-shard-size]

The `size` parameter can be set to define how many term buckets should be returned out of the overall terms list. By default, the node coordinating the search process will request each shard to provide its own top term buckets and once all shards respond, it will reduce the results to the final list that will then be returned to the client. If the number of unique terms is greater than `size`, the returned list can be slightly off and not accurate (it could be that the term counts are slightly off and it could even be that a term that should have been in the top size buckets was not returned).

To ensure better accuracy a multiple of the final `size` is used as the number of terms to request from each shard (`2 * (size * 1.5 + 10)`). To take manual control of this setting the `shard_size` parameter can be used to control the volumes of candidate terms produced by each shard.

Low-frequency terms can turn out to be the most interesting ones once all results are combined so the significant_terms aggregation can produce higher-quality results when the `shard_size` parameter is set to values significantly higher than the `size` setting. This ensures that a bigger volume of promising candidate terms are given a consolidated review by the reducing node before the final selection. Obviously large candidate term lists will cause extra network traffic and RAM usage so this is quality/cost trade off that needs to be balanced. If `shard_size` is set to -1 (the default) then `shard_size` will be automatically estimated based on the number of shards and the `size` parameter.

::::{note}
`shard_size` cannot be smaller than `size` (as it doesn’t make much sense). When it is, Elasticsearch will override it and reset it to be equal to `size`.
::::



### Minimum document count [_minimum_document_count_2]

It is possible to only return terms that match more than a configured number of hits using the `min_doc_count` option:

$$$significantterms-aggregation-min-document-example$$$

```console
GET /_search
{
  "aggs": {
    "tags": {
      "significant_terms": {
        "field": "tag",
        "min_doc_count": 10
      }
    }
  }
}
```

The above aggregation would only return tags which have been found in 10 hits or more. Default value is `3`.

Terms that score highly will be collected on a shard level and merged with the terms collected from other shards in a second step. However, the shard does not have the information about the global term frequencies available. The decision if a term is added to a candidate list depends only on the score computed on the shard using local shard frequencies, not the global frequencies of the word. The `min_doc_count` criterion is only applied after merging local terms statistics of all shards. In a way the decision to add the term as a candidate is made without being very *certain* about if the term will actually reach the required `min_doc_count`. This might cause many (globally) high frequent terms to be missing in the final result if low frequent but high scoring terms populated the candidate lists. To avoid this, the `shard_size` parameter can be increased to allow more candidate terms on the shards. However, this increases memory consumption and network traffic.


### `shard_min_doc_count` [search-aggregations-bucket-significantterms-shard-min-doc-count]

The parameter `shard_min_doc_count` regulates the *certainty* a shard has if the term should actually be added to the candidate list or not with respect to the `min_doc_count`. Terms will only be considered if their local shard frequency within the set is higher than the `shard_min_doc_count`. If your dictionary contains many low frequent terms and you are not interested in those (for example misspellings), then you can set the `shard_min_doc_count` parameter to filter out candidate terms on a shard level that will with a reasonable certainty not reach the required `min_doc_count` even after merging the local counts. `shard_min_doc_count` is set to `0` per default and has no effect unless you explicitly set it.

::::{warning}
Setting `min_doc_count` to `1` is generally not advised as it tends to return terms that are typos or other bizarre curiosities. Finding more than one instance of a term helps reinforce that, while still rare, the term was not the result of a one-off accident. The default value of 3 is used to provide a minimum weight-of-evidence. Setting `shard_min_doc_count` too high will cause significant candidate terms to be filtered out on a shard level. This value should be set much lower than `min_doc_count/#shards`.
::::



### Custom background context [_custom_background_context]

The default source of statistical information for background term frequencies is the entire index and this scope can be narrowed through the use of a `background_filter` to focus in on significant terms within a narrower context:

$$$significantterms-aggregation-custom-background-example$$$

```console
GET /_search
{
  "query": {
    "match": {
      "city": "madrid"
    }
  },
  "aggs": {
    "tags": {
      "significant_terms": {
        "field": "tag",
        "background_filter": {
          "term": { "text": "spain" }
        }
      }
    }
  }
}
```

The above filter would help focus in on terms that were peculiar to the city of Madrid rather than revealing terms like "Spanish" that are unusual in the full index’s worldwide context but commonplace in the subset of documents containing the word "Spain".

::::{warning}
Use of background filters will slow the query as each term’s postings must be filtered to determine a frequency
::::



### Filtering Values [_filtering_values_2]

It is possible (although rarely required) to filter the values for which buckets will be created. This can be done using the `include` and `exclude` parameters which are based on a regular expression string or arrays of exact terms. This functionality mirrors the features described in the [terms aggregation](/reference/data-analysis/aggregations/search-aggregations-bucket-terms-aggregation.md) documentation.



## Collect mode [_collect_mode]

To avoid memory issues, the `significant_terms` aggregation always computes child aggregations in `breadth_first` mode. A description of the different collection modes can be found in the [terms aggregation](/reference/data-analysis/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-collect) documentation.


## Execution hint [_execution_hint_2]

There are different mechanisms by which terms aggregations can be executed:

* by using field values directly in order to aggregate data per-bucket (`map`)
* by using [global ordinals](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md) of the field and allocating one bucket per global ordinal (`global_ordinals`)

Elasticsearch tries to have sensible defaults so this is something that generally doesn’t need to be configured.

`global_ordinals` is the default option for `keyword` field, it uses global ordinals to allocates buckets dynamically so memory usage is linear to the number of values of the documents that are part of the aggregation scope.

`map` should only be considered when very few documents match a query. Otherwise the ordinals-based execution mode is significantly faster. By default, `map` is only used when running an aggregation on scripts, since they don’t have ordinals.

$$$significantterms-aggregation-execution-hint-example$$$

```console
GET /_search
{
  "aggs": {
    "tags": {
      "significant_terms": {
        "field": "tags",
        "execution_hint": "map" <1>
      }
    }
  }
}
```

1. the possible values are `map`, `global_ordinals`


Please note that Elasticsearch will ignore this execution hint if it is not applicable.


