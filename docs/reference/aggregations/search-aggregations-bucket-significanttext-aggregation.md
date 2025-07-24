---
navigation_title: "Significant text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significanttext-aggregation.html
---

# Significant text aggregation [search-aggregations-bucket-significanttext-aggregation]


An aggregation that returns interesting or unusual occurrences of free-text terms in a set. It is like the [significant terms](/reference/aggregations/search-aggregations-bucket-significantterms-aggregation.md) aggregation but differs in that:

* It is specifically designed for use on type `text` fields
* It does not require field data or doc-values
* It re-analyzes text content on-the-fly meaning it can also filter duplicate sections of noisy text that otherwise tend to skew statistics.

::::{warning}
Re-analyzing *large* result sets will require a lot of time and memory. It is recommended that the significant_text aggregation is used as a child of either the [sampler](/reference/aggregations/search-aggregations-bucket-sampler-aggregation.md) or [diversified sampler](/reference/aggregations/search-aggregations-bucket-diversified-sampler-aggregation.md) aggregation to limit the analysis to a *small* selection of top-matching documents e.g. 200. This will typically improve speed, memory use and quality of results.
::::


* Suggesting "H5N1" when users search for "bird flu" to help expand queries
* Suggesting keywords relating to stock symbol $ATI for use in an automated news classifier

In these cases the words being selected are not simply the most popular terms in results. The most popular words tend to be very boring (*and, of, the, we, I, they* … ). The significant words are the ones that have undergone a significant change in popularity measured between a *foreground* and *background* set. If the term "H5N1" only exists in 5 documents in a 10 million document index and yet is found in 4 of the 100 documents that make up a user’s search results that is significant and probably very relevant to their search. 5/10,000,000 vs 4/100 is a big swing in frequency.

## Basic use [_basic_use_2]

In the typical use case, the *foreground* set of interest is a selection of the top-matching search results for a query and the *background* set used for statistical comparisons is the index or indices from which the results were gathered.

Example:

$$$significanttext-aggregation-example$$$

```console
GET news/_search
{
  "query": {
    "match": { "content": "Bird flu" }
  },
  "aggregations": {
    "my_sample": {
      "sampler": {
        "shard_size": 100
      },
      "aggregations": {
        "keywords": {
          "significant_text": { "field": "content" }
        }
      }
    }
  }
}
```

Response:

```console-result
{
  "took": 9,
  "timed_out": false,
  "_shards": ...,
  "hits": ...,
    "aggregations" : {
        "my_sample": {
            "doc_count": 100,
            "keywords" : {
                "doc_count": 100,
                "buckets" : [
                    {
                        "key": "h5n1",
                        "doc_count": 4,
                        "score": 4.71235374214817,
                        "bg_count": 5
                    }
                    ...
                ]
            }
        }
    }
}
```

The results show that "h5n1" is one of several terms strongly associated with bird flu. It only occurs 5 times in our index as a whole (see the `bg_count`) and yet 4 of these were lucky enough to appear in our 100 document sample of "bird flu" results. That suggests a significant word and one which the user can potentially add to their search.


## Dealing with noisy data using `filter_duplicate_text` [filter-duplicate-text-noisy-data]

Free-text fields often contain a mix of original content and mechanical copies of text (cut-and-paste biographies, email reply chains, retweets, boilerplate headers/footers, page navigation menus, sidebar news links, copyright notices, standard disclaimers, addresses).

In real-world data these duplicate sections of text tend to feature heavily in `significant_text` results if they aren’t filtered out. Filtering near-duplicate text is a difficult task at index-time but we can cleanse the data on-the-fly at query time using the `filter_duplicate_text` setting.

First let’s look at an unfiltered real-world example using the [Signal media dataset](https://research.signalmedia.co/newsir16/signal-dataset.md) of a million news articles covering a wide variety of news. Here are the raw significant text results for a search for the articles mentioning "elasticsearch":

```js
{
  ...
  "aggregations": {
    "sample": {
      "doc_count": 35,
      "keywords": {
        "doc_count": 35,
        "buckets": [
          {
            "key": "elasticsearch",
            "doc_count": 35,
            "score": 28570.428571428572,
            "bg_count": 35
          },
          ...
          {
            "key": "currensee",
            "doc_count": 8,
            "score": 6530.383673469388,
            "bg_count": 8
          },
          ...
          {
            "key": "pozmantier",
            "doc_count": 4,
            "score": 3265.191836734694,
            "bg_count": 4
          },
          ...

}
```

The uncleansed documents have thrown up some odd-looking terms that are, on the face of it, statistically correlated with appearances of our search term "elasticsearch" e.g. "pozmantier". We can drill down into examples of these documents to see why pozmantier is connected using this query:

$$$significanttext-aggregation-pozmantier-example$$$

```console
GET news/_search
{
  "query": {
    "simple_query_string": {
      "query": "+elasticsearch  +pozmantier"
    }
  },
  "_source": [
    "title",
    "source"
  ],
  "highlight": {
    "fields": {
      "content": {}
    }
  }
}
```

The results show a series of very similar news articles about a judging panel for a number of tech projects:

```js
{
  ...
  "hits": {
    "hits": [
      {
        ...
        "_source": {
          "source": "Presentation Master",
          "title": "T.E.N. Announces Nominees for the 2015 ISE® North America Awards"
        },
        "highlight": {
          "content": [
            "City of San Diego Mike <em>Pozmantier</em>, Program Manager, Cyber Security Division, Department of",
            " Janus, Janus <em>ElasticSearch</em> Security Visualization Engine "
          ]
        }
      },
      {
        ...
        "_source": {
          "source": "RCL Advisors",
          "title": "T.E.N. Announces Nominees for the 2015 ISE(R) North America Awards"
        },
        "highlight": {
          "content": [
            "Mike <em>Pozmantier</em>, Program Manager, Cyber Security Division, Department of Homeland Security S&T",
            "Janus, Janus <em>ElasticSearch</em> Security Visualization Engine"
          ]
        }
      },
      ...
```

Mike Pozmantier was one of many judges on a panel and elasticsearch was used in one of many projects being judged.

As is typical, this lengthy press release was cut-and-paste by a variety of news sites and consequently any rare names, numbers or typos they contain become statistically correlated with our matching query.

Fortunately similar documents tend to rank similarly so as part of examining the stream of top-matching documents the significant_text aggregation can apply a filter to remove sequences of any 6 or more tokens that have already been seen. Let’s try this same query now but with the `filter_duplicate_text` setting turned on:

$$$significanttext-aggregation-filter-duplicate-text-example$$$

```console
GET news/_search
{
  "query": {
    "match": {
      "content": "elasticsearch"
    }
  },
  "aggs": {
    "sample": {
      "sampler": {
        "shard_size": 100
      },
      "aggs": {
        "keywords": {
          "significant_text": {
            "field": "content",
            "filter_duplicate_text": true
          }
        }
      }
    }
  }
}
```

The results from analysing our deduplicated text are obviously of higher quality to anyone familiar with the elastic stack:

```js
{
  ...
  "aggregations": {
    "sample": {
      "doc_count": 35,
      "keywords": {
        "doc_count": 35,
        "buckets": [
          {
            "key": "elasticsearch",
            "doc_count": 22,
            "score": 11288.001166180758,
            "bg_count": 35
          },
          {
            "key": "logstash",
            "doc_count": 3,
            "score": 1836.648979591837,
            "bg_count": 4
          },
          {
            "key": "kibana",
            "doc_count": 3,
            "score": 1469.3020408163263,
            "bg_count": 5
          }
        ]
      }
    }
  }
}
```

Mr Pozmantier and other one-off associations with elasticsearch no longer appear in the aggregation results as a consequence of copy-and-paste operations or other forms of mechanical repetition.

If your duplicate or near-duplicate content is identifiable via a single-value indexed field  (perhaps a hash of the article’s `title` text or an `original_press_release_url` field) then it would be more efficient to use a parent [diversified sampler](/reference/aggregations/search-aggregations-bucket-diversified-sampler-aggregation.md) aggregation to eliminate these documents from the sample set based on that single key. The less duplicate content you can feed into the significant_text aggregation up front the better in terms of performance.

::::{admonition} How are the significance scores calculated?
The numbers returned for scores are primarily intended for ranking different suggestions sensibly rather than something easily understood by end users. The scores are derived from the doc frequencies in *foreground* and *background* sets. In brief, a term is considered significant if there is a noticeable difference in the frequency in which a term appears in the subset and in the background. The way the terms are ranked can be configured, see "Parameters" section.

::::


::::{admonition} Use the "like this but not this" pattern
You can spot mis-categorized content by first searching a structured field e.g. `category:adultMovie` and use significant_text on the text "movie_description" field. Take the suggested words (I’ll leave them to your imagination) and then search for all movies NOT marked as category:adultMovie but containing these keywords. You now have a ranked list of badly-categorized movies that you should reclassify or at least remove from the "familyFriendly" category.

The significance score from each term can also provide a useful `boost` setting to sort matches. Using the `minimum_should_match` setting of the `terms` query with the keywords will help control the balance of precision/recall in the result set i.e a high setting would have a small number of relevant results packed full of keywords and a setting of "1" would produce a more exhaustive results set with all documents containing *any* keyword.

::::



## Limitations [_limitations_9]

### No support for child aggregations [_no_support_for_child_aggregations]

The significant_text aggregation intentionally does not support the addition of child aggregations because:

* It would come with a high memory cost
* It isn’t a generally useful feature and there is a workaround for those that need it

The volume of candidate terms is generally very high and these are pruned heavily before the final results are returned. Supporting child aggregations would generate additional churn and be inefficient. Clients can always take the heavily-trimmed set of results from a `significant_text` request and make a subsequent follow-up query using a `terms` aggregation with an `include` clause and child aggregations to perform further analysis of selected keywords in a more efficient fashion.


### No support for nested objects [_no_support_for_nested_objects]

The significant_text aggregation currently also cannot be used with text fields in nested objects, because it works with the document JSON source. This makes this feature inefficient when matching nested docs from stored JSON given a matching Lucene docID.


### Approximate counts [_approximate_counts_2]

The counts of how many documents contain a term provided in results are based on summing the samples returned from each shard and as such may be:

* low if certain shards did not provide figures for a given term in their top sample
* high when considering the background frequency as it may count occurrences found in deleted documents

Like most design decisions, this is the basis of a trade-off in which we have chosen to provide fast performance at the cost of some (typically small) inaccuracies. However, the `size` and `shard size` settings covered in the next section provide tools to help control the accuracy levels.



## Parameters [significanttext-aggregation-parameters]

### Significance heuristics [_significance_heuristics]

This aggregation supports the same scoring heuristics (JLH, mutual_information, gnd, chi_square etc) as the [significant terms](/reference/aggregations/search-aggregations-bucket-significantterms-aggregation.md) aggregation


### Size & Shard Size [sig-text-shard-size]

The `size` parameter can be set to define how many term buckets should be returned out of the overall terms list. By default, the node coordinating the search process will request each shard to provide its own top term buckets and once all shards respond, it will reduce the results to the final list that will then be returned to the client. If the number of unique terms is greater than `size`, the returned list can be slightly off and not accurate (it could be that the term counts are slightly off and it could even be that a term that should have been in the top size buckets was not returned).

To ensure better accuracy a multiple of the final `size` is used as the number of terms to request from each shard (`2 * (size * 1.5 + 10)`). To take manual control of this setting the `shard_size` parameter can be used to control the volumes of candidate terms produced by each shard.

Low-frequency terms can turn out to be the most interesting ones once all results are combined so the significant_terms aggregation can produce higher-quality results when the `shard_size` parameter is set to values significantly higher than the `size` setting. This ensures that a bigger volume of promising candidate terms are given a consolidated review by the reducing node before the final selection. Obviously large candidate term lists will cause extra network traffic and RAM usage so this is quality/cost trade off that needs to be balanced. If `shard_size` is set to -1 (the default) then `shard_size` will be automatically estimated based on the number of shards and the `size` parameter.

::::{note}
`shard_size` cannot be smaller than `size` (as it doesn’t make much sense). When it is, elasticsearch will override it and reset it to be equal to `size`.
::::



### Minimum document count [_minimum_document_count_3]

It is possible to only return terms that match more than a configured number of hits using the `min_doc_count` option. The Default value is 3.

Terms that score highly will be collected on a shard level and merged with the terms collected from other shards in a second step. However, the shard does not have the information about the global term frequencies available. The decision if a term is added to a candidate list depends only on the score computed on the shard using local shard frequencies, not the global frequencies of the word. The `min_doc_count` criterion is only applied after merging local terms statistics of all shards. In a way the decision to add the term as a candidate is made without being very *certain* about if the term will actually reach the required `min_doc_count`. This might cause many (globally) high frequent terms to be missing in the final result if low frequent but high scoring terms populated the candidate lists. To avoid this, the `shard_size` parameter can be increased to allow more candidate terms on the shards. However, this increases memory consumption and network traffic.

#### `shard_min_doc_count` [search-aggregations-bucket-significanttext-shard-min-doc-count]

The parameter `shard_min_doc_count` regulates the *certainty* a shard has if the term should actually be added to the candidate list or not with respect to the `min_doc_count`. Terms will only be considered if their local shard frequency within the set is higher than the `shard_min_doc_count`. If your dictionary contains many low frequent terms and you are not interested in those (for example misspellings), then you can set the `shard_min_doc_count` parameter to filter out candidate terms on a shard level that will with a reasonable certainty not reach the required `min_doc_count` even after merging the local counts. `shard_min_doc_count` is set to `0` per default and has no effect unless you explicitly set it.

::::{warning}
Setting `min_doc_count` to `1` is generally not advised as it tends to return terms that are typos or other bizarre curiosities. Finding more than one instance of a term helps reinforce that, while still rare, the term was not the result of a one-off accident. The default value of 3 is used to provide a minimum weight-of-evidence. Setting `shard_min_doc_count` too high will cause significant candidate terms to be filtered out on a shard level. This value should be set much lower than `min_doc_count/#shards`.
::::




### Custom background context [_custom_background_context_2]

The default source of statistical information for background term frequencies is the entire index and this scope can be narrowed through the use of a `background_filter` to focus in on significant terms within a narrower context:

$$$significanttext-aggregation-custom-background-example$$$

```console
GET news/_search
{
  "query": {
    "match": {
      "content": "madrid"
    }
  },
  "aggs": {
    "tags": {
      "significant_text": {
        "field": "content",
        "background_filter": {
          "term": { "content": "spain" }
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



### Dealing with source and index mappings [_dealing_with_source_and_index_mappings]

Ordinarily the indexed field name and the original JSON field being retrieved share the same name. However with more complex field mappings using features like `copy_to` the source JSON field(s) and the indexed field being aggregated can differ. In these cases it is possible to list the JSON _source fields from which text will be analyzed using the `source_fields` parameter:

$$$significanttext-aggregation-mappings-example$$$

```console
GET news/_search
{
  "query": {
    "match": {
      "custom_all": "elasticsearch"
    }
  },
  "aggs": {
    "tags": {
      "significant_text": {
        "field": "custom_all",
        "source_fields": [ "content", "title" ]
      }
    }
  }
}
```


### Filtering Values [_filtering_values_3]

It is possible (although rarely required) to filter the values for which buckets will be created. This can be done using the `include` and `exclude` parameters which are based on a regular expression string or arrays of exact terms. This functionality mirrors the features described in the [terms aggregation](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md) documentation.



