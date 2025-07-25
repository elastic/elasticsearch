---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/retriever.html
applies_to:
  stack: all
  serverless: ga
---

# Retrievers [retriever]

A retriever is a specification to describe top documents returned from a search. A retriever replaces other elements of the [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) that also return top documents such as [`query`](/reference/query-languages/querydsl.md) and [`knn`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-api-knn). A retriever may have child retrievers where a retriever with two or more children is considered a compound retriever. This allows for complex behavior to be depicted in a tree-like structure, called the retriever tree, which clarifies the order of operations that occur during a search.

::::{tip}
Refer to [*Retrievers*](docs-content://solutions/search/retrievers-overview.md) for a high level overview of the retrievers abstraction. Refer to [Retrievers examples](retrievers/retrievers-examples.md) for additional examples.

::::

The following retrievers are available:

`knn`
:   The [knn](retrievers/knn-retriever.md) retriever replaces the functionality of a [knn search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-api-knn).

`linear`
:   The [linear](retrievers/linear-retriever.md) retriever linearly combines the scores of other retrievers for the top documents.

`pinned` {applies_to}`stack: GA 9.1`
:   The [pinned](retrievers/pinned-retriever.md) retriever always places specified documents at the top of the results, with the remaining hits provided by a secondary retriever.

`rescorer`
:   The [rescorer](retrievers/rescorer-retriever.md) retriever replaces the functionality of the [query rescorer](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore).

`rrf`
:   The [rrf](retrievers/rrf-retriever.md) retriever produces top documents from [reciprocal rank fusion (RRF)](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md).

`rule`
:   The [rule](retrievers/rule-retriever.md) retriever applies contextual [Searching with query rules](/reference/elasticsearch/rest-apis/searching-with-query-rules.md#query-rules) to pin or exclude documents for specific queries.

`standard`
:   The [standard](retrievers/standard-retriever.md) retriever replaces the functionality of a traditional [query](/reference/query-languages/querydsl.md).

`text_similarity_reranker`
:   The [text_similarity_reranker](retrievers/text-similarity-reranker-retriever.md) retriever enhances search results by re-ranking documents based on semantic similarity to a specified inference text, using a machine learning model.

## Common usage guidelines [retriever-common-parameters]


### Using `from` and `size` with a retriever tree [retriever-size-pagination]

The [`from`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-from-param) and [`size`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-size-param) parameters are provided globally as part of the general [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search). They are applied to all retrievers in a retriever tree, unless a specific retriever overrides the `size` parameter using a different parameter such as `rank_window_size`. Though, the final search hits are always limited to `size`.


### Using aggregations with a retriever tree [retriever-aggregations]

[Aggregations](/reference/aggregations/index.md) are globally specified as part of a search request. The query used for an aggregation is the combination of all leaf retrievers as `should` clauses in a [boolean query](/reference/query-languages/query-dsl/query-dsl-bool-query.md).


### Restrictions on search parameters when specifying a retriever [retriever-restrictions]

When a retriever is specified as part of a search, the following elements are not allowed at the top-level:

* [`query`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#request-body-search-query)
* [`knn`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-api-knn)
* [`search_after`](/reference/elasticsearch/rest-apis/paginate-search-results.md#search-after)
* [`terminate_after`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#request-body-search-terminate-after)
* [`sort`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-sort-param)
* [`rescore`](/reference/elasticsearch/rest-apis/filter-search-results.md#rescore) use a [rescorer retriever](retrievers/rescorer-retriever.md) instead


## Multi-field query format [multi-field-query-format]
```yaml {applies_to}
stack: ga 9.1
```

The [`linear`](retrievers/linear-retriever.md) and [`rrf`](retrievers/rrf-retriever.md) retrievers support a multi-field query format that provides a simplified way to define searches across multiple fields without explicitly specifying inner retrievers.
This format automatically generates appropriate inner retrievers based on the field types and query parameters.
This is a great way to search an index, knowing little to nothing about its schema, while also handling normalization across lexical and semantic matches.

### Field grouping [multi-field-field-grouping]

The multi-field query format groups queried fields into two categories:

- **Lexical fields**: fields that support term queries, such as `keyword` and `text` fields.
- **Semantic fields**: [`semantic_text` fields](/reference/elasticsearch/mapping-reference/semantic-text.md).

Each field group is queried separately and the scores/ranks are normalized such that each contributes 50% to the final score/rank.
This balances the importance of lexical and semantic fields.
Most indices contain more lexical than semantic fields, and without this grouping the results would often bias towards lexical field matches.

::::{warning}
In the `linear` retriever, this grouping relies on using a normalizer other than `none` (i.e., `minmax` or `l2_norm`).
If you use the `none` normalizer, the scores across field groups will not be normalized and the results may be biased towards lexical field matches.
::::

### Linear retriever field boosting [multi-field-field-boosting]

When using the `linear` retriever, fields can be boosted using the `^` notation:

```console
GET books/_search
{
  "retriever": {
    "knn": { <1>
      "field": "vector", <2>
      "query_vector": [10, 22, 77], <3>
      "k": 10, <4>
      "num_candidates": 10 <5>
    }
  }
}
```

1. Configuration for k-nearest neighbor (knn) search, which is based on vector similarity.
2. Specifies the field name that contains the vectors.
3. The query vector against which document vectors are compared in the `knn` search.
4. The number of nearest neighbors to return as top hits. This value must be fewer than or equal to `num_candidates`.
5. The size of the initial candidate set from which the final `k` nearest neighbors are selected.




## Linear Retriever [linear-retriever]

A retriever that normalizes and linearly combines the scores of other retrievers.


#### Parameters [linear-retriever-parameters]

`retrievers`
:   (Required, array of objects)

    A list of the sub-retrievers' configuration, that we will take into account and whose result sets we will merge through a weighted sum. Each configuration can have a different weight and normalization depending on the specified retriever.

`normalizer`
:   (Optional, String)

    Specifies a normalizer to be applied to all sub-retrievers. This provides a simple way to configure normalization for all retrievers at once.

    The `normalizer` can be specified at the top level, at the per-retriever level, or both, with the following rules:

    * If only the top-level `normalizer` is specified, it applies to all sub-retrievers.
    * If both a top-level and a per-retriever `normalizer` are specified, the per-retriever normalizer must be identical to the top-level one. If they differ, the request will fail.
    * If only per-retriever normalizers are specified, they can be different for each sub-retriever.
    * If no normalizer is specified at any level, no normalization is applied.

    Available values are: `minmax`, `l2_norm`, and `none`. Defaults to `none`.

Each entry in the `retrievers` array specifies the following parameters:

`retriever`
:   (Required, a `retriever` object)

    Specifies the retriever for which we will compute the top documents for. The retriever will produce `rank_window_size` results, which will later be merged based on the specified `weight` and `normalizer`.

`weight`
:   (Optional, float)

    The weight that each score of this retriever’s top docs will be multiplied with. Must be greater or equal to 0. Defaults to 1.0.

`normalizer`
:   (Optional, String)

    Specifies how we will normalize this specific retriever’s scores, before applying the specified `weight`. If a top-level `normalizer` is also specified, this normalizer must be the same. Available values are: `minmax`, `l2_norm`, and `none`. Defaults to `none`.

    * `none`
    * `minmax` : A `MinMaxScoreNormalizer` that normalizes scores based on the following formula

        ```
        score = (score - min) / (max - min)
        ```

    * `l2_norm` : An `L2ScoreNormalizer` that normalizes scores using the L2 norm of the score values.

See also [this hybrid search example](docs-content://solutions/search/retrievers-examples.md#retrievers-examples-linear-retriever) using a linear retriever on how to independently configure and apply normalizers to retrievers.

`rank_window_size`
:   (Optional, integer)

    This value determines the size of the individual result sets per query. A higher value will improve result relevance at the cost of performance. The final ranked result set is pruned down to the search request’s [size](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-size-param). `rank_window_size` must be greater than or equal to `size` and greater than or equal to `1`. Defaults to the `size` parameter.


`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies the specified [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to all of the specified sub-retrievers, according to each retriever’s specifications.



## RRF Retriever [rrf-retriever]

An [RRF](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md) retriever returns top documents based on the RRF formula, equally weighting two or more child retrievers. Reciprocal rank fusion (RRF) is a method for combining multiple result sets with different relevance indicators into a single result set.


#### Parameters [rrf-retriever-parameters]

`retrievers`
:   (Required, array of retriever objects)

    A list of child retrievers to specify which sets of returned top documents will have the RRF formula applied to them. Each child retriever carries an equal weight as part of the RRF formula. Two or more child retrievers are required.


`rank_constant`
:   (Optional, integer)

    This value determines how much influence documents in individual result sets per query have over the final ranked result set. A higher value indicates that lower ranked documents have more influence. This value must be greater than or equal to `1`. Defaults to `60`.


`rank_window_size`
:   (Optional, integer)

    This value determines the size of the individual result sets per query. A higher value will improve result relevance at the cost of performance. The final ranked result set is pruned down to the search request’s [size](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-size-param). `rank_window_size` must be greater than or equal to `size` and greater than or equal to `1`. Defaults to the `size` parameter.


`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies the specified [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to all of the specified sub-retrievers, according to each retriever’s specifications.



### Example: Hybrid search [rrf-retriever-example-hybrid]

A simple hybrid search example (lexical search + dense vector search) combining a `standard` retriever with a `knn` retriever using RRF:

```console
GET /restaurants/_search
{
  "retriever": {
    "rrf": { <1>
      "retrievers": [ <2>
        {
          "standard": { <3>
            "query": {
              "multi_match": {
                "query": "Austria",
                "fields": [
                  "city",
                  "region"
                ]
              }
            }
          }
        },
        {
          "knn": { <4>
            "field": "vector",
            "query_vector": [10, 22, 77],
            "k": 10,
            "num_candidates": 10
          }
        }
=======
    "linear": {
      "query": "elasticsearch",
      "fields": [
        "title^3",                <1>
        "description^2",          <2>
        "title_semantic",         <3>
        "description_semantic^2"
      ],
      "normalizer": "minmax"
    }
  }
}
```

1. 3x weight
2. 2x weight
3. 1x weight (default)

Due to how the [field group scores](#multi-field-field-grouping) are normalized, per-field boosts have no effect on the range of the final score.
Instead, they affect the importance of the field's score within its group.

For example, if the schema looks like:

```console
PUT /books
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "copy_to": "title_semantic"
      },
      "description": {
        "type": "text",
        "copy_to": "description_semantic"
      },
      "title_semantic": {
        "type": "semantic_text"
      },
      "description_semantic": {
        "type": "semantic_text"
      }
    }
  }
}
```

And we run this query:

```console
GET books/_search
{
  "retriever": {
    "linear": {
      "query": "elasticsearch",
      "fields": [
        "title",
        "description",
        "title_semantic",
        "description_semantic"
      ],
      "normalizer": "minmax"
    }
  }
}
```

The score breakdown would be:

* Lexical fields (50% of score):
  * `title`: 50% of lexical fields group score, 25% of final score
  * `description`: 50% of lexical fields group score, 25% of final score
* Semantic fields (50% of score):
  * `title_semantic`: 50% of semantic fields group score, 25% of final score
  * `description_semantic`: 50% of semantic fields group score, 25% of final score

If we apply per-field boosts like so:

```console
GET books/_search
{
  "retriever": {
    "linear": {
      "query": "elasticsearch",
      "fields": [
        "title^3",
        "description^2",
        "title_semantic",
        "description_semantic^2"
      ],
      "normalizer": "minmax"
    }
  }
}
```

The score breakdown would change to:

* Lexical fields (50% of score):
    * `title`: 60% of lexical fields group score, 30% of final score
    * `description`: 40% of lexical fields group score, 20% of final score
* Semantic fields (50% of score):
    * `title_semantic`: 33% of semantic fields group score, 16.5% of final score
    * `description_semantic`: 66% of semantic fields group score, 33% of final score

### Wildcard field patterns [multi-field-wildcard-field-patterns]

Field names support the `*` wildcard character to match multiple fields:

```console
GET books/_search
{
  "retriever": {
    "rrf": {
      "query": "machine learning",
      "fields": [
        "title*",    <1>
        "*_text"     <2>
      ]
    }
  }
}
```

1. Match fields that start with `title`
2. Match fields that end with `_text`

Note, however, that wildcard field patterns will only resolve to fields that either:

- Support term queries, such as `keyword` and `text` fields
- Are `semantic_text` fields

### Limitations

- **Single index**: Multi-field queries currently work with single index searches only
- **CCS (Cross Cluster Search)**: Multi-field queries do not support remote cluster searches

### Examples

- [RRF with the multi-field query format](retrievers/retrievers-examples.md#retrievers-examples-rrf-multi-field-query-format)
- [Linear retriever with the multi-field query format](retrievers/retrievers-examples.md#retrievers-examples-linear-multi-field-query-format)