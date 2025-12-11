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

`diversify` {applies_to}`serverless: preview` {applies_to}`stack: preview 9.3`
:   The [diversify](retrievers/diversify-retriever.md) retriever reduces the results from another retriever by applying a diversification strategy to the top-N results.

`knn`
:   The [knn](retrievers/knn-retriever.md) retriever replaces the functionality of a [knn search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#search-api-knn).

`linear`
:   The [linear](retrievers/linear-retriever.md) retriever linearly combines the scores of other retrievers for the top documents.

`pinned` {applies_to}`stack: GA 9.1`
:   The [pinned](retrievers/pinned-retriever.md) retriever always places specified documents at the top of the results, with the remaining hits provided by a secondary retriever.

`rescorer`
:   The [rescorer](retrievers/rescorer-retriever.md) retriever replaces the functionality of the [query rescorer](/reference/elasticsearch/rest-apis/rescore-search-results.md#rescore).

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
* [`rescore`](/reference/elasticsearch/rest-apis/rescore-search-results.md#rescore) use a [rescorer retriever](retrievers/rescorer-retriever.md) instead


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

<!--
```console
PUT /restaurants
{
  "mappings": {
    "properties": {
      "region": { "type": "keyword" },
      "year": { "type": "keyword" },
      "vector": {
        "type": "dense_vector",
        "dims": 3
      }
    }
  }
}

POST /restaurants/_bulk?refresh
{"index":{}}
{"region": "Austria", "year": "2019", "vector": [10, 22, 77]}
{"index":{}}
{"region": "France", "year": "2019", "vector": [10, 22, 78]}
{"index":{}}
{"region": "Austria", "year": "2020", "vector": [10, 22, 79]}
{"index":{}}
{"region": "France", "year": "2020", "vector": [10, 22, 80]}

PUT /movies

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

PUT _query_rules/my-ruleset
{
    "rules": [
        {
            "rule_id": "my-rule1",
            "type": "pinned",
            "criteria": [
                {
                    "type": "exact",
                    "metadata": "query_string",
                    "values": [ "pugs" ]
                }
            ],
            "actions": {
                "ids": [
                    "id1"
                ]
            }
        }
    ]
}
```
% TESTSETUP

```console
DELETE /restaurants
DELETE /movies
DELETE /books
```
% TEARDOWN
-->

```console
GET books/_search
{
  "retriever": {
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
% TEST[continued]

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
% TEST[continued]

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
% TEST[continued]

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
% TEST[continued]

1. Match fields that start with `title`
2. Match fields that end with `_text`

Note, however, that wildcard field patterns will only resolve to fields that either:

- Support term queries, such as `keyword` and `text` fields
- Are `semantic_text` fields

### Limitations

- **Single index**: Until 9.2, multi-field queries only work with single index searches.
- **CCS (Cross Cluster Search)**: Multi-field queries do not support remote cluster searches

### Examples

- [RRF with the multi-field query format](retrievers/retrievers-examples.md#retrievers-examples-rrf-multi-field-query-format)
- [Linear retriever with the multi-field query format](retrievers/retrievers-examples.md#retrievers-examples-linear-multi-field-query-format)
