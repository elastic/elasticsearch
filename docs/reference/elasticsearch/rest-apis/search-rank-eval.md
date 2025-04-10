---
applies_to:
  serverless: all
  stack: all
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-rank-eval.html
---
# Ranking evaluation

The ranking evaluation API enables you to evaluate the quality of ranked search results over a set of typical search queries.
Given this set of queries and a list of manually rated documents, the `_rank_eval` endpoint calculates and returns typical information retrieval metrics like *mean reciprocal rank*, *precision* or *discounted cumulative gain*.

Search quality evaluation starts with looking at the users of your search application, and the things that they are searching for.
Users have a specific *information need*; for example, they are looking for gift in a web shop or want to book a flight for their next holiday.
They usually enter some search terms into a search box or some other web form.
All of this information, together with meta information about the user (for example the browser, location, earlier preferences and so on) then gets translated into a query to the underlying search system.

The challenge for search engineers is to tweak this translation process from user entries to a concrete query, in such a way that the search results contain the most relevant information with respect to the user's information need.
This can only be done if the search result quality is evaluated constantly across a representative test suite of typical user queries, so that improvements in the rankings for one particular query don't negatively affect the ranking for other types of queries.

In order to get started with search quality evaluation, you need three basic things:

1. A collection of documents you want to evaluate your query performance against, usually one or more data streams or indices.
2. A collection of typical search requests that users enter into your system.
3. A set of document ratings that represent the documents' relevance with respect to a search request.

It is important to note that one set of document ratings is needed per test query, and that the relevance judgements are based on the information need of the user that entered the query.

The ranking evaluation API provides a convenient way to use this information in a ranking evaluation request to calculate different search evaluation metrics.
This gives you a first estimation of your overall search quality, as well as a measurement to optimize against when fine-tuning various aspect of the query generation in your application.

For full details about the API syntax, refer to:

* [{{es}} API documentation]({{es-apis}}operation/operation-rank-eval)
* [{{es-serverless}} API documentation]({{es-serverless-apis}}operation/operation-rank-eval)

## Examples

In its most basic form, a request to the `_rank_eval` endpoint has two sections:

```console
GET /my-index-000001/_rank_eval
{
  "requests": [ ... ],                            <1>
  "metric": {                                     <2>
    "mean_reciprocal_rank": { ... }               <3>
  }
}
```

1. A set of typical search requests, together with their provided ratings.
2. A definition of the evaluation metric to calculate.
3. A specific metric and its parameters.

The request section contains several search requests typical to your application, along with the document ratings for each particular search request.

```console
GET /my-index-000001/_rank_eval
{
  "requests": [
    {
      "id": "amsterdam_query",                                  <1>
      "request": {                                              <2>
          "query": { "match": { "text": "amsterdam" } }
      },
      "ratings": [                                              <3>
        { "_index": "my-index-000001", "_id": "doc1", "rating": 0 },
        { "_index": "my-index-000001", "_id": "doc2", "rating": 3 },
        { "_index": "my-index-000001", "_id": "doc3", "rating": 1 }
      ]
    },
    {
      "id": "berlin_query",
      "request": {
        "query": { "match": { "text": "berlin" } }
      },
      "ratings": [
        { "_index": "my-index-000001", "_id": "doc1", "rating": 1 }
      ]
    }
  ]
}
```

1. The search request's ID, used to group result details later.
2. The query being evaluated.
3. A list of document ratings.

Each entry in the list of document ratings contains the following arguments:

* `_index`: The document's index. For data streams, this should be the document's backing index.
* `_id`: The document ID.
* `rating`: The document's relevance with regard to this search request.

A document `rating` can be any integer value that expresses the relevance of the document on a user-defined scale. 
For some of the metrics, just giving a binary rating (for example `0` for irrelevant and `1` for relevant) will be sufficient, while other metrics can use a more fine-grained scale.

### Template-based ranking evaluation

As an alternative to having to provide a single query per test request, it is possible to specify query templates in the evaluation request and later refer to them.
This way, queries with a similar structure that differ only in their parameters don't have to be repeated all the time in the `requests` section.
In typical search systems, where user inputs usually get filled into a small set of query templates, this helps make the evaluation request more succinct.

```console
GET /my-index-000001/_rank_eval
{
   [...]
  "templates": [
     {
        "id": "match_one_field_query",  <1>
        "template": { <2>
            "inline": {
                "query": {
                  "match": { "{{field}}": { "query": "{{query_string}}" }}
                }
            }
        }
     }
  ],
  "requests": [
      {
         "id": "amsterdam_query",
         "ratings": [ ... ],
         "template_id": "match_one_field_query", <3>
         "params": { <4>
            "query_string": "amsterdam",
            "field": "text"
          }
     },
    [...]
  ]
}
```

1. The template ID.
2. The template definition to use.
3. A reference to a previously defined template.
4. The parameters to use to fill the template.

You can also use a stored [search template](docs-content://solutions/search/search-templates.md).
For example:

```console
GET /my_index/_rank_eval
{
   [...]
  "templates": [
     {
        "id": "match_one_field_query",  <1>
        "template": { <2>
            "id": "match_one_field_query"
        }
     }
  ],
  "requests": [...]
}
```

1. The template ID used for requests.
2. The template ID stored in the cluster state.

### Available evaluation metrics

The `metric` section determines which of the available evaluation metrics will be used.
The following metrics are supported:

#### Precision at K (P@k) [k-precision] 

This metric measures the proportion of relevant results in the top k search results.
It's a form of the well-known [precision metric](https://en.wikipedia.org/wiki/Evaluation_measures_(information_retrieval)#Precision) that only looks at the top k documents.
It is the fraction of relevant documents in those first k results.
A precision at 10 (P@10) value of 0.6 then means 6 out of the 10 top hits are relevant with respect to the user's information need.

P@k works well as a simple evaluation metric that has the benefit of being easy to understand and explain.
Documents in the collection need to be rated as either relevant or irrelevant with respect to the current query.
P@k is a set-based metric and does not take into account the position of the relevant documents within the top k results, so a ranking of ten results that contains one relevant result in position 10 is equally as good as a ranking of ten results that contains one relevant result in position 1.

```console
GET /my-index-000001/_rank_eval
{
  "requests": [
    {
      "id": "JFK query",
      "request": { "query": { "match_all": {} } },
      "ratings": []
    } ],
  "metric": {
    "precision": {
      "k": 20,
      "relevant_rating_threshold": 1,
      "ignore_unlabeled": false
    }
  }
}
```

The `precision` metric takes optional parameters, including:

| Parameter | Description |
| --- | --- |
| `k` | Sets the maximum number of documents retrieved per query. This value will act in place of the usual `size` parameter in the query.|
| `relevant_rating_threshold` | Sets the rating threshold above which documents are considered to be "relevant". |
| `ignore_unlabeled` | Controls how unlabeled documents in the search results are counted. If set to `true`, unlabeled documents are ignored and neither count as relevant or irrelevant. Set to `false`, they are treated as irrelevant. |

Refer to the API documentation for the definitive list of parameters.

#### Recall at K (R@k) [k-recall] 

This metric measures the total number of relevant results in the top k search results.
It's a form of the well-known [recall metric](https://en.wikipedia.org/wiki/Evaluation_measures_(information_retrieval)#Recall).
It is the fraction of relevant documents in those first k results relative to all possible relevant results.
A recall at 10 (R@10) value of 0.5 then means 4 out of 8 relevant documents, with respect to the user's information need, were retrieved in the 10 top hits.

R@k works well as a simple evaluation metric that has the benefit of being easy to understand and explain. 
Documents in the collection need to be rated as either relevant or irrelevant with respect to the current query. 
R@k is a set-based metric and does not take into account the position of the relevant documents within the top k results, so a ranking of ten results that contains one relevant result in position 10 is equally as good as a ranking of ten results that contains one relevant result in position 1.

```console
GET /my-index-000001/_rank_eval
{
  "requests": [
    {
      "id": "JFK query",
      "request": { "query": { "match_all": {} } },
      "ratings": []
    } ],
  "metric": {
    "recall": {
      "k": 20,
      "relevant_rating_threshold": 1
    }
  }
}
```

The `recall` metric takes optional parameters including:

| Parameter | Description |
| --- | --- |
| `k` | Sets the maximum number of documents retrieved per query. This value will act in place of the usual `size` parameter in the query. |
| `relevant_rating_threshold` | Sets the rating threshold above which documents are considered to be "relevant". |

Refer to the API documentation for the definitive list of parameters.

#### Mean reciprocal rank [_mean_reciprocal_rank] 

For every query in the test suite, this metric calculates the reciprocal of the rank of the first relevant document.
For example, finding the first relevant result in position 3 means the reciprocal rank is 1/3.
The reciprocal rank for each query is averaged across all queries in the test suite to give the [mean reciprocal rank](https://en.wikipedia.org/wiki/Mean_reciprocal_rank).

```console
GET /my-index-000001/_rank_eval
{
  "requests": [
    {
      "id": "JFK query",
      "request": { "query": { "match_all": {} } },
      "ratings": []
    } ],
  "metric": {
    "mean_reciprocal_rank": {
      "k": 20,
      "relevant_rating_threshold": 1
    }
  }
}
```

The `mean_reciprocal_rank` metric takes optional parameters including:

| Parameter | Description |
| --- | --- |
| `k` | Sets the maximum number of documents retrieved per query. This value will act in place of the usual `size` parameter in the query. |
| `relevant_rating_threshold` | Sets the rating threshold above which documents are considered to be "relevant". |

Refer to the API documentation for the definitive list of parameters.

#### Discounted cumulative gain (DCG) [_discounted_cumulative_gain_dcg] 

In contrast to the two metrics above, [discounted cumulative gain](https://en.wikipedia.org/wiki/Discounted_cumulative_gain) takes both the rank and the rating of the search results into account.

The assumption is that highly relevant documents are more useful for the user when appearing at the top of the result list.
Therefore, the DCG formula reduces the contribution that high ratings for documents on lower search ranks have on the overall DCG metric.

```console
GET /my-index-000001/_rank_eval
{
  "requests": [
    {
      "id": "JFK query",
      "request": { "query": { "match_all": {} } },
      "ratings": []
    } ],
  "metric": {
    "dcg": {
      "k": 20,
      "normalize": false
    }
  }
}
```

The `dcg` metric takes optional parameters including:

| Parameter | Description |
| --- | --- |
| `k` | Sets the maximum number of documents retrieved per query. This value will act in place of the usual `size` parameterin the query. |
| `normalize` | If set to `true`, this metric will calculate the [Normalized DCG](https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Normalized_DCG). |

Refer to the API documentation for the definitive list of parameters.

#### Expected Reciprocal Rank (ERR) [_expected_reciprocal_rank_err] 

Expected Reciprocal Rank (ERR) is an extension of the classical reciprocal rank for the graded relevance case (Olivier Chapelle, Donald Metzler, Ya Zhang, and Pierre Grinspan. Jan 2009. [Expected reciprocal rank for graded relevance](https://www.researchgate.net/publication/220269787_Expected_reciprocal_rank_for_graded_relevance).)

It is based on the assumption of a cascade model of search, in which a user scans through ranked search results in order and stops at the first document that satisfies the information need.
For this reason, it is a good metric for question answering and navigation queries, but less so for survey-oriented information needs where the user is interested in finding many relevant documents in the top k results.

The metric models the expectation of the reciprocal of the position at which a user stops reading through the result list.
This means that a relevant document in a top ranking position will have a large contribution to the overall score. 
However, the same document will contribute much less to the score if it appears in a lower rank; even more so if there are some relevant (but maybe less relevant) documents preceding it.
In this way, the ERR metric discounts documents that are shown after very relevant documents.
This introduces a notion of dependency in the ordering of relevant documents that, for example, precision or DCG don't account for.

```console
GET /my-index-000001/_rank_eval
{
  "requests": [
    {
      "id": "JFK query",
      "request": { "query": { "match_all": {} } },
      "ratings": []
    } ],
  "metric": {
    "expected_reciprocal_rank": {
      "maximum_relevance": 3,
      "k": 20
    }
  }
}
```

The `expected_reciprocal_rank` metric takes parameters including:

| Parameter | Description |
| --- | --- |
| `maximum_relevance` | Mandatory parameter. The highest relevance grade used in the user-supplied relevance judgments. |
| `k` | Sets the maximum number of documents retrieved per query. This value will act in place of the usual `size` parameterin the query.|

Refer to the API documentation for the definitive list of parameters.

### Response format [_response_format_2]

The response of the `_rank_eval` endpoint contains the overall calculated result for the defined quality metric, a `details` section with a breakdown of results for each query in the test suite and an optional `failures` section that shows potential errors of individual queries.
The response has the following format:

```js
{
  "rank_eval": {
    "metric_score": 0.4,                          <1>
      "details": {
      "my_query_id1": {                           <2>
        "metric_score": 0.6,                      <3>
        "unrated_docs": [                         <4>
          {
            "_index": "my-index-000001",
            "_id": "1960795"
          }, ...
        ],
        "hits": [
          {
            "hit": {                              <5>
              "_index": "my-index-000001",
              "_type": "page",
              "_id": "1528558",
              "_score": 7.0556192
            },
            "rating": 1
          }, ...
        ],
        "metric_details": {                       <6>
          "precision": {
            "relevant_docs_retrieved": 6,
            "docs_retrieved": 10
          }
        }
      },
      "my_query_id2": { [... ] }
    },
    "failures": { [... ] }
  }
}
```

1. The overall evaluation quality calculated by the defined metric.
2. The `details` section contains one entry for every query in the original `requests` section, keyed by the search request ID.
3. The `metric_score` in the `details` section shows the contribution of this query to the global quality metric score.
4. The `unrated_docs` section contains an `_index` and `_id` entry for each document in the search result for this query that didn't have a ratings value. This can be used to ask the user to supply ratings for these documents
5. The `hits` section shows a grouping of the search results with their supplied ratings.
6. The `metric_details` give additional information about the calculated quality metric. For example, how many of the retrieved documents were relevant. The content varies for each metric but allows for better interpretation of the results.
