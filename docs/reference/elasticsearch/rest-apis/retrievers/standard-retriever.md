---
applies_to:
 stack: all
 serverless: ga
---
# Standard retriever [standard-retriever]

A standard retriever returns top documents from a traditional [query](/reference/query-languages/querydsl.md).


### Parameters: [standard-retriever-parameters]

`query`
:   (Optional, [query object](/reference/query-languages/querydsl.md))

    Defines a query to retrieve a set of top documents.


`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies a [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to this retriever, where all documents must match this query but do not contribute to the score.


`search_after`
:   (Optional, [search after object](/reference/elasticsearch/rest-apis/paginate-search-results.md#search-after))

    Defines a search after object parameter used for pagination.


`terminate_after`
:   (Optional, integer) Maximum number of documents to collect for each shard. If a query reaches this limit, {{es}} terminates the query early. {{es}} collects documents before sorting.

    ::::{important}
    Use with caution. {{es}} applies this parameter to each shard handling the request. When possible, let {{es}} perform early termination automatically. Avoid specifying this parameter for requests that target data streams with backing indices across multiple data tiers.
    ::::


`sort`
:   (Optional, [sort object](/reference/elasticsearch/rest-apis/sort-search-results.md)) A sort object that specifies the order of matching documents.


`min_score`
:   (Optional, `float`)

    Minimum [`_score`](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) for matching documents. Documents with a lower `_score` are not included in the top documents.


`collapse`
:   (Optional, [collapse object](/reference/elasticsearch/rest-apis/collapse-search-results.md))

    Collapses the top documents by a specified key into a single top document per key.


## Restrictions [_restrictions]

When a retriever tree contains a compound retriever (a retriever with two or more child retrievers) the [search after](/reference/elasticsearch/rest-apis/paginate-search-results.md#search-after) parameter is not supported.

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

## Example [standard-retriever-example]

```console
GET /restaurants/_search
{
  "retriever": { <1>
    "standard": { <2>
      "query": { <3>
        "bool": { <4>
          "should": [ <5>
            {
              "match": { <6>
                "region": "Austria"
              }
            }
          ],
          "filter": [ <7>
            {
              "term": { <8>
                "year": "2019" <9>
              }
            }
          ]
        }
      }
    }
  }
}
```

1. Opens the `retriever` object.
2. The `standard` retriever is used for defining traditional {{es}} queries.
3. The entry point for defining the search query.
4. The `bool` object allows for combining multiple query clauses logically.
5. The `should` array indicates conditions under which a document will match. Documents matching these conditions will have increased relevancy scores.
6. The `match` object finds documents where the `region` field contains the word "Austria."
7. The `filter` array provides filtering conditions that must be met but do not contribute to the relevancy score.
8. The `term` object is used for exact matches, in this case, filtering documents by the `year` field.
9. The exact value to match in the `year` field.
