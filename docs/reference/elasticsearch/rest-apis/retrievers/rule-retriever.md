---
applies_to:
 stack: all
 serverless: ga
---

# Query rules retriever [rule-retriever]

The `rule` retriever enables fine-grained control over search results by applying contextual [query rules](/reference/elasticsearch/rest-apis/searching-with-query-rules.md#query-rules) to pin or exclude documents for specific queries. This retriever has similar functionality to the [rule query](/reference/query-languages/query-dsl/query-dsl-rule-query.md), but works out of the box with other retrievers.

## Prerequisites [_prerequisites_16]

To use the `rule` retriever you must first create one or more query rulesets using the [query rules management APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules).

## Parameters [rule-retriever-parameters]

`retriever`
:   (Required, `retriever`)

    The child retriever that returns the results to apply query rules on top of. This can be a standalone retriever such as the [standard](standard-retriever.md) or [knn](knn-retriever.md) retriever, or it can be a compound retriever.


`ruleset_ids`
:   (Required, `array`)

    An array of one or more unique [query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules) IDs with query-based rules to match and apply as applicable. Rulesets and their associated rules are evaluated in the order in which they are specified in the query and ruleset. The maximum number of rulesets to specify is 10.


`match_criteria`
:   (Required, `object`)

    Defines the match criteria to apply to rules in the given query ruleset(s). Match criteria should match the keys defined in the `criteria.metadata` field of the rule.


`rank_window_size`
:   (Optional, `int`)

    The number of top documents to return from the `rule` retriever. Defaults to `10`.

## Example: Rule retriever [rule-retriever-example]

This example shows the rule retriever executed without any additional retrievers. It runs the query defined by the `retriever` and applies the rules from `my-ruleset` on top of the returned results.

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
GET movies/_search
{
  "retriever": {
    "rule": {
      "match_criteria": {
        "query_string": "harry potter"
      },
      "ruleset_ids": [
        "my-ruleset"
      ],
      "retriever": {
        "standard": {
          "query": {
            "query_string": {
              "query": "harry potter"
            }
          }
        }
      }
    }
  }
}
```
% TEST[skip:uses ELSER]

## Example: Rule retriever combined with RRF [rule-retriever-example-rrf]

This example shows how to combine the `rule` retriever with other rerank retrievers such as [rrf](rrf-retriever.md) or [text_similarity_reranker](text-similarity-reranker-retriever.md).

::::{warning}
The `rule` retriever will apply rules to any documents returned from its defined `retriever` or any of its sub-retrievers. This means that for the best results, the `rule` retriever should be the outermost defined retriever. Nesting a `rule` retriever as a sub-retriever under a reranker such as `rrf` or `text_similarity_reranker` may not produce the expected results.

::::


```console
GET movies/_search
{
  "retriever": {
    "rule": { <1>
      "match_criteria": {
        "query_string": "harry potter"
      },
      "ruleset_ids": [
        "my-ruleset"
      ],
      "retriever": {
        "rrf": { <2>
          "retrievers": [
            {
              "standard": {
                "query": {
                  "query_string": {
                    "query": "sorcerer's stone"
                  }
                }
              }
            },
            {
              "standard": {
                "query": {
                  "query_string": {
                    "query": "chamber of secrets"
                  }
                }
              }
            }
          ]
        }
      }
    }
  }
}
```
% TEST[skip:uses ELSER]

1. The `rule` retriever is the outermost retriever, applying rules to the search results that were previously reranked using the `rrf` retriever.
2. The `rrf` retriever returns results from all of its sub-retrievers, and the output of the `rrf` retriever is used as input to the `rule` retriever.

