---
navigation_title: "Searching with query rules"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-using-query-rules.html
applies_to:
  stack: all
---

# Searching with query rules [search-using-query-rules]


$$$query-rules$$$
*Query rules* allow customization of search results for queries that match specified criteria metadata. This allows for more control over results, for example ensuring that promoted documents that match defined criteria are returned at the top of the result list. Metadata is defined in the query rule, and is matched against the query criteria. Query rules use metadata to match a query. Metadata is provided as part of the search request as an object and can be anything that helps differentiate the query, for example:

* A user-entered query string
* Personalized metadata about users (e.g. country, language, etc)
* A particular topic
* A referring site
* etc.

Query rules define a metadata key that will be used to match the metadata provided in the [rule retriever](/reference/elasticsearch/rest-apis/retrievers.md#rule-retriever) with the criteria specified in the rule.

When a query rule matches the rule metadata according to its defined criteria, the query rule action is applied to the underlying `organic` query.

For example, a query rule could be defined to match a user-entered query string of `pugs` and a country `us` and promote adoptable shelter dogs if the rule query met both criteria.

Rules are defined using the [query rules API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules) and searched using the [rule retriever](/reference/elasticsearch/rest-apis/retrievers.md#rule-retriever) or the [rule query](/reference/query-languages/query-dsl/query-dsl-rule-query.md).


## Rule definition [query-rule-definition]

When defining a rule, consider the following:


### Rule type [query-rule-type]

The type of rule we want to apply. We support the following rule types:

* `pinned` will re-write the query into a [pinned query](/reference/query-languages/query-dsl/query-dsl-pinned-query.md), pinning specified results matching the query rule at the top of the returned result set.
* `exclude` will exclude specified results from the returned result set.


### Rule criteria [query-rule-criteria]

The criteria for which this rule will match. Criteria is defined as `type`, `metadata`, and `values`. Allowed criteria types are:

| Type | Match Requirements |
| --- | --- |
| `exact` | Rule metadata matches the specified value exactly. |
| `fuzzy` | Rule metadata matches the specified value within an allowed [Levenshtein edit distance](https://en.wikipedia.org/wiki/Levenshtein_distance). |
| `prefix` | Rule metadata starts with the specified value. |
| `suffix` | Rule metadata ends with the specified value. |
| `contains` | Rule metadata contains the specified value. |
| `lt` | Rule metadata is less than the specified value. |
| `lte` | Rule metadata is less than or equal to the specified value. |
| `gt` | Rule metadata is greater than the specified value. |
| `gte` | Rule metadata is greater than or equal to the specified value. |
| `always` | Always matches for all rule queries. |


### Rule actions [query-rule-actions]

The actions to take when the rule matches a query:

* `ids` will select the specified [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md)s.
* `docs` will select the specified documents in the specified indices.

Use `ids` when searching over a single index, and `docs` when searching over multiple indices. `ids` and `docs` cannot be combined in the same query.


## Add query rules [add-query-rules]

You can add query rules using the [Create or update query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-put-ruleset) call. This adds a ruleset containing one or more query rules that will be applied to queries that match their specified criteria.

The following command will create a query ruleset called `my-ruleset` with two query rules:

* The first rule will generate a [Pinned Query](/reference/query-languages/query-dsl/query-dsl-pinned-query.md) pinning the [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md)s `id1` and `id2` when the `query_string` metadata value is a fuzzy match to either `puggles` or `pugs` *and* the user’s location is in the US.
* The second rule will generate a query that excludes the [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) `id3` specifically from the `my-index-000001` index and `id4` from the `my-index-000002` index when the `query_string` metadata value contains `beagles`.

```console
PUT /_query_rules/my-ruleset
{
  "rules": [
    {
      "rule_id": "rule1",
      "type": "pinned",
      "criteria": [
        {
          "type": "fuzzy",
          "metadata": "query_string",
          "values": [ "puggles", "pugs" ]
        },
        {
          "type": "exact",
          "metadata": "user_country",
          "values": [ "us" ]
        }
      ],
      "actions": {
        "ids": [
          "id1",
          "id2"
        ]
      }
    },
    {
      "rule_id": "rule2",
      "type": "exclude",
      "criteria": [
        {
          "type": "contains",
          "metadata": "query_string",
          "values": [ "beagles" ]
        }
      ],
      "actions": {
        "docs": [
          {
            "_index": "my-index-000001",
            "_id": "id3"
          },
          {
            "_index": "my-index-000002",
            "_id": "id4"
          }
        ]
      }
    }
  ]
}
```

The API response returns a results of `created` or `updated` depending on whether this was a new or edited ruleset.

::::{note}
There is a limit of 100 rules per ruleset. This can be increased up to 1000 using the `xpack.applications.rules.max_rules_per_ruleset` cluster setting.
::::


```console-result
{
  "result": "created"
}
```

You can use the [Get query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-get-ruleset) call to retrieve the ruleset you just created, the [List query rulesets](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-list-rulesets) call to retrieve a summary of all query rulesets, and the [Delete query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-delete-ruleset) call to delete a query ruleset.


## Search using query rules [rule-query-search]

Once you have defined one or more query rulesets, you can search using these rulesets using the [rule retriever](/reference/elasticsearch/rest-apis/retrievers.md#rule-retriever) or the [rule query](/reference/query-languages/query-dsl/query-dsl-rule-query.md). Retrievers are the recommended way to use rule queries, as they will work out of the box with other reranking retrievers such as [Reciprocal rank fusion](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md).

Rulesets are evaluated in order, so rules in the first ruleset you specify will be applied before any subsequent rulesets.

An example query for the `my-ruleset` defined above is:

```console
GET /my-index-000001/_search
{
  "retriever": {
    "rule": {
      "retriever": {
        "standard": {
          "query": {
            "query_string": {
              "query": "puggles"
            }
          }
        }
      },
      "match_criteria": {
        "query_string": "puggles",
        "user_country": "us"
      },
      "ruleset_ids": [ "my-ruleset" ]
    }
  }
}
```

This rule query will match against `rule1` in the defined query ruleset, and will convert the organic query into a pinned query with `id1` and `id2` pinned as the top hits. Any other matches from the organic query will be returned below the pinned results.

It’s possible to have multiple rules in a ruleset match a single [rule query](/reference/query-languages/query-dsl/query-dsl-rule-query.md). In this case, the rules are applied in the following order:

* Where the matching rule appears in the ruleset
* If multiple documents are specified in a single rule, in the order they are specified
* If a document is matched by both a `pinned` rule and an `exclude` rule, the `exclude` rule will take precedence

You can specify reranking retrievers such as [rrf](/reference/elasticsearch/rest-apis/retrievers.md#rrf-retriever) or [text_similarity_reranker](/reference/elasticsearch/rest-apis/retrievers.md#text-similarity-reranker-retriever) in the rule query to apply query rules on already-reranked results. Here is an example:

```console
GET my-index-000001/_search
{
  "retriever": {
    "rule": {
      "match_criteria": {
        "query_string": "puggles",
        "user_country": "us"
      },
      "ruleset_ids": [
        "my-ruleset"
      ],
      "retriever": {
        "rrf": {
          "retrievers": [
            {
              "standard": {
                "query": {
                  "query_string": {
                    "query": "pugs"
                  }
                }
              }
            },
            {
              "standard": {
                "query": {
                  "query_string": {
                    "query": "puggles"
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

This will apply pinned and excluded query rules on top of the content that was reranked by RRF.

