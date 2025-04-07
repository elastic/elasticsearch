---
navigation_title: "Rule"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-rule-query.html
---

# Rule query [query-dsl-rule-query]


::::{warning}
`rule_query` was renamed to `rule` in 8.15.0. The old syntax using `rule_query` and `ruleset_id` is deprecated and will be removed in a future release, so it is strongly advised to migrate existing rule queries to the new API structure.

::::


::::{tip}
The rule query is not supported for use alongside reranking. If you want to use query rules in conjunction with reranking, use the [rule retriever](/reference/elasticsearch/rest-apis/retrievers.md#rule-retriever) instead.

::::


Applies [query rules](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules) to the query before returning results. Query rules can be used to promote documents in the manner of a [Pinned query](/reference/query-languages/query-dsl/query-dsl-pinned-query.md) based on matching defined rules, or to identify specific documents to exclude from a contextual result set. If no matching query rules are defined, the "organic" matches for the query are returned. All matching rules are applied in the order in which they appear in the query ruleset. If the same document matches both an `exclude` rule and a `pinned` rule, the document will be excluded.

::::{note}
To use the rule query, you first need a defined set of query rules. Use the [query rules management APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules) to create and manage query rules. For more information and examples see [Searching with query rules](/reference/elasticsearch/rest-apis/searching-with-query-rules.md).

::::


## Example request [_example_request_2]

```console
GET /_search
{
  "query": {
    "rule": {
      "match_criteria": {
        "user_query": "pugs"
      },
      "ruleset_ids": ["my-ruleset"],
      "organic": {
        "match": {
          "description": "puggles"
        }
      }
    }
  }
}
```


## Top-level parameters for `rule_query` [rule-query-top-level-parameters]

`ruleset_ids`
:   (Required, array) An array of one or more unique [query ruleset](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-query_rules) ID with query-based rules to match and apply as applicable. Rulesets and their associated rules are evaluated in the order in which they are specified in the query and ruleset. The maximum number of rulesets to specify is 10.

`match_criteria`
:   (Required, object) Defines the match criteria to apply to rules in the given query ruleset. Match criteria should match the keys defined in the `criteria.metadata` field of the rule.

`organic`
:   (Required, object) Any choice of [query](/reference/query-languages/querydsl.md) used to return results, that may be modified by matching query rules. If no query rules are matched and applied, this query will be executed with no modification.


