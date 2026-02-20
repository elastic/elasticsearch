---
navigation_title: "Has child"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-has-child-query.html
---

# Has child query [query-dsl-has-child-query]


Returns parent documents whose [joined](/reference/elasticsearch/mapping-reference/parent-join.md) child documents match a provided query. You can create parent-child relationships between documents in the same index using a [join](/reference/elasticsearch/mapping-reference/parent-join.md) field mapping.

::::{warning}
Because it performs a join, the `has_child` is slow compared to other queries. Its performance degrades as the number of matching child documents pointing to unique parent documents increases. Each `has_child` query in a search can increase query time significantly.

If you care about query performance, do not use this query. If you need to use the `has_child` query, use it as rarely as possible.

::::


## Example request [has-child-query-ex-request]

### Index setup [has-child-index-setup]

To use the `has_child` query, your index must include a [join](/reference/elasticsearch/mapping-reference/parent-join.md) field mapping. For example:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "my-join-field": {
        "type": "join",
        "relations": {
          "parent": "child"
        }
      }
    }
  }
}
```
% TESTSETUP


### Example query [has-child-query-ex-query]

```console
GET /_search
{
  "query": {
    "has_child": {
      "type": "child",
      "query": {
        "match_all": {}
      },
      "max_children": 10,
      "min_children": 2,
      "score_mode": "min"
    }
  }
}
```



## Top-level parameters for `has_child` [has-child-top-level-params]

`type`
:   (Required, string) Name of the child relationship mapped for the [join](/reference/elasticsearch/mapping-reference/parent-join.md) field.

`query`
:   (Required, query object) Query you wish to run on child documents of the `type` field. If a child document matches the search, the query returns the parent document.

`ignore_unmapped`
:   (Optional, Boolean) Indicates whether to ignore an unmapped `type` and not return any documents instead of an error. Defaults to `false`.

If `false`, {{es}} returns an error if the `type` is unmapped.

You can use this parameter to query multiple indices that may not contain the `type`.


`max_children`
:   (Optional, integer) Maximum number of child documents that match the `query` allowed for a returned parent document. If the parent document exceeds this limit, it is excluded from the search results.

`min_children`
:   (Optional, integer) Minimum number of child documents that match the `query` required to match the query for a returned parent document. If the parent document does not meet this limit, it is excluded from the search results.

`score_mode`
:   (Optional, string) Indicates how scores for matching child documents affect the root parent document’s [relevance score](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores). Valid values are:

`none` (Default)
:   Do not use the relevance scores of matching child documents. The query assigns parent documents a score of `0`.

`avg`
:   Use the mean relevance score of all matching child documents.

`max`
:   Uses the highest relevance score of all matching child documents.

`min`
:   Uses the lowest relevance score of all matching child documents.

`sum`
:   Add together the relevance scores of all matching child documents.



## Notes [has-child-query-notes]

### Sorting [has-child-query-performance]

You cannot sort the results of a `has_child` query using standard [sort options](/reference/elasticsearch/rest-apis/sort-search-results.md).

If you need to sort returned documents by a field in their child documents, use a `function_score` query and sort by `_score`. For example, the following query sorts returned documents by the `click_count` field of their child documents.

```console
GET /_search
{
  "query": {
    "has_child": {
      "type": "child",
      "query": {
        "function_score": {
          "script_score": {
            "script": "_score * doc['click_count'].value"
          }
        }
      },
      "score_mode": "max"
    }
  }
}
```



