---
navigation_title: "Reverse nested"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-reverse-nested-aggregation.html
---

# Reverse nested aggregation [search-aggregations-bucket-reverse-nested-aggregation]


A special single bucket aggregation that enables aggregating on parent docs from nested documents. Effectively this aggregation can break out of the nested block structure and link to other nested structures or the root document, which allows nesting other aggregations that aren’t part of the nested object in a nested aggregation.

The `reverse_nested` aggregation must be defined inside a `nested` aggregation.

* `path` - Which defines to what nested object field should be joined back. The default is empty, which means that it joins back to the root / main document level. The path cannot contain a reference to a nested object field that falls outside the `nested` aggregation’s nested structure a `reverse_nested` is in.

For example, lets say we have an index for a ticket system with issues and comments. The comments are inlined into the issue documents as nested documents. The mapping could look like:

$$$reversed-nested-aggregation-example$$$

```console
PUT /issues
{
  "mappings": {
    "properties": {
      "tags": { "type": "keyword" },
      "comments": {                            <1>
        "type": "nested",
        "properties": {
          "username": { "type": "keyword" },
          "comment": { "type": "text" }
        }
      }
    }
  }
}
```

1. The `comments` is an array that holds nested documents under the `issue` object.


The following aggregations will return the top commenters' username that have commented and per top commenter the top tags of the issues the user has commented on:

```console
GET /issues/_search
{
  "query": {
    "match_all": {}
  },
  "aggs": {
    "comments": {
      "nested": {
        "path": "comments"
      },
      "aggs": {
        "top_usernames": {
          "terms": {
            "field": "comments.username"
          },
          "aggs": {
            "comment_to_issue": {
              "reverse_nested": {}, <1>
              "aggs": {
                "top_tags_per_comment": {
                  "terms": {
                    "field": "tags"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

As you can see above, the `reverse_nested` aggregation is put in to a `nested` aggregation as this is the only place in the dsl where the `reverse_nested` aggregation can be used. Its sole purpose is to join back to a parent doc higher up in the nested structure.

1. A `reverse_nested` aggregation that joins back to the root / main document level, because no `path` has been defined. Via the `path` option the `reverse_nested` aggregation can join back to a different level, if multiple layered nested object types have been defined in the mapping


Possible response snippet:

```console-result
{
  "aggregations": {
    "comments": {
      "doc_count": 1,
      "top_usernames": {
        "doc_count_error_upper_bound" : 0,
        "sum_other_doc_count" : 0,
        "buckets": [
          {
            "key": "username_1",
            "doc_count": 1,
            "comment_to_issue": {
              "doc_count": 1,
              "top_tags_per_comment": {
                "doc_count_error_upper_bound" : 0,
                "sum_other_doc_count" : 0,
                "buckets": [
                  {
                    "key": "tag_1",
                    "doc_count": 1
                  }
                  ...
                ]
              }
            }
          }
          ...
        ]
      }
    }
  }
}
```

