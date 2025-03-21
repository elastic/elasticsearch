---
navigation_title: "Children"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-children-aggregation.html
---

# Children aggregation [search-aggregations-bucket-children-aggregation]


A special single bucket aggregation that selects child documents that have the specified type, as defined in a [`join` field](/reference/elasticsearch/mapping-reference/parent-join.md).

This aggregation has a single option:

* `type` - The child type that should be selected.

For example, let’s say we have an index of questions and answers. The answer type has the following `join` field in the mapping:

$$$children-aggregation-example$$$

```console
PUT child_example
{
  "mappings": {
    "properties": {
      "join": {
        "type": "join",
        "relations": {
          "question": "answer"
        }
      }
    }
  }
}
```

The `question` document contain a tag field and the `answer` documents contain an owner field. With the `children` aggregation the tag buckets can be mapped to the owner buckets in a single request even though the two fields exist in two different kinds of documents.

An example of a question document:

```console
PUT child_example/_doc/1
{
  "join": {
    "name": "question"
  },
  "body": "<p>I have Windows 2003 server and i bought a new Windows 2008 server...",
  "title": "Whats the best way to file transfer my site from server to a newer one?",
  "tags": [
    "windows-server-2003",
    "windows-server-2008",
    "file-transfer"
  ]
}
```

Examples of `answer` documents:

```console
PUT child_example/_doc/2?routing=1
{
  "join": {
    "name": "answer",
    "parent": "1"
  },
  "owner": {
    "location": "Norfolk, United Kingdom",
    "display_name": "Sam",
    "id": 48
  },
  "body": "<p>Unfortunately you're pretty much limited to FTP...",
  "creation_date": "2009-05-04T13:45:37.030"
}

PUT child_example/_doc/3?routing=1&refresh
{
  "join": {
    "name": "answer",
    "parent": "1"
  },
  "owner": {
    "location": "Norfolk, United Kingdom",
    "display_name": "Troll",
    "id": 49
  },
  "body": "<p>Use Linux...",
  "creation_date": "2009-05-05T13:45:37.030"
}
```

The following request can be built that connects the two together:

```console
POST child_example/_search?size=0
{
  "aggs": {
    "top-tags": {
      "terms": {
        "field": "tags.keyword",
        "size": 10
      },
      "aggs": {
        "to-answers": {
          "children": {
            "type" : "answer" <1>
          },
          "aggs": {
            "top-names": {
              "terms": {
                "field": "owner.display_name.keyword",
                "size": 10
              }
            }
          }
        }
      }
    }
  }
}
```

1. The `type` points to type / mapping with the name `answer`.


The above example returns the top question tags and per tag the top answer owners.

Possible response:

```console-result
{
  "took": 25,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
      "value": 3,
      "relation": "eq"
    },
    "max_score": null,
    "hits": []
  },
  "aggregations": {
    "top-tags": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": "file-transfer",
          "doc_count": 1, <1>
          "to-answers": {
            "doc_count": 2, <2>
            "top-names": {
              "doc_count_error_upper_bound": 0,
              "sum_other_doc_count": 0,
              "buckets": [
                {
                  "key": "Sam",
                  "doc_count": 1
                },
                {
                  "key": "Troll",
                  "doc_count": 1
                }
              ]
            }
          }
        },
        {
          "key": "windows-server-2003",
          "doc_count": 1, <1>
          "to-answers": {
            "doc_count": 2, <2>
            "top-names": {
              "doc_count_error_upper_bound": 0,
              "sum_other_doc_count": 0,
              "buckets": [
                {
                  "key": "Sam",
                  "doc_count": 1
                },
                {
                  "key": "Troll",
                  "doc_count": 1
                }
              ]
            }
          }
        },
        {
          "key": "windows-server-2008",
          "doc_count": 1, <1>
          "to-answers": {
            "doc_count": 2, <2>
            "top-names": {
              "doc_count_error_upper_bound": 0,
              "sum_other_doc_count": 0,
              "buckets": [
                {
                  "key": "Sam",
                  "doc_count": 1
                },
                {
                  "key": "Troll",
                  "doc_count": 1
                }
              ]
            }
          }
        }
      ]
    }
  }
}
```

1. The number of question documents with the tag `file-transfer`, `windows-server-2003`, etc.
2. The number of answer documents that are related to question documents with the tag `file-transfer`, `windows-server-2003`, etc.


