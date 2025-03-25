---
navigation_title: "Join"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html
---

# Join field type [parent-join]


The `join` data type is a special field that creates parent/child relation within documents of the same index. The `relations` section defines a set of possible relations within the documents, each relation being a parent name and a child name.

::::{warning}
We don’t recommend using multiple levels of relations to replicate a relational model. Each level of relation adds an overhead at query time in terms of memory and computation. For better search performance, denormalize your data instead.
::::


A parent/child relation can be defined as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_id": {
        "type": "keyword"
      },
      "my_join_field": { <1>
        "type": "join",
        "relations": {
          "question": "answer" <2>
        }
      }
    }
  }
}
```

1. The name for the field
2. Defines a single relation where `question` is parent of `answer`.


To index a document with a join, the name of the relation and the optional parent of the document must be provided in the `source`. For instance the following example creates two `parent` documents in the `question` context:

```console
PUT my-index-000001/_doc/1?refresh
{
  "my_id": "1",
  "text": "This is a question",
  "my_join_field": {
    "name": "question" <1>
  }
}

PUT my-index-000001/_doc/2?refresh
{
  "my_id": "2",
  "text": "This is another question",
  "my_join_field": {
    "name": "question"
  }
}
```

1. This document is a `question` document.


When indexing parent documents, you can choose to specify just the name of the relation as a shortcut instead of encapsulating it in the normal object notation:

```console
PUT my-index-000001/_doc/1?refresh
{
  "my_id": "1",
  "text": "This is a question",
  "my_join_field": "question" <1>
}

PUT my-index-000001/_doc/2?refresh
{
  "my_id": "2",
  "text": "This is another question",
  "my_join_field": "question"
}
```

1. Simpler notation for a parent document just uses the relation name.


When indexing a child, the name of the relation as well as the parent id of the document must be added in the `_source`.

::::{warning}
It is required to index the lineage of a parent in the same shard so you must always route child documents using their greater parent id.
::::


For instance the following example shows how to index two `child` documents:

```console
PUT my-index-000001/_doc/3?routing=1&refresh <1>
{
  "my_id": "3",
  "text": "This is an answer",
  "my_join_field": {
    "name": "answer", <2>
    "parent": "1" <3>
  }
}

PUT my-index-000001/_doc/4?routing=1&refresh
{
  "my_id": "4",
  "text": "This is another answer",
  "my_join_field": {
    "name": "answer",
    "parent": "1"
  }
}
```

1. The routing value is mandatory because parent and child documents must be indexed on the same shard
2. `answer` is the name of the join for this document
3. The parent id of this child document


## Parent-join and performance [_parent_join_and_performance]

The join field shouldn’t be used like joins in a relation database. In Elasticsearch the key to good performance is to de-normalize your data into documents. Each join field, `has_child` or `has_parent` query adds a significant tax to your query performance. It can also trigger [global ordinals](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md) to be built.

The only case where the join field makes sense is if your data contains a one-to-many relationship where one entity significantly outnumbers the other entity. An example of such case is a use case with products and offers for these products. In the case that offers significantly outnumbers the number of products then it makes sense to model the product as parent document and the offer as child document.


## Parent-join restrictions [_parent_join_restrictions]

* Only one `join` field mapping is allowed per index.
* Parent and child documents must be indexed on the same shard. This means that the same `routing` value needs to be provided when [getting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get), [deleting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete), or [updating](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) a child document.
* An element can have multiple children but only one parent.
* It is possible to add a new relation to an existing `join` field.
* It is also possible to add a child to an existing element but only if the element is already a parent.


## Searching with parent-join [_searching_with_parent_join]

The parent-join creates one field to index the name of the relation within the document (`my_parent`, `my_child`, …​).

It also creates one field per parent/child relation. The name of this field is the name of the `join` field followed by `#` and the name of the parent in the relation. So for instance for the `my_parent` → [`my_child`, `another_child`] relation, the `join` field creates an additional field named `my_join_field#my_parent`.

This field contains the parent `_id` that the document links to if the document is a child (`my_child` or `another_child`) and the `_id` of document if it’s a parent (`my_parent`).

When searching an index that contains a `join` field, these two fields are always returned in the search response:

```console
GET my-index-000001/_search
{
  "query": {
    "match_all": {}
  },
  "sort": ["my_id"]
}
```

Will return:

```console-result
{
  ...,
  "hits": {
    "total": {
      "value": 4,
      "relation": "eq"
    },
    "max_score": null,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": null,
        "_source": {
          "my_id": "1",
          "text": "This is a question",
          "my_join_field": "question"         <1>
        },
        "sort": [
          "1"
        ]
      },
      {
        "_index": "my-index-000001",
        "_id": "2",
        "_score": null,
        "_source": {
          "my_id": "2",
          "text": "This is another question",
          "my_join_field": "question"          <2>
        },
        "sort": [
          "2"
        ]
      },
      {
        "_index": "my-index-000001",
        "_id": "3",
        "_score": null,
        "_routing": "1",
        "_source": {
          "my_id": "3",
          "text": "This is an answer",
          "my_join_field": {
            "name": "answer",                 <3>
            "parent": "1"                     <4>
          }
        },
        "sort": [
          "3"
        ]
      },
      {
        "_index": "my-index-000001",
        "_id": "4",
        "_score": null,
        "_routing": "1",
        "_source": {
          "my_id": "4",
          "text": "This is another answer",
          "my_join_field": {
            "name": "answer",
            "parent": "1"
          }
        },
        "sort": [
          "4"
        ]
      }
    ]
  }
}
```

1. This document belongs to the `question` join
2. This document belongs to the `question` join
3. This document belongs to the `answer` join
4. The linked parent id for the child document



## Parent-join queries and aggregations [_parent_join_queries_and_aggregations]

See the [`has_child`](/reference/query-languages/query-dsl/query-dsl-has-child-query.md) and [`has_parent`](/reference/query-languages/query-dsl/query-dsl-has-parent-query.md) queries, the [`children`](/reference/aggregations/search-aggregations-bucket-children-aggregation.md) aggregation, and [inner hits](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md#parent-child-inner-hits) for more information.

The value of the `join` field is accessible in aggregations and scripts, and may be queried with the [`parent_id` query](/reference/query-languages/query-dsl/query-dsl-parent-id-query.md):

```console
GET my-index-000001/_search
{
  "query": {
    "parent_id": { <1>
      "type": "answer",
      "id": "1"
    }
  },
  "aggs": {
    "parents": {
      "terms": {
        "field": "my_join_field#question", <2>
        "size": 10
      }
    }
  },
  "runtime_mappings": {
    "parent": {
      "type": "long",
      "script": """
        emit(Integer.parseInt(doc['my_join_field#question'].value)) <3>
      """
    }
  },
  "fields": [
    { "field": "parent" }
  ]
}
```

1. Querying the `parent id` field (also see the [`has_parent` query](/reference/query-languages/query-dsl/query-dsl-has-parent-query.md) and the [`has_child` query](/reference/query-languages/query-dsl/query-dsl-has-child-query.md))
2. Aggregating on the `parent id` field (also see the [`children`](/reference/aggregations/search-aggregations-bucket-children-aggregation.md) aggregation)
3. Accessing the `parent id` field in scripts.



## Global ordinals [_global_ordinals]

The `join` field uses [global ordinals](/reference/elasticsearch/mapping-reference/eager-global-ordinals.md) to speed up joins. Global ordinals need to be rebuilt after any change to a shard. The more parent id values are stored in a shard, the longer it takes to rebuild the global ordinals for the `join` field.

Global ordinals, by default, are built eagerly: if the index has changed, global ordinals for the `join` field will be rebuilt as part of the refresh. This can add significant time to the refresh. However most of the times this is the right trade-off, otherwise global ordinals are rebuilt when the first parent-join query or aggregation is used. This can introduce a significant latency spike for your users and usually this is worse as multiple global ordinals for the `join` field may be attempt rebuilt within a single refresh interval when many writes are occurring.

When the `join` field is used infrequently and writes occur frequently it may make sense to disable eager loading:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_join_field": {
        "type": "join",
        "relations": {
           "question": "answer"
        },
        "eager_global_ordinals": false
      }
    }
  }
}
```

The amount of heap used by global ordinals can be checked per parent relation as follows:

```console
# Per-index
GET _stats/fielddata?human&fields=my_join_field#question

# Per-node per-index
GET _nodes/stats/indices/fielddata?human&fields=my_join_field#question
```


## Multiple children per parent [_multiple_children_per_parent]

It is also possible to define multiple children for a single parent:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_join_field": {
        "type": "join",
        "relations": {
          "question": ["answer", "comment"]  <1>
        }
      }
    }
  }
}
```

1. `question` is parent of `answer` and `comment`.



## Multiple levels of parent join [_multiple_levels_of_parent_join]

::::{warning}
We don’t recommend using multiple levels of relations to replicate a relational model. Each level of relation adds an overhead at query time in terms of memory and computation. For better search performance, denormalize your data instead.
::::


Multiple levels of parent/child:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_join_field": {
        "type": "join",
        "relations": {
          "question": ["answer", "comment"],  <1>
          "answer": "vote" <2>
        }
      }
    }
  }
}
```

1. `question` is parent of `answer` and `comment`
2. `answer` is parent of `vote`


The mapping above represents the following tree:

```
   question
    /    \
   /      \
comment  answer
           |
           |
          vote
```
Indexing a grandchild document requires a `routing` value equals to the grand-parent (the greater parent of the lineage):

```console
PUT my-index-000001/_doc/3?routing=1&refresh <1>
{
  "text": "This is a vote",
  "my_join_field": {
    "name": "vote",
    "parent": "2" <2>
  }
}
```

1. This child document must be on the same shard than its grand-parent and parent
2. The parent id of this document (must points to an `answer` document)



