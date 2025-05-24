---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-index-sorting.html
navigation_title: Sorting
---

# Index sorting settings [index-modules-index-sorting]

When creating a new index in Elasticsearch it is possible to configure how the segments inside each shard will be sorted. By default Lucene does not apply any sort. The `index.sort.*` settings define which fields should be used to sort the documents inside each Segment.

::::{warning}
It is allowed to apply index sorting to mappings with nested objects, so long as the `index.sort.*` setting contains no nested fields.
::::


For instance the following example shows how to define a sort on a single field:

```console
PUT my-index-000001
{
  "settings": {
    "index": {
      "sort.field": "date", <1>
      "sort.order": "desc"  <2>
    }
  },
  "mappings": {
    "properties": {
      "date": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by the `date` field
2. …  in descending order.


It is also possible to sort the index by more than one field:

```console
PUT my-index-000001
{
  "settings": {
    "index": {
      "sort.field": [ "username", "date" ], <1>
      "sort.order": [ "asc", "desc" ]       <2>
    }
  },
  "mappings": {
    "properties": {
      "username": {
        "type": "keyword",
        "doc_values": true
      },
      "date": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by `username` first then by `date`
2. …  in ascending order for the `username` field and in descending order for the `date` field.


Index sorting supports the following settings:

`index.sort.field`
:   The list of fields used to sort the index. Only `boolean`, `numeric`, `date` and `keyword` fields with `doc_values` are allowed here.

`index.sort.order`
:   The sort order to use for each field. The order option can have the following values:

    * `asc`:  For ascending order
    * `desc`: For descending order.


`index.sort.mode`
:   Elasticsearch supports sorting by multi-valued fields. The mode option controls what value is picked to sort the document. The mode option can have the following values:

    * `min`: 	Pick the lowest value.
    * `max`: 	Pick the highest value.


`index.sort.missing`
:   The missing parameter specifies how docs which are missing the field should be treated. The missing value can have the following values:

    * `_last`: Documents without value for the field are sorted last.
    * `_first`: Documents without value for the field are sorted first.


::::{warning}
Index sorting can be defined only once at index creation. It is not allowed to add or update a sort on an existing index. Index sorting also has a cost in terms of indexing throughput since documents must be sorted at flush and merge time. You should test the impact on your application before activating this feature.
::::



## Early termination of search request [early-terminate]

By default in Elasticsearch a search request must visit every document that matches a query to retrieve the top documents sorted by a specified sort. Though when the index sort and the search sort are the same it is possible to limit the number of documents that should be visited per segment to retrieve the N top ranked documents globally. For example, let’s say we have an index that contains events sorted by a timestamp field:

```console
PUT events
{
  "settings": {
    "index": {
      "sort.field": "timestamp",
      "sort.order": "desc" <1>
    }
  },
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by timestamp in descending order (most recent first)


You can search for the last 10 events with:

```console
GET /events/_search
{
  "size": 10,
  "sort": [
    { "timestamp": "desc" }
  ]
}
```
% TEST[continued]

Elasticsearch will detect that the top docs of each segment are already sorted in the index and will only compare the first N documents per segment. The rest of the documents matching the query are collected to count the total number of results and to build aggregations.

If you’re only looking for the last 10 events and have no interest in the total number of documents that match the query you can set `track_total_hits` to false:

```console
GET /events/_search
{
  "size": 10,
  "sort": [ <1>
      { "timestamp": "desc" }
  ],
  "track_total_hits": false
}
```
% TEST[continued]

1. The index sort will be used to rank the top documents and each segment will early terminate the collection after the first 10 matches.


This time, Elasticsearch will not try to count the number of documents and will be able to terminate the query as soon as N documents have been collected per segment.

```console-result
{
  "_shards": ...
   "hits" : {  <1>
      "max_score" : null,
      "hits" : []
  },
  "took": 20,
  "timed_out": false
}
```
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": "$body._shards",/]
% TESTRESPONSE[s/"took": 20,/"took": "$body.took",/]

1. The total number of hits matching the query is unknown because of early termination.


::::{note}
Aggregations will collect all documents that match the query regardless of the value of `track_total_hits`
::::



