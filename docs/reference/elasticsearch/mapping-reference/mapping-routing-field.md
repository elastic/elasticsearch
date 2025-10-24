---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-routing-field.html
---

# _routing field [mapping-routing-field]

A document is routed to a particular shard in an index using the following formulas:

```
routing_factor = num_routing_shards / num_primary_shards
shard_num = (hash(_routing) % num_routing_shards) / routing_factor
```
`num_routing_shards` is the value of the [`index.number_of_routing_shards`](/reference/elasticsearch/index-settings/index-modules.md#index-number-of-routing-shards) index setting. `num_primary_shards` is the value of the [`index.number_of_shards`](/reference/elasticsearch/index-settings/index-modules.md#index-number-of-shards) index setting.

The default `_routing` value is the document’s [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md). Custom routing patterns can be implemented by specifying a custom `routing` value per document. For instance:

```console
PUT my-index-000001/_doc/1?routing=user1&refresh=true <1>
{
  "title": "This is a document"
}

GET my-index-000001/_doc/1?routing=user1 <2>
```
% TESTSETUP

1. This document uses `user1` as its routing value, instead of its ID.
2. The same `routing` value needs to be provided when [getting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get), [deleting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete), or [updating](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) the document.


The value of the `_routing` field is accessible in queries:

```console
GET my-index-000001/_search
{
  "query": {
    "terms": {
      "_routing": [ "user1" ] <1>
    }
  }
}
```

1. Querying on the `_routing` field (also see the [`ids` query](/reference/query-languages/query-dsl/query-dsl-ids-query.md))


::::{note}
Data streams do not support custom routing unless they were created with the [`allow_custom_routing`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-index-template) setting enabled in the template.
::::


## Searching with custom routing [_searching_with_custom_routing]

Custom routing can reduce the impact of searches. Instead of having to fan out a search request to all the shards in an index, the request can be sent to just the shard that matches the specific routing value (or values):

```console
GET my-index-000001/_search?routing=user1,user2 <1>
{
  "query": {
    "match": {
      "title": "document"
    }
  }
}
```

1. This search request will only be executed on the shards associated with the `user1` and `user2` routing values.



## Making a routing value required [_making_a_routing_value_required]

When using custom routing, it is important to provide the routing value whenever [indexing](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create), [getting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get), [deleting](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete), or [updating](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update) a document.

Forgetting the routing value can lead to a document being indexed on more than one shard. As a safeguard, the `_routing` field can be configured to make a custom `routing` value required for all CRUD operations:

```console
PUT my-index-000002
{
  "mappings": {
    "_routing": {
      "required": true <1>
    }
  }
}

PUT my-index-000002/_doc/1 <2>
{
  "text": "No routing value provided"
}
```
% TEST[catch:bad_request]

1. Routing is required for all documents.
2. This index request throws a `routing_missing_exception`.



## Unique IDs with custom routing [_unique_ids_with_custom_routing]

When indexing documents specifying a custom `_routing`, the uniqueness of the `_id` is not guaranteed across all of the shards in the index. In fact, documents with the same `_id` might end up on different shards if indexed with different `_routing` values.

It is up to the user to ensure that IDs are unique across the index.


## Routing to an index partition [routing-index-partition]

An index can be configured such that custom routing values will go to a subset of the shards rather than a single shard. This helps mitigate the risk of ending up with an imbalanced cluster while still reducing the impact of searches.

This is done by providing the index level setting [`index.routing_partition_size`](/reference/elasticsearch/index-settings/index-modules.md#routing-partition-size) at index creation. As the partition size increases, the more evenly distributed the data will become at the expense of having to search more shards per request.

When this setting is present, the formulas for calculating the shard become:

```
routing_value = hash(_routing) + hash(_id) % routing_partition_size
shard_num = (routing_value % num_routing_shards) / routing_factor
```
That is, the `_routing` field is used to calculate a set of shards within the index and then the `_id` is used to pick a shard within that set.

To enable this feature, the `index.routing_partition_size` should have a value greater than 1 and less than `index.number_of_shards`.

Once enabled, the partitioned index will have the following limitations:

* Mappings with [`join` field](/reference/elasticsearch/mapping-reference/parent-join.md) relationships cannot be created within it.
* All mappings within the index must have the `_routing` field marked as required.


