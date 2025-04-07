---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/optimistic-concurrency-control.html
applies_to:
  stack: all
---

# Optimistic concurrency control [optimistic-concurrency-control]

Elasticsearch is distributed. When documents are created, updated, or deleted, the new version of the document has to be replicated to other nodes in the cluster. Elasticsearch is also asynchronous and concurrent, meaning that these replication requests are sent in parallel, and may arrive at their destination out of sequence. Elasticsearch needs a way of ensuring that an older version of a document never overwrites a newer version.

To ensure an older version of a document doesn’t overwrite a newer version, every operation performed to a document is assigned a sequence number by the primary shard that coordinates that change. The sequence number is increased with each operation and thus newer operations are guaranteed to have a higher sequence number than older operations. Elasticsearch can then use the sequence number of operations to make sure a newer document version is never overridden by a change that has a smaller sequence number assigned to it.

For example, the following indexing command will create a document and assign it an initial sequence number and primary term:

```console
PUT products/_doc/1567
{
  "product" : "r2d2",
  "details" : "A resourceful astromech droid"
}
```

You can see the assigned sequence number and primary term in the `_seq_no` and `_primary_term` fields of the response:

```console-result
{
  "_shards": {
    "total": 2,
    "failed": 0,
    "successful": 1
  },
  "_index": "products",
  "_id": "1567",
  "_version": 1,
  "_seq_no": 362,
  "_primary_term": 2,
  "result": "created"
}
```

Elasticsearch keeps tracks of the sequence number and primary term of the last operation to have changed each of the documents it stores. The sequence number and primary term are returned in the `_seq_no` and `_primary_term` fields in the response of the [GET API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get):

```console
GET products/_doc/1567
```

returns:

```console-result
{
  "_index": "products",
  "_id": "1567",
  "_version": 1,
  "_seq_no": 362,
  "_primary_term": 2,
  "found": true,
  "_source": {
    "product": "r2d2",
    "details": "A resourceful astromech droid"
  }
}
```

Note: The [Search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search) can return the `_seq_no` and `_primary_term` for each search hit by setting [`seq_no_primary_term` parameter](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search#request-body-search-seq-no-primary-term).

The sequence number and the primary term uniquely identify a change. By noting down the sequence number and primary term returned, you can make sure to only change the document if no other change was made to it since you retrieved it. This is done by setting the `if_seq_no` and `if_primary_term` parameters of the [index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create), [update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update), or [delete API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete).

For example, the following indexing call will make sure to add a tag to the document without losing any potential change to the description or an addition of another tag by another API:

```console
PUT products/_doc/1567?if_seq_no=362&if_primary_term=2
{
  "product": "r2d2",
  "details": "A resourceful astromech droid",
  "tags": [ "droid" ]
}
```

