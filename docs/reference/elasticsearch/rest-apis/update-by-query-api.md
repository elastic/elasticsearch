---
navigation_title: "Update By Query API"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/docs-update.html#update-api-example
applies_to:
  stack: all
---

# Update by query API examples

This page provides examples of how to use the [Update by query API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query).

You can learn how to:

- [Run a basic `_update_by_query` request](#basic-usage)
- Use custom batch sizes or limits
- Modify document source fields with a script
- Use ingest pipelines
- Monitor or cancel long-running update operations
- Throttle update operations
- Use slicing to parallelize updates
- Apply updates after mapping changes

## Basic usage

The simplest usage of `_update_by_query` just performs an update on every document in the data stream or index without changing the source. This is useful to [pick up a new property](#pick-up-a-new-property) or some other online mapping change. 

To update selected documents, specify a query in the request body:

```console
POST my-index-000001/_update_by_query?conflicts=proceed
{
  "query": { <1>
    "term": {
      "user.id": "kimchy"
    }
  }
}
```
% TEST[setup:my_index]

1. The query must be passed as a value to the `query` key, in the same way as the [Search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search). You can also use the `q` parameter in the same way as the search API.

### Target multiple indices

Update documents in multiple data streams or indices:

```console
POST my-index-000001,my-index-000002/_update_by_query
```
% TEST[s/^/PUT my-index-000001\nPUT my-index-000002\n/]

### Filter by routing

Limit the update by query operation to shards that a particular routing value:

```console
POST my-index-000001/_update_by_query?routing=1
```
% TEST[setup:my_index]

### Change batch size

By default update by query uses scroll batches of 1000. You can change the batch size with the `scroll_size` parameter:

```console
POST my-index-000001/_update_by_query?scroll_size=100
```
% TEST[setup:my_index]

## Update the document

Update a document using a unique attribute:

```console
POST my-index-000001/_update_by_query
{
  "query": {
    "term": {
      "user.id": "kimchy"
    }
  },
  "max_docs": 1
}
```
% TEST[setup:my_index]

### Update the document source

Update by query supports scripts to update the document source. For example, the following request increments the `count` field for all documents with a `user.id` of `kimchy` in `my-index-000001`:

<!--
```console
PUT my-index-000001/_create/1
{
  "user": {
    "id": "kimchy"
  },
  "count": 1
}
```
-->

```console
POST my-index-000001/_update_by_query
{
  "script": {
    "source": "ctx._source.count++",
    "lang": "painless"
  },
  "query": {
    "term": {
      "user.id": "kimchy"
    }
  }
}
```
% TEST[continued]

Note that `conflicts=proceed` is not specified in this example. In this case, a version conflict should halt the process so you can handle the failure.

As with the [Update API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update), you can set `ctx.op` to change the operation that is performed:

`noop`
:   Set `ctx.op = "noop"` if your script decides that it doesn't have to make any changes.
The update by query operation skips updating the document and increments the  `noop` counter.

`delete`
:   Set `ctx.op = "delete"` if your script decides that the document should be deleted.
The update by query operation deletes the document and increments the  `deleted` counter.

Update by query only supports `index`, `noop`, and `delete`. Setting `ctx.op` to anything else is an error. Setting any other field in `ctx` is an error.
This API only enables you to modify the source of matching documents, you cannot move them.

### Update documents using an ingest pipeline

Update by query can use the [ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md) feature by specifying a `pipeline`:

```console
PUT _ingest/pipeline/set-foo
{
  "description" : "sets foo",
  "processors" : [ {
      "set" : {
        "field": "foo",
        "value": "bar"
      }
  } ]
}
POST my-index-000001/_update_by_query?pipeline=set-foo
```
% TEST[setup:my_index]

### Get the status of update by query operations

You can fetch the status of all running update by query requests with the [Task API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks):

```console
GET _tasks?detailed=true&actions=*byquery
```
% TEST[skip:No tasks to retrieve]

The responses looks like:

```console-result
{
  "nodes" : {
    "r1A2WoRbTwKZ516z6NEs5A" : {
      "name" : "r1A2WoR",
      "transport_address" : "127.0.0.1:9300",
      "host" : "127.0.0.1",
      "ip" : "127.0.0.1:9300",
      "attributes" : {
        "testattr" : "test",
        "portsfile" : "true"
      },
      "tasks" : {
        "r1A2WoRbTwKZ516z6NEs5A:36619" : {
          "node" : "r1A2WoRbTwKZ516z6NEs5A",
          "id" : 36619,
          "type" : "transport",
          "action" : "indices:data/write/update/byquery",
          "status" : {    <1>
            "total" : 6154,
            "updated" : 3500,
            "created" : 0,
            "deleted" : 0,
            "batches" : 4,
            "version_conflicts" : 0,
            "noops" : 0,
            "retries": {
              "bulk": 0,
              "search": 0
            },
            "throttled_millis": 0
          },
          "description" : ""
        }
      }
    }
  }
}
```

1. This object contains the actual status. It is just like the response JSON with the important addition of the `total` field. `total` is the total number of operations that the reindex expects to perform. You can estimate the progress by adding the `updated`, `created`, and `deleted` fields. The request will finish when their sum is equal to the `total` field.

With the task id you can look up the task directly. The following example retrieves information about task `r1A2WoRbTwKZ516z6NEs5A:36619`:

```console
GET /_tasks/r1A2WoRbTwKZ516z6NEs5A:36619
```
% TEST[catch:missing]

The advantage of this API is that it integrates with `wait_for_completion=false` to transparently return the status of completed tasks. If the task is completed and `wait_for_completion=false` was set on it, then it'll come back with a `results` or an `error` field. The cost of this feature is the document that `wait_for_completion=false` creates at `.tasks/task/${taskId}`. It is up to you to delete that document.

### Cancel an update by query operation

Any update by query can be cancelled using the [Cancel API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks):

```console
POST _tasks/r1A2WoRbTwKZ516z6NEs5A:36619/_cancel
```

The task ID can be found using the [Task API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks).

Cancellation should happen quickly but might take a few seconds. The task status API above will continue to list the update by query task until this task checks that it has been cancelled and terminates itself.

## Change throttling for a request

The value of `requests_per_second` can be changed on a running update by query using the `_rethrottle` API:

```console
POST _update_by_query/r1A2WoRbTwKZ516z6NEs5A:36619/_rethrottle?requests_per_second=-1
```

The task ID can be found using the [Task API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks).

Just like when setting it on the `_update_by_query` API, `requests_per_second` can be either `-1` to disable throttling or any decimal number like `1.7` or `12` to throttle to that level. Rethrottling that speeds up the query takes effect immediately, but rethrotting that slows down the query will take effect after completing the current batch. This prevents scroll timeouts.

## Slice manually

Slice an update by query manually by providing a slice id and total number of slices to each request:

```console
POST my-index-000001/_update_by_query
{
  "slice": {
    "id": 0,
    "max": 2
  },
  "script": {
    "source": "ctx._source['extra'] = 'test'"
  }
}
POST my-index-000001/_update_by_query
{
  "slice": {
    "id": 1,
    "max": 2
  },
  "script": {
    "source": "ctx._source['extra'] = 'test'"
  }
}
```
% TEST[setup:my_index_big]

Which you can verify works with:

```console
GET _refresh
POST my-index-000001/_search?size=0&q=extra:test&filter_path=hits.total
```
% TEST[continued]

Which results in a sensible `total` like this one:

```console-result
{
  "hits": {
    "total": {
        "value": 120,
        "relation": "eq"
    }
  }
}
```

## Use automatic slicing

You can also let update by query automatically parallelize using [slice-scroll](paginate-search-results.md#slice-scroll) to slice on `_id`. Use `slices` to specify the number of slices to use:

```console
POST my-index-000001/_update_by_query?refresh&slices=5
{
  "script": {
    "source": "ctx._source['extra'] = 'test'"
  }
}
```
% TEST[setup:my_index_big]

Which you also can verify works with:

```console
POST my-index-000001/_search?size=0&q=extra:test&filter_path=hits.total
```
% TEST[continued]

Which results in a sensible `total` like this one:

```console-result
{
  "hits": {
    "total": {
        "value": 120,
        "relation": "eq"
    }
  }
}
```

Setting `slices` to `auto` will let Elasticsearch choose the number of slices to use. This setting will use one slice per shard, up to a certain limit. If there are multiple source data streams or indices, it will choose the number of slices based on the index or backing index with the smallest number of shards.

Adding `slices` to `_update_by_query` just automates the manual process used in the section above, creating sub-requests which means it has some quirks:

- You can see these requests in the [Tasks APIs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query). These sub-requests are "child" tasks of the task for the request with `slices`.
- Fetching the status of the task for the request with `slices` only contains the status of completed slices.
- These sub-requests are individually addressable for things like cancellation and rethrottling.
- Rethrottling the request with `slices` will rethrottle the unfinished sub-request proportionally.
- Canceling the request with `slices` will cancel each sub-request.
- Due to the nature of `slices` each sub-request won't get a perfectly even portion of the documents. All documents will be addressed, but some slices may be larger than others. Expect larger slices to have a more even distribution.
- Parameters like `requests_per_second` and `max_docs` on a request with `slices` are distributed proportionally to each sub-request. Combine that with the point above about distribution being uneven and you should conclude that using `max_docs` with `slices` might not result in exactly `max_docs` documents being updated.
- Each sub-request gets a slightly different snapshot of the source data stream or index though these are all taken at approximately the same time.

## Pick up a new property

Say you created an index without dynamic mapping, filled it with data, and then added a mapping value to pick up more fields from the data:

```console
PUT test
{
  "mappings": {
    "dynamic": false,   <1>
    "properties": {
      "text": {"type": "text"}
    }
  }
}

POST test/_doc?refresh
{
  "text": "words words",
  "flag": "bar"
}
POST test/_doc?refresh
{
  "text": "words words",
  "flag": "foo"
}
PUT test/_mapping   <2>
{
  "properties": {
    "text": {"type": "text"},
    "flag": {"type": "text", "analyzer": "keyword"}
  }
}
```

1. This means that new fields won't be indexed, just stored in `_source`.

2. This updates the mapping to add the new `flag` field. To pick up the new field you have to reindex all documents with it.

Searching for the data won't find anything:

```console
POST test/_search?filter_path=hits.total
{
  "query": {
    "match": {
      "flag": "foo"
    }
  }
}
```
% TEST[continued]

```console-result
{
  "hits" : {
    "total": {
        "value": 0,
        "relation": "eq"
    }
  }
}
```

But you can issue an `_update_by_query` request to pick up the new mapping:

```console
POST test/_update_by_query?refresh&conflicts=proceed
POST test/_search?filter_path=hits.total
{
  "query": {
    "match": {
      "flag": "foo"
    }
  }
}
```
% TEST[continued]

```console-result
{
  "hits" : {
    "total": {
        "value": 1,
        "relation": "eq"
    }
  }
}
```

You can do the exact same thing when adding a field to a multifield.


