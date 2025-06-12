---
navigation_title: "Reindex indices"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html
applies_to:
  stack: all
---

# Reindex indices examples

::::{admonition} New API reference
For the most up-to-date API details, refer to [Document APIs](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex).
::::

## Running reindex asynchronously [docs-reindex-task-api]

If the request contains `wait_for_completion=false`, {{es}} performs some preflight checks, launches the request, and returns a `task` you can use to cancel or get the status of the task. {{es}} creates a record of this task as a document at `_tasks/<task_id>`.

## Reindex from multiple sources [docs-reindex-from-multiple-sources]

If you have many sources to reindex it is generally better to reindex them one at a time rather than using a glob pattern to pick up multiple sources.
That way you can resume the process if there are any errors by removing the partially completed source and starting over.
It also makes parallelizing the process fairly simple: split the list of sources to reindex and run each list in parallel.

One-off bash scripts seem to work nicely for this:

```bash
for index in i1 i2 i3 i4 i5; do
  curl -HContent-Type:application/json -XPOST localhost:9200/_reindex?pretty -d'{
    "source": {
      "index": "'$index'"
    },
    "dest": {
      "index": "'$index'-reindexed"
    }
  }'
done
```

## Throttling [docs-reindex-throttle]

Set `requests_per_second` to any positive decimal number (for example, `1.4`, `6`, or `1000`) to throttle the rate at which the reindex API issues batches of index operations.
Requests are throttled by padding each batch with a wait time.
To disable throttling, set `requests_per_second` to `-1`.

The throttling is done by waiting between batches so that the `scroll` that the reindex API uses internally can be given a timeout that takes into account the padding. The padding time is the difference between the batch size divided by the `requests_per_second` and the time spent writing. By default the batch size is `1000`, so if `requests_per_second` is set to `500`:

```txt
target_time = 1000 / 500 per second = 2 seconds
wait_time = target_time - write_time = 2 seconds - .5 seconds = 1.5 seconds
```

Since the batch is issued as a single `_bulk` request, large batch sizes cause {{es}} to create many requests and then wait for a while before starting the next set. This is "bursty" instead of "smooth".

## Rethrottling [docs-reindex-rethrottle]

The value of `requests_per_second` can be changed on a running reindex using the `_rethrottle` API. For example:

```console
POST _reindex/r1A2WoRbTwKZ516z6NEs5A:36619/_rethrottle?requests_per_second=-1
```

The task ID can be found using the [task management APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks).

Just like when setting it on the Reindex API, `requests_per_second` can be either `-1` to disable throttling or any decimal number like `1.7` or `12` to throttle to that level.
Rethrottling that speeds up the query takes effect immediately, but rethrottling that slows down the query will take effect after completing the current batch.
This prevents scroll timeouts.

## Slicing [docs-reindex-slice]

Reindex supports [sliced scroll](paginate-search-results.md#slice-scroll) to parallelize the reindexing process.
This parallelization can improve efficiency and provide a convenient way to break the request down into smaller parts.

::::{note}
Reindexing from remote clusters does not support manual or automatic slicing.
::::

### Manual slicing [docs-reindex-manual-slice]

Slice a reindex request manually by providing a slice id and total number of slices to each request:

```console
POST _reindex
{
  "source": {
    "index": "my-index-000001",
    "slice": {
      "id": 0,
      "max": 2
    }
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
POST _reindex
{
  "source": {
    "index": "my-index-000001",
    "slice": {
      "id": 1,
      "max": 2
    }
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

You can verify this works by:

```console
GET _refresh
POST my-new-index-000001/_search?size=0&filter_path=hits.total
```

which results in a sensible `total` like this one:

```console-result
{
  "hits": {
    "total" : {
        "value": 120,
        "relation": "eq"
    }
  }
}
```

### Automatic slicing [docs-reindex-automatic-slice]

You can also let the reindex API automatically parallelize using [sliced scroll](paginate-search-results.md#slice-scroll) to slice on `_id`.
Use `slices` to specify the number of slices to use:

```console
POST _reindex?slices=5&refresh
{
  "source": {
    "index": "my-index-000001"
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

You can also verify this works by:

```console
POST my-new-index-000001/_search?size=0&filter_path=hits.total
```

which results in a sensible `total` like this one:

```console-result
{
  "hits": {
    "total" : {
        "value": 120,
        "relation": "eq"
    }
  }
}
```

Setting `slices` to `auto` will let {{es}} choose the number of slices to use.
This setting will use one slice per shard, up to a certain limit.
If there are multiple sources, it will choose the number of slices based on the index or backing index with the smallest number of shards.

Adding `slices` to the reindex API just automates the manual process used in the section above, creating sub-requests which means it has some quirks:

* You can see these requests in the [task management APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks). These sub-requests are "child" tasks of the task for the request with `slices`.
* Fetching the status of the task for the request with `slices` only contains the status of completed slices.
* These sub-requests are individually addressable for things like cancellation and rethrottling.
* Rethrottling the request with `slices` will rethrottle the unfinished sub-request proportionally.
* Canceling the request with `slices` will cancel each sub-request.
* Due to the nature of `slices` each sub-request won't get a perfectly even portion of the documents. All documents will be addressed, but some slices may be larger than others. Expect larger slices to have a more even distribution.
* Parameters like `requests_per_second` and `max_docs` on a request with `slices` are distributed proportionally to each sub-request. Combine that with the point above about distribution being uneven and you should conclude that using `max_docs` with `slices` might not result in exactly `max_docs` documents being reindexed.
* Each sub-request gets a slightly different snapshot of the source, though these are all taken at approximately the same time.

### Picking the number of slices [docs-reindex-picking-slices]

If slicing automatically, setting `slices` to `auto` will choose a reasonable number for most indices. If slicing manually or otherwise tuning automatic slicing, use these guidelines.

Query performance is most efficient when the number of `slices` is equal to the number of shards in the index. If that number is large (for example, 500), choose a lower number as too many `slices` will hurt performance. Setting `slices` higher than the number of shards generally does not improve efficiency and adds overhead.

Indexing performance scales linearly across available resources with the number of slices.

Whether query or indexing performance dominates the runtime depends on the documents being reindexed and cluster resources.

## Reindex routing [docs-reindex-routing]

By default if the reindex API sees a document with routing then the routing is preserved unless it's changed by the script. You can set `routing` on the `dest` request to change this.
For example:

`keep`
:   Sets the routing on the bulk request sent for each match to the routing on the match. This is the default value.

`discard`
:   Sets the routing on the bulk request sent for each match to `null`.

`=<some text>`
:   Sets the routing on the bulk request sent for each match to all text after the `=`.

You can use the following request to copy all documents from the `source` with the company name `cat` into the `dest`  with routing set to `cat`:

```console
POST _reindex
{
  "source": {
    "index": "source",
    "query": {
      "match": {
        "company": "cat"
      }
    }
  },
  "dest": {
    "index": "dest",
    "routing": "=cat"
  }
}
```

By default the reindex API uses scroll batches of 1000. You can change the batch size with the `size` field in the `source` element:

```console
POST _reindex
{
  "source": {
    "index": "source",
    "size": 100
  },
  "dest": {
    "index": "dest",
    "routing": "=cat"
  }
}
```

## Reindex with an ingest pipeline [reindex-with-an-ingest-pipeline]

Reindex can also use the [ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md) feature by specifying a `pipeline` like this:

```console
POST _reindex
{
  "source": {
    "index": "source"
  },
  "dest": {
    "index": "dest",
    "pipeline": "some_ingest_pipeline"
  }
}
```

## Reindex select documents with a query [docs-reindex-select-query]

You can limit the documents by adding a query to the `source`. For example, the following request only copies documents with a `user.id` of `kimchy` into `my-new-index-000001`:

```console
POST _reindex
{
  "source": {
    "index": "my-index-000001",
    "query": {
      "term": {
        "user.id": "kimchy"
      }
    }
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

### Reindex select documents with `max_docs` [docs-reindex-select-max-docs]

You can limit the number of processed documents by setting `max_docs`.
For example, this request copies a single document from `my-index-000001` to `my-new-index-000001`:

```console
POST _reindex
{
  "max_docs": 1,
  "source": {
    "index": "my-index-000001"
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

### Reindex from multiple sources [docs-reindex-multiple-sources]

The `index` attribute in `source` can be a list, allowing you to copy from lots of sources in one request.
This will copy documents from the `my-index-000001` and `my-index-000002` indices:

```console
POST _reindex
{
  "source": {
    "index": ["my-index-000001", "my-index-000002"]
  },
  "dest": {
    "index": "my-new-index-000002"
  }
}
```

::::{note}
The reindex API makes no effort to handle ID collisions so the last document written will "win" but the order isn't usually predictable so it is not a good idea to rely on this behavior. Instead, make sure that IDs are unique using a script.
::::

### Reindex select fields with a source filter [docs-reindex-filter-source]

You can use source filtering to reindex a subset of the fields in the original documents.
For example, the following request only reindexes the `user.id` and `_doc` fields of each document:

```console
POST _reindex
{
  "source": {
    "index": "my-index-000001",
    "_source": ["user.id", "_doc"]
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

### Reindex to change the name of a field [docs-reindex-change-name]

The reindex API can be used to build a copy of an index with renamed fields.
Say you create an index containing documents that look like this:

```console
POST my-index-000001/_doc/1?refresh
{
  "text": "words words",
  "flag": "foo"
}
```

If you don't like the name `flag` and want to replace it with `tag`, the reindex API can create the other index for you:

```console
POST _reindex
{
  "source": {
    "index": "my-index-000001"
  },
  "dest": {
    "index": "my-new-index-000001"
  },
  "script": {
    "source": "ctx._source.tag = ctx._source.remove(\"flag\")"
  }
}
```

Now you can get the new document:

```console
GET my-new-index-000001/_doc/1
```

...which will return:

```console-result
{
  "found": true,
  "_id": "1",
  "_index": "my-new-index-000001",
  "_version": 1,
  "_seq_no": 44,
  "_primary_term": 1,
  "_source": {
    "text": "words words",
    "tag": "foo"
  }
}
```

## Reindex daily indices [docs-reindex-daily-indices]

You can use the reindex API in combination with [Painless](/reference/scripting-languages/painless/painless.md) to reindex daily indices to apply a new template to the existing documents.

Assuming you have indices that contain documents like:

```console
PUT metricbeat-2016.05.30/_doc/1?refresh
{"system.cpu.idle.pct": 0.908}
PUT metricbeat-2016.05.31/_doc/1?refresh
{"system.cpu.idle.pct": 0.105}
```

The new template for the `metricbeat-*` indices is already loaded into {{es}}, but it applies only to the newly created indices. Painless can be used to reindex the existing documents and apply the new template.

The script below extracts the date from the index name and creates a new index with `-1` appended. All data from `metricbeat-2016.05.31` will be reindexed into `metricbeat-2016.05.31-1`.

```console
POST _reindex
{
  "source": {
    "index": "metricbeat-*"
  },
  "dest": {
    "index": "metricbeat"
  },
  "script": {
    "lang": "painless",
    "source": "ctx._index = 'metricbeat-' + (ctx._index.substring('metricbeat-'.length(), ctx._index.length())) + '-1'"
  }
}
```

All documents from the previous metricbeat indices can now be found in the `*-1` indices.

```console
GET metricbeat-2016.05.30-1/_doc/1
GET metricbeat-2016.05.31-1/_doc/1
```

The previous method can also be used in conjunction with [changing a field name](#docs-reindex-change-name) to load only the existing data into the new index and rename any fields if needed.

## Extract a random subset of the source [docs-reindex-api-subset]

The reindex API can be used to extract a random subset of the source for testing:

```console
POST _reindex
{
  "max_docs": 10,
  "source": {
    "index": "my-index-000001",
    "query": {
      "function_score" : {
        "random_score" : {},
        "min_score" : 0.9    <1>
      }
    }
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

1. You may need to adjust the `min_score` depending on the relative amount of data extracted from source.

## Modify documents during reindexing [reindex-scripts]

Like `_update_by_query`, the reindex API supports a script that modifies the document.
Unlike `_update_by_query`, the script is allowed to modify the document's metadata.
This example bumps the version of the source document:

```console
POST _reindex
{
  "source": {
    "index": "my-index-000001"
  },
  "dest": {
    "index": "my-new-index-000001",
    "version_type": "external"
  },
  "script": {
    "source": "if (ctx._source.foo == 'bar') {ctx._version++; ctx._source.remove('foo')}",
    "lang": "painless"
  }
}
```

Just as in `_update_by_query`, you can set `ctx.op` to change the operation that is run on the destination:

`noop`
:   Set `ctx.op = "noop"` if your script decides that the document doesn't have to be indexed in the destination. This no operation will be reported in the `noop` counter in the response body.

`delete`
:   Set `ctx.op = "delete"` if your script decides that the document must be deleted from the destination. The deletion will be reported in the `deleted` counter in the response body.

Setting `ctx.op` to anything else will return an error, as will setting any other field in `ctx`.

Think of the possibilities! Just be careful; you are able to change:

* `_id`
* `_index`
* `_version`
* `_routing`

Setting `_version` to `null` or clearing it from the `ctx` map is just like not sending the version in an indexing request; it will cause the document to be overwritten in the destination regardless of the version on the target or the version type you use in the reindex API request.

## Reindex from remote [reindex-from-remote]

Reindex supports reindexing from a remote {{es}} cluster:

```console
POST _reindex
{
  "source": {
    "remote": {
      "host": "http://otherhost:9200",
      "username": "user",
      "password": "pass"
    },
    "index": "my-index-000001",
    "query": {
      "match": {
        "test": "data"
      }
    }
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

The `host` parameter must contain a scheme, host, port (for example, `https://otherhost:9200`), and optional path (for example, `https://otherhost:9200/proxy`).
The `username` and `password` parameters are optional, and when they are present the reindex API will connect to the remote {{es}} node using basic auth.
Be sure to use `https` when using basic auth or the password will be sent in plain text. There are a range of settings available to configure the behaviour of the `https` connection.

When using {{ecloud}}, it is also possible to authenticate against the remote cluster through the use of a valid API key:

```console
POST _reindex
{
  "source": {
    "remote": {
      "host": "http://otherhost:9200",
      "headers": {
        "Authorization": "ApiKey API_KEY_VALUE"
      }
    },
    "index": "my-index-000001",
    "query": {
      "match": {
        "test": "data"
      }
    }
  },
  "dest": {
    "index": "my-new-index-000001"
  }
}
```

Remote hosts have to be explicitly allowed in `elasticsearch.yml` using the `reindex.remote.whitelist` property.
It can be set to a comma delimited list of allowed remote `host` and `port` combinations. 
Scheme is ignored, only the host and port are used. For example:

```yaml
reindex.remote.whitelist: [otherhost:9200, another:9200, 127.0.10.*:9200, localhost:*"]
```
The list of allowed hosts must be configured on any nodes that will coordinate the reindex.
This feature should work with remote clusters of any version of {{es}} you are likely to find. This should allow you to upgrade from any version of {{es}} to the current version by reindexing from a cluster of the old version.
::::{warning}
{{es}} does not support forward compatibility across major versions. For example, you cannot reindex from a 7.x cluster into a 6.x cluster.
::::
To enable queries sent to older versions of {{es}} the `query` parameter is sent directly to the remote host without validation or modification.

::::{note}
Reindexing from remote clusters does not support manual or automatic slicing.
::::

Reindexing from a remote server uses an on-heap buffer that defaults to a maximum size of 100mb.
If the remote index includes very large documents you'll need to use a smaller batch size. 
The example below sets the batch size to `10` which is very, very small.

```console
POST _reindex
{
  "source": {
    "remote": {
      "host": "http://otherhost:9200",
      ...
    },
    "index": "source",
    "size": 10,
    "query": {
      "match": {
        "test": "data"
      }
    }
  },
  "dest": {
    "index": "dest"
  }
}
```

It is also possible to set the socket read timeout on the remote connection with the `socket_timeout` field and the connection timeout with the `connect_timeout` field.
Both default to 30 seconds.
This example sets the socket read timeout to one minute and the connection timeout to 10 seconds:

```console
POST _reindex
{
  "source": {
    "remote": {
      "host": "http://otherhost:9200",
      ...,
      "socket_timeout": "1m",
      "connect_timeout": "10s"
    },
    "index": "source",
    "query": {
      "match": {
        "test": "data"
      }
    }
  },
  "dest": {
    "index": "dest"
  }
}
```

## Configuring SSL parameters [reindex-ssl]

Reindex from remote supports configurable SSL settings.
These must be specified in the `elasticsearch.yml` file, with the exception of the secure settings, which you add in the {{es}} keystore.
It is not possible to configure SSL in the body of the reindex API request.
Refer to [Reindex settings](/reference/elasticsearch/configuration-reference/index-management-settings.md#reindex-settings).