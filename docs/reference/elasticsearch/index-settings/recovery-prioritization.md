---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/recovery-prioritization.html
navigation_title: Index recovery prioritization
---

# Index recovery prioritization settings [recovery-prioritization]

Unallocated shards are recovered in order of priority, whenever possible. Indices are sorted into priority order as follows:

* the optional `index.priority` setting (higher before lower)
* the index creation date (higher before lower)
* the index name (higher before lower)

This means that, by default, newer indices will be recovered before older indices.

Use the per-index dynamically updatable `index.priority` setting to customise the index prioritization order. For instance:

```console
PUT index_1

PUT index_2

PUT index_3
{
  "settings": {
    "index.priority": 10
  }
}

PUT index_4
{
  "settings": {
    "index.priority": 5
  }
}
```

In the above example:

* `index_3` will be recovered first because it has the highest `index.priority`.
* `index_4` will be recovered next because it has the next highest priority.
* `index_2` will be recovered next because it was created more recently.
* `index_1` will be recovered last.

This setting accepts an integer, and can be updated on a live index with the [update index settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings):

```console
PUT index_4/_settings
{
  "index.priority": 1
}
```

