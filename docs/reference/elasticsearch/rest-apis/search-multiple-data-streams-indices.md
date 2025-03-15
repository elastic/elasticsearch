---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multiple-indices.html
applies_to:
  stack: all
---

# Search multiple data streams and indices [search-multiple-indices]

There are two main methods for searching across multiple data streams and indices in {{es}}:

* **Query Level**: Directly specify indices in the search request path or use index patterns to target multiple indices.
* **Index level**: Use [index aliases](docs-content://manage-data/data-store/aliases.md), which act as pointers to one or more backing indices, enabling logical grouping and management of indices.

To search multiple data streams and indices, add them as comma-separated values in the [search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search)'s request path.

The following request searches the `my-index-000001` and `my-index-000002` indices.

```console
GET /my-index-000001,my-index-000002/_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```
% TEST[setup:my_index]
% TEST[s/^/PUT my-index-000002\n/]

You can also search multiple data streams and indices using an index pattern.

The following request targets the `my-index-*` index pattern. The request searches any data streams or indices in the cluster that start with `my-index-`.

```console
GET /my-index-*/_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```
% TEST[setup:my_index]

You can exclude specific indices from a search. The request will retrieve data from all indices starting with `my-index-`, except for `my-index-01`.

```console
GET /my-index-*/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "user.id": "kimchy"
          }
        }
      ],
      "must_not": [
        {
          "terms": {
            "_index": ["my-index-01"]
          }
        }
      ]
    }
  }
}
```
% TEST[setup:my_index]

To search all data streams and indices in a cluster, omit the target from the request path. Alternatively, you can use `_all` or `*`.

The following requests are equivalent and search all data streams and indices in the cluster.

```console
GET /_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}

GET /_all/_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}

GET /*/_search
{
  "query": {
    "match": {
      "user.id": "kimchy"
    }
  }
}
```
% TEST[setup:my_index]


## Index boost [index-boost]

When searching multiple indices, you can use the `indices_boost` parameter to boost results from one or more specified indices. This is useful when hits coming from some indices matter more than hits from other.

::::{note}
You cannot use `indices_boost` with data streams.
::::


```console
GET /_search
{
  "indices_boost": [
    { "my-index-000001": 1.4 },
    { "my-index-000002": 1.3 }
  ]
}
```
% TEST[s/^/PUT my-index-000001\nPUT my-index-000002\n/]

Aliases and index patterns can also be used:

```console
GET /_search
{
  "indices_boost": [
    { "my-alias":  1.4 },
    { "my-index*": 1.3 }
  ]
}
```
% TEST[s/^/PUT my-index-000001\nPUT my-index-000001/_alias/my-alias\n/]

If multiple matches are found, the first match will be used. For example, if an index is included in `alias1` and matches the `my-index*` pattern, a boost value of `1.4` is applied.

