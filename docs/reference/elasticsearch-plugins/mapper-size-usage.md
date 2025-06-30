---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-size-usage.html
---

# Using the _size field [mapper-size-usage]

In order to enable the `_size` field, set the mapping as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "_size": {
      "enabled": true
    }
  }
}
```

The value of the `_size` field is accessible in queries, aggregations, scripts, and when sorting. It can be retrieved using the [fields parameter](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#search-fields-param):

```console
# Example documents
PUT my-index-000001/_doc/1
{
  "text": "This is a document"
}

PUT my-index-000001/_doc/2
{
  "text": "This is another document"
}

GET my-index-000001/_search
{
  "query": {
    "range": {
      "_size": {                      <1>
        "gt": 10
      }
    }
  },
  "aggs": {
    "sizes": {
      "terms": {
        "field": "_size",             <2>
        "size": 10
      }
    }
  },
  "sort": [
    {
      "_size": {                      <3>
        "order": "desc"
      }
    }
  ],
  "fields": ["_size"],                <4>
  "script_fields": {
    "size": {
      "script": "doc['_size']"        <5>
    }
  }
}
```

1. Querying on the `_size` field
2. Aggregating on the `_size` field
3. Sorting on the `_size` field
4. Use the `fields` parameter to return the `_size` in the search response.
5. Uses a [script field](/reference/elasticsearch/rest-apis/retrieve-selected-fields.md#script-fields) to return the `_size` field in the search response.


::::{admonition} Using _size in {{kib}}
:class: note

To use the `_size` field in {{kib}}, update the `metaFields` setting and add `_size` to the list of meta fields. `metaFields` can be configured in {{kib}} from the Advanced Settings page in Management.

::::


