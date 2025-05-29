---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html
---

# _meta field [mapping-meta-field]

A mapping type can have custom meta data associated with it. These are not used at all by Elasticsearch, but can be used to store application-specific metadata, such as the class that a document belongs to:

```console
PUT my-index-000001
{
  "mappings": {
    "_meta": { <1>
      "class": "MyApp::User",
      "version": {
        "min": "1.0",
        "max": "1.3"
      }
    }
  }
}
```

1. This `_meta` info can be retrieved with the [GET mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping) API.


The `_meta` field can be updated on an existing type using the [update mapping](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) API:

```console
PUT my-index-000001/_mapping
{
  "_meta": {
    "class": "MyApp2::User3",
    "version": {
      "min": "1.3",
      "max": "1.5"
    }
  }
}
```
% TEST[continued]

