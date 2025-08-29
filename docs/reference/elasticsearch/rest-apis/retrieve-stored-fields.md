---
navigation_title: "Retrieve stored fields"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
applies_to:
  stack: all
---

# Retrieve stored fields using the Get document API [get-stored-fields]

Use the `stored_fields` query parameter in a [Get document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get) API request to retrieve fields marked as stored (`"store": true`) in the index mapping.

Fields not marked as stored are excluded from the response, even if specified in the request.

::::{tip}
In most cases, the [`fields`](retrieve-selected-fields.md#search-fields-param) and [`_source`](retrieve-selected-fields.md#source-filtering) parameters produce better results than `stored_fields`.
::::

For example, these PUT requests define a stored field in the mapping and add a document:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "counter": {
        "type": "integer",
        "store": false
      },
      "tags": {
        "type": "keyword",
        "store": true
      }
    }
  }
}
```

```console
PUT my-index-000001/_doc/1
{
  "counter": 1,
  "tags": [ "production" ]
}
```

% TEST[continued]

This request retrieves the stored fields from the document:

```console
GET my-index-000001/_doc/1?stored_fields=tags,counter
```

% TEST[continued]

The API returns the following response:

```console-result
{
  "_index": "my-index-000001",
  "_id": "1",
  "_version": 1,
  "_seq_no": 22,
  "_primary_term": 1,
  "found": true,
  "fields": {
    "tags": [
      "production"
    ]
  }
}
```

% TESTRESPONSE[s/"_seq_no" : \d+/"_seq_no" : $body._seq_no/ s/"_primary_term" : 1/"_primary_term" : $body._primary_term/]

Although the `counter` field is specified in the request, it's not included in the response because it's not actually a stored field.

Field values are returned as an array.

::::{note}
Only leaf fields can be retrieved with the `stored_fields` parameter. If you specify an object field instead, an error is returned.
::::

