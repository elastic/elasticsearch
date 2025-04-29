---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-prefixes.html
---

# index_prefixes [index-prefixes]

The `index_prefixes` parameter enables the indexing of term prefixes to speed up prefix searches. It accepts the following optional settings:

`min_chars`
:   The minimum prefix length to index. Must be greater than 0, and defaults to 2. The value is inclusive.

`max_chars`
:   The maximum prefix length to index. Must be less than 20, and defaults to 5. The value is inclusive.

This example creates a text field using the default prefix length settings:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "body_text": {
        "type": "text",
        "index_prefixes": { }    <1>
      }
    }
  }
}
```

1. An empty settings object will use the default `min_chars` and `max_chars` settings


This example uses custom prefix length settings:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "full_name": {
        "type": "text",
        "index_prefixes": {
          "min_chars" : 1,
          "max_chars" : 10
        }
      }
    }
  }
}
```

`index_prefixes` parameter instructs {{ES}} to create a subfield "._index_prefix". This field will be used to do fast prefix queries. When doing highlighting, add "._index_prefix" subfield to the `matched_fields` parameter to highlight the main field based on the found matches of the prefix field, like in the request below:

```console
GET my-index-000001/_search
{
  "query": {
    "prefix": {
      "full_name": {
        "value": "ki"
      }
    }
  },
  "highlight": {
    "fields": {
      "full_name": {
        "matched_fields": ["full_name._index_prefix"]
      }
    }
  }
}
```
% TEST[continued]

