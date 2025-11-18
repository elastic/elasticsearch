---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-annotated-text-highlighter.html
---

# Using the annotated highlighter [mapper-annotated-text-highlighter]

The `annotated-text` plugin includes a custom highlighter designed to mark up search hits in a way which is respectful of the original markup:

```console
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "The cat sat on the [mat](sku3578)"
}

GET my-index-000001/_search
{
  "query": {
    "query_string": {
        "query": "cats"
    }
  },
  "highlight": {
    "fields": {
      "my_field": {
        "type": "annotated", <1>
        "require_field_match": false
      }
    }
  }
}
```

1. The `annotated` highlighter type is designed for use with annotated_text fields


The annotated highlighter is based on the `unified` highlighter and supports the same settings but does not use the `pre_tags` or `post_tags` parameters. Rather than using html-like markup such as `<em>cat</em>` the annotated highlighter uses the same markdown-like syntax used for annotations and injects a key=value annotation where `_hit_term` is the key and the matched search term is the value e.g.

```
The [cat](_hit_term=cat) sat on the [mat](sku3578)
```
The annotated highlighter tries to be respectful of any existing markup in the original text:

* If the search term matches exactly the location of an existing annotation then the `_hit_term` key is merged into the url-like syntax used in the `(...)` part of the existing annotation.
* However, if the search term overlaps the span of an existing annotation it would break the markup formatting so the original annotation is removed in favour of a new annotation with just the search hit information in the results.
* Any non-overlapping annotations in the original text are preserved in highlighter selections

