---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-mapping-ignore-above.html
---

# index.mapping.ignore_above [index-mapping-ignore-above]

The `ignore_above` setting, typically used at the field level, can also be applied at the index level using `index.mapping.ignore_above`. This setting lets you define a maximum string length for all applicable fields across the index, including `keyword`, `wildcard`, and keyword values in `flattened` fields. Any values that exceed this limit will be ignored during indexing and won’t be stored.

This index-wide setting ensures a consistent approach to managing excessively long values. It works the same as the field-level setting—if a string’s length goes over the specified limit, that string won’t be indexed or stored. When dealing with arrays, each element is evaluated separately, and only the elements that exceed the limit are ignored.

```console
PUT my-index-000001
{
  "settings": {
    "index.mapping.ignore_above": 256
  }
}
```

In this example, all applicable fields in `my-index-000001` will ignore any strings longer than 256 characters.

::::{tip}
You can override this index-wide setting for specific fields by specifying a custom `ignore_above` value in the field mapping.
::::


::::{note}
Just like the field-level `ignore_above`, this setting only affects indexing and storage. The original values are still available in the `_source` field if `_source` is enabled, which is the default behavior in Elasticsearch.
::::


