---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/enabled.html
---

# enabled [enabled]

Elasticsearch tries to index all of the fields you give it, but sometimes you want to just store the field without indexing it. For instance, imagine that you are using Elasticsearch as a web session store. You may want to index the session ID and last update time, but you don’t need to query or run aggregations on the session data itself.

The `enabled` setting, which can be applied only to the top-level mapping definition and to [`object`](/reference/elasticsearch/mapping-reference/object.md) fields, causes Elasticsearch to skip parsing of the contents of the field entirely. The JSON can still be retrieved from the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field, but it is not searchable or stored in any other way:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "user_id": {
        "type":  "keyword"
      },
      "last_updated": {
        "type": "date"
      },
      "session_data": { <1>
        "type": "object",
        "enabled": false
      }
    }
  }
}

PUT my-index-000001/_doc/session_1
{
  "user_id": "kimchy",
  "session_data": { <2>
    "arbitrary_object": {
      "some_array": [ "foo", "bar", { "baz": 2 } ]
    }
  },
  "last_updated": "2015-12-06T18:20:22"
}

PUT my-index-000001/_doc/session_2
{
  "user_id": "jpountz",
  "session_data": "none", <3>
  "last_updated": "2015-12-06T18:22:13"
}
```

1. The `session_data` field is disabled.
2. Any arbitrary data can be passed to the `session_data` field as it will be entirely ignored.
3. The `session_data` will also ignore values that are not JSON objects.


The entire mapping may be disabled as well, in which case the document is stored in the [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field, which means it can be retrieved, but none of its contents are indexed in any way:

```console
PUT my-index-000001
{
  "mappings": {
    "enabled": false <1>
  }
}

PUT my-index-000001/_doc/session_1
{
  "user_id": "kimchy",
  "session_data": {
    "arbitrary_object": {
      "some_array": [ "foo", "bar", { "baz": 2 } ]
    }
  },
  "last_updated": "2015-12-06T18:20:22"
}

GET my-index-000001/_doc/session_1 <2>

GET my-index-000001/_mapping <3>
```

1. The entire mapping is disabled.
2. The document can be retrieved.
3. Checking the mapping reveals that no fields have been added.


The `enabled` setting for existing fields and the top-level mapping definition cannot be updated.

Note that because Elasticsearch completely skips parsing the field contents, it is possible to add non-object data to a disabled field:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "session_data": {
        "type": "object",
        "enabled": false
      }
    }
  }
}

PUT my-index-000001/_doc/session_1
{
  "session_data": "foo bar" <1>
}
```

1. The document is added successfully, even though `session_data` contains non-object data.


