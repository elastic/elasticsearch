---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-ingest.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Using ingest processors in Painless [painless-ingest]

Painless scripts in [ingest pipelines](https://www.elastic.co/docs/manage-data/ingest/transform-enrich/ingest-pipelines) can access certain [ingest processor](https://www.elastic.co/docs/reference/scripting-languages/painless/painless-ingest-processor-context) functionality through the `Processors` namespace, enabling custom logic while leveraging {{es}} built-in transformations. Scripts execute within the `ctx` context to modify documents during ingestion.

Only a subset of ingest processors expose methods in the `Processors` namespace for use in Painless scripts. The following ingest processors expose methods in Painless:

* Bytes  
* Lowercase  
* Uppercase  
* Json 


## When to choose each approach


| Method | Use for | Pros  | Cons |
| :---- | :---- | :---- | :---- |
| [script processor](/reference/enrich-processor/script-processor.md) (Painless) | * complex logic <br>* conditional operations <br>* multi-field validation | * full control <br>* custom business logic <br>* cross-field operations | * performance overhead <br>* complexity |
| [ingest processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md) | * common transformations <br>* standard operations | * optimized performance <br>* built-in validation <br>* simple configuration | * limited logic <br>* single-field focus |
| [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md) | * query-time calculations <br>* schema flexibility | * no reindexing required * dynamic computation during queries | * query-time performance cost <br>* read-only operations <br>* not used in ingest pipeline.  |

**Performance considerations:** Script processors can impact pipeline performance. Prefer ingest processors for simple transformations.

## Method usage [_method_usage]

All ingest methods available in Painless are scoped to the `Processors` namespace. For example:

```console
POST /_ingest/pipeline/_simulate?verbose
{
  "pipeline": {
    "processors": [
      {
        "script": {
          "lang": "painless",
          "source": """
            long bytes = Processors.bytes(ctx.size);
            ctx.size_in_bytes = bytes;
          """
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "size": "1kb"
      }
    }
  ]
}
```


## Ingest methods reference [_ingest_methods_reference]

### Byte conversion [_byte_conversion]

Use the [bytes processor](/reference/enrich-processor/bytes-processor.md) to return the number of bytes in the human-readable byte value supplied in the `value` parameter.

```painless
long bytes(String value);
```


### Lowercase conversion [_lowercase_conversion]

Use the [lowercase processor](/reference/enrich-processor/lowercase-processor.md) to convert the supplied string in the `value` parameter to its lowercase equivalent.

```painless
String lowercase(String value);
```


### Uppercase conversion [_uppercase_conversion]

Use the [uppercase processor](/reference/enrich-processor/uppercase-processor.md) to convert the supplied string in the `value` parameter to its uppercase equivalent.

```painless
String uppercase(String value);
```


### JSON parsing [_json_parsing]

Use the [JSON processor](/reference/enrich-processor/json-processor.md) to parse a string containing JSON data into a structured object, string, or other value. There are two `json` methods:

```painless
void json(Map<String, Object> map, String key);
Object json(Object value);
```

The first `json` method accepts a map and a key. The processor parses the JSON string in the given map at the given key to a structured object. The entries in that object are added directly to the given map.

For example, if the input document looks like this:

```js
{
  "foo": {
    "inputJsonString": "{\"bar\": 999}"
  }
}
```
% NOTCONSOLE

then executing this script:

```painless
Processors.json(ctx.foo, 'inputJsonString');
```

will result in this document:

```js
{
  "foo": {
    "inputJsonString": "{\"bar\": 999}",
    "bar" : 999
  }
}
```
% NOTCONSOLE

The second `json` method accepts a JSON string in the `value` parameter and returns a structured object or other value.

You can then add this object to the document through the context object:

```painless
ctx.parsedJson = Processors.json(ctx.inputJsonString);
```


### URL decoding [_url_decoding]

Use the [URL decode processor](/reference/enrich-processor/urldecode-processor.md) to URL-decode the string supplied in the `value` parameter.

```painless
String urlDecode(String value);
```


### URI decomposition [_uri_decomposition]

Use the [URI parts processor](/reference/enrich-processor/uri-parts-processor.md) to decompose the URI string supplied in the `value` parameter. Returns a map of key-value pairs in which the key is the name of the URI component such as `domain` or `path` and the value is the corresponding value for that component.

```painless
String uriParts(String value);
```


### Network community ID [_network_community_id]

Use the [community ID processor](/reference/enrich-processor/community-id-processor.md) to compute the network community ID for network flow data.

```painless
String communityId(String sourceIpAddrString, String destIpAddrString, Object ianaNumber, Object transport, Object sourcePort, Object destinationPort, Object icmpType, Object icmpCode, int seed)
String communityId(String sourceIpAddrString, String destIpAddrString, Object ianaNumber, Object transport, Object sourcePort, Object destinationPort, Object icmpType, Object icmpCode)
```



