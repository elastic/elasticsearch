---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-ingest.html
---

# Using ingest processors in Painless [painless-ingest]

Some [ingest processors](/reference/enrich-processor/index.md) expose behavior through Painless methods that can be called in Painless scripts that execute in ingest pipelines.

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

Use the [JSON processor](/reference/enrich-processor/json-processor.md) to convert JSON strings to structured JSON objects. The first `json` method accepts a map and a key. The processor converts the JSON string in the map as specified by the `key` parameter to structured JSON content. That content is added directly to the `map` object.

The second `json` method accepts a JSON string in the `value` parameter and returns a structured JSON object.

```painless
void json(Map<String, Object> map, String key);
Object json(Object value);
```

You can then add this object to the document through the context object:

```painless
Object json = Processors.json(ctx.inputJsonString);
ctx.structuredJson = json;
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



