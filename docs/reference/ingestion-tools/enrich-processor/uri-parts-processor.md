---
navigation_title: "URI parts"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/uri-parts-processor.html
---

# URI parts processor [uri-parts-processor]


Parses a Uniform Resource Identifier (URI) string and extracts its components as an object. This URI object includes properties for the URI’s domain, path, fragment, port, query, scheme, user info, username, and password.

$$$uri-parts-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | Field containing the URI string. |
| `target_field` | no | `url` | Output field for the URI object. |
| `keep_original` | no | true | If `true`, the processor copies theunparsed URI to `<target_field>.original`. |
| `remove_if_successful` | no | false | If `true`, the processor removesthe `field` after parsing the URI string. If parsing fails, the processor does notremove the `field`. |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

Here is an example definition of the URI parts processor:

```js
{
  "description" : "...",
  "processors" : [
    {
      "uri_parts": {
        "field": "input_field",
        "target_field": "url",
        "keep_original": true,
        "remove_if_successful": false
      }
    }
  ]
}
```
% NOTCONSOLE

When the above processor executes on the following document:

```js
{
  "_source": {
    "input_field": "http://myusername:mypassword@www.example.com:80/foo.gif?key1=val1&key2=val2#fragment"
  }
}
```
% NOTCONSOLE

It produces this result:

```js
"_source" : {
  "input_field" : "http://myusername:mypassword@www.example.com:80/foo.gif?key1=val1&key2=val2#fragment",
  "url" : {
    "path" : "/foo.gif",
    "fragment" : "fragment",
    "extension" : "gif",
    "password" : "mypassword",
    "original" : "http://myusername:mypassword@www.example.com:80/foo.gif?key1=val1&key2=val2#fragment",
    "scheme" : "http",
    "port" : 80,
    "user_info" : "myusername:mypassword",
    "domain" : "www.example.com",
    "query" : "key1=val1&key2=val2",
    "username" : "myusername"
  }
}
```
% NOTCONSOLE

