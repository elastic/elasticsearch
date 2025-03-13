---
navigation_title: "Convert"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/convert-processor.html
---

# Convert processor [convert-processor]


Converts a field in the currently ingested document to a different type, such as converting a string to an integer. If the field value is an array, all members will be converted.

The supported types include: `integer`, `long`, `float`, `double`, `string`, `boolean`, `ip`, and `auto`.

Specifying `boolean` will set the field to true if its string value is equal to `true` (ignore case), to false if its string value is equal to `false` (ignore case), or it will throw an exception otherwise.

Specifying `ip` will set the target field to the value of `field` if it contains a valid IPv4 or IPv6 address that can be indexed into an [IP field type](/reference/elasticsearch/mapping-reference/ip.md).

Specifying `auto` will attempt to convert the string-valued `field` into the closest non-string, non-IP type. For example, a field whose value is `"true"` will be converted to its respective boolean type: `true`. Do note that float takes precedence of double in `auto`. A value of `"242.15"` will "automatically" be converted to `242.15` of type `float`. If a provided field cannot be appropriately converted, the processor will still process successfully and leave the field value as-is. In such a case, `target_field` will be updated with the unconverted field value.

$$$convert-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field whose value is to be converted |
| `target_field` | no | `field` | The field to assign the converted value to, by default `field` is updated in-place |
| `type` | yes | - | The type to convert the existing value to |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist or is `null`, the processor quietly exits without modifying the document |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

```js
PUT _ingest/pipeline/my-pipeline-id
{
  "description": "converts the content of the id field to an integer",
  "processors" : [
    {
      "convert" : {
        "field" : "id",
        "type": "integer"
      }
    }
  ]
}
```
% NOTCONSOLE

