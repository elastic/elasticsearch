---
navigation_title: "Convert"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/convert-processor.html
---

# Convert processor [convert-processor]

Converts a field in the currently ingested document to a different type, such as converting a string to an integer. If the field value is an array, all members will be converted.

## Supported types

The supported types are: `integer`, `long`, `float`, `double`, `string`, `boolean`, `ip`, and `auto` (all case-insensitive).

| Target `type` | Supported input values                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `integer`     | `Integer` values<br><br>`Long` values in 32-bit signed integer range<br><br>`String` values representing an integer in 32-bit signed integer range in either decimal format (without a decimal point) or hex format (e.g. `"123"` or `"0x7b"`)                                                                                                                                                                                                                                                                                |
| `long`        | `Integer` values<br><br>`Long` values<br><br>`String` values representing an integer in 64-bit signed integer range in either decimal format (without a decimal point) or hex format (e.g. `"123"` or `"0x7b"`)                                                                                                                                                                                                                                                                                                               |
| `float`       | `Integer` values (may lose precision for absolute values greater than 2^24^)<br><br>`Long` values (may lose precision for absolute values greater than 2^24^)<br><br>`Float` values<br><br>`Double` values (may lose precision)<br><br>`String` values representing a floating point number in decimal, scientific, or hex format (e.g. `"123.0"`, `"123.45"`, `"1.23e2"`, or `"0x1.ecp6"`) or an integer (may lose precision, and will give positive or negative infinity if out of range for a 32-bit floating point value) |
| `double`      | `Integer` values<br><br>`Long` values (may lose precision for absolute values greater than 2^53^)<br><br>`Float` values<br><br>`Double` values<br><br>`String` values representing a floating point number in decimal, scientific, or hex format (e.g. `"123.0"`, `"123.45"`, `"1.23e2"`, or `"0x1.ecp6"`) or an integer (may lose precision, and will give positive or negative infinity if out of range for a 64-bit floating point value)                                                                                  |
| `string`      | All values                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `boolean`     | `Boolean` values<br><br>`String` values matching `"true"` or `"false"` (case-insensitive)                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `ip`          | `String` values containing a valid IPv4 or IPv6 address that can be indexed into an [IP field type](/reference/elasticsearch/mapping-reference/ip.md)                                                                                                                                                                                                                                                                                                                                                                         |
| `auto`        | All values (see below)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

Specifying `auto` will attempt to convert a string-valued `field` into the closest non-string, non-IP type:
 - A string whose value is `"true"` or `"false"` (case insensitive) will be converted to a `Boolean`.
 - A string representing an integer in decimal or hex format (e.g. `"123"` or `"0x7b"`) will be converted to an `Integer` if the number fits in a 32-bit signed integer, else to a `Long` if it fits in a 64-bit signed integer, else to a `Float` (in which case it may
lose precision, and will give positive or negative infinity if out of range for a 32-bit floating point value).
 - A string representing a floating point number in decimal, scientific, or hex format (e.g. `"123.0"`, `"123.45"`, `"1.23e2"`, or `"0x1.ecp6"`) will be converted to a `Float` (and may lose precision, and will give positive or negative infinity if out of range for a 32-bit floating point value).

Using `auto` to convert a `field` which is either not a `String` or a `String` which cannot be converted will leave the
field value as-is. In such a case, `target_field` will be updated with the unconverted field value.

:::{tip}
 If conversions other than those provided by this processor are required, the
[`script`](/reference/enrich-processor/script-processor.md) processor may be used to implement the desired behavior.

The performance of the `script` processor should be as good or better than the `convert` processor.
:::


## Options
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

