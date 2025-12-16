---
navigation_title: "CEF"
mapped_pages:
- https://www.elastic.co/guide/en/elasticsearch/reference/current/cef-processor.html
---

# CEF processor [cef-processor]


Extracts fields from Common Event Format (CEF) message document.

$$$cef-options$$$

| Name                  | Required | Default | Description                                                                                                                                                                                     |
|-----------------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `field`               | yes      | -       | The field to be parsed.                                                                                                                                                                         |
| `target_field`        | no       | `field` | The field that the parsed structured object will be written into. Any existing content in this field will be overwritten.                                                                       |
| `ignore_missing`      | no       | `false` | If `true` and `field` does not exist or is `null`, the processor quietly exits without modifying the document                                                                                   |
| `ignore_empty_values` | no       | `true`  | If `true` then keys with empty values are quietly ignored in the document                                                                                                                       |
| `timezone`            | no       | UTC     | The default [timezone](#cef-processor-timezones) used by the processor. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `description`         | no       | -       | Description of the processor. Useful for describing the purpose of the processor or its configuration.                                                                                          |
| `if`                  | no       | -       | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor).                   |
| `ignore_failure`      | no       | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures).                         |
| `on_failure`          | no       | -       | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures).                         |
| `tag`                 | no       | -       | Identifier for the processor. Useful for debugging and metrics.                                                                                                                                 |

## Timezones [cef-processor-timezones]

The `timezone` option may have two effects on the behavior of the processor:
- If the string being parsed matches a format representing a local date-time, such as `yyyy-MM-dd HH:mm:ss`, it will be assumed to be in the timezone specified by this option. This is not applicable if the string matches a format representing a zoned date-time, such as `yyyy-MM-dd HH:mm:ss zzz`: in that case, the timezone parsed from the string will be used. It is also not applicable if the string matches an absolute time format, such as `epoch_millis`.
- The date-time will be converted into the timezone given by this option before it is formatted and written into the target field. This is not applicable if the `output_format` is an absolute time format such as `epoch_millis`.

::::{warning}
We recommend avoiding the use of short abbreviations for timezone names, since they can be ambiguous. For example, one JDK might interpret `PST` as `America/Tijuana`, i.e. Pacific (Standard) Time, while another JDK might interpret it as `Asia/Manila`, i.e. Philippine Standard Time. If your input data contains such abbreviations, you should convert them into either standard full names or UTC offsets before parsing them, using your own knowledge of what each abbreviation means in your data.
::::

```js
{
  "cef": {
    "field": "message",
    "target_fields": "my_cef",
  }
}
```
% NOTCONSOLE
