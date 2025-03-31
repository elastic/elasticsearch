---
navigation_title: "Date"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/date-processor.html
---

# Date processor [date-processor]


Parses dates from fields, and then uses the date or timestamp as the timestamp for the document. By default, the date processor adds the parsed date as a new field called `@timestamp`. You can specify a different field by setting the `target_field` configuration parameter. Multiple date formats are supported as part of the same date processor definition. They will be used sequentially to attempt parsing the date field, in the same order they were defined as part of the processor definition.

$$$date-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to get the date from. |
| `target_field` | no | @timestamp | The field that will hold the parsed date. |
| `formats` | yes | - | An array of the expected date formats. Can be a [java time pattern](/reference/elasticsearch/mapping-reference/mapping-date-format.md) or one of the following formats: ISO8601, UNIX, UNIX_MS, or TAI64N. |
| `timezone` | no | UTC | The timezone to use when parsing the date. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `locale` | no | ENGLISH | The locale to use when parsing the date, relevant when parsing month names or week days. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `output_format` | no | `yyyy-MM-dd'T'HH:mm:ss.SSSXXX` | The format to use when writing the date to `target_field`. Must be a valid [java time pattern](/reference/elasticsearch/mapping-reference/mapping-date-format.md). |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

Here is an example that adds the parsed date to the `timestamp` field based on the `initial_date` field:

```js
{
  "description" : "...",
  "processors" : [
    {
      "date" : {
        "field" : "initial_date",
        "target_field" : "timestamp",
        "formats" : ["dd/MM/yyyy HH:mm:ss"],
        "timezone" : "Europe/Amsterdam"
      }
    }
  ]
}
```

The `timezone` and `locale` processor parameters are templated. This means that their values can be extracted from fields within documents. The example below shows how to extract the locale/timezone details from existing fields, `my_timezone` and `my_locale`, in the ingested document that contain the timezone and locale values.

```js
{
  "description" : "...",
  "processors" : [
    {
      "date" : {
        "field" : "initial_date",
        "target_field" : "timestamp",
        "formats" : ["ISO8601"],
        "timezone" : "{{{my_timezone}}}",
        "locale" : "{{{my_locale}}}"
      }
    }
  ]
}
```

