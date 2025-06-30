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
| `timezone` | no | UTC | The default [timezone](#date-processor-timezones) used by the processor. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `locale` | no | ENGLISH | The locale to use when parsing the date, relevant when parsing month names or week days. Supports [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). |
| `output_format` | no | `yyyy-MM-dd'T'HH:mm:ss.SSSXXX` | The format to use when writing the date to `target_field`. Must be a valid [java time pattern](/reference/elasticsearch/mapping-reference/mapping-date-format.md). |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

## Timezones [date-processor-timezones]

The `timezone` option may have two effects on the behavior of the processor:
 - If the string being parsed matches a format representing a local date-time, such as `yyyy-MM-dd HH:mm:ss`, it will be assumed to be in the timezone specified by this option. This is not applicable if the string matches a format representing a zoned date-time, such as `yyyy-MM-dd HH:mm:ss zzz`: in that case, the timezone parsed from the string will be used. It is also not applicable if the string matches an absolute time format, such as `epoch_millis`.
 - The date-time will be converted into the timezone given by this option before it is formatted and written into the target field. This is not applicable if the `output_format` is an absolute time format such as `epoch_millis`.

::::{warning}
We recommend avoiding the use of short abbreviations for timezone names, since they can be ambiguous. For example, one JDK might interpret `PST` as `America/Tijuana`, i.e. Pacific (Standard) Time, while another JDK might interpret it as `Asia/Manila`, i.e. Philippine Standard Time. If your input data contains such abbreviations, you should convert them into either standard full names or UTC offsets before parsing them, using your own knowledge of what each abbreviation means in your data. See [below](#date-processor-short-timezone-example) for an example. (This does not apply to `UTC`, which is safe.)
::::

## Examples [date-processor-examples]

### Simple example [date-processor-simple-example]

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

### Example using templated parameters [date-processor-templated-example]

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

### Example dealing with short timezone abbreviations safely [date-processor-short-timezone-example]

In the example below, the `message` field in the input is expected to be a string formed of a local date-time in `yyyyMMddHHmmss` format, a timezone abbreviated to one of `PST`, `CET`, or `JST` representing Pacific, Central European, or Japan time, and a payload. This field is split up using a `grok` processor, then the timezones are converted into full names using a `script` processor, then the date-time is parsed using a `date` processor, and finally the unwanted fields are discarded using a `remove` processor.

```js
{
  "description" : "...",
  "processors": [
    {
      "grok": {
        "field": "message",
        "patterns": ["%{DATESTAMP_EVENTLOG:local_date_time} %{TZ:short_tz} %{GREEDYDATA:payload}"],
        "pattern_definitions": {
          "TZ": "[A-Z]{3}"
        }
      }
    },
    {
      "script": {
        "source": "ctx['full_tz'] = params['tz_map'][ctx['short_tz']]",
        "params": {
          "tz_map": {
            "PST": "America/Los_Angeles",
            "CET": "Europe/Amsterdam",
            "JST": "Asia/Tokyo"
          }
        }
      }
    },
    {
      "date": {
        "field": "local_date_time",
        "formats": ["yyyyMMddHHmmss"],
        "timezone": "{{{full_tz}}}"
      }
    },
    {
      "remove": {
        "field": ["message", "local_date_time", "short_tz", "full_tz"]
      }
    }
  ]
}
```

With this pipeline, a `message` field with the value `20250102123456 PST Hello world` will result in a `@timestamp` field with the value `2025-01-02T12:34:56.000-08:00` and a `payload` field with the value `Hello world`. (Note: A `@timestamp` field will normally be mapped to a `date` type, and therefore it will be indexed as an integer representing milliseconds since the epoch, although the original format and timezone may be preserved in the `_source`.)