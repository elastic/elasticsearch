---
applies_to:
stack: ga 9.3
---
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


## Examples [cef-processor-examples]

### Simple example [cef-processor-simple-example]

```js
{
  "cef": {
    "field": "message",
    "target_fields": "my_cef",
  }
}
```
% NOTCONSOLE

### Full Example [cef-processor-full-example]

Here is a cef processor config

```js
{
  "description" : "...",
  "processors" : [
    {
      "cef" : {
        "field" : "message",
        "target_field" : "my_cef",
      }
    }
  ]
}
```
% NOTCONSOLE
When the above processor executes the following message

```
CEF:0|Elastic|Vaporware|1.0.0-alpha|18|Web request|low|eventId=3457 requestMethod=POST slat=38.915 slong=-77.511 proto=TCP sourceServiceName=httpd requestContext=https://www.google.com src=1.2.3.4 spt=33876 dst=192.168.10.1 dpt=443 request=https://www.example.com/cart
```
% NOTCONSOLE

it produces the result

```json
{
  "my_cef": {
    "severity": "low",
    "name": "Web request",
    "device": {
      "product": "Vaporware",
      "event_class_id": 18,
      "vendor": "Elastic",
      "version": "1.0.0-alpha"
    },
    "version": 0
  },
  "observer": {
    "product": "Vaporware",
    "vendor": "Elastic",
    "version": "1.0.0-alpha"
  },
  "destination": {
    "port": 443,
    "ip": "192.168.10.1"
  },
  "http": {
    "request": {
      "referrer": "https://example.com",
      "method": "POST"
    }
  },
  "source": {
    "geo": {
      "location": {
        "lon": -77.511,
        "lat": 38.915
      }
    },
    "port": 33876,
    "service": {
      "name": "httpd"
    },
    "ip": "1.2.3.4"
  },
  "event": {
    "code": 18,
    "id": 3457
  },
  "url": {
    "original": "https://example.com"
  },
  "network": {
    "transport": "TCP"
  }
}
```
% NOTCONSOLE

### Example using `ignore_empty_values` [cef-processor-example-using-ignore-empty-values]

```js
{
  "cef": {
    "field": "message",
    "target_fields": "my_cef",
    "ignore_empty_values": false
  }
}
```
% NOTCONSOLE

The final document will have fields with empty values when the corresponding CEF key has no value.

```json
{
  "my_cef": {
    "severity": "low",
    "name": "Web request",
    "device": {
      "product": "Vaporware",
      "event_class_id": 18,
      "vendor": "Elastic",
      "version": "1.0.0-alpha"
    },
    "version": 0
  },
  "http": {
    "request": {
      "referrer": "",
      "method": "POST"
    }
  }
}
```

% NOTCONSOLE

### Exception scenarios [cef-processor-exception-scenarios]

If the CEF message is invalid according to the spec then an IllegalArgumentException is thrown by the processor.
Various scenarios include:
- CEF header does not start with "CEF:"
- Escaped pipe in extensions (moo=this\|has an escaped pipe)
- Equals symbol in message (moo=this =has = equals\= )
- Malformed escape sequences (moo='Foo-Bar/2018.1.7; =Email:user@example.com;)
- Tab character is not a separator in extensions (msg=Tab is not a separator\tsrc=127.0.0.1)
- When CEF header is truncated (CEF:0|SentinelOne|Mgmt|activityID=1111111111111111111)
- If there are invalid timestamps or mac addresses or ip addresses.s
