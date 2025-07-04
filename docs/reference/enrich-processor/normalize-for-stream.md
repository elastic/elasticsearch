---
navigation_title: "Normalize for Stream"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/normalize-for-stream-processor.html
---

# Normalize-for-Stream processor [normalize-for-stream-processor]


Detects whether a document is OpenTelemetry-compliant and if not -
normalizes it as described below. If used in combination with the OTel-related
mappings such as the ones defined in `logs-otel@template`, the resulting
document can be queried seamlessly by clients that expect either [ECS](https://www.elastic.co/guide/en/ecs/current/index.html) or OpenTelemetry-[Semantic-Conventions](https://github.com/open-telemetry/semantic-conventions) formats.

::::{note}
This processor is in tech preview and is not available in our serverless offering.
::::

## Detecting OpenTelemetry compliance

The processor detects OpenTelemetry compliance by checking the following fields:
* `resource` exists as a key and the value is a map
* `resource` either doesn't contain an `attributes` field, or contains an `attributes` field of type map
* `scope` is either missing or a map
* `attributes` is either missing or a map
* `body` is either missing or a map
* `body` either doesn't contain a `text` field, or contains a `text` field of type `String`
* `body` either doesn't contain a `structured` field, or contains a `structured` field that is not of type `String`

If all of these conditions are met, the document is considered OpenTelemetry-compliant and is not modified by the processor.

## Normalization

If the document is not OpenTelemetry-compliant, the processor normalizes it as follows:
* Specific ECS fields are renamed to have their corresponding OpenTelemetry Semantic Conventions attribute names. These include the following:

  | ECS Field   | Semantic Conventions Attribute |
  |-------------|--------------------------------|
  | `span.id`   | `span_id`                      |
  | `trace.id`  | `trace_id`                     |
  | `message`   | `body.text`                    |
  | `log.level` | `severity_text`                |

  The processor first looks for the nested form of the ECS field and if such does not exist, it looks for a top-level field with the dotted field name.
* Other specific ECS fields that describe resources and have corresponding counterparts in the OpenTelemetry Semantic Conventions are moved to the `resource.attribtues` map. Fields that are considered resource attributes are such that conform to the following conditions:
    * They are ECS fields that have corresponding counterparts (either with
      the same name or with a different name) in OpenTelemetry Semantic Conventions.
    * The corresponding OpenTelemetry attribute is defined in
      [Semantic Conventions](https://github.com/open-telemetry/semantic-conventions/tree/main/model)
      within a group that is defined as `type: enitity`.
* All other fields, except for `@timestamp`, are moved to the `attributes` map.
* All non-array entries of the `attributes` and `resource.attributes` maps are flattened. Flattening means that nested objects are merged into their parent object, and the keys are concatenated with a dot. See examples below.

## Examples

If an OpenTelemetry-compliant document is detected, the processor does nothing. For example, the following document will stay unchanged:

```json
{
  "resource": {
    "attributes": {
      "service.name": "my-service"
    }
  },
  "scope": {
    "name": "my-library",
    "version": "1.0.0"
  },
  "attributes": {
    "http.method": "GET"
  },
  "body": {
    "text": "Hello, world!"
  }
}
```

If a non-OpenTelemetry-compliant document is detected, the processor normalizes it. For example, the following document:

```json
{
  "@timestamp": "2023-10-01T12:00:00Z",
  "service": {
    "name": "my-service",
    "version": "1.0.0",
    "environment": "production",
    "language": {
      "name": "python",
      "version": "3.8"
    }
  },
  "log": {
    "level": "INFO"
  },
  "message": "Hello, world!",
  "http": {
    "method": "GET",
    "url": {
      "path": "/api/v1/resource"
    },
    "headers": [
      {
        "name": "Authorization",
        "value": "Bearer token"
      },
      {
        "name": "User-Agent",
        "value": "my-client/1.0"
      }
    ]
  },
  "span" : {
    "id": "1234567890abcdef"
  },
  "span.id": "abcdef1234567890",
  "trace.id": "abcdef1234567890abcdef1234567890"
}
```
will be normalized into the following form:

```json
{
  "@timestamp": "2023-10-01T12:00:00Z",
  "resource": {
    "attributes": {
      "service.name": "my-service",
      "service.version": "1.0.0",
      "service.environment": "production"
    }
  },
  "attributes": {
    "service.language.name": "python",
    "service.language.version": "3.8",
    "http.method": "GET",
    "http.url.path": "/api/v1/resource",
    "http.headers": [
      {
        "name": "Authorization",
        "value": "Bearer token"
      },
      {
        "name": "User-Agent",
        "value": "my-client/1.0"
      }
    ]
  },
  "body": {
    "text": "Hello, world!"
  },
  "span_id": "1234567890abcdef",
  "trace_id": "abcdef1234567890abcdef1234567890"
}
```
