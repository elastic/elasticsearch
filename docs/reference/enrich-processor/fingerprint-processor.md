---
navigation_title: "Fingerprint"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/fingerprint-processor.html
---

# Fingerprint processor [fingerprint-processor]


Computes a hash of the document’s content. You can use this hash for [content fingerprinting](https://en.wikipedia.org/wiki/Fingerprint_(computing)).

$$$fingerprint-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `fields` | yes | n/a | Array of fields to include inthe fingerprint. For objects, the processor hashes both the field key andvalue. For other fields, the processor hashes only the field value. |
| `target_field` | no | `fingerprint` | Output field for the fingerprint. |
| `salt` | no | <none> | [Salt value](https://en.wikipedia.org/wiki/Salt_(cryptography)) for the hash function. |
| `method` | no | `SHA-1` | The hash method used tocompute the fingerprint. Must be one of `MD5`, `SHA-1`, `SHA-256`, `SHA-512`, or`MurmurHash3`. |
| `ignore_missing` | no | `false` | If `true`, the processorignores any missing `fields`. If all fields are missing, the processor silentlyexits without modifying the document. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |


## Example [fingerprint-processor-ex]

The following example illustrates the use of the fingerprint processor:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "fingerprint": {
          "fields": ["user"]
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "user": {
          "last_name": "Smith",
          "first_name": "John",
          "date_of_birth": "1980-01-15",
          "is_active": true
        }
      }
    }
  ]
}
```

Which produces the following result:

```console-result
{
  "docs": [
    {
      "doc": {
        ...
        "_source": {
          "fingerprint" : "WbSUPW4zY1PBPehh2AA/sSxiRjw=",
          "user" : {
            "last_name" : "Smith",
            "first_name" : "John",
            "date_of_birth" : "1980-01-15",
            "is_active" : true
          }
        }
      }
    }
  ]
}
```

