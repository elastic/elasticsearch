---
navigation_title: "Registered domain"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/registered-domain-processor.html
---

# Registered domain processor [registered-domain-processor]


Extracts the registered domain (also known as the effective top-level domain or eTLD), sub-domain, and top-level domain from a fully qualified domain name (FQDN). Uses the registered domains defined in the [Mozilla Public Suffix List](https://publicsuffix.org/).

$$$registered-domain-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes |  | Field containing the source FQDN. |
| `target_field` | no | `<empty string>` | Object field containingextracted domain components. If an `<empty string>`, the processor addscomponents to the document’s root. |
| `ignore_missing` | no | `true` | If `true` and any required fieldsare missing, the processor quietly exits without modifying the document. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |


## Examples [registered-domain-processor-ex]

The following example illustrates the use of the registered domain processor:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "registered_domain": {
          "field": "fqdn",
          "target_field": "url"
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "fqdn": "www.example.ac.uk"
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
          "fqdn": "www.example.ac.uk",
          "url": {
            "subdomain": "www",
            "registered_domain": "example.ac.uk",
            "top_level_domain": "ac.uk",
            "domain": "www.example.ac.uk"
          }
        }
      }
    }
  ]
}
```
% TESTRESPONSE[s/\.\.\./"_index":"_index","_id":"_id","_version":"-3","_ingest":{"timestamp":$body.docs.0.doc._ingest.timestamp},/]

