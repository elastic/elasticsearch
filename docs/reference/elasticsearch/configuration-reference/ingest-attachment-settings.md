---
applies_to:
  stack: ga 9.5
---

# Attachment settings [ingest-attachment-settings]

You can configure this attachment setting in the `elasticsearch.yml` file. For more information, see [Attachment processor](/reference/enrich-processor/attachment.md).

`ingest.attachment.max_field_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), [byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units) or percentage/ratio of the JVM heap) Maximum allowed size of the raw attachment source field on this node. Accepts an absolute size (for example, `10mb`, `1gb`) or a relative value as a percentage or ratio of the JVM heap (for example, `5%` or `0.05`). Defaults to `-1`, meaning no node-wide limit. Applies to all attachment processors on the node and supersedes the per-processor `max_field_bytes` option.
