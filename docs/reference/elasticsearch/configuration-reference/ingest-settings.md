---
applies_to:
  deployment:
    ess:
    self:
---

# Ingest settings [ingest-settings]

You can configure these ingest settings in the `elasticsearch.yml` file.

`ingest.max_cumulative_field_value_size`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The maximum cumulative number of bytes that a single document's ingest pipeline(s) may write into that document's fields, across all processors and any nested pipelines. This limit exists because some processors (for example, `set`, `append`, `join`, and `gsub`) can copy or expand an already-large field value into other fields. Chaining many such processors together can produce a document that's dangerously large, even though no single processor call is large on its own. If a document's pipeline exceeds this limit, that document fails processing with an error; other documents in the same request are unaffected. Defaults to `50mb`.
