---
applies_to:
  deployment:
    ess:
    self:
---

# Ingest settings [ingest_settings]

You can configure these ingest settings in the `elasticsearch.yml` file. For more information, see [Ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md).

Ingest pipelines are stored in the cluster state, which is held in heap on every node and serialized on every cluster state update. The following node settings bound how much data pipelines can contribute to the cluster state, to keep an unbounded or oversized set of pipelines from destabilizing the cluster. They are safety limits intended only to reject abusive input, not to constrain legitimate use, and can all be updated dynamically.

`ingest.pipeline.max_pipelines` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of ingest pipelines that may exist at once. Defaults to `10000`. This limit is only enforced when creating a new pipeline, so existing pipelines above the limit continue to work.

`ingest.pipeline.max_pipeline_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum serialized size of a single ingest pipeline, bounding the total contribution of one pipeline (including its description, processors, and any other fields) to the cluster state. Defaults to `1mb`. A put-pipeline request whose body exceeds this size is also rejected before it is parsed. This limit is enforced when creating or updating a pipeline.

`ingest.pipeline.max_total_metadata_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum combined serialized size of all ingest pipelines. Per-pipeline and per-count limits do not bound the aggregate, so many pipelines each just under the per-pipeline limit could otherwise accumulate enough data in the cluster state to destabilize the cluster. Defaults to `50mb`. This limit is only enforced when creating a new pipeline, so existing pipelines above the limit continue to work.
