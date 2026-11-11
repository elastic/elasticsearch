---
applies_to:
  deployment:
    ess:
    self:
---

# Ingest settings [ingest_settings]

You can configure these ingest settings in the `elasticsearch.yml` file. For more information, see [Ingest pipelines](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md).

Ingest pipelines are stored in the cluster state, which is held in heap on every node and serialized on every cluster state update. The following node settings bound how much data pipelines can contribute to the cluster state, to keep an unbounded or oversized set of pipelines from destabilizing the cluster. They are safety limits intended only to reject abusive input, not to constrain legitimate use, and can all be updated dynamically.

::::{note}
These limits also apply on {{serverless-full}}, but the settings are not self-configurable there. If a project needs a higher limit, request an increase through support.
::::

`ingest.pipeline.max_pipelines` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of ingest pipelines that may exist at once. Defaults to `10000`. This limit is only enforced when creating a new pipeline, so existing pipelines above the limit continue to work.

`ingest.pipeline.max_pipeline_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum serialized size of a single ingest pipeline, bounding the total contribution of one pipeline (including its description, processors, and any other fields) to the cluster state. Defaults to `1mb`. This limit is enforced when creating or updating a pipeline.

    The same value also acts as a coarser pre-filter on the request itself: a put-pipeline request whose raw body is larger than this is rejected before it is parsed, so that an oversized definition is never expanded into memory. The raw request body and the stored pipeline are measured differently, since the punctuation and whitespace of the request body count toward the former but not the latter. A request can therefore be turned away by the pre-filter even though the pipeline it describes would have fit within the limit. If that happens, send the same pipeline more compactly (for example, without pretty-printing) or raise this setting.

`ingest.pipeline.max_total_metadata_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum combined serialized size of all ingest pipelines. Per-pipeline and per-count limits do not bound the aggregate, so many pipelines each just under the per-pipeline limit could otherwise accumulate enough data in the cluster state to destabilize the cluster. Defaults to `25mb`. This limit is only enforced against changes that grow the combined size, so existing pipelines above the limit continue to work, and you can still replace one with a definition no larger than the one it supersedes. To bring a cluster back under the limit, delete pipelines you no longer need or replace large ones with smaller definitions.
