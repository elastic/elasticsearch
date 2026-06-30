---
applies_to:
  deployment:
    ess:
    self:
---

# Enrich settings [enrich_settings]

You can configure these enrich settings in the `elasticsearch.yml` file. For more information, see [Set up an enrich processor](docs-content://manage-data/ingest/transform-enrich/set-up-an-enrich-processor.md).

The enrich coordinator supports the following node settings:

`enrich.cache_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum size of the cache that stores the results of searches used to enrich documents. You can specify the size in three units:

    * A raw number of cached searches, for example `1000`.
    * An absolute size in bytes, for example `100Mb`.
    * A percentage of the node's maximum heap space, for example `1%`.

    If you specify an absolute byte size or a heap percentage, {{es}} doesn't guarantee that the cache stays exactly within the configured maximum. The cache size is measured using the byte size of the serialized search response, which is a good approximation of actual heap usage but not an exact match. There is a single cache for all enrich processors in the cluster. Defaults to `1%`.

`enrich.coordinator_proxy.max_concurrent_requests` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of concurrent multi-search requests to run when enriching documents. Defaults to `8`.

`enrich.coordinator_proxy.max_lookups_per_request` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of searches to include in a multi-search request when enriching documents. Defaults to `128`.

`enrich.coordinator_proxy.queue_capacity` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of enrichment lookups the coordinator can queue while waiting to run them. When the queue is full, {{es}} rejects new enrichment requests with an HTTP 429 error. Defaults to `max_concurrent_requests * max_lookups_per_request`.

The enrich policy executor supports the following node settings:

`enrich.fetch_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum batch size when reindexing a source index into an enrich index. Defaults to `10000`.

`enrich.max_force_merge_attempts` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of force merge attempts allowed on an enrich index. Defaults to `3`.

`enrich.cleanup_period` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   How often {{es}} checks whether unused enrich indices can be deleted. Defaults to `15m`.

`enrich.max_concurrent_policy_executions` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of enrich policies to run concurrently. Defaults to `50`.