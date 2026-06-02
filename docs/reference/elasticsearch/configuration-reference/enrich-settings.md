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
:   Maximum size of the cache that caches searches for enriching documents. The size can be specified in three units: the raw number of cached searches (for example, `1000`), an absolute size in bytes (for example, `100Mb`), or a percentage of the max heap space of the node (for example, `1%`). Both for the absolute byte size and the percentage of heap space, {{es}} does not guarantee that the enrich cache size will adhere exactly to that maximum, as {{es}} uses the byte size of the serialized search response which is a good representation of the used space on the heap, but not an exact match. Defaults to `1%`. There is a single cache for all enrich processors in the cluster.

`enrich.coordinator_proxy.max_concurrent_requests` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of concurrent multi-search requests to run when enriching documents. Defaults to `8`.

`enrich.coordinator_proxy.max_lookups_per_request` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of searches to include in a multi-search request when enriching documents. Defaults to `128`.

`enrich.coordinator_proxy.queue_capacity` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Coordinator queue capacity. Defaults to `max_concurrent_requests * max_lookups_per_request`.

The enrich policy executor supports the following node settings:

`enrich.fetch_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum batch size when reindexing a source index into an enrich index. Defaults to `10000`.

`enrich.max_force_merge_attempts` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of force merge attempts allowed on an enrich index. Defaults to `3`.

`enrich.cleanup_period` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   How often {{es}} checks whether unused enrich indices can be deleted. Defaults to `15m`.

`enrich.max_concurrent_policy_executions` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of enrich policies to execute concurrently. Defaults to `50`.