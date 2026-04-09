---
applies_to:
  deployment:
    ess:
    self:
---

# Enrich settings [enrich_settings]

You can configure these enrich settings in the `elasticsearch.yml` file. For more information, see [Set up an enrich processor](docs-content://manage-data/ingest/transform-enrich/set-up-an-enrich-processor.md).

`enrich.cache_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of searches to cache for enriching documents. Defaults to 1000. There is a single cache for all enrich processors in the cluster. This setting determines the size of that cache.

`enrich.coordinator_proxy.max_concurrent_requests` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of concurrent multi-search requests to run when enriching documents. Defaults to 8.

`enrich.coordinator_proxy.max_lookups_per_request` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of searches to include in a multi-search request when enriching documents. Defaults to 128.

`enrich.coordinator_proxy.queue_capacity` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   coordinator queue capacity, defaults to max_concurrent_requests * max_lookups_per_request