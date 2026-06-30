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

`enrich.max_policies` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum number of enrich policies that may exist at once. Enrich policies are stored in the cluster state, so an unbounded number of them can destabilize the cluster. Defaults to 1000. This limit is only enforced when creating a new policy, so existing policies above the limit continue to work. This setting can be updated dynamically.

`enrich.max_field_name_length` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum length, in characters, allowed for the `match_field` and for each entry of `enrich_fields` of an enrich policy. Defaults to 1024. This limit is only enforced when creating a new policy. This setting can be updated dynamically.

`enrich.max_policy_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum serialized size of a single enrich policy, bounding the total contribution of one policy (including its query, indices, and fields) to the cluster state. Defaults to `256kb`. A create-policy request whose body exceeds this size is also rejected before it is parsed. This limit is only enforced when creating a new policy. This setting can be updated dynamically.

`enrich.max_total_metadata_size` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Maximum combined serialized size of all enrich policies. Per-policy and per-count limits do not bound the aggregate, so many policies each just under the per-policy limit could otherwise accumulate enough data in the cluster state to destabilize the cluster. Defaults to `25mb`. This limit is only enforced when creating a new policy, so existing policies above the limit continue to work. This setting can be updated dynamically.