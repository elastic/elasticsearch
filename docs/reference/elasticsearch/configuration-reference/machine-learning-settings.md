---
navigation_title: "Machine learning settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# Machine learning settings in Elasticsearch [ml-settings]


$$$ml-settings-description$$$
You do not need to configure any settings to use {{ml}}. It is enabled by default.

::::{important}
{{ml-cap}} uses SSE4.2 instructions on x86_64 machines, so it works only on x86_64 machines whose CPUs [support](https://en.wikipedia.org/wiki/SSE4#Supporting_CPUs) SSE4.2. (This limitation does not apply to aarch64 machines.) If you run {{es}} on older x86_64 hardware, you must disable {{ml}} (by setting `xpack.ml.enabled` to `false`). In this situation you should not attempt to use {{ml}} functionality in your cluster at all.
::::


::::{tip}
To control memory usage used by {{ml}} jobs, you can use the [machine learning circuit breaker settings](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-model-inference).
::::



## General machine learning settings [general-ml-settings]

`node.roles: [ ml ]`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set `node.roles` to contain `ml` to identify the node as a *{{ml}} node*. If you want to run {{ml}} jobs, there must be at least one {{ml}} node in your cluster.

    If you set `node.roles`, you must explicitly specify all the required roles for the node. To learn more, refer to [Node settings](/reference/elasticsearch/configuration-reference/node-settings.md).

    ::::{important}
    * On dedicated coordinating nodes or dedicated master nodes, do not set the `ml` role.
    * It is strongly recommended that dedicated {{ml}} nodes also have the `remote_cluster_client` role; otherwise, {{ccs}} fails when used in {{ml}} jobs or {{dfeeds}}. See [Remote-eligible node](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#remote-node).

    ::::


`xpack.ml.enabled`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The default value (`true`) enables {{ml}} APIs on the node.

    ::::{important}
    If you want to use {{ml-features}} in your cluster, it is recommended that you use the default value for this setting on all nodes.
    ::::


    If set to `false`, the {{ml}} APIs are disabled on the node. For example, the node cannot open jobs, start {{dfeeds}}, receive transport (internal) communication requests, or requests from clients (including {{kib}}) related to {{ml}} APIs. If `xpack.ml.enabled` is not set uniformly across all nodes in your cluster then you are likely to experience problems with {{ml}} functionality not fully working.

    You must not use any {{ml}} functionality from ingest pipelines if `xpack.ml.enabled` is `false` on any node. Before setting `xpack.ml.enabled` to `false` on a node, consider whether you really meant to just exclude `ml` from the `node.roles`. Excluding `ml` from the [`node.roles`](/reference/elasticsearch/configuration-reference/node-settings.md#node-roles) will stop the node from running {{ml}} jobs and NLP models, but it will still be aware that {{ml}} functionality exists. Setting `xpack.ml.enabled` to `false` should be reserved for situations where you cannot use {{ml}} functionality at all in your cluster due to hardware limitations as described [above](#ml-settings-description).


`xpack.ml.inference_model.cache_size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The maximum inference cache size allowed. The inference cache exists in the JVM heap on each ingest node. The cache affords faster processing times for the `inference` processor. The value can be a static byte sized value (such as `2gb`) or a percentage of total allocated heap. Defaults to `40%`. See also [{{ml-cap}} circuit breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-model-inference).

$$$xpack-interference-model-ttl$$$

`xpack.ml.inference_model.time_to_live` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The time to live (TTL) for trained models in the inference model cache. The TTL is calculated from last access. Users of the cache (such as the inference processor or inference aggregator) cache a model on its first use and reset the TTL on every use. If a cached model is not accessed for the duration of the TTL, it is flagged for eviction from the cache. If a document is processed later, the model is again loaded into the cache. To update this setting in {{ess}}, see [Add {{es}} user settings](/reference/elasticsearch/configuration-reference/index.md). Defaults to `5m`.

`xpack.ml.max_inference_processors`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The total number of `inference` type processors allowed across all ingest pipelines. Once the limit is reached, adding an `inference` processor to a pipeline is disallowed. Defaults to `50`.

`xpack.ml.max_machine_memory_percent`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum percentage of the machine’s memory that {{ml}} may use for running analytics processes. These processes are separate to the {{es}} JVM. The limit is based on the total memory of the machine, not current free memory. Jobs are not allocated to a node if doing so would cause the estimated memory use of {{ml}} jobs to exceed the limit. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. The minimum value is `5`; the maximum value is `200`. Defaults to `30`.

    ::::{tip}
    Do not configure this setting to a value higher than the amount of memory left over after running the {{es}} JVM unless you have enough swap space to accommodate it and have determined this is an appropriate configuration for a specialist use case. The maximum setting value is for the special case where it has been determined that using swap space for {{ml}} jobs is acceptable. The general best practice is to not use swap on {{es}} nodes.
    ::::


`xpack.ml.max_model_memory_limit`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum `model_memory_limit` property value that can be set for any {{ml}} jobs in this cluster. If you try to create a job with a `model_memory_limit` property value that is greater than this setting value, an error occurs. Existing jobs are not affected when you update this setting. If this setting is `0` or unset, there is no maximum `model_memory_limit` value. If there are no nodes that meet the memory requirements for a job, this lack of a maximum memory limit means it’s possible to create jobs that cannot be assigned to any available nodes. For more information about the `model_memory_limit` property, see [Create {{anomaly-jobs}}](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-job) or [Create {{dfanalytics-jobs}}](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-ml-put-data-frame-analytics). Defaults to `0` if `xpack.ml.use_auto_machine_memory_percent` is `false`. If `xpack.ml.use_auto_machine_memory_percent` is `true` and `xpack.ml.max_model_memory_limit` is not explicitly set then it will default to the largest `model_memory_limit` that could be assigned in the cluster.

$$$xpack.ml.max_open_jobs$$$

`xpack.ml.max_open_jobs`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum number of jobs that can run simultaneously on a node. In this context, jobs include both {{anomaly-jobs}} and {{dfanalytics-jobs}}. The maximum number of jobs is also constrained by memory usage. Thus if the estimated memory usage of the jobs would be higher than allowed, fewer jobs will run on a node. Prior to version 7.1, this setting was a per-node non-dynamic setting. It became a cluster-wide dynamic setting in version 7.1. As a result, changes to its value after node startup are used only after every node in the cluster is running version 7.1 or higher. The minimum value is `1`; the maximum value is `512`. Defaults to `512`.

`xpack.ml.nightly_maintenance_requests_per_second`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The rate at which the nightly maintenance task deletes expired model snapshots and results. The setting is a proxy to the [`requests_per_second`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete-by-query) parameter used in the delete by query requests and controls throttling. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. Valid values must be greater than `0.0` or equal to `-1.0`, where `-1.0` means a default value is used. Defaults to `-1.0`

`xpack.ml.node_concurrent_job_allocations`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum number of jobs that can concurrently be in the `opening` state on each node. Typically, jobs spend a small amount of time in this state before they move to `open` state. Jobs that must restore large models when they are opening spend more time in the `opening` state. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. Defaults to `2`.


## Advanced machine learning settings [advanced-ml-settings]

These settings are for advanced use cases; the default values are generally sufficient:

`xpack.ml.enable_config_migration`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Reserved. When the {{operator-feature}} is enabled, this setting can be updated only by operator users.

`xpack.ml.max_anomaly_records`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum number of records that are output per bucket. Defaults to `500`.

`xpack.ml.max_lazy_ml_nodes`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The number of lazily spun up {{ml}} nodes. Useful in situations where {{ml}} nodes are not desired until the first {{ml}} job opens. If the current number of {{ml}} nodes is greater than or equal to this setting, it is assumed that there are no more lazy nodes available as the desired number of nodes have already been provisioned. If a job is opened and this setting has a value greater than zero and there are no nodes that can accept the job, the job stays in the `OPENING` state until a new {{ml}} node is added to the cluster and the job is assigned to run on that node. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. Defaults to `0`.

    ::::{important}
    This setting assumes some external process is capable of adding {{ml}} nodes to the cluster. This setting is only useful when used in conjunction with such an external process.
    ::::


`xpack.ml.max_ml_node_size`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum node size for {{ml}} nodes in a deployment that supports automatic cluster scaling. If you set it to the maximum possible size of future {{ml}} nodes, when a {{ml}} job is assigned to a lazy node it can check (and fail quickly) when scaling cannot support the size of the job. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. Defaults to `0b`, which means it will be assumed that automatic cluster scaling can add arbitrarily large nodes to the cluster.

$$$xpack.ml.model_repository$$$

`xpack.ml.model_repository`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The location of the {{ml}} model repository where the model artifact files are available in case of a model installation in a restricted or closed network. `xpack.ml.model_repository` can be a string of a file location or an HTTP/HTTPS server. Example values are:

    ```
    xpack.ml.model_repository: file://${path.home}/config/models/
    ```

    or

    ```
    xpack.ml.model_repository: https://my-custom-backend
    ```

    If `xpack.ml.model_repository` is a file location, it must point to a subdirectory of the `config` directory of {{es}}.


`xpack.ml.persist_results_max_retries`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The maximum number of times to retry bulk indexing requests that fail while processing {{ml}} results. If the limit is reached, the {{ml}} job stops processing data and its status is `failed`. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. The minimum value is `0`; the maximum value is `50`. Defaults to `20`.

`xpack.ml.process_connect_timeout`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) The connection timeout for {{ml}} processes that run separately from the {{es}} JVM. When such processes are started they must connect to the {{es}} JVM. If the process does not connect within the time period specified by this setting then the process is assumed to have failed. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. The minimum value is `5s`. Defaults to `10s`.

`xpack.ml.use_auto_machine_memory_percent`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) If this setting is `true`, the `xpack.ml.max_machine_memory_percent` setting is ignored. Instead, the maximum percentage of the machine’s memory that can be used for running {{ml}} analytics processes is calculated automatically and takes into account the total node size and the size of the JVM on the node. When the {{operator-feature}} is enabled, this setting can be updated only by operator users. The default value is `false`.

    ::::{important}
    * If you do not have dedicated {{ml}} nodes (that is to say, the node has multiple roles), do not enable this setting. Its calculations assume that {{ml}} analytics are the main purpose of the node.
    * The calculation assumes that dedicated {{ml}} nodes have at least `256MB` memory reserved outside of the JVM. If you have tiny {{ml}} nodes in your cluster, you shouldn’t use this setting.

    ::::


    If this setting is `true` it also affects the default value for `xpack.ml.max_model_memory_limit`. In this case `xpack.ml.max_model_memory_limit` defaults to the largest size that could be assigned in the current cluster.


