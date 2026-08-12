---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/circuit-breaker.html
applies_to:
  deployment:
    ess:
    self:
---

# Circuit breaker settings [circuit-breaker]

$$$circuit-breaker-description$$$
{{es}} contains multiple circuit breakers used to prevent operations from using an excessive amount of memory. Each breaker tracks the memory used by certain operations and specifies a limit for how much memory it may track. Additionally, there is a parent-level breaker that specifies the total amount of memory that may be tracked across all breakers.

When a circuit breaker reaches its limit, {{es}} will reject further operations. See [Circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md) for information about errors raised by circuit breakers.

Circuit breakers do not track all memory usage in {{es}} and therefore provide only incomplete protection against excessive memory usage. If {{es}} uses too much memory then it may suffer from performance issues and nodes may even fail with an `OutOfMemoryError`. See [High JVM memory pressure](docs-content://troubleshoot/elasticsearch/high-jvm-memory-pressure.md) for help with troubleshooting high heap usage.

Except where noted otherwise, these settings can be dynamically updated on a live cluster with the [cluster-update-settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) API.

For information about circuit breaker errors, see [Circuit breaker errors](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).


### Parent circuit breaker [parent-circuit-breaker]

The parent-level breaker can be configured with the following settings:

`indices.breaker.total.use_real_memory`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Determines whether the parent breaker should take real memory usage into account (`true`) or only consider the amount that is reserved by child circuit breakers (`false`). Defaults to `true`. Note: the [native memory circuit breaker](#native-memory-circuit-breaker) is not included in the parent total regardless of this setting.

$$$indices-breaker-total-limit$$$

`indices.breaker.total.limit` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Starting limit for overall parent breaker. Defaults to 70% of JVM heap if `indices.breaker.total.use_real_memory` is `false`. If `indices.breaker.total.use_real_memory` is `true`, defaults to 95% of the JVM heap.


### Field data circuit breaker [fielddata-circuit-breaker]

The field data circuit breaker estimates the heap memory required to load a field into the [field data cache](/reference/elasticsearch/configuration-reference/field-data-cache-settings.md). If loading the field would cause the cache to exceed a predefined memory limit, the circuit breaker stops the operation and returns an error.

$$$fielddata-circuit-breaker-limit$$$

`indices.breaker.fielddata.limit` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Limit for fielddata breaker. Defaults to 40% of JVM heap.

$$$fielddata-circuit-breaker-overhead$$$

`indices.breaker.fielddata.overhead` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A constant that all field data estimations are multiplied with to determine a final estimation. Defaults to `1.03`.


### Request circuit breaker [request-circuit-breaker]

The request circuit breaker allows Elasticsearch to prevent per-request data structures (for example, memory used for calculating aggregations during a request) from exceeding a certain amount of memory.

$$$request-breaker-limit$$$

`indices.breaker.request.limit` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Limit for request breaker, defaults to 60% of JVM heap.

$$$request-breaker-overhead$$$

`indices.breaker.request.overhead` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A constant that all request estimations are multiplied with to determine a final estimation. Defaults to `1`.


### Native memory circuit breaker [native-memory-circuit-breaker]

```{applies_to}
stack: ga 9.6
serverless: ga
```

The native memory circuit breaker limits off-heap (native) memory allocations, such as Arrow buffers used by ES|QL. Unlike the other breakers, this breaker is **not** included in the parent circuit breaker's total: the parent's limit is expressed in JVM heap bytes, and adding off-heap bytes to it would mix units. Each resource is bounded independently.

`indices.breaker.native_memory.limit`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Limit for the native memory breaker. Defaults to `50%` of the JVM's maximum direct memory (`-XX:MaxDirectMemorySize`). Reaching this limit returns a `[native_memory] Data too large` error to the caller without affecting the parent breaker or causing a node restart. **On nodes where machine learning is enabled**, the ML native process runs in the same cgroup and its memory is not subtracted from this budget automatically. On such nodes, set an explicit byte value rather than a percentage to avoid the two budgets overlapping under the cgroup limit.

`indices.breaker.native_memory.overhead`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A constant that all native memory estimations are multiplied with to determine a final estimation. Defaults to `1.0`. Unlike heap estimations, native charges are exact byte counts, so no padding is needed.

`indices.breaker.native_memory.type`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Determines the type of the native memory breaker. Set to `noop` to deactivate enforcement (the breaker accepts all allocations but still reports usage). Defaults to `memory`.

#### Native memory cgroup backstop [native-memory-cgroup-backstop]

On nodes running inside a Linux cgroup with a memory limit, native memory allocation admission can also be controlled by the following settings. The backstop reads actual cgroup memory usage (including ML native processes, compression libraries, and any other off-heap consumer) and refuses new native memory allocations when usage exceeds a configurable high-watermark. When not running in a container with memory limits, the backstop is inactive.

`indices.breaker.native_memory.cgroup_high_watermark_percent`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The cgroup memory usage percentage at which new native memory allocations are refused. When usage drops below this value minus 5 percentage points (the hysteresis band), allocations are admitted again. Defaults to `85`. Accepted range: `50`–`99`.

`indices.breaker.native_memory.cgroup_poll_interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) How often the cgroup memory usage is sampled. Requires a node restart to change. Defaults to `5s`. Minimum: `1s`.


### In flight requests circuit breaker [in-flight-circuit-breaker]

The in flight requests circuit breaker allows Elasticsearch to limit the memory usage of all currently active incoming requests on transport or HTTP level from exceeding a certain amount of memory on a node. The memory usage is based on the content length of the request itself. This circuit breaker also considers that memory is not only needed for representing the raw request but also as a structured object which is reflected by default overhead.

`network.breaker.inflight_requests.limit`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Limit for in flight requests breaker, defaults to 100% of JVM heap. This means that it is bound by the limit configured for the parent circuit breaker.

`network.breaker.inflight_requests.overhead`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A constant that all in flight requests estimations are multiplied with to determine a final estimation. Defaults to 2.


### Script compilation circuit breaker [script-compilation-circuit-breaker]

Slightly different than the previous memory-based circuit breaker, the script compilation circuit breaker limits the number of inline script compilations within a period of time.

See the "prefer-parameters" section of the [scripting](docs-content://explore-analyze/scripting/modules-scripting-using.md) documentation for more information.

`script.max_compilations_rate`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Limit for the number of unique dynamic scripts within a certain interval that are allowed to be compiled. Defaults to `150/5m`, meaning 150 every 5 minutes.

If the cluster regularly hits the given `max_compilation_rate`, it’s possible the script cache is undersized, use [Nodes Stats](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats) to inspect the number of recent cache evictions, `script.cache_evictions_history` and compilations `script.compilations_history`.  If there are a large number of recent cache evictions or compilations, the script cache may be undersized, consider doubling the size of the script cache via the setting `script.cache.max_size`.


### Regex circuit breaker [regex-circuit-breaker]

Poorly written regular expressions can degrade cluster stability and performance. The regex circuit breaker limits the use and complexity of [regex in Painless scripts](/reference/scripting-languages/painless/painless-regexes.md).

$$$script-painless-regex-enabled$$$

`script.painless.regex.enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Enables regex in Painless scripts. Accepts:

    `limited` (Default)
    :   Enables regex but limits complexity using the [`script.painless.regex.limit-factor`](#script-painless-regex-limit-factor) cluster setting.

    `true`
    :   Enables regex with no complexity limits. Disables the regex circuit breaker.

    `false`
    :   Disables regex. Any Painless script containing a regular expression returns an error.


$$$script-painless-regex-limit-factor$$$

`script.painless.regex.limit-factor`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Limits the number of characters a regular expression in a Painless script can consider. {{es}} calculates this limit by multiplying the setting value by the script input’s character length.

    For example, the input `foobarbaz` has a character length of `9`. If `script.painless.regex.limit-factor` is `6`, a regular expression on `foobarbaz` can consider up to 54 (9 * 6) characters. If the expression exceeds this limit, it triggers the regex circuit breaker and returns an error.

    {{es}} only applies this limit if [`script.painless.regex.enabled`](#script-painless-regex-enabled) is `limited`.



## EQL circuit breaker [circuit-breakers-page-eql]

When a [sequence](/reference/query-languages/eql/eql-syntax.md#eql-sequences) query is executed, the node handling the query needs to keep some structures in memory, which are needed by the algorithm implementing the sequence matching. When large amounts of data need to be processed, and/or a large amount of matched sequences is requested by the user (by setting the [size](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-eql-search) query param), the memory occupied by those structures could potentially exceed the available memory of the JVM. This would cause an `OutOfMemory` exception which would bring down the node.

To prevent this from happening, a special circuit breaker is used, which limits the memory allocation during the execution of a [sequence](/reference/query-languages/eql/eql-syntax.md#eql-sequences) query. When the breaker is triggered, an `org.elasticsearch.common.breaker.CircuitBreakingException` is thrown and a descriptive error message including `circuit_breaking_exception` is returned to the user.

This circuit breaker can be configured using the following settings:

`breaker.eql_sequence.limit`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The limit for circuit breaker used to restrict the memory utilisation during the execution of an EQL sequence query. This value is defined as a percentage of the JVM heap. Defaults to `50%`. If the [parent circuit breaker](#parent-circuit-breaker) is set to a value less than `50%`, this setting uses that value as its default instead.

`breaker.eql_sequence.overhead`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A constant that sequence query memory estimates are multiplied by to determine a final estimate. Defaults to `1`.

`breaker.eql_sequence.type`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Circuit breaker type. Valid values are:

    `memory` (Default)
    :   The breaker limits memory usage for EQL sequence queries.

    `noop`
    :   Disables the breaker.



## ES|QL circuit breaker [circuit-breakers-page-esql]

{{esql}} executes queries by building blocks of data (columns, aggregations, and other pipeline structures) in memory. When processing large datasets or running queries with high-cardinality aggregations, the memory used by these structures can grow significantly and potentially exceed the available JVM heap, which could cause an `OutOfMemoryError` and bring down the node.

To prevent this, {{esql}} uses the [request circuit breaker](#request-circuit-breaker), which limits memory allocation during query execution. There is no dedicated {{esql}} circuit breaker; {{esql}} shares the request breaker with other per-request operations such as aggregations. When the breaker is triggered, an `org.elasticsearch.common.breaker.CircuitBreakingException` is thrown and a descriptive error message including `circuit_breaking_exception` is returned to the user.

The request breaker settings (`indices.breaker.request.limit`, `indices.breaker.request.overhead`, and `indices.breaker.request.type`) are documented in the [Request circuit breaker](#request-circuit-breaker) section.



### {{ml-cap}} circuit breaker [circuit-breakers-page-model-inference]

`breaker.model_inference.limit`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The limit for the trained model circuit breaker. This value is defined as a percentage of the JVM heap. Defaults to `50%`. If the [parent circuit breaker](#parent-circuit-breaker) is set to a value less than `50%`, this setting uses that value as its default instead.

`breaker.model_inference.overhead`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A constant that all trained model estimations are multiplied by to determine a final estimation. Defaults to `1`.

`breaker.model_inference.type`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) The underlying type of the circuit breaker. There are two valid options: `noop` and `memory`. `noop` means the circuit breaker does nothing to prevent too much memory usage. `memory` means the circuit breaker tracks the memory used by trained models and can potentially break and prevent `OutOfMemory` errors. The default value is `memory`.


## Synonym circuit breaker [circuit-breakers-page-synonym]

```{applies_to}
stack: ga 9.4
serverless: ga
```

The [`synonym`](/reference/text-analysis/analysis-synonym-tokenfilter.md) and [`synonym_graph`](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md) token filters check real heap memory usage when building the synonyms map, to prevent large synonym sets from causing out-of-memory errors. This check uses the [parent circuit breaker](#parent-circuit-breaker), which trips when memory usage exceeds the `indices.breaker.total.limit` threshold (defaults to 95% of JVM heap when `indices.breaker.total.use_real_memory` is `true`).

The threshold is configurable using the [parent circuit breaker settings](#parent-circuit-breaker). {applies_to}`serverless: unavailable`

For details on behavior when the circuit breaker trips, refer to the [synonym token filter](/reference/text-analysis/analysis-synonym-tokenfilter.md#synonym-tokenizer-circuit-breaker) and [synonym graph token filter](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md#synonym-graph-tokenizer-circuit-breaker) documentation.

