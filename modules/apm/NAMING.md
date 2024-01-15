# Metrics Naming Guidelines

We propose a set of guidelines to build consistent and readable names for metrics. The guidelines cover how to build a good **hierarchical** name, the syntax of **elements** in a name, the usage of **dimensions** (attributes), **pluralization **and** suffixes**.

This set of “rules” has been built by looking at naming conventions and best practices used by other software (e.g. Prometheus, Datadog) or standards (OpenTelemetry, OpenMetrics - see  for details). \
They follow OpenTelemetry guidelines most closely with some ES specifics.

## Guidelines

A metric name should be composed of **elements** limited by **separators** to organize them in a **hierarchy**.

**Elements** should be lower-case, and use underscore (`_`) to combine words in the same element level (e.g. `blob_cache`).

The **separator** character is dot (`.`)

The **hierarchy** should be built by putting "more common" elements at the beginning, in order to facilitate the creation of new metrics under a common namespace. Each element in the metric name specializes or describes the prefix that precedes it. Rule of thumb: you could truncate the name at any segment, and what you're left with is something that makes sense by itself.

Example:
* prefer `es.indices.docs.deleted.total `to `es.indices.total.deleted.docs`
* This way you can later add` es.indices.docs.total, es.indices.docs.ingested.total`, etc.)

Prefix metrics:
* Always use `es` as our root application name: this will give us a separate namespace and avoid any possibility of clashes with other metrics, and quick identification of Elasticsearch metrics on a dashboard.
* Follow the root prefix with a simple module name, team or area of code. E.g. `snapshot, repositories, indices, threadpool`. Notice the mix of singular and plural - here this is intentional, to reflect closely the existing names in the codebase (e.g. `reindex` and `indices`)
* In building a metric name, look for existing prefixes (e.g. module name and/or area of code, e.g. `blob_cache`) and for existing sub-elements as well (e.g. `error`) to build a good, consistent name. E.g. prefer the consistent use of `error.total` rather than introducing `failures`, `failed.total` or `errors`.` `
* Avoid having sub-metrics under a name that is also a metric (e.g. do not create names like `es.repositories.elements`,` es.repositories.elements.utilization`; use` es.repositories.element.total` and` es.repositories.element.utilization `instead). Such metrics are hard to handle well in Elasticsearch, or in some internal structures (e.g. nested maps).

Keep the hierarchy compact: do not add elements if you don’t need to. There is a description field when registering a metric, prefer using that as an explanation. \
For example, if emitting existing metrics from node stats, do not use the whole “object path”, but choose the most significant terms.

The metric name can be generated but there should be no dynamic or variable content in the name: that content belongs to a **dimension** (attributes/labels).

* Node name, node id, cluster id, etc. are all considered dynamic content that belongs to attributes, not to the metric name.
* When there are different "flavors" of a metric (i.e. `s3`, `azure`, etc) use an attribute rather than inserting it in the metric name.
* Rule of thumb: you should be able to do aggregations (e.g. sum, avg) across a dimension of a given metric (without the need to aggregate over different metric names); on the other hand, any aggregation across any dimension of a given metric should be meaningful.
* There might be exceptions of course. For example:
    * When similar metrics have significantly different implementations/related metrics.  \
      If we have only common metrics like  `es.repositories.element.total, es.repositories.element.utilization, es.repositories.writes.total` for every blob storage implementation, then `s3,azure` should be an attribute. \
      If we have specific metrics, e.g. for s3 storage classes, prefer using prefixed metric names for the specific metrics:  <code>es.repositories.<strong>s3</strong>.deep_archive_access.total</code> (but keep `es.repositories.elements`)
    * When you have a finite and fixed set of names it might be OK to have them in the name (e.g. "`young`" and "`old`" for GC generations).

The metric name should NOT include its **unit**. Instead, the associated physical quantity should be added as a suffix, possibly following the general semantic names ([link](https://opentelemetry.io/docs/specs/semconv/general/metrics/#instrument-naming)).
Examples :
* <code>es.process.jvm.collection.<strong>time</strong></code> instead of <code>es.process.jvm.collection.<strong>seconds</strong></code>.
* <code>es.process.mem.virtual.<strong>size</strong></code>, <code>es.indices.storage.<strong>size</strong></code> (instead of <code>es.process.mem.virtual.<strong>bytes</strong></code>, <code>es.indices.storage.<strong>bytes</strong></code>)
* In case `size` has a known upper limit, consider using `usage` (e.g.: <code>es.process.jvm.heap.<strong>usage</strong></code> when there is a <code>es.process.jvm.heap.<strong>limit</strong></code>)
* <code>es.indices.storage.write.<strong>io</strong></code>, instead of <code>es.indices.storage.write.<strong>bytes_per_sec</strong></code>
* These can all be composed with the suffixes below, e.g. <code>es.process.jvm.collection.<strong>time.total</strong></code>, <code>es.indices.storage.write.<strong>total</strong></code> to represent the monotonic sum of time spent in GC and the total number of bytes written to indices respectively.

**Suffixes**:
* Use `total` as a suffix for monotonic metrics (always increasing counter) (e.g. <code>es.indices.docs.deleted.<strong>total</strong></code>)
  * Note: even though async counter is reporting a total cumulative value, it is till monotonic.
* Use `current` to represent the non-monotonic metrics (like gauges, upDownCounters)
  * e.g. `current` vs `total` We can have <code>es.process.jvm.classes.loaded.<strong>current</strong></code> to express the number of classes currently loaded by the JVM, and the total number of classes loaded since the JVM started as <code>es.process.jvm.classes.loaded.<strong>total</strong></code>
* Use `ratio` to represent the ratio of two measures with identical unit (or unit-less) or measures that represent a fraction in the range [0, 1]. Examples:
    * Exception: consider using utilization when the ratio is between a usage and its limit, e.g. the ratio between <code>es.process.jvm.heap.<strong>usage</strong></code> and <code>es.process.jvm.heap.<strong>limit</strong></code> should be <code>es.process.jvm.heap.<strong>utilization</strong></code>
* Use `status` to represent enum like gauges. example <code>es.health.overall.red.status</code> have values 1/0 to represent true/false
* Use `usage` to represent the amount used ouf of the known resource size
* Use `size` to represent the overall size of the resource measured
* Use `utilisation` to represent a fraction of usage out of the overall size of a resource measured
* Use `histogram` to represent instruments of type histogram
* Use `time` to represent passage of time
* If it has a unit of measure, then it should not be plural (and also not include the unit of measure, see above). Examples:  <code>es.process.jvm.collection.time, es.process.mem.virtual.usage<strong>, </strong>es.indices.storage.utilization</code>

### Attributes

Attribute names should follow the same rules. In particular, these rules apply to attributes too:
* elements and separators
* hierarchy/namespaces
* units
* pluralization (when an attribute represents a measurement)

For **pluralization**, when an attribute represents an entity, the attribute name should be singular (e.g.` es.security.realm_type`, not` es.security.realms_type` or `es.security.realm_types`), unless it represents a collection (e.g.` es.rest.request_headers`)


### List of previously registered metric names
You can inspect all previously registered metrics names with
`./gradlew run -Dtests.es.logger.org.elasticsearch.telemetry.apm=debug`
This should help you find out the already registered group that your meteric
might fit
