# Metric naming guidelines

This guide covers conventions to ensure reliable, consistent and discoverable metrics and attributes.
In particular, that is
- how to build a good **hierarchical** metric name consisting of **segments**,
- required **suffixes** for metric names,
- **pluralization** and
- the usage of metric **attributes** (aka dimensions).

The conventions described here follow OpenTelemetry guidelines for the most part and are adapted to ES where necessary.

## Metric names

Metric names are composed of **hierarchical segments** split by a **separator** and prefixed with `es.`:

```
es(.<segment>)+.<suffix>
```

- Each **segment** should be lower-case ascii and digits only, with underscore (`_`) as word separator where necessary (e.g. `blob_cache`);
  - max 30 characters per segment are permitted and
  - there's a limit of 8 segments per metric name.
- The segment **separator** is dot (`.`).
- The **suffix** is [standardized](#name-suffix) to describe the semantics of the metric.
- The total metric name may not exceed 255 characters.

### Hierarchy and segments

Always use `es.` as the root segment to easily discover ES metrics and avoid the possibility of name clashes.
Follow with a module name, team or area of code, e.g. `snapshot, repositories, indices, threadpool` using existing terminology (whether singular and plural).

The **hierarchy** of segments should be built by putting "more common" segments at the beginning. This facilitates the creation of new metrics under a common namespace.
Each element in the metric name specializes or describes the prefix that precedes it. Rule of thumb: you could truncate the name at any segment, and what you're left with is something that makes sense by itself.

Example: Prefer `.docs.deleted.total` over `.deleted.docs.total` so `.docs.ingested.total` could be added later.

Recommendations:
- When adding new metrics, [look for existing segments](#inspect-registered-metrics) as prefix in your domain. Also keep language consistent across the hierarchy, re-using segment names / terminology when possible.
E.g. stick to using `.error.total` if already in use rather than introducing `.failures.total`.
- Metric names should not contain dynamic segments, consider moving these into [metric attributes](#metric-attributes) instead.
- Do not add metrics as subobjects of an existing metric, e.g. adding `.disk.usage.status` when `.disk.usage` exists. This might lead to mapping issues in Elasticsearch.
- Keep metric names meaningful, but concise. Use the metric description field at registering time for further explanation.

### Name suffix

The metric suffix is essential to describe the semantics of a metric and guide consumers on how to interpret and use a metric appropriately.
If multiple suffixes are applicable, chose the most specific one.

*  `total`: a monotonic metric (always increasing counter), e.g. <code>es.indices.docs.deleted.<strong>total</strong></code>)
    * Note: such metrics typically report deltas that must be accumulated to get the total over a time window
*  `current`: a general non-monotonic metric (like gauges, upDownCounters)
    * An example to clarify `current` vs `total`:
      * <code>es.process.jvm.classes.loaded.<strong>current</strong></code>: the number of classes currently loaded by the JVM
      * <code>es.process.jvm.classes.loaded.<strong>total</strong></code>: the number of classes loaded by the JVM in this time window
*  `usage`: a non-monotonic metric representing the absolute amount used of some resource (with a limit of `size`)
*  `size`: the overall size of the resource
*  `utilization`: ratio of `usage` and overall `size` of the resource
*  `ratio`: ratio of two measures with identical unit (other than `usage` and `size`) or a fraction in the range [0, 1]
*  `status`: enum like gauges
    * E.g <code>es.health.overall.red.status</code> with values `1`/`0` to represent `true`/`false`
*  `histogram`: a histogram metric
*  `time`: to represent passage of time

### Metric units

Do not include units of measurements in metric names.
Instead, use a suffix that describes the physical quantity being measured (e.g. `time`, `size`, `usage`, etc.) as described above.
Units are configured at registration time of the metric.


## Metric attributes

**WARNING** Do not use **high cardinality** attributes / dimensions. This might result in the APM Java agent dropping events.

It is not always straight forward to decide if something should part of the metric name or an attribute (dimension) of that metric.  As a rule of thumb:
- any aggregation across any dimensions of a metric should be meaningful, and
- meaningful aggregations should be possible without having to aggregate over different metrics.

Naming conventions for attributes are very similar to metric names.
Always use `es_` as the root segment to easily discover ES attributes.
Follow with a module name, team or area of code, e.g. `snapshot, repositories, indices, threadpool` using existing terminology (whether singular and plural).

```
es(_<segment>)+
```

- Each **segment** should be lower-case ascii and digits only;
    - max 30 characters per segment are permitted,
    - at least two segments are required and
    - there's a limit of 8 segments per metric name.
- The segment **separator** is the underscore (`_`).
- The name may not exceed 255 characters.

Attributes that represent an entity should be named in singular.
If the attribute value represents a collection, it should be named in plural.

### Migration of existing, invalid attributes

Unfortunately, validation of attribute names was only introduced retrospectively.
Existing attributes, that do not comply with naming conventions, are tracked in a skip list to not fail validation.
However, their usage will be logged when running with assertions enabled.

Please migrate usage of such attributes following these steps:
1. Check if the attribute is used on dashboards or alerts (elasticsearch-o11y-resources). If not, rename the attribute following above conventions; otherwise continue.
2. Duplicate the attribute using a name following above conventions.
3. Once there's sufficient history, update alerts and dashboards to use the new attribute.
4. Remove the old attribute from the skip list and delete all usages.

## Inspect registered metrics

To inspect currently registered metrics, run ES using:
```
./gradlew run -Dtests.es.logger.org.elasticsearch.telemetry.apm=debug
```
Use this as guidance where your new metric could fit in and name accordingly.
