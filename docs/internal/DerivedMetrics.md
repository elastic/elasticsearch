# Derived Metrics

Derived metrics let a data stream emit compact operational metrics into a managed TSDS while documents are written to the source stream. The goal is to make common metric queries scale with the number of configured metrics and dimensions instead of the source write rate.

## Scope

The V1 metadata model supports:

- `data_stream_options.derived_metrics`
- built-in ingest metrics with default `["ingest.*"]`
- one default interval, overridable per metric, each writing to its own destination
- additive global and per-metric dimensions
- user-defined `counter`, `gauge`, and `histogram` metrics
- gauge aggregations `min`, `max`, `avg` and `sum`
- script-free predicates and values

Counter, gauge and histogram metrics, along with the built-in ingest metrics, are collected and emitted at runtime.

## Data Stream Option

```json
{
  "data_stream_options": {
    "derived_metrics": {
      "enabled": true,
      "builtin": ["ingest.*"],
      "default_interval": "10s",
      "destinations": {
        "1m": { "lifecycle": { "data_retention": "90d" } }
      },
      "dimensions": ["service.name", "cloud.region"],
      "metrics": [
        {
          "name": "http.requests",
          "type": "counter",
          "when": {
            "exists": {
              "field": "http.request.method"
            }
          },
          "value": 1,
          "dimensions": ["http.request.method", "http.response.status_code"]
        }
      ]
    }
  }
}
```

Omitted fields use defaults when the concrete data stream option is built:

- `enabled`: `true`
- `builtin`: `["ingest.*"]`
- `default_interval`: `10s`
- `destinations`: `{}`
- `dimensions`: `[]`
- `metrics`: `[]`

Template composition is additive for `builtin`, `dimensions`, and `metrics`. `default_interval` and each entry of `destinations` are
overwritten by the most specific template that defines them. `enabled` is overwritten by the most specific template that defines it. A duplicate user metric name is allowed only when the full metric definition is identical.

## User Metric Fields

Every user metric has:

- `name`: metric name outside the reserved `ingest.*` namespace.
- `type`: `counter`, `gauge`, or `histogram`.
- `when`: optional predicate.
- `value`: numeric constant or field reference.
- `dimensions`: optional extra dimensions added to global dimensions.
- `interval`: optional override of `default_interval`. The metric is accumulated separately and written to that interval's own
  destination, so the interval must have an entry in `destinations`.
- `preference`: optional, 1 to 10,000, relative to a default of 100. Biases which bucket the node gives up first under memory pressure,
  for the case where cost and importance disagree. Raising it makes a metric proportionally less likely to be flushed early. Because
  flushing early is lossless, this trades extra documents rather than data. Two templates defining the same metric with different
  preferences is an error, as it is for every other field of a metric.

Counters default `value` to `1` when omitted.

Gauges support `aggregation`:

- `min`
- `max`
- `avg`
- `sum`

Non-gauge metrics reject `aggregation`.

**`first_value` and `last_value` are deliberately not offered.** Every reduction here combines
associatively across nodes — sums add, minima take the minimum, and `avg` emits sum and count so the
mean is a ratio. First and last do not: each node holds its own earliest or latest observation and
recovering the cluster-wide answer needs a global ordering, which a distributed system does not have.
Timestamps do not rescue it — ordering would then be bounded by inter-node clock skew, and an
approximate answer to "what was the last value" is exactly the kind of thing people build alerts on
without realising it is approximate.

Elasticsearch downsampling does offer this semantic safely, because it works on one shard of a TSDS
where a series is co-located by construction. A plain data stream spreads one entity across shards,
so the guarantee that makes it sound is absent.

Use `max` where you want a headline number, or model the entity as a dimension so that the series you
read is produced by a single writer.

Histograms keep the whole distribution of the observed values rather than reducing them to a number, so they take no `aggregation`. Each
series accumulates into a bounded exponential histogram of `data_streams.derived_metrics.histogram_buckets` buckets, which is the knob
that trades a histogram series' precision against its size. A histogram series is by far the most expensive kind: hundreds of buckets
against the handful of primitives a scalar series needs, which is why the circuit breaker matters more here than anywhere else.

## Predicates

Supported predicates are intentionally limited to deterministic, script-free forms that runtime code can evaluate on the write path:

```json
{ "exists": { "field": "event.duration" } }
```

```json
{ "term": { "event.outcome": "failure" } }
```

```json
{ "terms": { "http.response.status_code": [500, 502, 503] } }
```

```json
{ "range": { "event.duration": { "gt": 0, "lte": 1000000000 } } }
```

```json
{
  "and": [
    { "exists": { "field": "event.duration" } },
    { "term": { "event.outcome": "success" } }
  ]
}
```

`or` and `not` are also supported.

## Runtime

The runtime lives in the `data-streams` module, under `org.elasticsearch.datastreams.derivedmetrics`.

`DerivedMetricsIndexingListener` observes writes through `IndexingOperationListener`. Only operations whose origin is the primary are
observed, so a document is counted exactly once regardless of how many replicas it is written to. Resolving an index to its data stream
configuration is cached per cluster state version, so the steady-state per-document cost is one volatile read and a comparison.

`CompiledDerivedMetrics` is the write-path form of the configuration: predicates are compiled once, built-in selectors are expanded, and
the set of source paths any metric needs is known up front. When that set is empty — the common case for a stream that only asks for the
built-in ingest metrics without dimensions — the write path never parses `_source` at all. When it is not empty, only those paths are
walked for.

Every path any metric reads — dimensions, predicate fields, value fields — is numbered into a slot at compile time and arranged as a trie.
The document is then walked exactly once, values are written straight into an array indexed by slot, and any field no configured path leads
to has its whole subtree skipped. Nothing intermediate is built: no filtered copy of the source, no map, no path strings at all. Two metrics
naming the same field share a slot, so it is extracted once however many of them want it.

The obvious implementation — build a filtered map and look paths up in it — is what this used to do, and it cost several kilobytes per
document. It was in fact *more* expensive than parsing the document with no filter at all, because the filtering machinery is not free
either.

### What observing a write costs

Measured by `DerivedMetricsObservationBench` in the `benchmarks` project — run it with `-prof gc`, because the allocation number matters
at least as much as the time. This path runs once per document on the indexing thread, inside the shard's operation permit.

What it costs to add one thing at a time, which is the question anyone configuring this actually has:

| configuration | ns/op | B/op |
|---|---|---|
| configured, but no metric triggered by this write | 2 | 0 |
| one metric, no dimensions | 110 | 0 |
| that same metric with three dimensions | 537 | 256 |
| one histogram over a field, 100 buckets | 246 | 6 |

The first row deserves a caveat rather than a boast. It is `record` returning on its trigger check — a stream that has metrics configured
where this particular write matches none of them, which happens when only failure-triggered metrics are configured and the write
succeeded. **It is not what an index with no derived metrics pays**, because such an index never reaches `record` at all: the listener
returns after its origin check and cached cluster state resolution. That cost is real and unmeasured here, because measuring it needs a
`ClusterService` the benchmark project cannot stand up.

These are the figures for a mapping that lets every configured value be read from the document Elasticsearch has already parsed, which is
what an ECS-shaped stream gives you. A mapping that cannot falls back to parsing `_source` again, and that is what the cliff used to be
for everyone:

| configuration | from the parsed document | by re-parsing `_source` |
|---|---|---|
| one metric with three dimensions | 537 ns / 256 B | 1,199 ns / 1,984 B |
| five dimensions and a predicate | 1,282 ns / 576 B | 2,110 ns / 2,296 B |
| one histogram over a field | 246 ns / 6 B | 1,159 ns / 1,854 B |

Allocation falls by 75% to 99% and time by 39% to 79%. The histogram case is the clearest, because it reads a single field and configures
no dimensions: six bytes per document, which is nothing. What remains in the other two is not the read at all — it is resolving and
encoding the dimension values, which is why five dimensions still costs more than three.

### What the hot path is actually spending

`readSource` in the same benchmark isolates the filtered parse, and comparing it against `observe` settles where the time goes. Parsing
the document with **no paths configured at all** costs **913 ns and 1,848 bytes** — that is the price of creating a parser and scanning
the document, before derived metrics extracts anything.

| shape | observe | readSource | parse share of time | of allocation |
|---|---|---|---|---|
| one metric + three dimensions | 1,237 ns / 1,984 B | 1,035 ns / 2,016 B | 84% | ~100% |
| one histogram, 100 buckets | 1,168 ns / 1,854 B | 979 ns / 1,872 B | 84% | ~100% |
| five dimensions + a predicate | 2,264 ns / 2,301 B | 1,146 ns / 2,088 B | 51% | 91% |

So the hot path decomposes as roughly **913 ns to touch `_source` at all, ~122 ns to extract three configured values, and ~200 ns for
everything derived metrics then does with them** — predicate, dimension encoding, hash probe and accumulation.

#### The long-term shape, recorded so it is not rediscovered

Reading the parsed document is the *contained* answer, and it is the one implemented. The architecturally better one is what
`TimeSeriesIdFieldMapper` already does for the tsid: it does not read dimension values back out of the document at all. The leaf mappers
push them into a side channel during parse — `KeywordFieldMapper.indexValue` calls `context.getRoutingFields().addString(...)` — and
`RoutingPathFields` collects them. No re-parse, no name lookup, no ambiguity about normalisation because the mapper chooses what to
capture, and multi-valued fields arrive already separated.

The analogue here would be a collector on `DocumentParserContext` populated by the mappers of fields a derived metric actually reads,
delivered on `ParsedDocument`. It would remove the remaining per-document field scan as well as the parse.

The reason it is not what we built first: it requires touching `DocumentParserContext` and every mapper type involved, and it couples core
mappers to a data-streams module feature, where reading the parsed document stays entirely inside the module. `RoutingPathFields` itself
cannot be reused as-is — it is only populated for indices with a `routing_path`, which a plain log data stream does not have.

That is why the write path no longer parses the document twice, and the figures above are the ones with that in place. `DocumentParser`
has already parsed the document by the time `postIndex` runs, so `DerivedMetricsDocumentReader` reads the values from the materialised
fields instead — but only where the stored value is provably the source value.

Which mappings qualify is the whole of the design, because a metric that changes meaning depending on which reader ran would be worse than
a slow one:

| mapping | how `service.name` is read |
|---|---|
| keyword, indexed, no normalizer | directly, the term is the source value |
| `text` with a `.keyword` sub-field, the default for a dynamically mapped string | from the **parent**, which holds the raw string the parser saw — analysis happens later inside the index writer, so the sub-field and its `ignore_above` are never involved |
| keyword with a normalizer | refused: the value was rewritten before storage |
| keyword with any effective `ignore_above` | refused: an over-long value is absent, and absent-because-too-long cannot be told from absent-because-missing |
| not indexed and not doc-valued | refused: being mapped is not being present |
| unmapped | refused |

Numerics are read from doc values and decoded rather than taken at face value — `DoubleField` stores
`NumericUtils.doubleToSortableLong`, so reading `numericValue()` directly would produce a plausible and completely wrong number.
`half_float` is refused outright, being lossy against the source.

One unreadable path sends the whole document back to `_source`, and the split is reported as
`es.derived_metrics.documents.read.from_index.total` and `...from_source.total`, so a stream paying for a second parse can be found rather
than guessed at.

Configuration always names the logical field. Requiring `service.name.keyword` would make a metric definition non-portable between an ECS
mapping, where `service.name` is already a keyword, and a dynamic one, where it is not — and would leak the sub-field into the emitted
dimension name.

Fuller configurations, for scale. These predate the parsed-document reader and therefore show the source-parsing path:

| configuration | ns/op | B/op |
|---|---|---|
| built-in ingest metrics, no dimensions | 409 | 160 |
| built-in ingest metrics, one dimension | 1,509 | 2,112 |
| built-in plus a predicate-guarded counter, five dimensions | 2,264 | 2,301 |

The first row is the one almost every index in almost every cluster sits on, because the indexing listener is registered on every index
and not only on the ones that asked for this. Two nanoseconds and no allocation: `record` compares the write's trigger against the
configured set and returns. Be precise about what that number does and does not cover — it is the service's early-out, measured by
calling `record` directly. Above it the listener does a volatile read and a cluster state version comparison before it gets that far, and
measuring *that* needs a real `ClusterService` the benchmark project cannot easily stand up. So the honest statement is: the measured
part is free, and the unmeasured part above it is a volatile read and an integer comparison.

Those figures are single-threaded. The per-table monitor does not scale, and it is worth being blunt
about that rather than reporting the flattering shape. Measured on the **default** configuration —
`builtin: ["ingest.*"]` with no dimensions, which is four success-triggered metrics of one series
each, so every write thread serialises on four monitors:

| threads | ns/op | aggregate documents/sec |
|---|---|---|
| 1 | 409 ± 7 | 2,450,000 |
| 2 | 755 ± 151 | 2,650,000 |
| 4 | 1,237 ± 354 | 3,230,000 |
| 8 | 3,848 ± 549 | 2,080,000 |

Throughput peaks at four threads and then **regresses below the single-thread figure**. That is
contention collapse, not sublinear scaling.

The cause is precise, and the contrast with a configuration that reads `_source` shows it:

| threads | built-ins, no dimensions | five dimensions and a predicate |
|---|---|---|
| 1 | 2,450,000 docs/sec | 444,000 docs/sec |
| 4 | 3,230,000 docs/sec | 1,070,000 docs/sec |
| 8 | 2,080,000 docs/sec — collapsed | 1,296,000 docs/sec — still climbing |

**The collapse belongs to configurations that never read `_source`.** With a parse in the path, half
the work is lock-free and throughput scales monotonically, 2.9x from one thread to eight. Without one,
`record` is essentially nothing but the monitor, so threads convoy on it. The default configuration is
the shape that produces it, because the fewer dimensions a metric has the fewer series it spreads
across, and a metric with no dimensions has exactly one.

Two things keep this from being a live problem, and one keeps it on the list.

**The absolute ceiling is far above any real node.** Even collapsed, the node observes 2.08M
documents a second. The demo node sustained 7,926 writes/sec; an optimistic per-node ceiling is around
70,000. That is 30x to 260x of headroom.

**The benchmark's duty cycle cannot occur in production.** Its threads do nothing but call `record`,
so they are inside the monitor essentially all the time. A real write thread spends the overwhelming
majority of its time in Lucene: at the demo's measured rate, all the time spent inside `record` adds
up to **0.32% of one core**. Two write threads contending for the same table's monitor is a
correspondingly rare event, and the collapse above requires them to contend continuously.

**But the shape is still wrong**, and it will matter on a node with many more write threads than the
eight measured here — the `write` pool is sized to the processor count, so a 64 core node has 64 of
them.

Striping by series hash does not fix it: the shape that contends worst has one series and could never
occupy more than one stripe. Striping per *thread* fixes exactly that case, and the reason it is
viable is that **the two failure modes are inverse** — the configuration that contends worst is the
cheapest to replicate per thread, and the configuration that is expensive to replicate barely contends
because its observations already spread across many series.

| configuration | series | cost of a per-thread copy at 64 threads |
|---|---|---|
| built-ins, no dimensions | 4 | 39 KB — free |
| at the 10,000 series cap | 10,000 | 97 MB against a 200 MB breaker — unacceptable |

So striping has to be bounded rather than unconditional: a table is striped or shared, decided when it
is created from that metric's cardinality in the previous bucket, with new metrics starting striped
because they start small. Above roughly 64 series it stays shared, by which point observations are
spread widely enough that the monitor is no longer hot. A cardinality spike then costs at most one
bucket at `64 series x 64 threads x 152 bytes`, about 620 KB for that metric, before it flips.
Not attempted here.

The critical section probes the table once rather than twice — it records and reads the returned sign
to learn whether a series was created, instead of asking first and then recording — which was worth
about 17% under contention.

The shape of the single-threaded table is the important part, and it is close to flat. A stream that only wants the built-in ingest metrics never reads
`_source` at all and costs essentially nothing. Every configuration that does read it pays about the same, because what dominates is
touching the source at all rather than how much of it is wanted: creating a parser and scanning the document with *nothing* configured
already costs about 1,850 bytes, which is 89% of the five-dimension figure. Adding four more dimensions and a predicate on top of one
dimension costs 216 bytes.

So the remaining floor is the parser, not anything derived metrics does with it. Getting below it would mean not parsing the source
separately at all — reusing the parse the indexing path already performs — which is a much larger change than anything here.

`DerivedMetricsBuffer` holds one table per metric per interval bucket, and within a table one accumulator slot per series. A series is
interned to a dense ordinal by `DerivedMetricsSeriesTable` and its state lives in parallel `BigArrays` arrays indexed by that ordinal —
the same shape the metric aggregations use. There is no per-series object for a scalar metric, and recording an observation against a
series that already exists allocates nothing at all, because the dimension tuple is encoded into a reusable per-thread buffer and looked
up by hash.

### What a series costs

Measured and asserted by `DerivedMetricsBufferTests#testWhatASeriesOfEachKindCosts`, so a regression fails a test rather than surfacing as
a node running out of room sooner than expected.

| metric kind | bytes per series |
|---|---|
| counter or gauge | ~119 |
| histogram, idle | ~3,459 |
| **histogram, busy** | **~5,223** |

A histogram series costs more once it is busy, and capacity planning has to use the busy number. The generator buffers raw values and only
folds them into an accumulating histogram once that buffer fills, which happens after as many observations as the series has buckets — at
the default of 160, more than sixteen observations a second on one node at a 10s interval. A series quiet enough never to fill it never
pays for the accumulator; every series in a real workload does. Both figures are asserted by tests.

A scalar series is 48 bytes of accumulator columns plus its interned dimension tuple and the hash slots that find it. A histogram series
is dominated by the distribution itself and scales with `histogram_buckets`. The two differ by a factor of about forty, which is why they
cannot share a capacity assumption: at the breaker's default of 5% of heap, a 4 GB node has room for roughly 1.7 million scalar series or
38,000 histogram series.

Bucket **counts** are held in the narrowest integer width that fits them, promoted in place from `byte` through `short` and `int` to `long`
the first time a count overflows — the approach opentelemetry-java takes. Most buckets in a real distribution hold small counts, so this
took a busy series from 7,529 bytes to 5,223.

Bucket **indices** are deliberately left as `long`. At the maximum scale an everyday value already indexes around 10¹², and a base plus
offset encoding does not rescue it either: a 32-bit offset at that scale spans a value ratio of about 1.005. Narrowing them would only pay
off for heavily downscaled histograms.

The largest remaining win is not narrowing but **lazy sizing**. Every one of these arrays is allocated eagerly at full capacity, while
published measurements of Prometheus native histograms show 1–80 populated buckets per series and a mean around 21–25, against a capacity
of 160. Growing on demand would plausibly beat what narrowing achieved, at the cost of allocating on a hot path — a separate piece of work.

Nothing is coordinated across nodes, so none of this is duplicated anywhere: each node holds only what it observed.

Because the storage comes from `BigArrays` against the `derived_metrics` circuit breaker, the memory is bounded and reportable through
`_nodes/stats/breakers`. The breaker's limit defaults to 5% of the heap, so a node with a small heap gets a proportionally small budget
without anyone configuring one.

Buckets are aligned to the epoch, so every node agrees on boundaries without coordination. `DerivedMetricsService` flushes buckets whose interval ended more than a grace period ago, emitting one
document per bucket, and emits nothing at all for intervals with no observations.

Nothing is coordinated across nodes. Each node emits partial series carrying its own `derived_metrics.node` dimension, and queries reduce
across that dimension to get stream-wide values. This is what avoids a cluster-wide hot counter on the write path.

Series count is the one thing that grows with the data, because dimension values come from documents. It is capped per node by
`data_streams.derived_metrics.max_series_per_node` and per source stream by `max_series_per_stream`. The per-stream cap exists because the
node budget would otherwise be first-come-first-served, letting one high-cardinality stream starve every other stream on the node.

Emission is bounded too. A flush converts and sends in bulk-sized chunks rather than materialising every closed bucket first, and no
more than `max_in_flight_bulks` bulks' worth of documents are outstanding at once. Without that ceiling a destination that cannot keep up
would let every flush add to a queue with nothing bounding it; documents shed for this reason are logged. The ceiling counts documents
rather than requests, because otherwise it would depend on how the documents happened to be divided up — flushing early emits a handful
at a time, and a request-based ceiling would shed almost all of them while barely any memory was actually in flight.

### Isolation

Only the observation runs on the indexing thread, because only it needs the document. It runs inside the shard's operation permit, so
anything that needs every permit — relocation hand-off, a primary-term bump, shard close — waits behind it. That is why the numbers above
matter and why the work done there is bounded: when a bucket refuses an observation under `flush_early`, the write path drains that one
bucket and nothing else, rather than walking every bucket the node holds.

Everything after that — the periodic flush, building documents, sending bulks — runs on the feature's own `derived_metrics` threadpool,
sized at an eighth of the node's processors with a bounded queue. It exists because the obvious alternative, `management`, is capped at
five threads, has an unbounded queue that never rejects, and carries dynamic mapping updates and cluster-info collection: a flush storm
there would delay work the cluster cannot afford to have delayed, and would queue indefinitely rather than shedding. Here the queue is
bounded on purpose, and what it sheds is counted. Operators can resize it through `data_streams.derived_metrics.thread_pool`.

One hop cannot be isolated: the bulk that carries the metrics is executed by the `write` pool, the same pool that served the writes being
observed. That is not a deadlock — emission is fire and forget and observation never waits on it — but it is a feedback loop, which is
what the in-flight ceiling and the indexing-pressure ceiling below are there to damp.

### Indexing pressure

Emitted bulks are charged against the same node-wide indexing pressure budget as the user writes that produced them. The
`derived_metrics` origin buys a security context, not a separate allowance, and the bypass that system indices get is a bypass of the
check rather than a budget of its own — taking it would be the opposite of isolation.

So derived metrics decline instead. Above `data_streams.derived_metrics.indexing_pressure_ceiling` of the node's budget, emission is
skipped and counted rather than competing with the writes it is measuring. Set it to `1.0` to never decline.

### Memory pressure

`data_streams.derived_metrics.memory_pressure_policy` decides what happens when the buffer can take no more, either because a series cap
was reached or because the circuit breaker refused the memory a new series needed.

`flush_early`, the default, emits what has been collected so far as a partial bucket and carries on collecting. Nothing is lost, because
partials of one bucket are reduced together at query time exactly as partials from different nodes already are. The costs are more
documents while the pressure lasts, and a timestamp that sits a few milliseconds after its bucket start rather than exactly on it.

**Which bucket is given up is chosen, not arbitrary.** The node flushes whichever bucket is holding the most memory, so relieving
pressure actually frees something; previously it flushed whichever bucket happened to receive the refused observation, which could be one
holding a single series. Size is measured in bytes rather than series, because a histogram series is worth roughly thirty scalar ones and
a table with far fewer series can be the larger one. A table reports its own size by asking the structures that hold it, so the number
cannot drift away from what is really allocated.

Scope follows the cap that refused: a node-wide refusal considers every bucket, while a stream that has spent its own per-stream share
considers only its own, since freeing another stream's memory gives it none of its share back.

Because flushing early is lossless, this decides which metric pays in **extra documents**, not which one loses data. A metric may set an
optional `preference` to bias the ranking where cost and importance disagree; a stream that configures nothing is ranked purely by size.

The offset is not cosmetic. A time series `_id` is `createId(routingHash, tsid, timestamp)` and the destination is written with
`op_type=create`, so two partials of the same series in the same bucket would produce the same `_id` and the second would be silently
rejected. That rejection is now counted as `es.derived_metrics.documents.rejected.total`; before it was logged once and otherwise
invisible, which meant the mechanism guarded against a failure nobody could detect. Partial *N* is therefore stamped at `bucketStart + N` milliseconds. The alternative — putting the partial number in a dimension —
was rejected because the tsid *is* the series identity: partials would become separate series, downsampling would keep them apart, and
tsid cardinality would grow precisely when the node is already under pressure. The offset keeps one logical series one series, keeps the
document inside the same `date_histogram` bucket. Intervals are at least one second, so up to a thousand partials fit before the offset could reach the next bucket.

The partial number is seeded per service instance from the wall clock, so a node restarting inside a bucket it had already emitted for
does not resume at zero and collide with itself. **That seeding is probabilistic, and it is worth being precise about what it does not
do.** With 128 slots, two nodes picking the same seed is likely in any cluster of a dozen or more — across 20 nodes the birthday
probability is around 78%. It is `derived_metrics.node` in the tsid that actually guarantees two nodes never collide; the offset only
separates partials emitted by the *same* node. Anyone tempted to remove the node dimension on the grounds that the offset already
prevents collisions should read that sentence twice.

The same seeding is why derived metrics output is not replayable. Re-emitting an identical aggregate produces an identical `_id`, which
`op_type=create` rejects — at-most-once, no double counting. But a replay after a restart picks a different seed, lands the same data at a
different timestamp as a different partial, and partials sum. Making the seed deterministic would fix replay and break restart, which is
the case that actually exists today; see the backfill discussion below.

`drop` discards the observation instead. Document volume stays perfectly flat and timestamps stay exactly on bucket boundaries, at the
cost of losing data. This is what Micrometer and the Prometheus Java client do, and it is the right choice for anyone aligning query
windows to bucket boundaries by hand. Either way the buffer names the setting in a warning when it happens.

### Histograms

A histogram metric emits `metric.histogram` instead of `metric.value`. The field is an `exponential_histogram`, which already carries its
own sum, count, min and max, so no scalar travels alongside it and merging partials is the field's own concern rather than something a
query has to reconstruct.

That mapper ships in `x-pack-analytics` rather than in the server, so the destination template depends on it. The plugin is always bundled
in the default distribution, and the OTel metrics templates map the same type unconditionally, so this is the same coupling those already
take on. On a build without that plugin, installing the destination template fails outright rather than only histogram metrics failing —
worth knowing, because it takes the whole feature with it.

Exemplars are not collected. There is nowhere in a time series data stream to put them today.

### What is guaranteed, and what is not

Derived metrics are best-effort telemetry about writes, not a second copy of them. Being explicit about that is more useful than implying
more:

**A hard kill loses the open interval.** The buffer is heap only and nothing is persisted. Every loss the node can *see coming* is
avoided, though: a shard flushes what it collected before it leaves the node (`beforeIndexShardClosed`, which relocation hand-off reaches
only after draining the shard's permits), and the whole buffer flushes as soon as the node is marked for shutdown in cluster state. That
last one is the latest point at which a flush can still land — by the time plugins are closed the cluster service, the indices service and
the transport service are already down, so `close()` reports what is being lost rather than firing a bulk that cannot arrive.

**Counting is at-least-once, not exactly-once, for a few paths.** Recovery replay is not one of them: replicas, peer recovery, local
translog replay and engine resets all carry a non-primary origin and are ignored, so a restarted node does not re-count. But an update with
`retry_on_conflict` is applied on the primary once per attempt, and each attempt is observed; a coordinating retry after a primary failover
is explicitly flagged as *possibly already executed* and is observed again on the new primary; and when a whole batch throws, every
operation in it is reported as failed, including ones the engine never attempted. All three inflate failure-triggered metrics rather than
success-triggered ones. A CCR follower also replays leader operations as primary writes, so a follower stream with derived metrics
configured counts them as its own.

**Everything shed is counted and published.** Series dropped at a cap, documents dropped for backpressure or indexing pressure, buckets
flushed early, bulks that failed, and series lost because they could not be flushed in time are all `es.derived_metrics.*` metrics, not
just log lines.

### The destination describes itself

Every emitted document carries `derived_metrics.reduction` — `sum`, `rate`, `min`, `max`, `avg` or
`histogram`. Without it, how to combine `metric.value` across nodes and buckets would be
knowable only from the source stream's configuration, and a consumer that guessed wrong would be
wrong invisibly. It is a dimension but adds no cardinality, since the reduction is functionally
determined by `metric.name`.

The rule it encodes:

| reduction | combine across nodes and buckets with |
|---|---|
| `sum`, `rate` | `SUM(metric.value)` |
| `min` | `MIN(metric.value)` |
| `max` | `MAX(metric.value)` |
| `avg` | `SUM(metric.value) / SUM(metric.count)` |
| `histogram` | any aggregation on `metric.histogram` |

### Capacity planning

Derived document volume does not depend on write rate. It is:

```
derived docs/sec  =  series × nodes ÷ interval
```

Series is the number of distinct dimension combinations a metric sees; nodes appears because each
node emits its own partial for every series it observes, which is what keeps the write path
coordination-free. So the reduction ratio is:

```
reduction  =  write rate ÷ (series × nodes ÷ interval)
```

It improves linearly with write rate, degrades linearly with fleet size, and improves linearly with
interval. **The feature is insensitive to volume and sensitive to cardinality.**

**Measured inputs**, all from the demo in `derived-metrics-demo` on one node with a 512 MB heap:

| | value | configuration |
|---|---|---|
| derived docs | 47/sec/node | 12 metrics, 4 dimensions, one histogram |
| | 3/sec/node | 3 metrics, 1 dimension |
| observation cost | 398 ns, 160 B | built-in ingest metrics, no dimensions |
| | 2,473 ns, 2,328 B | five dimensions plus a predicate |
| series memory | 152 B | counter or gauge |
| | 4,616 B | histogram |
| reduction | 21× / 348× | rich / lean, at ~4k writes/sec |

The property that matters was measured directly: across a load cycle the source rate varied **40.6×**
while the derived rate varied **1.2×**.

**Per-node overhead is small.** At 10,000 documents/sec with five dimensions, observation costs about
2.5% of one core and allocates ~23 MB/sec. Series state is negligible until cardinality is large:
100,000 scalar series is 15 MB, against a breaker defaulting to 5% of heap.

**A worked example, with its extrapolation flagged.** At 1M documents/sec:

- One demo node sustained **7,926 writes/sec at 11% CPU** on 12 cores with a 512 MB heap — and the
  load generator was competing for those cores. That is the only per-node throughput figure here that
  was measured.
- Extrapolating linearly gives a ceiling near 70,000 writes/sec/node, so 1M/sec is **tens of nodes,
  not hundreds**. Treat this as an order of magnitude only: GC, merge and refresh pressure do not
  scale linearly, a 512 MB heap would bind before CPU does, these are ~400-byte documents with a
  simple mapping, and there are no replicas.
- At ~15 nodes and the rich configuration, derived volume is roughly 700 docs/sec against 1M — a
  reduction in the low thousands, an order of magnitude better than the demo's 21× because the
  numerator grows and the denominator does not.

**What breaks first is cardinality, not volume.** A dimension like `host.name` or `pod` across a
large fleet multiplies series directly. The caps and the breaker hold, but under `flush_early` memory
pressure becomes document count, which erodes the reduction exactly when it is most wanted. Budget
dimensions by their cardinality product, not by how many there are.

Two other things to size at that scale: the destination's shard count, which defaults to one and is
overridable through `derived-metrics@custom`, and `max_in_flight_bulks`, since every node flushes its
whole series set once per interval.

**Node identity churns, and it is a dimension.** `derived_metrics.node` is a tsid component, so every
value it has ever taken is a distinct set of series — one per series the node emitted. It carries the
node's persistent ID rather than its name, which is strictly better: `node.name` is typically the pod
name in a containerised deployment and changes on every restart and every scale event, whereas the ID
is written into the data path and survives a restart wherever that data path does. On a fully
ephemeral node it churns exactly as the name would.

The consequence is worth stating plainly rather than hoping it away: at daily pod churn over 30 days
of retention, the destination holds 30× the series the configuration implies, and 29/30 of them are
dead. Three things bound the damage. Retention is the main one — the destination's own lifecycle
sweeps old series out, which is why the destination is given a lifecycle at creation rather than left
to grow. Dead series cost index size but not memory on the write path, since the buffer only ever
holds the series *this* node is currently emitting. And a `date_histogram` over `metric.value`
grouped by anything other than node is unaffected: the dead series simply contribute nothing to
buckets after they stopped.

Whether node identity belongs in the tsid at all is a real design question and is deliberately left
open — but one argument for removing it has been checked and does not hold.

The dimension exists so that two nodes emitting the same series in the same bucket do not collide on
the deterministic `_id`. The partial-offset mechanism looks like it addresses the same collision, so
the tempting conclusion is that the dimension is now redundant. **It is not.** The offset is seeded
from the wall clock into 128 slots, which separates partials from one node reliably but separates two
*different* nodes only by chance — across 20 nodes the odds that some pair collides are around 78%.
The node dimension is what actually guarantees it.

Removing the dimension would end the churn and let partials of one series from different nodes share
a series. It would also cost per-node attribution in queries, and it would first require the offset to
separate nodes deterministically — which is a different scheme than the one that exists. Not attempted
here, and not to be attempted on the assumption that the offset already covers it.

### Designing against bad cardinality

Cardinality is this feature's weak point, so it is worth being explicit about what can and cannot be done about it.

**It cannot be validated at configuration time.** Nothing in a template knows how many distinct values `user.id` will take. The
configuration limits that do exist — 16 global dimensions, 16 per-metric dimensions, 64 user metrics, 8 destinations — bound the *number*
of dimensions and say nothing about the cardinality of their values. Any design that claims to catch a bad dimension before it has seen
data is theatre.

So the work divides into making the runtime signal attributable, and degrading gracefully rather than failing.

**Two budgets, not one, and they measure different things.** The series caps are a *cardinality* guard: every series, scalar or histogram,
becomes exactly one time series in the destination and costs one tsid. The circuit breaker is a *memory* guard, and there a histogram
series is worth about thirty scalar ones.

It is tempting to weight the series cap by memory so that a histogram series counts for more. That would be wrong: those series cost
exactly as many tsids as scalar ones, so weighting a cardinality limit by per-series bytes conflates two orthogonal resources. Memory
asymmetry belongs to the breaker and to what gets shed first. This matches how the ecosystem separates the two — OpenTelemetry caps
cardinality through `aggregation_cardinality_limit` and memory through `MaxSize`, and Mimir caps `max_global_series_per_user` and
`max_native_histogram_buckets` independently. Nobody weights a cardinality limit by per-series cost.

**Which cap binds first is heap-dependent, and that is worth stating rather than assuming.** At the default cap of 10,000, a full
complement of *busy* histogram series is about 52 MB. The breaker is 5% of heap, so the cap binds first only above roughly a 1 GB heap.
Below that — small deployments, development clusters — the breaker binds first, and the refusal is reported as
`series.dropped.breaker` rather than `series.dropped.node_cap`, which sends an operator looking in the wrong place.

Note also that the product of `max_series_per_node` and `histogram_buckets` is not validated against the breaker limit. Both settings are
independently in range at, say, 10,000 series and 640 buckets, and together they ask for more than a 4 GB node's breaker allows.

**What the runtime now tells you.** Refusals are counted by cause rather than lumped together: `series.dropped.node_cap` and
`series.dropped.stream_cap` say whether to raise the budget or go and find the stream, and `series.dropped.breaker` says the problem is
memory rather than cardinality. `observations.skipped.missing_value` catches the case that used to be silent — a metric configured against
a field that does not exist, which is what a misspelled field name looks like and which otherwise emits nothing forever with no signal.

**Not built, and worth knowing why.** Two things would materially improve this and are deliberately deferred:

- *Which dimension is the problem* is still unanswerable. A `HyperLogLogPlusPlus` per (metric, dimension) would answer it directly — it is
  breaker-aware, `BigArrays`-backed, and starts in a cheap linear-counting mode, about 256 bytes per sketch at `p=8`, so 64 metrics by 16
  dimensions is a quarter of a megabyte. The reason to wait is that nothing surfaces it yet.
- *A stats API*. There is none: every counter is node-wide with no attributes, so nothing breaks down by metric, stream or project.
  `RestDataStreamLifecycleStatsAction` in the same module is the precedent.

A third idea is recorded here because it is the right shape and not obvious. Elasticsearch already prefers graceful degradation to
rejection for mapping explosions: `index.mapping.total_fields.ignore_dynamic_beyond_limit` drops the *field* and keeps indexing the
document. The analogue would be to collapse a runaway dimension to a placeholder rather than dropping the metric — the metric stays
bounded and aggregable, and only the breakdown by the offending dimension is lost. That is a much better failure mode than one dimension
starving every other metric on the node through a shared cap.

### Backfilling history

Derived metrics only ever sees writes as they happen, so it cannot produce metrics for data that arrived before it was configured. This is
the clearest thing a transform still does better, and a backfill is feasible — the seam is shallow, since
`DerivedMetricsService.record(...)` is the single entry point and the indexing listener is a thin adapter over it.

The reusable pieces already exist. `ReindexDataStreamPersistentTaskExecutor` is the closest structural template: a persistent task
orchestrating per-index sub-jobs, with a durable cursor, a status API and cancellation — the two-level shape a data stream backfill wants.
Downsampling is the closest analogue for the scan itself, and its entire resume cursor is a single tsid in cluster state, because
`TimeSeriesIndexSearcher` gives it a total order to resume from.

Two prerequisites, and the first is the interesting one.

**The backfill must number its own partials.** Live emission seeds partial numbering from the wall clock so that a node restarting inside a
bucket does not collide with what it already emitted. That seeding is what makes output non-replayable: the same data processed twice lands
at different timestamps as different partials, and partials sum. The instinct is to make the seed deterministic — and it is wrong, because
a deterministic seed makes a restarting node collide with itself and lose that data, trading a problem that exists today for one that does
not. The two jobs are separable. A backfill processes a closed range in one pass and is not responding to memory pressure, so it can emit
exactly one partial per series and bucket at offset zero, replay-safe by construction, with live emission untouched.

**`record` would need to accept raw source.** It takes a `ParsedDocument` today but only uses it for `source().parser(...)` and
`estimatedSizeInBytes()`; every bulk reader produces `BytesReference` and an `XContentType`.

One thing to design around rather than discover: slices run on different nodes, and series state is node-local and node-capped. Slicing by
document multiplies memory without splitting series correctly, so a backfill has to partition by dimension hash.

Worth being honest that this changes what the feature is. Today it is best-effort telemetry about writes. A backfill makes it a
recomputable aggregate, which is a different and stronger promise.

### Retention

Each destination is given a lifecycle once, when it is first created, from the `destinations` entry for its interval. Without one it
falls back to the cluster-wide `data_streams.lifecycle.retention.default`, and to 30 days when that is unset, so a destination is never
unbounded by accident.

The lifecycle is applied once and never reconciled: changing `destinations` does not alter destinations that already exist, and a
lifecycle edited by hand on a destination is left alone. See `docs/internal/DerivedMetricsLifecycleDesign.md`.

### Settings

| setting | default | meaning |
|---|---|---|
| `data_streams.derived_metrics.flush_interval` | `1s` | how often closed buckets are emitted |
| `data_streams.derived_metrics.flush_grace_period` | `5s` | how long a bucket stays open past the end of its interval |
| `data_streams.derived_metrics.max_series_per_node` | `10000` | per-node series cap |
| `data_streams.derived_metrics.max_series_per_stream` | the node cap | per-source-stream cap, so one stream cannot spend the whole node budget |
| `data_streams.derived_metrics.bulk_size` | `1000` | documents per bulk request to the destination |
| `data_streams.derived_metrics.max_in_flight_bulks` | `8` | ceiling on emission outstanding at once, counted as this many `bulk_size` documents |
| `data_streams.derived_metrics.thread_pool.size` | an eighth of the node's processors | size of the feature's own pool |
| `data_streams.derived_metrics.thread_pool.queue_size` | `128` | bounded on purpose, so backlog is shed and counted |
| `data_streams.derived_metrics.indexing_pressure_ceiling` | `0.7` | share of the node's indexing budget above which emission is skipped |
| `data_streams.derived_metrics.histogram_buckets` | `160` | bucket capacity of each histogram series, trading precision against size |
| `data_streams.derived_metrics.memory_pressure_policy` | `flush_early` | `flush_early` emits a partial bucket and keeps collecting; `drop` sheds the observation |

## Managed Destination

Each source data stream writes to one hidden time series data stream per interval it uses,
`derived-metrics-<source data stream>-<interval>`. Splitting per interval is what makes retention resolution-dependent, and it means a
query over one destination never has to filter by interval. It is created on demand
by the first metric document written to it, backed by the managed `derived-metrics@template` index template that Elasticsearch installs
in every project. `derived-metrics-*` is therefore a reserved namespace: a user data stream with that prefix would be captured by the
managed template.

The destination is hidden rather than dot-prefixed, so it stays out of ordinary wildcard expressions while remaining directly queryable.

Because the name is a concatenation rather than a hash, a long source stream produces a destination whose backing indices would exceed the
255-byte index name limit. `derived-metrics-`, the interval suffix and the `.ds-<date>-<generation>` a backing index adds leave roughly 213
bytes for the source name at a 10s interval, and fewer at a longer one. Enabling derived metrics on a stream that does not fit is rejected
by `PUT _data_stream/<name>/_options` with the number of bytes to remove, rather than being accepted and then silently emitting nothing
forever. Templates match patterns rather than names, so this cannot be caught at template definition time.

Emitted documents look like this:

| field | type | meaning |
|---|---|---|
| `@timestamp` | `date` | start of the interval bucket |
| `metric.name` | keyword dimension | the metric name |
| `metric.value` | `double`, gauge | this node's partial value for the interval; for an `avg` gauge this is the **sum** |
| `metric.count` | `long`, gauge | observation count, present only on `avg` gauges |
| `metric.histogram` | `exponential_histogram` | the whole distribution; present on histogram metrics **instead of** `metric.value` |
| `derived_metrics.source` | keyword dimension | the source data stream |
| `derived_metrics.interval` | keyword dimension | the interval, matching the destination's suffix |
| `derived_metrics.node` | keyword dimension | the emitting node's persistent ID |
| `derived_metrics.node_name` | keyword, **not** a dimension | the emitting node's name, for legibility |
| `derived_metrics.reduction` | keyword dimension | how to combine `metric.value`; see [The destination describes itself](#the-destination-describes-itself) |
| `dimensions.*` | keyword dimensions | user dimensions, dynamically mapped |

A user dimension `service.name` is written as `dimensions.service.name`. Dimensions a document does not have are simply absent, rather
than filled with a placeholder value.

`metric.value` is a gauge even for counter metrics, because what is emitted is a per-interval partial with no counter reset semantics to
preserve. Counters are summed at query time.

Elasticsearch controls the destination entirely. Users do not configure `data_stream.dataset`, `data_stream.namespace`, the destination
index mode, the routing path, or any internal dimension field. Those are either constant for a one-source destination or managed
internally for compatibility and future multi-source policies.

## Built-In Ingest Metrics

`ingest.*` expands to:

- `ingest.docs.count`
- `ingest.docs.rate`
- `ingest.bytes.count`
- `ingest.bytes.rate`
- `ingest.failures.count`
- `ingest.failures.rate`

Counts are emitted as the sum of what the node observed during the interval. Rates are that same sum divided by the interval length in
seconds. Both are partials: a stream-wide value is the sum across the emitting-node dimension, which for a rate is meaningful because
every partial covers the same interval.

`ingest.docs.*` and `ingest.bytes.*` count successful writes; `ingest.failures.*` counts failed ones. Byte counts come from the size of
the document's source. Global dimensions apply to the built-ins as well as to user metrics.

### Averages

An `avg` gauge does **not** emit the mean. It emits its sum in `metric.value` and the observation count in `metric.count`, so the mean is
`SUM(metric.value) / SUM(metric.count)`.

This matters because a mean cannot be re-aggregated. Averaging per-interval means weights every interval equally regardless of how busy
it was, which on a stream whose busy intervals differ from its quiet ones reads 30–50% low — measured at 131ms against a true 186ms in
one sample and 81ms against 156ms in another.

## Query Examples

Counter:

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "metric.name": "http.requests" } },
        { "term": { "derived_metrics.interval": "1m" } }
      ]
    }
  },
  "aggs": {
    "per_minute": {
      "date_histogram": {
        "field": "@timestamp",
        "fixed_interval": "1m"
      },
      "aggs": {
        "requests": {
          "sum": {
            "field": "metric.value"
          }
        }
      }
    }
  }
}
```

Gauge:

```json
{
  "query": {
    "term": {
      "metric.name": "queue.depth"
    }
  },
  "aggs": {
    "by_service": {
      "terms": {
        "field": "service.name"
      },
      "aggs": {
        "depth": {
          "max": {
            "field": "metric.value"
          }
        }
      }
    }
  }
}
```

Histogram:

```json
{
  "query": {
    "term": {
      "metric.name": "http.request.duration"
    }
  },
  "aggs": {
    "latency": {
      "histogram": {
        "field": "metric.histogram"
      }
    }
  }
}
```

