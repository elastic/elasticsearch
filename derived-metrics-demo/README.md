# Derived metrics demo

A local playground for the derived metrics feature. One command builds and starts Elasticsearch from
this checkout, runs Kibana in a container against it, configures a data stream with derived metrics,
and indexes documents at a rate that deliberately swings up and down.

It sets up **two** source streams that receive identical documents but ask for very different amounts
of derived data, so you can see what configuration costs.

The thing to watch: **the source write rate varies by orders of magnitude, and the derived document
rate does not move.** The derived metrics still track the load, because the load is in the metric
*values*, not in the number of documents.

## Requirements

- A JDK and the usual Elasticsearch build prerequisites (this uses `./gradlew run`).
- Docker, for Kibana. On macOS the script starts Docker Desktop if it is not running.
- Python 3.9+ for the load generator. No packages needed, standard library only.

## Usage

```bash
cd derived-metrics-demo
./demo.sh up        # build + start ES, start Kibana, configure the stream, start indexing
./demo.sh status    # source rate vs derived rate, and the current metric values
./demo.sh health    # what the feature costs the node: its breaker and its threadpool
./demo.sh logs      # follow the load generator
./demo.sh eslogs    # follow Elasticsearch
./demo.sh down      # stop everything
```

The first `up` can take a while if the distribution is not built yet. Everything after that is fast.

The node runs from a real tar distribution, not `./gradlew run`, and gets its own copy of it under
`.run/elasticsearch` so gradle never owns the running cluster. That is not incidental: `./gradlew
run` caps the JVM at two processors and enables assertions and paranoid Netty leak detection, which
together starve the collector badly enough that this demo's 4,000/s spike OOMs a 512 MB node within
minutes. On a real distribution the same heap and the same spike are comfortable — see
`ES_HEAP` in `config.env`.

To skip Kibana entirely: `KIBANA=skip ./demo.sh up`.

| | |
|---|---|
| Elasticsearch | <http://localhost:9200> — `elastic-admin` / `elastic-password` |
| Kibana | <http://localhost:5601> — same credentials |
| rich source stream | `logs-derived-demo-default` |
| lean source stream | `logs-derived-lean-default` — the same documents, far less configured |
| derived metrics | `derived-metrics-<source>-10s` (hidden), one per source |
| dashboards | [rich](http://localhost:5601/app/dashboards#/view/derived-metrics-demo-dashboard) · [lean](http://localhost:5601/app/dashboards#/view/derived-metrics-demo-dashboard-lean) |

## What `./demo.sh status` shows

```
  Documents per 10s bucket over the last 6m
    source   ▂▂▃▃▃▄███▇▆▄▃▂▂▂▂▂▂▂▂▂▁▁▁▁▁▁▁▁▁▁▁▁▁  min     192  max  38,818
    derived  ▇████████████▇▇▇▇▇▇▇▇▇▅▆▆▆▆▇▆      min     262  max     420

    source rate varied by   202.2x
    derived rate varied by    1.6x   <- this is the point

  Peak value per metric over the last 6m (10s interval, summed across nodes)
    http.requests                   38,818.00   (3632 series-buckets)
    http.errors                      8,822.00   (1525 series-buckets)
    ingest.docs.count               38,892.00   (928 series-buckets)
    queue.depth.max                  3,530.00   (928 series-buckets)
    ...
```

The source swung by 202x across the load cycle; the derived document count stayed within 1.6x. The
derived count is a function of *configuration* — metrics × intervals × dimension combinations — not
of write volume. The residual is dimension combinations coming and going: the `http.errors`
series only exist while something is returning 5xx.

If a phase is quiet enough that an interval sees no writes at all, that interval emits nothing rather
than a zero, and `status` says so.

## Two configurations, one input

Every document is written to **both** source streams. They differ only in what they ask for:

| | rich (`logs-derived-demo-default`) | lean (`logs-derived-lean-default`) |
|---|---|---|
| built-ins | all three `ingest.*` | `ingest.docs.count` only |
| dimensions | `service.name`, `cloud.region` | `service.name` |
| user metrics | seven | two |
| **derived documents** | **~350 per 10s** | **~26 per 10s** |

Same write volume, ~13x less derived data. Derived cost is a function of metrics x dimensions x
intervals, and nothing else — which is exactly the knob you have.

`./demo.sh status` prints that comparison, and the lean dashboard leads with it.

The lean stream keeps one dimension, so its metrics are still broken down per service across ten
services, plotted as a line per service in a single graph.

## Looking at it in Kibana

`./demo.sh up` builds two dashboards. **Derived metrics — derived vs source** covers the rich stream;
every row asks the same
question twice: the **left** panel answers it from the derived metrics, the **right** panel answers it
from the raw data stream. Reading down:

| rows | metric | source-side equivalent |
|---|---|---|
| 1 | documents observed | `COUNT(*)` written — should match |
| 2 | documents stored to answer that | `COUNT(*)` — should not match |
| 3 | ingest rate, docs/sec | count per bucket / 10 |
| 4 | ingest throughput MB/sec | none — `_source` size is not queryable |
| 5 | failed writes | none — a failed write leaves nothing behind |
| 6 | HTTP requests | count where method exists |
| 7 | 5xx errors | count where status >= 500 |
| 8 | 4xx client errors | count where 400 <= status < 500 |
| 9 | response payload MB | `SUM(http.response.body.bytes)` |
| 10 | peak queue depth | `MAX(queue.depth)` |
| 12 | mean latency (weighted) | `AVG(event.duration)` |

Rows with no source equivalent span the full width. Row 12 weights the `avg` gauge by
`ingest.docs.count`; the naive `AVG` of an avg gauge is biased, see section 5 of `compare.console`.

It opens on **now-15m to now**, refreshing every 10 seconds.

Expect the derived side to sit slightly below the source side. Two separate things cause that, and
only one of them is constant:

**The unflushed tail — constant in time, variable in documents.** A metric document cannot exist
until its interval is over *and* has been flushed: 10s interval + 2s grace + up to 1s flush tick,
about 13 seconds here. Whatever the source stream received in those 13 seconds is counted on the
right and cannot yet be counted on the left. The lag is always ~13s, but 13s is 260 documents during
the quiet phase and 52,000 during the spike.

**The leading edge — variable.** Derived documents are stamped at the *start* of their interval. When
a window begins at an arbitrary moment, the interval straddling that moment is stamped before the
window and so is excluded whole, while the source contributes the fraction of it that falls inside.
How many documents that is depends on the write rate at that instant, which swings 200x across the
load cycle.

That second term is why lengthening the window changes the gap. Measured on a running demo:

| window ending at now | gap, unaligned start | gap, start aligned to 10s |
|---|---|---|
| 5m | 1,152 docs | 425 docs |
| 15m | 9,532 docs | 378 docs |
| 25–30m | 13,748 docs | 463 docs |

Align the leading edge and the gap collapses to a constant — the tail, and nothing else. That is what
`compare.console` and `./demo.sh window` do, and why they agree to ~0.15%.

`dashboard.py` regenerates both — edit that file and re-run `./demo.sh setup` to change the panels.

The second dashboard, **Derived metrics — lean configuration, and what it saves**, leads with the cost
comparison above and then shows the lean stream's own metrics, each split per service in one graph.

Four data views are created for you — a source and a derived one per stream, named `demo …` and
`lean …`. The derived ones need `allowHidden`, which `setup.sh` sets, because the destinations are
hidden data streams.

`queries.esql` in this directory has a dozen ready-made ES|QL queries, all verified against a running
demo. Paste them into Discover in ES|QL mode.

In Discover on *demo derived metrics*, useful things to break down by:

- `metric.name` — which metric a document belongs to.
- `derived_metrics.interval` — `10s` here. The demo configures a single interval to keep things
  simple; add more in `setup.sh` and every query then needs to filter on one of them, or it sums the
  same data at two resolutions.
- `derived_metrics.node` — the emitting node's persistent ID. Each node emits its own partial; sum across this
  dimension for a stream-wide value.
- `dimensions.*` — the configured user dimensions.

For a chart in Lens: filter `metric.name: ingest.docs.count`, then plot **sum of `metric.counter`**
over `@timestamp`, divided by 10 for a per-second figure. That is the source stream's ingest rate,
reconstructed from a handful of documents per interval. Put a count of the source data stream next to
it and the two lines track each other.

Which field to plot follows the metric type, because a field carries exactly one `time_series_metric`:
a counter is in `metric.counter`, a gauge in `metric.value`, a histogram in `metric.histogram`. Every
document carries `derived_metrics.reduction` so you can tell which without looking up the
configuration.

Good comparisons to try:

| filter | shows |
|---|---|
| `metric.name: ingest.docs.count` | write rate, tracking the load generator's phases |
| `metric.name: http.errors` | 5xx counter, spikes during the `spike` phase |
| `metric.name: queue.depth.max` | gauge, `max` per interval |
| `metric.name: event.duration.avg` | gauge, `avg` per interval |

## The load profile

`loadgen.py` cycles through phases, and the error ratio moves with them so the error counter and the
latency gauge spike together with the write rate:

| phase | duration | rate | 5xx |
|---|---|---|---|
| calm | 60s | 100/s | 1% |
| ramp up | 90s | 100 → 1500/s | 2% |
| spike | 30s | 4000/s | 25% |
| recovery | 45s | 4000 → 200/s | 10% |
| steady | 90s | 400/s | 2% |
| quiet | 30s | 20/s | 0% |

Other profiles: `LOAD_PROFILE=flat` or `LOAD_PROFILE=spiky ./demo.sh up`.

## Configuration

Everything is in `config.env` and every value is overridable from the environment. The derived metrics
themselves — intervals, dimensions, and the user metrics — are defined in `setup.sh`. Edit that file
and re-run `./demo.sh setup`: it rewrites the index template *and* applies the configuration to the
running data stream through the options API, since options inherited from a template only take effect
when the stream is created. The runtime picks the change up on the next cluster state update, with no
restart.

The demo runs Elasticsearch with a 1s flush interval and a 2s grace period, rather than the 1s/5s
defaults, so changes show up quickly. Everything else is left at its default, which is the point:
the feature's own threadpool, its circuit breaker at 5% of heap, its series caps, the
`flush_early` memory pressure policy and the indexing pressure ceiling all apply here exactly as
they would in production. `./demo.sh health` shows the two that move.

## Notes

- **Elasticsearch binds `http.host` to `0.0.0.0`,** because the Kibana container reaches it through
  `host.docker.internal`. That means the dev cluster is reachable from your network, protected only
  by the dev password. Set `ES_BIND_HOST=127.0.0.1 KIBANA=skip` if that matters to you.
- The transport layer stays on loopback, which keeps the node out of production bootstrap checks.
- `KIBANA_IMAGE` must match `build-tools-internal/version.properties`. It is pinned to
  `9.6.0-SNAPSHOT` here.
- **Machine learning is disabled** (`xpack.ml.enabled=false`). A trial licence otherwise creates a
  default ELSER inference endpoint and deploys the model, which downloads it and indexes it in
  chunks — hundreds of megabytes through the heap of a dev node that has a few hundred megabytes to
  work with. It OOMs within minutes, and it has nothing to do with derived metrics.
- The `exponential_histogram` mapper the destination uses for histogram metrics ships in
  `x-pack-analytics`, which the default distribution always bundles. Nothing to install.
- State lives in `.run/` — logs and pid files. `./demo.sh down` cleans up the processes; the data
  directory is managed by `./gradlew run` and is wiped on each start.
