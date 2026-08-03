/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;

/**
 * Node-local state for derived metrics: one table per metric per interval bucket, and within a table one accumulator slot per series.
 *
 * <p>This is what makes derived document intake independent of write volume. However many documents a stream receives, a node only ever
 * holds one accumulator per (source stream, metric, interval, dimension combination) and emits one document for it per interval. Nothing
 * is coordinated across nodes; each node emits its own partial series and queries reduce across the emitting-node dimension.
 *
 * <p>Series identity is interned to a dense ordinal inside {@link DerivedMetricsSeriesTable}, so the only thing allocated per document is
 * nothing at all once the series exists: the dimension tuple is encoded into a caller-owned scratch buffer and looked up by hash. All
 * storage comes from {@link BigArrays} against the derived metrics circuit breaker, so the memory is accounted and visible.
 *
 * <p>Series count is the one thing that grows with the data, since dimension values come from documents. It is capped per node and per
 * source stream — the per-stream cap exists because a single node budget is first-come-first-served, and lets one high-cardinality
 * stream starve every other stream.
 *
 * <p>A bucket is held as a {@link DerivedMetricsStripedTable}, which is either one table shared by every write thread or one per thread,
 * decided when the bucket is created. See {@link #stripesFor} for what decides it and that class for why the choice has to be made at all.
 */
public class DerivedMetricsBuffer implements Releasable {

    /**
     * Identifies one table: every series of one metric, in one interval bucket. Dimensions are deliberately absent — they identify a
     * series <em>within</em> a table, and keeping them out means this key is built once per bucket rather than once per document.
     */
    public record TableKey(
        ProjectId project,
        String sourceDataStream,
        CompiledMetric metric,
        long bucketStartMillis,
        long intervalMillis
    ) {}

    /**
     * One drained table, together with which partial of its bucket it is. A bucket is normally emitted once, as partial zero; it is
     * emitted more than once only when memory pressure forced it out early, and the emitter uses the partial number to keep the documents
     * from colliding.
     *
     * <p>The caller owns the table and <em>must</em> close it, or its circuit breaker accounting leaks.
     */
    public record Drained(TableKey key, DerivedMetricsSeriesTable table, int partial) {}

    /**
     * What became of one observation. Which cap refused it decides what would actually help: giving up the biggest table anywhere on the
     * node frees the node budget, but a stream that has spent its own share is only helped by giving up one of <em>its</em> tables.
     */
    public enum Outcome {
        RECORDED,
        REFUSED_NODE_CAP,
        REFUSED_STREAM_CAP,
        REFUSED_HISTOGRAM_CAP,
        REFUSED_BREAKER;

        public boolean recorded() {
            return this == RECORDED;
        }
    }

    /** Identifies a source data stream within its project, which is what the per-stream budget is keyed on. */
    public record StreamKey(ProjectId project, String sourceDataStream) {}

    /**
     * What one dimension of one metric has been seen to hold. This is the answer to the question the series caps could never answer:
     * series were refused, but by <em>which</em> dimension.
     *
     * @param estimatedValues approximate distinct values, within a few per cent — see {@link DerivedMetricsDimensionCardinality}
     * @param collapsed       whether the metric has stopped breaking down by this dimension because it outgrew its budget, in which case
     *                        the estimate rests at roughly that budget rather than continuing to climb
     */
    public record DimensionCardinality(
        ProjectId project,
        String sourceDataStream,
        String metric,
        String dimension,
        long estimatedValues,
        boolean collapsed
    ) {}

    private static StreamKey streamKey(TableKey key) {
        return new StreamKey(key.project(), key.sourceDataStream());
    }

    /**
     * What one metric of one stream is holding on this node right now, summed across every bucket of it that is still open.
     *
     * <p>Built by walking the tables when someone asks, never kept: the numbers are already maintained for the shedding decision, so
     * reporting them costs a walk of a map with one entry per live bucket and nothing at all on the write path.
     *
     * @param seriesHeld a series interned by two stripes of a striped bucket counts twice, because it really is held twice — this is the
     *                   memory the metric is responsible for rather than the series a query of the destination would see
     */
    public record MetricSnapshot(
        ProjectId project,
        String sourceDataStream,
        String metric,
        String interval,
        long seriesHeld,
        long bytesHeld,
        boolean histogram
    ) {}

    /**
     * Series one source data stream has had refused, by which budget refused them. The node-wide totals say the node ran out; this says
     * <em>which stream</em> spent it, which is the difference between an operator raising a setting and an operator fixing a stream.
     */
    public record StreamRefusals(
        ProjectId project,
        String sourceDataStream,
        long atNodeCap,
        long atStreamCap,
        long atHistogramCap,
        long atBreaker,
        long bucketsDropped
    ) {
        boolean any() {
            return atNodeCap > 0 || atStreamCap > 0 || atHistogramCap > 0 || atBreaker > 0 || bucketsDropped > 0;
        }
    }

    /** The mutable form of {@link StreamRefusals}. Only ever touched when an observation was already being refused. */
    private static final class Refusals {
        private final LongAdder atNodeCap = new LongAdder();
        private final LongAdder atStreamCap = new LongAdder();
        private final LongAdder atHistogramCap = new LongAdder();
        private final LongAdder atBreaker = new LongAdder();
        private final LongAdder bucketsDropped = new LongAdder();
    }

    /**
     * Where refusals go for a stream past {@link #MAX_TRACKED_STREAMS}. Shared and never read, so the counting is thrown away rather than
     * conditional — the alternative is a null check on a path that is already the degraded one.
     */
    private static final Refusals UNTRACKED = new Refusals();

    /**
     * How many source data streams the per-stream refusal breakdown is kept for. Each entry is four {@link LongAdder}s, and unlike the
     * per-stream budgets these are never forgotten, because a total that resets when a stream briefly holds nothing would be worse than
     * no total at all. A node whose streams are refused in the thousands has a problem the node-wide counters already report.
     */
    static final int MAX_TRACKED_STREAMS = 1_024;

    /**
     * Identifies one metric of one stream across every bucket it will ever have, which is the scope the cardinality memory behind
     * {@link #stripesFor} is kept at.
     *
     * <p>By name rather than by the compiled metric, unlike {@link TableKey}: a configuration change recompiles every metric into fresh
     * objects, and what a metric's cardinality turned out to be last interval is still true of the metric that replaced it.
     */
    private record MetricKey(ProjectId project, String sourceDataStream, String metric, long intervalMillis) {}

    /**
     * The bucket starts a metric currently holds. Small, bounded by {@code maxIntervalBuckets}, and guarded by its own monitor because
     * admitting an observation has to decide and act atomically.
     */
    private static final class OpenBuckets {
        private final long[] starts;
        private int size;
        /**
         * Wall clock at the last bucket this metric opened, which is how long its cardinality is remembered for after it goes quiet. A
         * bucket is opened at most once per interval and the memory is measured in intervals, so there is nothing finer to gain by
         * updating it per observation, and doing so would put a shared write on the hot path.
         */
        private volatile long lastWrittenMillis;

        OpenBuckets(int capacity) {
            this.starts = new long[capacity];
        }

        /**
         * @return the bucket start that had to be given up to make room, or {@link Long#MIN_VALUE} when none did
         */
        synchronized long admit(long bucketStart) {
            for (int i = 0; i < size; i++) {
                if (starts[i] == bucketStart) {
                    return Long.MIN_VALUE;
                }
            }
            if (size < starts.length) {
                starts[size++] = bucketStart;
                return Long.MIN_VALUE;
            }
            // Every slot is taken, so the oldest gives way. It is emitted rather than discarded, which is what makes an unbounded spread
            // of timestamps cost extra documents rather than lost observations.
            int oldest = 0;
            for (int i = 1; i < size; i++) {
                if (starts[i] < starts[oldest]) {
                    oldest = i;
                }
            }
            long evicted = starts[oldest];
            starts[oldest] = bucketStart;
            return evicted;
        }

        synchronized void release(long bucketStart) {
            for (int i = 0; i < size; i++) {
                if (starts[i] == bucketStart) {
                    starts[i] = starts[--size];
                    return;
                }
            }
        }

        synchronized boolean holdsAnything() {
            return size > 0;
        }

        void touch(long nowMillis) {
            lastWrittenMillis = nowMillis;
        }

        long lastWrittenMillis() {
            return lastWrittenMillis;
        }
    }

    private static MetricKey metricKey(TableKey key) {
        return new MetricKey(key.project(), key.sourceDataStream(), key.metric().name(), key.intervalMillis());
    }

    /**
     * How many distinct series one metric was last seen holding, and the bucket that was measured in. The bucket travels with it so that a
     * spike is remembered at its highest within a bucket rather than being overwritten by whatever the last partial of it happened to
     * hold, and so that a stream that stopped writing can be forgotten.
     */
    private record Cardinality(long bucketStartMillis, long series) {}

    /**
     * The per-stream budget for a table's stream, creating it if this is the first series.
     *
     * <p>Nested by project rather than keyed on a composite, because this runs once per metric per document and building a composite key
     * would allocate on the write path — which is the one place this feature must not.
     */
    private AtomicInteger heldFor(TableKey key) {
        return perStream.computeIfAbsent(key.project(), unused -> new ConcurrentHashMap<>())
            .computeIfAbsent(key.sourceDataStream(), unused -> new AtomicInteger());
    }

    /**
     * The refusal counters for a table's stream, creating them if this is the first refusal it has suffered.
     *
     * <p>Only ever called once an observation is already being refused, so the two map probes it costs are paid by the path that is by
     * definition not the fast one — and under {@code flush_early} they sit next to a walk of every bucket the node holds.
     */
    private Refusals refusalsFor(TableKey key) {
        ConcurrentHashMap<String, Refusals> forProject = refusals.computeIfAbsent(key.project(), unused -> new ConcurrentHashMap<>());
        Refusals known = forProject.get(key.sourceDataStream());
        if (known != null) {
            return known;
        }
        if (refusalStreamsTracked.get() >= MAX_TRACKED_STREAMS) {
            return UNTRACKED;
        }
        return forProject.computeIfAbsent(key.sourceDataStream(), unused -> {
            refusalStreamsTracked.incrementAndGet();
            return new Refusals();
        });
    }

    private static boolean sameStream(StreamKey within, TableKey key) {
        return within.project().equals(key.project()) && within.sourceDataStream().equals(key.sourceDataStream());
    }

    private final BigArrays bigArrays;
    private final ConcurrentHashMap<TableKey, DerivedMetricsStripedTable> tables = new ConcurrentHashMap<>();
    /**
     * How many times each bucket has already been emitted. Only ever non-zero under {@code flush_early}, and swept alongside the tables
     * once the bucket is closed.
     */
    private final ConcurrentHashMap<TableKey, AtomicInteger> partials = new ConcurrentHashMap<>();
    /**
     * What each metric's cardinality last turned out to be, which is what a new bucket's striping decision is made from. One entry per
     * metric per stream, swept once a stream has stopped writing; see {@link #forget}.
     */
    private final ConcurrentHashMap<MetricKey, Cardinality> cardinality = new ConcurrentHashMap<>();
    /**
     * What each metric's dimensions have been seen to hold, which is what says <em>which</em> dimension is spending the series budget and
     * what decides when a metric gives one up. Keyed the same way as {@link #cardinality} and swept alongside it, because a dimension's
     * value count is a property of the metric rather than of any one bucket.
     */
    private final ConcurrentHashMap<MetricKey, DerivedMetricsDimensionCardinality> dimensionCardinality = new ConcurrentHashMap<>();
    /**
     * Which interval buckets a metric currently has open, and when it was last written to.
     *
     * <p>Buckets are not anchored to this node's clock. They exist where the data is: the first observation opens a bucket at whatever
     * moment it carries, and a metric holds up to {@code maxIntervalBuckets} of them at once, wherever they sit on the timeline. A fleet
     * writing in real time and a single producer replaying half an hour of history occupy two slots and do not interfere; a window
     * measured from now could not express that, because it would treat the replay as hopelessly late and refuse it.
     */
    private final ConcurrentHashMap<MetricKey, OpenBuckets> openBuckets = new ConcurrentHashMap<>();
    /**
     * Series held per source data stream, scoped to the project the stream belongs to. Two projects may each have a data stream of the
     * same name, and they must not share a budget: one tenant's cardinality would then refuse another tenant's series.
     */
    private final ConcurrentHashMap<ProjectId, ConcurrentHashMap<String, AtomicInteger>> perStream = new ConcurrentHashMap<>();
    /**
     * Refusals broken down by the stream that suffered them. Nested by project for the same reason {@link #perStream} is: a composite key
     * would have to be allocated to look one up, and this is looked up from the write path — only on the refusal branch, but a cardinality
     * explosion is exactly when every observation takes it.
     */
    private final ConcurrentHashMap<ProjectId, ConcurrentHashMap<String, Refusals>> refusals = new ConcurrentHashMap<>();
    private final AtomicInteger refusalStreamsTracked = new AtomicInteger();
    private final LongAdder droppedSeriesAtNodeCap = new LongAdder();
    private final LongAdder droppedSeriesAtStreamCap = new LongAdder();
    private final LongAdder droppedSeriesAtBreaker = new LongAdder();
    private final LongAdder partialsExhausted = new LongAdder();
    private final LongAdder tablesRetired = new LongAdder();
    private final LongAdder bucketsReopened = new LongAdder();
    private final LongAdder bucketsDropped = new LongAdder();
    /**
     * Drops since the last flush, and the largest any one flush has seen. The maximum is the actionable half: it says how many intervals a
     * metric was collecting into beyond the slots it had, so an operator can raise {@code max_interval_buckets} by roughly that much
     * rather than guessing.
     */
    private final LongAdder droppedThisCycle = new LongAdder();
    private volatile long maxDroppedInACycle;
    /** Dimensions a metric has given up breaking down by, counted once each so that degrading is visible rather than silent. */
    private final LongAdder dimensionsCollapsed = new LongAdder();
    /**
     * Tables removed mid-bucket because they could no longer accept observations. They still hold real series, so they are handed to the
     * next flush rather than dropped.
     */
    private final ConcurrentLinkedQueue<Drained> retired = new ConcurrentLinkedQueue<>();
    private final AtomicInteger totalSeries = new AtomicInteger();
    private final int maxSeries;
    private final int maxSeriesPerStream;
    private final int maxIntervalBuckets;
    private final int maxHistogramSeries;
    /** Histogram series held on this node, tracked apart from the general count because they cost about forty times as much each. */
    private final AtomicInteger histogramSeries = new AtomicInteger();
    private final LongAdder droppedSeriesAtHistogramCap = new LongAdder();
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    private final CircuitBreaker breaker;
    private final int partialSeed;
    private final int stripes;
    private final int maxDimensionCardinality;

    public DerivedMetricsBuffer(BigArrays bigArrays, int maxSeries) {
        this(bigArrays, maxSeries, maxSeries, DEFAULT_HISTOGRAM_BUCKETS, 0);
    }

    public DerivedMetricsBuffer(BigArrays bigArrays, int maxSeries, int maxSeriesPerStream) {
        this(bigArrays, maxSeries, maxSeriesPerStream, DEFAULT_HISTOGRAM_BUCKETS, 0);
    }

    /**
     * @param partialSeed the offset the first partial of any bucket is stamped at. Non-zero so that a node which restarts inside a bucket
     *                    it had already emitted a partial for does not reuse an offset, which would produce the same time series
     *                    {@code _id} and be silently rejected by {@code op_type=create}.
     */
    public DerivedMetricsBuffer(BigArrays bigArrays, int maxSeries, int maxSeriesPerStream, int histogramBuckets, int partialSeed) {
        this(bigArrays, maxSeries, maxSeriesPerStream, histogramBuckets, partialSeed, DEFAULT_STRIPES);
    }

    /**
     * @param stripes how many per-thread copies a low-cardinality bucket may be split into. Sized to the write pool, since that is how
     *                many threads can be inside an observation at once; one disables striping entirely.
     */
    public DerivedMetricsBuffer(
        BigArrays bigArrays,
        int maxSeries,
        int maxSeriesPerStream,
        int histogramBuckets,
        int partialSeed,
        int stripes
    ) {
        this(
            bigArrays,
            maxSeries,
            maxSeriesPerStream,
            histogramBuckets,
            partialSeed,
            stripes,
            DEFAULT_MAX_DIMENSION_CARDINALITY,
            DEFAULT_MAX_HISTOGRAM_SERIES
        );
    }

    /** A buffer whose histogram budget is the default; used by tests that are not about that budget. */
    public DerivedMetricsBuffer(
        BigArrays bigArrays,
        int maxSeries,
        int maxSeriesPerStream,
        int histogramBuckets,
        int partialSeed,
        int stripes,
        int maxDimensionCardinality
    ) {
        this(
            bigArrays,
            maxSeries,
            maxSeriesPerStream,
            histogramBuckets,
            partialSeed,
            stripes,
            maxDimensionCardinality,
            DEFAULT_MAX_HISTOGRAM_SERIES
        );
    }

    /**
     * What the general series budget is worth in histogram series, if nothing else bounded them. A histogram series is measured at about
     * 5,264 bytes against a scalar series' 120, so ten thousand of them would ask for 52 MB — nearly double the whole circuit breaker on a
     * node with a 512 MB heap, where the breaker is 5% of it. The general cap therefore cannot protect a small node from histograms, and
     * weighting it would be the wrong fix, because a histogram series costs exactly one tsid in the destination like any other.
     *
     * <p>Two thousand is 39% of that small node's breaker, which leaves room for the scalar series alongside it, and is what the
     * OpenTelemetry specification uses for its own {@code aggregation_cardinality_limit}.
     */
    public static final int DEFAULT_MAX_HISTOGRAM_SERIES = 2000;

    /**
     * @param maxDimensionCardinality how many distinct values one dimension of one metric may take before the metric stops breaking down
     *                                by it, or zero to only count and never collapse
     * @param maxHistogramSeries      how many histogram series this node may hold, counted against this <em>and</em> the general budget,
     *                                the way a nested field counts against both {@code nested_fields.limit} and {@code total_fields.limit}
     */
    public DerivedMetricsBuffer(
        BigArrays bigArrays,
        int maxSeries,
        int maxSeriesPerStream,
        int histogramBuckets,
        int partialSeed,
        int stripes,
        int maxDimensionCardinality,
        int maxHistogramSeries
    ) {
        this(
            bigArrays,
            maxSeries,
            maxSeriesPerStream,
            histogramBuckets,
            partialSeed,
            stripes,
            maxDimensionCardinality,
            maxHistogramSeries,
            DEFAULT_MAX_INTERVAL_BUCKETS
        );
    }

    /**
     * How many interval buckets a metric may have open at once when nothing says otherwise.
     *
     * <p>Four rather than two because the cost of coming up short is a dropped bucket, and the number of moments a metric collects into is
     * a small number a little above one in practice: a couple of producer lags and some clock skew. Two would put the common case right on
     * the edge.
     */
    public static final int DEFAULT_MAX_INTERVAL_BUCKETS = 4;

    /**
     * @param maxIntervalBuckets how many interval buckets one metric may hold at once, wherever they sit on the timeline. Two lets a fleet
     *                           writing in real time coexist with a single producer replaying history; more accommodates more distinct
     *                           moments at which data is arriving.
     */
    public DerivedMetricsBuffer(
        BigArrays bigArrays,
        int maxSeries,
        int maxSeriesPerStream,
        int histogramBuckets,
        int partialSeed,
        int stripes,
        int maxDimensionCardinality,
        int maxHistogramSeries,
        int maxIntervalBuckets
    ) {
        this.maxIntervalBuckets = maxIntervalBuckets;
        this.bigArrays = bigArrays;
        this.maxSeries = maxSeries;
        this.maxSeriesPerStream = maxSeriesPerStream;
        this.maxHistogramSeries = maxHistogramSeries;
        this.histogramBuckets = histogramBuckets;
        this.histogramBreaker = histogramBreaker(bigArrays);
        this.breaker = breaker(bigArrays);
        this.maxDimensionCardinality = maxDimensionCardinality;
        this.partialSeed = partialSeed;
        // rounded up so that choosing a stripe is a mask rather than a division; the stripes past the thread count are never allocated,
        // since a stripe is only created by the thread that lands on it
        this.stripes = Integer.highestOneBit(Math.max(1, stripes) * 2 - 1);
    }

    /**
     * The write pool is sized to the processor count, so that is how many threads can be recording at once and therefore how many stripes
     * a contended bucket needs before another one buys nothing.
     */
    private static final int DEFAULT_STRIPES = EsExecutors.allocatedProcessors(Settings.EMPTY);

    /**
     * Above how many series a bucket is shared rather than striped. It is the point where the two costs cross: below it a per-thread copy
     * of the table is a few tens of kilobytes and the monitor is very nearly the whole of an observation, above it the observations are
     * already spread across enough series that the monitor is not hot and replicating them would be worth megabytes.
     */
    static final int STRIPE_SERIES_THRESHOLD = 64;

    /**
     * How many partials a bucket may be split into. A partial is stamped at {@code bucketStart + partial} milliseconds, so the offset has
     * to stay inside the interval or the document would land in the following bucket. Reaching this means the node is flushing early
     * hundreds of times within a single interval, at which point shedding is the honest response.
     */
    private static int maxPartials(TableKey key) {
        return (int) Math.min(Integer.MAX_VALUE, key.intervalMillis());
    }

    /**
     * The bucket capacity of a histogram series, matching the OpenTelemetry default. It is what bounds a histogram series' size, and
     * therefore also its precision.
     */
    public static final int DEFAULT_HISTOGRAM_BUCKETS = 160;

    /**
     * Adapts the same breaker the rest of the buffer allocates against to the one-method interface the histogram library expects, so a
     * distribution is accounted exactly like the scalar columns beside it.
     */
    /**
     * How many distinct values one dimension of one metric may take before that metric gives up breaking down by it.
     *
     * <p>A thousand, which is a tenth of the default node budget of ten thousand series — so a single dimension of a single metric can
     * spend at most a tenth of the node on its own, and nine other metrics can still be broken down beside it. It is also comfortably
     * above what a dimension worth having ever reaches: HTTP status, method, index name, tier, node, service in all but the largest
     * deployments. The dimensions that pass it are the ones that were never going to be aggregatable anyway — user ids, pod names,
     * request ids — and those are exactly the ones worth losing the breakdown of.
     */
    public static final int DEFAULT_MAX_DIMENSION_CARDINALITY = 1_000;

    /**
     * How many metrics a node keeps dimension sketches for. Every entry is roughly 256 bytes per dimension, so this is a quarter of a
     * megabyte at sixteen dimensions each — bounded so that a node with thousands of streams cannot spend its metrics budget on
     * diagnostics about metrics rather than on the metrics. Past it, further metrics are simply not tracked and not collapsed.
     */
    static final int MAX_TRACKED_METRICS = 1_024;

    /** The breaker everything here allocates against, or a no-op one when the buffer was built without a breaker service. */
    private static CircuitBreaker breaker(BigArrays bigArrays) {
        return bigArrays.breakerService() == null
            ? new NoopCircuitBreaker(DerivedMetricsService.BREAKER_NAME)
            : bigArrays.breakerService().getBreaker(DerivedMetricsService.BREAKER_NAME);
    }

    private static ExponentialHistogramCircuitBreaker histogramBreaker(BigArrays bigArrays) {
        if (bigArrays.breakerService() == null) {
            return ExponentialHistogramCircuitBreaker.noop();
        }
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(DerivedMetricsService.BREAKER_NAME);
        return bytes -> {
            if (bytes > 0) {
                breaker.addEstimateBytesAndMaybeBreak(bytes, "derived_metrics_histogram");
            } else {
                breaker.addWithoutBreaking(bytes);
            }
        };
    }

    /**
     * Records one observation. Returns false when it was dropped, either because a cap was reached or because the circuit breaker
     * refused the memory the new series would have needed.
     *
     * @param values one entry per dimension the metric configures, null where the document did not have it
     */
    /** Records against the bucket the key names, with no separate notion of when the observation was seen; used by tests. */
    public Outcome record(TableKey key, String[] values, Scratch scratch, double value) {
        return record(key, values, scratch, value, key.bucketStartMillis());
    }

    public Outcome record(TableKey key, String[] values, Scratch scratch, double value, long nowMillis) {
        AtomicInteger held = heldFor(key);
        while (true) {
            DerivedMetricsStripedTable bucket = tables.get(key);
            if (bucket == null) {
                bucket = openTable(key, nowMillis);
                if (bucket == null) {
                    return Outcome.REFUSED_BREAKER;
                }
            }
            // Read from the bucket we have already looked up rather than from a map of its own, so that the collapse decision costs one
            // field read and one volatile read per observation rather than another concurrent hash probe.
            DerivedMetricsDimensionCardinality dimensions = bucket.dimensionCardinality();
            BytesRef encoded = DerivedMetricsDimensionCodec.encode(
                values,
                key.metric().dimensions().size(),
                dimensions.collapsedMask(),
                scratch
            );
            // On a striped bucket this is the thread's own table and the monitor below is uncontended; on a shared one it is the single
            // table every thread convoys on, which is the pre-striping behaviour and what a high-cardinality metric keeps.
            DerivedMetricsSeriesTable table = bucket.stripeForCurrentThread();
            if (table == null) {
                // the bucket was drained before this thread could take a stripe in it, so its replacement is the one to record into
                continue;
            }
            bucket.touch(nowMillis);
            boolean created = false;
            synchronized (table) {
                if (table.sealed()) {
                    // Drained while we were waiting for the lock, so it will never be emitted again. Starting over opens a fresh table
                    // for the same bucket, which the next flush emits as a further partial that sums with the one already written. That
                    // is how a late observation is counted in the bucket it belongs to rather than the one it arrived in.
                    bucketsReopened.increment();
                    continue;
                }
                // Probing the table is the expensive part of this critical section, so the common path does it once: record and let
                // the returned sign say whether a series was created. Only when a cap is already reached does it cost a second probe,
                // because there we must know before interning — the table has no way to remove a series it should not have taken.
                boolean histogram = key.metric().reduction().isHistogram();
                boolean atNodeCap = totalSeries.get() >= maxSeries;
                boolean atStreamCap = held.get() >= maxSeriesPerStream;
                boolean atHistogramCap = histogram && histogramSeries.get() >= maxHistogramSeries;
                if (atNodeCap || atStreamCap || atHistogramCap) {
                    if (table.contains(encoded) == false) {
                        // Counted per stream as well as node-wide, because "the node is out of budget" and "this stream is out of budget"
                        // send an operator to different places, and only the second one names the configuration to change.
                        Refusals stream = refusalsFor(key);
                        if (atHistogramCap) {
                            // Named separately because the answer is different: this one says the node is full of distributions, not that
                            // it is full, and the two are raised by different settings.
                            droppedSeriesAtHistogramCap.increment();
                            stream.atHistogramCap.increment();
                            return Outcome.REFUSED_HISTOGRAM_CAP;
                        }
                        // Which cap refused is the first thing an operator needs to know: one says raise the node budget, the other says
                        // go and find the stream. Counted separately rather than conflated into a single "dropped" number.
                        if (atNodeCap) {
                            droppedSeriesAtNodeCap.increment();
                            stream.atNodeCap.increment();
                            return Outcome.REFUSED_NODE_CAP;
                        }
                        droppedSeriesAtStreamCap.increment();
                        stream.atStreamCap.increment();
                        return Outcome.REFUSED_STREAM_CAP;
                    }
                }
                try {
                    if (table.record(encoded, value) >= 0) {
                        // A series interned by two stripes is charged twice, because it really is held twice. That is what makes the
                        // budget bound the memory rather than the distinct series count, and what makes striping have to be bounded.
                        totalSeries.incrementAndGet();
                        held.incrementAndGet();
                        if (histogram) {
                            histogramSeries.incrementAndGet();
                        }
                        created = true;
                    }
                } catch (CircuitBreakingException e) {
                    droppedSeriesAtBreaker.increment();
                    refusalsFor(key).atBreaker.increment();
                    if (table.poisoned()) {
                        // The hash cannot be probed again, so this table has to leave the map now rather than wait for its bucket to
                        // close. What it already holds is intact and still worth emitting, so it is set aside for the next flush rather
                        // than thrown away, and the next observation for this bucket opens a fresh table. The whole bucket goes, not just
                        // the one stripe, so that the striping decision is made once per bucket and the stripes stay mergeable.
                        retire(key, bucket);
                    }
                    return Outcome.REFUSED_BREAKER;
                }
            }
            if (created) {
                // Outside the table's monitor, because this takes a monitor of its own and holding both would order two locks that
                // nothing else orders. It only runs when a series was interned, which is what makes the cost of counting irrelevant:
                // a value that has never been seen before always produces a tuple that has never been seen before, so nothing is missed
                // by skipping every observation that only touched an existing series.
                dimensionsCollapsed.add(dimensions.observe(values));
            }
            return Outcome.RECORDED;
        }
    }

    /**
     * Removes whichever buffered bucket is holding the most memory, so that relieving pressure gives up the table actually filling the
     * node rather than whichever one happened to ask last. Bytes rather than series count, because a histogram series is worth roughly
     * thirty scalar ones and a table with far fewer series can be the larger one.
     *
     * @param within when set, only tables belonging to this stream are considered — which is what helps when it was the per-stream cap
     *               that refused, since freeing another stream's memory would not give this one any of its share back
     * @return the drained bucket, or null when there was nothing worth taking
     */
    public Drained drainLargest(@Nullable StreamKey within) {
        return drainLargest(within, false);
    }

    /**
     * @param histogramsOnly consider only buckets holding distributions. Freeing a scalar bucket does nothing for a node that has run out
     *                       of histogram budget, so a refusal at that cap has to give up one of the buckets actually holding it.
     */
    public Drained drainLargest(@Nullable StreamKey within, boolean histogramsOnly) {
        TableKey largestKey = null;
        DerivedMetricsStripedTable largest = null;
        long mostBytes = -1;
        for (Map.Entry<TableKey, DerivedMetricsStripedTable> entry : tables.entrySet()) {
            TableKey key = entry.getKey();
            if (within != null && sameStream(within, key) == false) {
                continue;
            }
            if (histogramsOnly && key.metric().reduction().isHistogram() == false) {
                continue;
            }
            if (partialsLeft(key) == false) {
                continue;
            }
            long bytes = entry.getValue().bytesHeld() * 100 / Math.max(1, preferenceOf(key));
            if (bytes > mostBytes) {
                mostBytes = bytes;
                largestKey = key;
                largest = entry.getValue();
            }
        }
        if (largest == null) {
            return null;
        }
        return take(largestKey, largest, true);
    }

    /**
     * How strongly a metric would rather keep its memory, as a percentage. A metric with no preference sits at 100, so an unconfigured
     * stream ranks purely by size; raising it makes a metric proportionally less likely to be the one given up.
     */
    private static int preferenceOf(TableKey key) {
        return key.metric().preference();
    }

    /** Whether a bucket can still be split into another partial without its timestamp offset running into the next bucket. */
    private boolean partialsLeft(TableKey key) {
        AtomicInteger counter = partials.get(key);
        return counter == null || counter.get() < maxPartials(key) - 1;
    }

    /** The stream a table belongs to, for scoping {@link #drainLargest}. */
    public StreamKey streamOf(TableKey key) {
        return streamKey(key);
    }

    /**
     * Removes a table that can no longer accept observations and keeps it for the next flush, so the series it already accumulated are
     * emitted rather than discarded. Called with one of the bucket's stripes' monitors held.
     */
    private void retire(TableKey key, DerivedMetricsStripedTable table) {
        Drained taken = take(key, table, true);
        if (taken == null) {
            // someone else drained the bucket first, and it is already on its way to being emitted
            return;
        }
        retired.add(taken);
        tablesRetired.increment();
    }

    /**
     * Creates the bucket, or returns null when the breaker refuses it.
     */
    private DerivedMetricsStripedTable openTable(TableKey key, long nowMillis) {
        // Resolved once per bucket rather than per observation, which is the whole reason it is handed to the bucket rather than looked
        // up on the write path.
        DerivedMetricsDimensionCardinality dimensions = dimensionCardinalityFor(key);
        // Taking a slot belongs here, where a bucket comes into existence, and not on the observation path: an observation into a bucket
        // that already exists is one that already holds its slot, so it has nothing to decide. That keeps the shared monitor below off the
        // hot path entirely, where it would be the one lock every write thread for a metric convoys on.
        takeSlot(key, nowMillis);
        try {
            return tables.computeIfAbsent(
                key,
                unused -> new DerivedMetricsStripedTable(
                    bigArrays,
                    key.metric().reduction(),
                    histogramBuckets,
                    histogramBreaker,
                    stripesFor(key),
                    dimensions
                )
            );
        } catch (CircuitBreakingException e) {
            droppedSeriesAtBreaker.increment();
            refusalsFor(key).atBreaker.increment();
            return null;
        }
    }

    /**
     * Claims one of the metric's interval slots for this bucket, writing out whichever bucket has to give up its own.
     *
     * <p>Emission is not done here, because this runs on the indexing thread. The bucket is set aside and the next flush writes it, which
     * is the route a table retired mid-bucket already takes.
     */
    private void takeSlot(TableKey key, long nowMillis) {
        OpenBuckets open = openBuckets.computeIfAbsent(metricKey(key), unused -> new OpenBuckets(maxIntervalBuckets));
        open.touch(nowMillis);
        long evicted = open.admit(key.bucketStartMillis());
        if (evicted == Long.MIN_VALUE) {
            return;
        }
        TableKey evictedKey = new TableKey(key.project(), key.sourceDataStream(), key.metric(), evicted, key.intervalMillis());
        DerivedMetricsStripedTable evictedTable = tables.remove(evictedKey);
        if (evictedTable == null) {
            return;
        }
        // Dropped rather than written out. Emitting would keep every observation, but an erratic producer would then cost one document per
        // document, which is the one thing this feature promises not to do. Losing the stalest bucket bounds the output whatever the
        // producer does, and the counters below are what tells an operator it happened and by how much they are under-bucketed.
        bucketsDropped.increment();
        droppedThisCycle.increment();
        refusalsFor(key).bucketsDropped.increment();
        evictedTable.close();
    }

    /**
     * The dimension sketches for a metric, creating them if this is the first bucket it has opened. Shared by every bucket of the metric,
     * because how many values a dimension takes is a property of the metric rather than of one interval.
     */
    private DerivedMetricsDimensionCardinality dimensionCardinalityFor(TableKey key) {
        if (key.metric().dimensions().isEmpty()) {
            return DerivedMetricsDimensionCardinality.DISABLED;
        }
        MetricKey metricKey = metricKey(key);
        DerivedMetricsDimensionCardinality known = dimensionCardinality.get(metricKey);
        if (known == null) {
            if (dimensionCardinality.size() >= MAX_TRACKED_METRICS) {
                return DerivedMetricsDimensionCardinality.DISABLED;
            }
            known = dimensionCardinality.computeIfAbsent(
                metricKey,
                unused -> DerivedMetricsDimensionCardinality.create(bigArrays, breaker, key.metric().dimensions(), maxDimensionCardinality)
            );
        }
        known.markLive(key.bucketStartMillis());
        return known;
    }

    /**
     * Whether a new bucket is striped per thread or shared, from what its metric's cardinality last turned out to be.
     *
     * <p>A metric nobody has seen yet starts striped, because a metric starts small — and because the alternative, starting shared and
     * flipping down, would leave the shape that contends worst contending for its first bucket every time the node restarts.
     */
    private int stripesFor(TableKey key) {
        if (stripes == 1 || key.metric().reduction().isHistogram()) {
            // a distribution is roughly thirty times a scalar series, which puts it on the wrong side of the trade striping rests on
            return 1;
        }
        Cardinality known = cardinality.get(metricKey(key));
        return known == null || known.series() <= STRIPE_SERIES_THRESHOLD ? stripes : 1;
    }

    /**
     * Remembers how many distinct series a metric turned out to hold, which is what the next bucket's striping decision is made from.
     * Within one bucket the highest figure wins, so that a bucket flushed early in several small pieces is not remembered as a small one.
     */
    private void remember(TableKey key, long series) {
        cardinality.merge(metricKey(key), new Cardinality(key.bucketStartMillis(), series), DerivedMetricsBuffer::later);
    }

    private static Cardinality later(Cardinality existing, Cardinality fresh) {
        if (fresh.bucketStartMillis() != existing.bucketStartMillis()) {
            return fresh.bucketStartMillis() > existing.bucketStartMillis() ? fresh : existing;
        }
        return fresh.series() > existing.series() ? fresh : existing;
    }

    /**
     * Drops what is remembered about metrics of streams that have stopped writing, so that the memory behind the striping decision is
     * bounded by what is live rather than by everything the node has ever seen.
     */
    private void forget(long nowMillis) {
        cardinality.entrySet()
            .removeIf(entry -> entry.getValue().bucketStartMillis() + CARDINALITY_MEMORY * entry.getKey().intervalMillis() < nowMillis);
        // The dimension sketches hold real memory, so forgetting them has to release it — and it must not release one a live bucket is
        // still using. Proximity to now cannot answer that any more: a bucket exists where its data is, so a metric replaying history has
        // live buckets arbitrarily far behind this node's clock while still being written to every second. The only sound test is whether
        // the metric is holding anything at all.
        dimensionCardinality.entrySet().removeIf(entry -> {
            OpenBuckets open = openBuckets.get(entry.getKey());
            if (open != null && open.holdsAnything()) {
                return false;
            }
            DerivedMetricsDimensionCardinality tracked = entry.getValue();
            if (open != null && open.lastWrittenMillis() + CARDINALITY_MEMORY * entry.getKey().intervalMillis() >= nowMillis) {
                return false;
            }
            tracked.close();
            return true;
        });
        // A bucket's partial count is what keeps the ids of its results distinct, so it has to outlive the bucket itself: a late
        // observation reopens a bucket that has already been written, and an offset starting over would collide with the result already
        // there. It is kept for as long as the metric's own data could still reach back that far, which is the same horizon the sketches
        // above use, measured against the metric's progression rather than this node's clock.
        partials.keySet().removeIf(key -> {
            OpenBuckets open = openBuckets.get(metricKey(key));
            return open == null || key.bucketStartMillis() + CARDINALITY_MEMORY * key.intervalMillis() < open.lastWrittenMillis();
        });
        // and a metric holding nothing, written to by nobody, need not be remembered as open either
        openBuckets.entrySet()
            .removeIf(
                entry -> entry.getValue().holdsAnything() == false
                    && entry.getValue().lastWrittenMillis() + CARDINALITY_MEMORY * entry.getKey().intervalMillis() < nowMillis
            );
    }

    /** How many intervals a metric's cardinality is remembered for after it stops producing buckets. */
    private static final int CARDINALITY_MEMORY = 10;

    /**
     * Removes every table that can no longer receive observations, that is every bucket whose interval ended at least
     * {@code graceMillis} ago. The grace period covers writes still in flight when the interval closes.
     *
     * <p>The caller owns the returned tables and <em>must</em> close them, or their circuit breaker accounting leaks.
     */
    public List<Drained> drainClosed(long nowMillis, long graceMillis) {
        // A bucket is normally written out because newer data needed its slot, which is decided by the data rather than here. This is
        // the backstop for the case nothing will ever push it out: a metric that has stopped being written to. Comparing this node's
        // clock against the bucket's own start would be wrong, because a producer replaying history has buckets that are always older
        // than now and would be written out one document at a time.
        BiPredicate<TableKey, DerivedMetricsStripedTable> closed = (key, table) -> table.lastWrittenMillis() + key.intervalMillis()
            + graceMillis <= nowMillis;
        List<Drained> drained = drain(closed, false);
        drainRetired(drained);
        long dropped = droppedThisCycle.sumThenReset();
        if (dropped > maxDroppedInACycle) {
            maxDroppedInACycle = dropped;
        }
        forget(nowMillis);
        return drained;
    }

    /**
     * Removes one bucket even though it is still open, and remembers that it has now been emitted once more so the next emission of the
     * same bucket does not collide with this one. This is the {@code flush_early} response to memory pressure: the observations already
     * collected are kept rather than the ones still to come being dropped.
     *
     * <p>Scoped to a single bucket because the caller is on the indexing thread. The caller owns the returned table and must close it.
     *
     * @return the drained bucket, or null if it had already been drained by someone else
     */
    public Drained drainForPressure(TableKey key) {
        DerivedMetricsStripedTable table = tables.get(key);
        if (table == null) {
            return null;
        }
        AtomicInteger counter = partials.get(key);
        if (counter != null && counter.get() >= maxPartials(key) - 1) {
            // no offset left inside the interval, so a further partial would collide or land in the next bucket
            partialsExhausted.increment();
            return null;
        }
        return take(key, table, true);
    }

    /**
     * Removes everything currently buffered, including buckets that are still open. Used on shutdown so partial intervals are not lost.
     */
    public List<Drained> drainAll() {
        List<Drained> drained = drain((key, table) -> true, false);
        drainRetired(drained);
        partials.clear();
        return drained;
    }

    /** Adds anything set aside by {@link #retire} to this flush. */
    private void drainRetired(List<Drained> drained) {
        Drained taken;
        while ((taken = retired.poll()) != null) {
            drained.add(taken);
        }
    }

    private List<Drained> drain(BiPredicate<TableKey, DerivedMetricsStripedTable> take, boolean reopening) {
        List<Drained> drained = new ArrayList<>();
        Iterator<Map.Entry<TableKey, DerivedMetricsStripedTable>> iterator = tables.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableKey, DerivedMetricsStripedTable> entry = iterator.next();
            TableKey key = entry.getKey();
            DerivedMetricsStripedTable table = entry.getValue();
            if (take.test(key, table) == false) {
                continue;
            }
            Drained taken = take(key, table, reopening);
            if (taken != null) {
                drained.add(taken);
            }
        }
        return drained;
    }

    /**
     * Removes one bucket and gives its budget back, or returns null if someone else got there first.
     *
     * <p>The bucket is removed from the map <em>before</em> it is sealed, so a writer that finds it sealed is guaranteed to see the
     * replacement on its next lookup rather than spinning. It is removed by value rather than by key, so a bucket recreated by a
     * concurrent write survives.
     *
     * <p>Only the thread that wins the removal seals and merges, which is what lets the stripes be folded together without a lock.
     */
    /**
     * Removes one bucket and gives back everything it was holding, including its slot, so a metric can open a bucket somewhere else.
     */
    private Drained take(TableKey key, DerivedMetricsStripedTable table, boolean reopening) {
        OpenBuckets open = openBuckets.get(metricKey(key));
        if (open != null) {
            open.release(key.bucketStartMillis());
        }
        return takeTable(key, table, reopening);
    }

    private Drained takeTable(TableKey key, DerivedMetricsStripedTable table, boolean reopening) {
        if (tables.remove(key, table) == false) {
            return null;
        }
        long released = table.seal();
        // The stripes are folded into one table before anything downstream sees them, so a series two threads recorded is one series in
        // one document — striping is invisible past this point.
        DerivedMetricsSeriesTable merged = table.merge();
        // Merging allocates, so the breaker can refuse partway through it. The budget for those series was already given back above, so
        // the accounting stays exact; what is lost is the observations, which are counted where every other breaker loss is.
        long lostMerging = table.seriesLostMerging();
        droppedSeriesAtBreaker.add(lostMerging);
        if (lostMerging > 0) {
            refusalsFor(key).atBreaker.add(lostMerging);
        }
        remember(key, merged.size());
        totalSeries.addAndGet(-(int) released);
        if (key.metric().reduction().isHistogram()) {
            histogramSeries.addAndGet(-(int) released);
        }
        releaseStreamBudget(key, released);
        AtomicInteger counter = partials.get(key);
        int partial = counter == null ? partialSeed : counter.get();
        if (reopening) {
            partials.computeIfAbsent(key, unused -> new AtomicInteger(partialSeed)).incrementAndGet();
        }
        return new Drained(key, merged, partial);
    }

    /** Gives a drained table's series back to its stream's budget, forgetting the stream once it holds nothing. */
    private void releaseStreamBudget(TableKey key, long released) {
        ConcurrentHashMap<String, AtomicInteger> forProject = perStream.get(key.project());
        if (forProject == null) {
            return;
        }
        AtomicInteger held = forProject.get(key.sourceDataStream());
        if (held != null && held.addAndGet(-(int) released) <= 0) {
            forProject.remove(key.sourceDataStream(), held);
        }
    }

    /**
     * Series currently held, across every table. A series that several stripes of a striped bucket each interned counts once per stripe,
     * because the point of this number is the memory it stands for rather than how many distinct series a query would see.
     */
    public int size() {
        return totalSeries.get();
    }

    /** Series refused because the node-wide budget was already spent. */
    public long droppedSeriesAtNodeCap() {
        return droppedSeriesAtNodeCap.sum();
    }

    /** Series refused because one source stream had already taken its share. */
    public long droppedSeriesAtStreamCap() {
        return droppedSeriesAtStreamCap.sum();
    }

    /** Series refused because the circuit breaker would not give the memory they needed. */
    /** Series refused because this node already holds as many distributions as it is allowed to. */
    public long droppedSeriesAtHistogramCap() {
        return droppedSeriesAtHistogramCap.sum();
    }

    /** Histogram series currently held on this node. */
    public int histogramSeries() {
        return histogramSeries.get();
    }

    public long droppedSeriesAtBreaker() {
        return droppedSeriesAtBreaker.sum();
    }

    /**
     * What every tracked metric's dimensions have been seen to hold, which is the answer to "which dimension is spending the budget".
     *
     * <p>Built on demand rather than kept, because reading a sketch's estimate means summing its registers and nothing does it on the
     * write path. Safe to call from anywhere.
     */
    public List<DimensionCardinality> dimensionCardinalities() {
        List<DimensionCardinality> cardinalities = new ArrayList<>();
        for (Map.Entry<MetricKey, DerivedMetricsDimensionCardinality> entry : dimensionCardinality.entrySet()) {
            MetricKey key = entry.getKey();
            DerivedMetricsDimensionCardinality tracked = entry.getValue();
            for (int dimension = 0; dimension < tracked.tracked(); dimension++) {
                cardinalities.add(
                    new DimensionCardinality(
                        key.project(),
                        key.sourceDataStream(),
                        key.metric(),
                        tracked.dimension(dimension),
                        tracked.estimatedValues(dimension),
                        tracked.collapsed(dimension)
                    )
                );
            }
        }
        return cardinalities;
    }

    /**
     * What every metric currently buffered is holding, one entry per metric per interval rather than per bucket: a metric normally has one
     * open bucket, but a flush that has not run yet leaves the previous one alongside it, and an operator asking what a metric costs means
     * the total.
     *
     * <p>Built on demand from the same figures the shedding decision already keeps, so nothing here is maintained on the write path.
     */
    public List<MetricSnapshot> metricSnapshots() {
        record Slot(ProjectId project, String sourceDataStream, String metric, String interval) {}
        Map<Slot, long[]> totals = new HashMap<>();
        Map<Slot, Boolean> histograms = new HashMap<>();
        for (Map.Entry<TableKey, DerivedMetricsStripedTable> entry : tables.entrySet()) {
            TableKey key = entry.getKey();
            Slot slot = new Slot(key.project(), key.sourceDataStream(), key.metric().name(), key.metric().interval().name());
            long[] held = totals.computeIfAbsent(slot, unused -> new long[2]);
            held[0] += entry.getValue().seriesHeld();
            held[1] += entry.getValue().bytesHeld();
            histograms.put(slot, key.metric().reduction().isHistogram());
        }
        List<MetricSnapshot> snapshots = new ArrayList<>(totals.size());
        totals.forEach(
            (slot, held) -> snapshots.add(
                new MetricSnapshot(
                    slot.project(),
                    slot.sourceDataStream(),
                    slot.metric(),
                    slot.interval(),
                    held[0],
                    held[1],
                    histograms.get(slot)
                )
            )
        );
        return snapshots;
    }

    /** Refusals broken down by the source data stream that suffered them, for every stream that has suffered any. */
    public List<StreamRefusals> streamRefusals() {
        List<StreamRefusals> perStream = new ArrayList<>();
        refusals.forEach((project, forProject) -> forProject.forEach((stream, counted) -> {
            StreamRefusals refused = new StreamRefusals(
                project,
                stream,
                counted.atNodeCap.sum(),
                counted.atStreamCap.sum(),
                counted.atHistogramCap.sum(),
                counted.atBreaker.sum(),
                counted.bucketsDropped.sum()
            );
            if (refused.any()) {
                perStream.add(refused);
            }
        }));
        return perStream;
    }

    /** Dimensions a metric has stopped breaking down by because they outgrew their budget, counted once each. */
    public long dimensionsCollapsed() {
        return dimensionsCollapsed.sum();
    }

    /**
     * Buckets given up because a metric needed a slot for a newer one. A steady rate here means the data arrives at more distinct moments
     * than {@code max_interval_buckets} allows for, which costs extra documents rather than accuracy.
     */
    public long bucketsDropped() {
        return bucketsDropped.sum();
    }

    /** The most buckets dropped between two flushes, which is how far short of the moments it needed a metric came. */
    public long maxBucketsDroppedInACycle() {
        return maxDroppedInACycle;
    }

    /** Observations that arrived for a bucket already emitted, and reopened it as a further partial. */
    public long bucketsReopened() {
        return bucketsReopened.sum();
    }

    /** Tables removed mid-bucket because their dimension hash could no longer be probed. */
    public long tablesRetired() {
        return tablesRetired.sum();
    }

    /** Buckets that could not be flushed early because no timestamp offset was left inside the interval. */
    public long partialsExhausted() {
        return partialsExhausted.sum();
    }

    public long droppedSeries() {
        return droppedSeriesAtNodeCap.sum() + droppedSeriesAtStreamCap.sum() + droppedSeriesAtBreaker.sum() + droppedSeriesAtHistogramCap
            .sum();
    }

    // visible for testing
    int partialsTracked() {
        return partials.size();
    }

    /**
     * How many per-thread stripes the bucket for this key currently has, or zero if there is no such bucket. One means it is shared.
     */
    // visible for testing
    int stripesOf(TableKey key) {
        DerivedMetricsStripedTable table = tables.get(key);
        return table == null ? 0 : table.stripeCount();
    }

    // visible for testing
    int seriesFor(String sourceDataStream) {
        return seriesFor(ProjectId.DEFAULT, sourceDataStream);
    }

    // visible for testing
    int seriesFor(ProjectId project, String sourceDataStream) {
        ConcurrentHashMap<String, AtomicInteger> forProject = perStream.get(project);
        AtomicInteger held = forProject == null ? null : forProject.get(sourceDataStream);
        return held == null ? 0 : held.get();
    }

    /**
     * The start of the bucket that {@code nowMillis} falls into. Buckets are aligned to the epoch so every node in the cluster agrees on
     * the boundaries without any coordination.
     */
    public static long bucketStart(long nowMillis, long intervalMillis) {
        return nowMillis - Math.floorMod(nowMillis, intervalMillis);
    }

    @Override
    public void close() {
        drainAll().forEach(drained -> drained.table().close());
        // The sketches are the one thing here that outlives every bucket, so nothing else would ever give their bytes back
        dimensionCardinality.values().forEach(DerivedMetricsDimensionCardinality::close);
        dimensionCardinality.clear();
    }
}
