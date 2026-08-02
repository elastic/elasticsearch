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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

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
        REFUSED_BREAKER;

        public boolean recorded() {
            return this == RECORDED;
        }
    }

    /** Identifies a source data stream within its project, which is what the per-stream budget is keyed on. */
    public record StreamKey(ProjectId project, String sourceDataStream) {}

    private static StreamKey streamKey(TableKey key) {
        return new StreamKey(key.project(), key.sourceDataStream());
    }

    /**
     * Identifies one metric of one stream across every bucket it will ever have, which is the scope the cardinality memory behind
     * {@link #stripesFor} is kept at.
     *
     * <p>By name rather than by the compiled metric, unlike {@link TableKey}: a configuration change recompiles every metric into fresh
     * objects, and what a metric's cardinality turned out to be last interval is still true of the metric that replaced it.
     */
    private record MetricKey(ProjectId project, String sourceDataStream, String metric, long intervalMillis) {}

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
     * Series held per source data stream, scoped to the project the stream belongs to. Two projects may each have a data stream of the
     * same name, and they must not share a budget: one tenant's cardinality would then refuse another tenant's series.
     */
    private final ConcurrentHashMap<ProjectId, ConcurrentHashMap<String, AtomicInteger>> perStream = new ConcurrentHashMap<>();
    private final LongAdder droppedSeriesAtNodeCap = new LongAdder();
    private final LongAdder droppedSeriesAtStreamCap = new LongAdder();
    private final LongAdder droppedSeriesAtBreaker = new LongAdder();
    private final LongAdder partialsExhausted = new LongAdder();
    private final LongAdder tablesRetired = new LongAdder();
    /**
     * Tables removed mid-bucket because they could no longer accept observations. They still hold real series, so they are handed to the
     * next flush rather than dropped.
     */
    private final ConcurrentLinkedQueue<Drained> retired = new ConcurrentLinkedQueue<>();
    private final AtomicInteger totalSeries = new AtomicInteger();
    private final int maxSeries;
    private final int maxSeriesPerStream;
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    private final int partialSeed;
    private final int stripes;

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
        this.bigArrays = bigArrays;
        this.maxSeries = maxSeries;
        this.maxSeriesPerStream = maxSeriesPerStream;
        this.histogramBuckets = histogramBuckets;
        this.histogramBreaker = histogramBreaker(bigArrays);
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
    public Outcome record(TableKey key, String[] values, Scratch scratch, double value) {
        BytesRef encoded = DerivedMetricsDimensionCodec.encode(values, key.metric().dimensions().size(), scratch);
        AtomicInteger held = heldFor(key);
        while (true) {
            DerivedMetricsStripedTable bucket = tables.get(key);
            if (bucket == null) {
                bucket = openTable(key);
                if (bucket == null) {
                    return Outcome.REFUSED_BREAKER;
                }
            }
            // On a striped bucket this is the thread's own table and the monitor below is uncontended; on a shared one it is the single
            // table every thread convoys on, which is the pre-striping behaviour and what a high-cardinality metric keeps.
            DerivedMetricsSeriesTable table = bucket.stripeForCurrentThread();
            if (table == null) {
                // the bucket was drained before this thread could take a stripe in it, so its replacement is the one to record into
                continue;
            }
            synchronized (table) {
                if (table.sealed()) {
                    // drained while we were waiting for the lock, so it will never be emitted again; start over on the fresh bucket
                    continue;
                }
                // Probing the table is the expensive part of this critical section, so the common path does it once: record and let
                // the returned sign say whether a series was created. Only when a cap is already reached does it cost a second probe,
                // because there we must know before interning — the table has no way to remove a series it should not have taken.
                boolean atNodeCap = totalSeries.get() >= maxSeries;
                boolean atStreamCap = held.get() >= maxSeriesPerStream;
                if (atNodeCap || atStreamCap) {
                    if (table.contains(encoded) == false) {
                        // Which cap refused is the first thing an operator needs to know: one says raise the node budget, the other says
                        // go and find the stream. Counted separately rather than conflated into a single "dropped" number.
                        if (atNodeCap) {
                            droppedSeriesAtNodeCap.increment();
                            return Outcome.REFUSED_NODE_CAP;
                        }
                        droppedSeriesAtStreamCap.increment();
                        return Outcome.REFUSED_STREAM_CAP;
                    }
                }
                try {
                    if (table.record(encoded, value) >= 0) {
                        // A series interned by two stripes is charged twice, because it really is held twice. That is what makes the
                        // budget bound the memory rather than the distinct series count, and what makes striping have to be bounded.
                        totalSeries.incrementAndGet();
                        held.incrementAndGet();
                    }
                } catch (CircuitBreakingException e) {
                    droppedSeriesAtBreaker.increment();
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
        TableKey largestKey = null;
        DerivedMetricsStripedTable largest = null;
        long mostBytes = -1;
        for (Map.Entry<TableKey, DerivedMetricsStripedTable> entry : tables.entrySet()) {
            TableKey key = entry.getKey();
            if (within != null && sameStream(within, key) == false) {
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
    private DerivedMetricsStripedTable openTable(TableKey key) {
        try {
            return tables.computeIfAbsent(
                key,
                unused -> new DerivedMetricsStripedTable(
                    bigArrays,
                    key.metric().reduction(),
                    histogramBuckets,
                    histogramBreaker,
                    stripesFor(key)
                )
            );
        } catch (CircuitBreakingException e) {
            droppedSeriesAtBreaker.increment();
            return null;
        }
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
        Predicate<TableKey> closed = key -> key.bucketStartMillis() + key.intervalMillis() + graceMillis <= nowMillis;
        List<Drained> drained = drain(closed, false);
        drainRetired(drained);
        // A closed bucket receives nothing further, so its partial count has nothing left to keep it honest. Sweeping here rather than in
        // drain covers buckets that were flushed early and then never saw another write.
        partials.keySet().removeIf(key -> closed.test(key) && tables.containsKey(key) == false);
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
        List<Drained> drained = drain(key -> true, false);
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

    private List<Drained> drain(Predicate<TableKey> take, boolean reopening) {
        List<Drained> drained = new ArrayList<>();
        Iterator<Map.Entry<TableKey, DerivedMetricsStripedTable>> iterator = tables.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableKey, DerivedMetricsStripedTable> entry = iterator.next();
            TableKey key = entry.getKey();
            DerivedMetricsStripedTable table = entry.getValue();
            if (take.test(key) == false) {
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
    private Drained take(TableKey key, DerivedMetricsStripedTable table, boolean reopening) {
        if (tables.remove(key, table) == false) {
            return null;
        }
        long released = table.seal();
        // The stripes are folded into one table before anything downstream sees them, so a series two threads recorded is one series in
        // one document — striping is invisible past this point.
        DerivedMetricsSeriesTable merged = table.merge();
        // Merging allocates, so the breaker can refuse partway through it. The budget for those series was already given back above, so
        // the accounting stays exact; what is lost is the observations, which are counted where every other breaker loss is.
        droppedSeriesAtBreaker.add(table.seriesLostMerging());
        remember(key, merged.size());
        totalSeries.addAndGet(-(int) released);
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
    public long droppedSeriesAtBreaker() {
        return droppedSeriesAtBreaker.sum();
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
        return droppedSeriesAtNodeCap.sum() + droppedSeriesAtStreamCap.sum() + droppedSeriesAtBreaker.sum();
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
    }
}
