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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    private final BigArrays bigArrays;
    private final ConcurrentHashMap<TableKey, DerivedMetricsSeriesTable> tables = new ConcurrentHashMap<>();
    /**
     * How many times each bucket has already been emitted. Only ever non-zero under {@code flush_early}, and swept alongside the tables
     * once the bucket is closed.
     */
    private final ConcurrentHashMap<TableKey, AtomicInteger> partials = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> perStream = new ConcurrentHashMap<>();
    private final LongAdder droppedSeries = new LongAdder();
    private final AtomicInteger totalSeries = new AtomicInteger();
    private final int maxSeries;
    private final int maxSeriesPerStream;
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    private final int partialSeed;

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
        this.bigArrays = bigArrays;
        this.maxSeries = maxSeries;
        this.maxSeriesPerStream = maxSeriesPerStream;
        this.histogramBuckets = histogramBuckets;
        this.histogramBreaker = histogramBreaker(bigArrays);
        this.partialSeed = partialSeed;
    }

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
    public boolean record(TableKey key, String[] values, Scratch scratch, double value) {
        BytesRef encoded = DerivedMetricsDimensionCodec.encode(values, key.metric().dimensions().size(), scratch);
        AtomicInteger held = perStream.computeIfAbsent(key.sourceDataStream(), unused -> new AtomicInteger());
        while (true) {
            DerivedMetricsSeriesTable table = tables.get(key);
            if (table == null) {
                table = openTable(key);
                if (table == null) {
                    return false;
                }
            }
            synchronized (table) {
                if (table.sealed()) {
                    // drained while we were waiting for the lock, so it will never be emitted again; start over on the fresh bucket
                    continue;
                }
                // Probing the table is the expensive part of this critical section, so the common path does it once: record and let
                // the returned sign say whether a series was created. Only when a cap is already reached does it cost a second probe,
                // because there we must know before interning — the table has no way to remove a series it should not have taken.
                if (totalSeries.get() >= maxSeries || held.get() >= maxSeriesPerStream) {
                    if (table.contains(encoded) == false) {
                        droppedSeries.increment();
                        return false;
                    }
                }
                try {
                    if (table.record(encoded, value) >= 0) {
                        totalSeries.incrementAndGet();
                        held.incrementAndGet();
                    }
                } catch (CircuitBreakingException e) {
                    droppedSeries.increment();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Creates the table for a bucket, or returns null when the breaker refuses it.
     */
    private DerivedMetricsSeriesTable openTable(TableKey key) {
        try {
            return tables.computeIfAbsent(
                key,
                unused -> new DerivedMetricsSeriesTable(bigArrays, key.metric().reduction(), histogramBuckets, histogramBreaker)
            );
        } catch (CircuitBreakingException e) {
            droppedSeries.increment();
            return null;
        }
    }

    /**
     * Removes every table that can no longer receive observations, that is every bucket whose interval ended at least
     * {@code graceMillis} ago. The grace period covers writes still in flight when the interval closes.
     *
     * <p>The caller owns the returned tables and <em>must</em> close them, or their circuit breaker accounting leaks.
     */
    public List<Drained> drainClosed(long nowMillis, long graceMillis) {
        Predicate<TableKey> closed = key -> key.bucketStartMillis() + key.intervalMillis() + graceMillis <= nowMillis;
        List<Drained> drained = drain(closed, false);
        // A closed bucket receives nothing further, so its partial count has nothing left to keep it honest. Sweeping here rather than in
        // drain covers buckets that were flushed early and then never saw another write.
        partials.keySet().removeIf(key -> closed.test(key) && tables.containsKey(key) == false);
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
        DerivedMetricsSeriesTable table = tables.get(key);
        if (table == null) {
            return null;
        }
        AtomicInteger counter = partials.get(key);
        if (counter != null && counter.get() >= maxPartials(key) - 1) {
            // no offset left inside the interval, so a further partial would collide or land in the next bucket
            droppedSeries.increment();
            return null;
        }
        return take(key, table, true);
    }

    /**
     * Removes everything currently buffered, including buckets that are still open. Used on shutdown so partial intervals are not lost.
     */
    public List<Drained> drainAll() {
        List<Drained> drained = drain(key -> true, false);
        partials.clear();
        return drained;
    }

    private List<Drained> drain(Predicate<TableKey> take, boolean reopening) {
        List<Drained> drained = new ArrayList<>();
        Iterator<Map.Entry<TableKey, DerivedMetricsSeriesTable>> iterator = tables.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableKey, DerivedMetricsSeriesTable> entry = iterator.next();
            TableKey key = entry.getKey();
            DerivedMetricsSeriesTable table = entry.getValue();
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
     * Removes one table and gives its budget back, or returns null if someone else got there first.
     *
     * <p>The table is removed from the map <em>before</em> it is sealed, so a writer that finds it sealed is guaranteed to see the
     * replacement on its next lookup rather than spinning. It is removed by value rather than by key, so a bucket recreated by a
     * concurrent write survives.
     */
    private Drained take(TableKey key, DerivedMetricsSeriesTable table, boolean reopening) {
        if (tables.remove(key, table) == false) {
            return null;
        }
        long released;
        synchronized (table) {
            released = table.seal();
        }
        totalSeries.addAndGet(-(int) released);
        AtomicInteger held = perStream.get(key.sourceDataStream());
        if (held != null && held.addAndGet(-(int) released) <= 0) {
            perStream.remove(key.sourceDataStream(), held);
        }
        AtomicInteger counter = partials.get(key);
        int partial = counter == null ? partialSeed : counter.get();
        if (reopening) {
            partials.computeIfAbsent(key, unused -> new AtomicInteger(partialSeed)).incrementAndGet();
        }
        return new Drained(key, table, partial);
    }

    /** Series currently held, across every table. */
    public int size() {
        return totalSeries.get();
    }

    public long droppedSeries() {
        return droppedSeries.sum();
    }

    // visible for testing
    int partialsTracked() {
        return partials.size();
    }

    // visible for testing
    int seriesFor(String sourceDataStream) {
        AtomicInteger held = perStream.get(sourceDataStream);
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
