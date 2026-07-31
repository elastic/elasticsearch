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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.CompiledMetric;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

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

    private final BigArrays bigArrays;
    private final ConcurrentHashMap<TableKey, DerivedMetricsSeriesTable> tables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> perStream = new ConcurrentHashMap<>();
    private final LongAdder droppedSeries = new LongAdder();
    private final AtomicInteger totalSeries = new AtomicInteger();
    private final int maxSeries;
    private final int maxSeriesPerStream;

    public DerivedMetricsBuffer(BigArrays bigArrays, int maxSeries) {
        this(bigArrays, maxSeries, maxSeries);
    }

    public DerivedMetricsBuffer(BigArrays bigArrays, int maxSeries, int maxSeriesPerStream) {
        this.bigArrays = bigArrays;
        this.maxSeries = maxSeries;
        this.maxSeriesPerStream = maxSeriesPerStream;
    }

    /**
     * Records one observation. Returns false when it was dropped, either because a cap was reached or because the circuit breaker
     * refused the memory the new series would have needed.
     *
     * @param values one entry per dimension the metric configures, null where the document did not have it
     */
    public boolean record(TableKey key, String[] values, Scratch scratch, double value) {
        DerivedMetricsSeriesTable table = tables.get(key);
        if (table == null) {
            table = openTable(key);
            if (table == null) {
                return false;
            }
        }
        BytesRef encoded = DerivedMetricsDimensionCodec.encode(values, key.metric().dimensions().size(), scratch);
        AtomicInteger held = perStream.computeIfAbsent(key.sourceDataStream(), unused -> new AtomicInteger());
        synchronized (table) {
            // Reserve before creating: a series that would exceed a cap must not be interned, or the table would hold it forever.
            if (table.contains(encoded) == false) {
                if (totalSeries.get() >= maxSeries || held.get() >= maxSeriesPerStream) {
                    droppedSeries.increment();
                    return false;
                }
                totalSeries.incrementAndGet();
                held.incrementAndGet();
            }
            try {
                table.record(encoded, value);
            } catch (CircuitBreakingException e) {
                totalSeries.decrementAndGet();
                held.decrementAndGet();
                droppedSeries.increment();
                return false;
            }
        }
        return true;
    }

    /**
     * Creates the table for a bucket, or returns null when the breaker refuses it.
     */
    private DerivedMetricsSeriesTable openTable(TableKey key) {
        try {
            return tables.computeIfAbsent(key, unused -> new DerivedMetricsSeriesTable(bigArrays));
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
    public List<Map.Entry<TableKey, DerivedMetricsSeriesTable>> drainClosed(long nowMillis, long graceMillis) {
        return drain(key -> key.bucketStartMillis() + key.intervalMillis() + graceMillis <= nowMillis);
    }

    /**
     * Removes everything currently buffered, including buckets that are still open. Used on shutdown so partial intervals are not lost.
     */
    public List<Map.Entry<TableKey, DerivedMetricsSeriesTable>> drainAll() {
        return drain(key -> true);
    }

    private List<Map.Entry<TableKey, DerivedMetricsSeriesTable>> drain(java.util.function.Predicate<TableKey> take) {
        List<Map.Entry<TableKey, DerivedMetricsSeriesTable>> drained = new ArrayList<>();
        Iterator<Map.Entry<TableKey, DerivedMetricsSeriesTable>> iterator = tables.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableKey, DerivedMetricsSeriesTable> entry = iterator.next();
            if (take.test(entry.getKey()) == false) {
                continue;
            }
            iterator.remove();
            DerivedMetricsSeriesTable table = entry.getValue();
            long released;
            synchronized (table) {
                released = table.size();
            }
            totalSeries.addAndGet(-(int) released);
            AtomicInteger held = perStream.get(entry.getKey().sourceDataStream());
            if (held != null && held.addAndGet(-(int) released) <= 0) {
                perStream.remove(entry.getKey().sourceDataStream(), held);
            }
            drained.add(Map.entry(entry.getKey(), table));
        }
        return drained;
    }

    /** Series currently held, across every table. */
    public int size() {
        return totalSeries.get();
    }

    public long droppedSeries() {
        return droppedSeries.sum();
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
        drainAll().forEach(entry -> entry.getValue().close());
    }
}
