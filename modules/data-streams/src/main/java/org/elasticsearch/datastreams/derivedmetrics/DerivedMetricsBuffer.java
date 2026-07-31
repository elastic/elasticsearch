/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Node-local state for derived metrics: one accumulator per series and interval bucket.
 *
 * <p>This is what makes derived document intake independent of write volume. However many documents a stream receives, a node only ever
 * holds one accumulator per (source stream, metric, interval, dimension combination) and emits one document for it per interval. Nothing
 * is coordinated across nodes; each node emits its own partial series and queries reduce across the emitting-node dimension.
 *
 * <p>Series count is the one thing that does grow with the data, since dimension values come from documents. It is capped per node, and
 * once the cap is reached new series are dropped rather than allowed to consume unbounded heap.
 */
public class DerivedMetricsBuffer {

    /**
     * Identifies one derived series. Dimension names and values are parallel lists holding only the dimensions the document actually
     * had, so documents missing a dimension form their own series rather than sharing an artificial "missing" value.
     */
    public record SeriesKey(
        ProjectId project,
        String sourceDataStream,
        String metricName,
        String interval,
        Reduction reduction,
        List<String> dimensionNames,
        List<String> dimensionValues
    ) {}

    public record BucketKey(SeriesKey series, long bucketStartMillis, long intervalMillis) {}

    /**
     * Mutable per-bucket state. Updates are serialized on the accumulator itself: contention is per series, and the critical section is
     * a handful of field updates.
     */
    public static final class Accumulator {
        private long count;
        private double sum;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double first;
        private double last;

        synchronized void add(double value) {
            if (count == 0) {
                first = value;
            }
            last = value;
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        public synchronized long count() {
            return count;
        }

        /**
         * Reduces the observations in this bucket into the single value that gets emitted.
         */
        public synchronized double reduce(Reduction reduction, long intervalMillis) {
            return switch (reduction) {
                case SUM -> sum;
                case MIN -> min;
                case MAX -> max;
                case AVG -> count == 0 ? 0.0 : sum / count;
                case FIRST -> first;
                case LAST -> last;
                case RATE -> sum / (intervalMillis / 1000.0);
            };
        }
    }

    private final ConcurrentHashMap<BucketKey, Accumulator> buckets = new ConcurrentHashMap<>();
    // Series held per source stream, so one stream's cardinality cannot be paid for out of another's budget.
    private final ConcurrentHashMap<String, AtomicInteger> perStream = new ConcurrentHashMap<>();
    private final LongAdder droppedSeries = new LongAdder();
    private final int maxSeries;
    private final int maxSeriesPerStream;

    public DerivedMetricsBuffer(int maxSeries) {
        this(maxSeries, maxSeries);
    }

    /**
     * @param maxSeries          ceiling for the node as a whole
     * @param maxSeriesPerStream ceiling for any single source stream. Without it the node budget is first-come-first-served, so one
     *                           high-cardinality stream can consume all of it and silently starve every other stream's metrics.
     */
    public DerivedMetricsBuffer(int maxSeries, int maxSeriesPerStream) {
        this.maxSeries = maxSeries;
        this.maxSeriesPerStream = maxSeriesPerStream;
    }

    /**
     * Records one observation. Returns false when the observation was dropped because the node is already tracking as many series as it
     * is allowed to.
     */
    public boolean record(BucketKey key, double value) {
        Accumulator accumulator = buckets.get(key);
        if (accumulator == null) {
            String stream = key.series().sourceDataStream();
            AtomicInteger held = perStream.computeIfAbsent(stream, unused -> new AtomicInteger());
            if (buckets.size() >= maxSeries || held.get() >= maxSeriesPerStream) {
                droppedSeries.increment();
                return false;
            }
            boolean[] created = new boolean[1];
            accumulator = buckets.computeIfAbsent(key, unused -> {
                created[0] = true;
                return new Accumulator();
            });
            if (created[0]) {
                held.incrementAndGet();
            }
        }
        accumulator.add(value);
        return true;
    }

    /**
     * Removes and returns every bucket that can no longer receive observations, that is every bucket whose interval ended at least
     * {@code graceMillis} ago. The grace period covers writes that are still in flight when the interval closes.
     */
    public List<Map.Entry<BucketKey, Accumulator>> drainClosed(long nowMillis, long graceMillis) {
        List<Map.Entry<BucketKey, Accumulator>> closed = new ArrayList<>();
        Iterator<Map.Entry<BucketKey, Accumulator>> iterator = buckets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BucketKey, Accumulator> entry = iterator.next();
            BucketKey key = entry.getKey();
            if (key.bucketStartMillis() + key.intervalMillis() + graceMillis <= nowMillis) {
                closed.add(Map.entry(key, entry.getValue()));
                iterator.remove();
                released(key);
            }
        }
        return closed;
    }

    /**
     * Removes and returns everything currently buffered, including buckets that are still open. Used on shutdown so that partial
     * intervals are not silently lost.
     */
    public List<Map.Entry<BucketKey, Accumulator>> drainAll() {
        List<Map.Entry<BucketKey, Accumulator>> drained = new ArrayList<>(buckets.size());
        Iterator<Map.Entry<BucketKey, Accumulator>> iterator = buckets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BucketKey, Accumulator> entry = iterator.next();
            drained.add(Map.entry(entry.getKey(), entry.getValue()));
            iterator.remove();
            released(entry.getKey());
        }
        return drained;
    }

    private void released(BucketKey key) {
        AtomicInteger held = perStream.get(key.series().sourceDataStream());
        if (held != null && held.decrementAndGet() <= 0) {
            perStream.remove(key.series().sourceDataStream(), held);
        }
    }

    public int size() {
        return buckets.size();
    }

    // visible for testing
    int seriesFor(String sourceDataStream) {
        AtomicInteger held = perStream.get(sourceDataStream);
        return held == null ? 0 : held.get();
    }

    public long droppedSeries() {
        return droppedSeries.sum();
    }

    /**
     * The start of the bucket that {@code nowMillis} falls into. Buckets are aligned to the epoch so that every node in the cluster
     * agrees on the boundaries without any coordination.
     */
    public static long bucketStart(long nowMillis, long intervalMillis) {
        return nowMillis - Math.floorMod(nowMillis, intervalMillis);
    }
}
