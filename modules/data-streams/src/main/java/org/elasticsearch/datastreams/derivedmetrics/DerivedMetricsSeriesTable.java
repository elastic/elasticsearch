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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramGenerator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;

/**
 * Every series of one metric within one interval bucket.
 *
 * <p>A dimension tuple is interned into a dense ordinal and the accumulator state lives in parallel arrays indexed by that ordinal, the
 * same shape the metric aggregations use. There is no per-series object for a scalar metric — 48 bytes of accumulator columns plus the
 * interned tuple, measured at about 152 bytes per series all in — and, just as importantly, recording an observation for a series that
 * already exists allocates nothing at all. A histogram series is far more expensive, since a distribution cannot be kept in primitives;
 * see {@link #histograms}.
 *
 * <p>All storage comes from {@link BigArrays}, so growth is accounted against the derived metrics circuit breaker and shows up in
 * {@code _nodes/stats/breakers}. The table is {@link Releasable} and <em>must</em> be closed once drained, or that accounting leaks.
 *
 * <p>Not thread safe: {@link BytesRefHash} is not, and neither is growing a {@link BigArrays} array. Callers synchronize on the table.
 * The critical section is a hash lookup and a handful of array writes, so one lock per metric per bucket is cheap; if it ever proves
 * otherwise the tables can be striped and merged at flush.
 */
public class DerivedMetricsSeriesTable implements Releasable {

    private final BigArrays bigArrays;
    private final BytesRefHash dimensions;
    private DoubleArray sum;
    private DoubleArray min;
    private DoubleArray max;
    private DoubleArray first;
    private DoubleArray last;
    private LongArray count;
    /**
     * One accumulator per series, and only for a histogram metric. Unlike the scalar columns this holds an object per series, because a
     * distribution cannot be kept in a handful of primitives; see {@link #histograms} usage in {@link #record}.
     */
    private ObjectArray<ExponentialHistogramGenerator> histograms;
    /**
     * Shared by every series in this table, so the scratch space merging needs is held once rather than per series. Measured at about 7%
     * of what a histogram series costs — the rest of it is the distribution itself, which is inherently per-series.
     */
    private final ExponentialHistogramMerger.Factory histogramMergers;
    /**
     * When the observation behind {@link #first}/{@link #last} happened, allocated only for those reductions.
     *
     * <p>Without it a first or last value cannot be resolved across nodes: each node holds its own last observation and there is no
     * ordering between them. Downsampling does not need this because it works on one shard of a TSDS, where a series is co-located by
     * construction; a plain data stream spreads one entity across shards, so the ordering has to be carried explicitly.
     */
    private LongArray observedAt;
    private final boolean keepEarliestObservation;
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    private boolean sealed;
    private boolean closed;

    /**
     * @param reduction        what this table's metric reduces to, which decides which columns it needs: a distribution for a histogram,
     *                         an observation time for first and last, nothing extra for the rest
     * @param histogramBuckets the bucket capacity of each series' histogram, which is what bounds its size
     */
    public DerivedMetricsSeriesTable(
        BigArrays bigArrays,
        Reduction reduction,
        int histogramBuckets,
        ExponentialHistogramCircuitBreaker histogramBreaker
    ) {
        this.bigArrays = bigArrays;
        this.histogramBuckets = histogramBuckets;
        this.histogramBreaker = histogramBreaker;
        boolean histogram = reduction.isHistogram();
        // first keeps the earliest observation, last the most recent; everything else needs no time at all
        this.keepEarliestObservation = reduction == Reduction.FIRST;
        boolean positional = reduction == Reduction.FIRST || reduction == Reduction.LAST;
        BytesRefHash hash = null;
        ExponentialHistogramMerger.Factory mergers = null;
        try {
            hash = new BytesRefHash(1, bigArrays);
            sum = bigArrays.newDoubleArray(1, true);
            min = bigArrays.newDoubleArray(1, false);
            max = bigArrays.newDoubleArray(1, false);
            first = bigArrays.newDoubleArray(1, true);
            last = bigArrays.newDoubleArray(1, true);
            count = bigArrays.newLongArray(1, true);
            histograms = histogram ? bigArrays.newObjectArray(1) : null;
            observedAt = positional ? bigArrays.newLongArray(1, true) : null;
            mergers = histogram ? ExponentialHistogramMerger.createFactory(histogramBuckets, histogramBreaker) : null;
            this.dimensions = hash;
            this.histogramMergers = mergers;
            hash = null;
        } finally {
            // if any allocation above tripped the breaker, give back what we did take
            if (hash != null) {
                Releasables.close(hash, sum, min, max, first, last, count, histograms, observedAt, mergers);
            }
        }
    }

    /**
     * Records one observation against the series identified by the encoded dimension tuple.
     *
     * @return the ordinal the observation landed on, which is negative when the series already existed. Callers use the sign to know
     *         whether a new series was created without a second lookup.
     */
    public long record(BytesRef encodedDimensions, double value, long observedAtMillis) {
        long ordinal = dimensions.add(encodedDimensions);
        boolean created = ordinal >= 0;
        if (created == false) {
            ordinal = -1 - ordinal;
        } else {
            grow(ordinal);
            min.set(ordinal, Double.POSITIVE_INFINITY);
            max.set(ordinal, Double.NEGATIVE_INFINITY);
        }
        boolean firstObservation = count.get(ordinal) == 0;
        if (firstObservation) {
            first.set(ordinal, value);
        }
        last.set(ordinal, value);
        if (observedAt != null && (firstObservation || keepEarliestObservation == false)) {
            // a first metric freezes the earliest observation time, a last metric tracks the most recent
            observedAt.set(ordinal, observedAtMillis);
        }
        count.increment(ordinal, 1);
        sum.increment(ordinal, value);
        min.set(ordinal, Math.min(min.get(ordinal), value));
        max.set(ordinal, Math.max(max.get(ordinal), value));
        if (histograms != null) {
            // The generator is created on first use rather than up front, so a series that is interned and then refused by a cap never
            // pays for one. It is charged against the same breaker as everything else here.
            ExponentialHistogramGenerator generator = histograms.get(ordinal);
            if (generator == null) {
                generator = ExponentialHistogramGenerator.create(histogramBuckets, histogramMergers, histogramBreaker);
                histograms.set(ordinal, generator);
            }
            generator.add(value);
        }
        return created ? ordinal : -1 - ordinal;
    }

    private void grow(long ordinal) {
        long size = ordinal + 1;
        sum = bigArrays.grow(sum, size);
        min = bigArrays.grow(min, size);
        max = bigArrays.grow(max, size);
        first = bigArrays.grow(first, size);
        last = bigArrays.grow(last, size);
        count = bigArrays.grow(count, size);
        if (observedAt != null) {
            observedAt = bigArrays.grow(observedAt, size);
        }
        if (histograms != null) {
            histograms = bigArrays.grow(histograms, size);
        }
    }

    /** Whether this table already holds the series, so the caller can charge its budget before interning a new one. */
    public boolean contains(BytesRef encodedDimensions) {
        return dimensions.find(encodedDimensions) >= 0;
    }

    /** How many distinct series this table holds. */
    public long size() {
        return dimensions.size();
    }

    /**
     * Marks this table as removed from the buffer, so that a writer which read it just before it was drained knows to look the bucket up
     * again rather than record into a table nobody will ever emit.
     *
     * @return the number of series the table held, which is what the buffer gives back to its budget
     */
    long seal() {
        sealed = true;
        return dimensions.size();
    }

    /** Whether this table has been drained. Read under the table's lock, like everything else here. */
    boolean sealed() {
        return sealed;
    }

    /** The dimension values of one series, one entry per configured dimension and null where the document had none. */
    public String[] dimensionsOf(long ordinal, int dimensionCount, BytesRef spare) {
        return DerivedMetricsDimensionCodec.decode(dimensions.get(ordinal, spare), dimensionCount);
    }

    /**
     * When the value this series would emit was observed. Only meaningful on a table backing a {@code first}/{@code last} reduction.
     */
    public long observedAtOf(long ordinal) {
        assert observedAt != null : "observedAtOf on a table that does not track observation times";
        return observedAt.get(ordinal);
    }

    public long countOf(long ordinal) {
        return count.get(ordinal);
    }

    /**
     * The distribution accumulated for one series. Only meaningful on a histogram table, and the caller owns the result: it must be
     * closed, or the memory it was charged for is never given back.
     */
    public ReleasableExponentialHistogram histogramOf(long ordinal) {
        assert histograms != null : "histogramOf on a table that does not accumulate distributions";
        ExponentialHistogramGenerator generator = histograms.get(ordinal);
        return generator == null ? ReleasableExponentialHistogram.empty() : generator.getAndClear();
    }

    /** Reduces one series into the single value that gets emitted. */
    public double reduce(long ordinal, Reduction reduction, long intervalMillis) {
        return switch (reduction) {
            // An avg gauge emits its sum and its count rather than the mean, so that averaging across partials and across buckets of
            // unequal volume stays exact. See DerivedMetricsEmitter.
            case SUM, AVG -> sum.get(ordinal);
            case MIN -> min.get(ordinal);
            case MAX -> max.get(ordinal);
            case FIRST -> first.get(ordinal);
            case LAST -> last.get(ordinal);
            case RATE -> sum.get(ordinal) / (intervalMillis / 1000.0);
            case HISTOGRAM -> throw new AssertionError("a histogram metric is emitted through histogramOf, not reduced to a value");
        };
    }

    /**
     * Idempotent, because a drained table can be closed both by the emission that consumed it and by the error path that gave up on it,
     * and releasing twice would credit the circuit breaker for memory it never got back.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (histograms != null) {
            for (long ordinal = 0; ordinal < histograms.size(); ordinal++) {
                Releasables.close(histograms.get(ordinal));
            }
        }
        Releasables.close(dimensions, sum, min, max, first, last, count, histograms, observedAt, histogramMergers);
    }
}
