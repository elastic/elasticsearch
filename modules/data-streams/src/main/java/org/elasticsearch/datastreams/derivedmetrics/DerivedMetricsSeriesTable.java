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
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;

/**
 * Every series of one metric within one interval bucket.
 *
 * <p>A dimension tuple is interned into a dense ordinal and the accumulator state lives in parallel arrays indexed by that ordinal, the
 * same shape the metric aggregations use. That is 56 bytes per series with no per-series object, against roughly 340 bytes when each
 * series was a key holding two lists plus an accumulator — and, just as importantly, recording an observation for a series that already
 * exists allocates nothing at all.
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
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    private boolean sealed;
    private boolean closed;

    /**
     * @param histogram        whether this table accumulates distributions rather than scalars, which is a property of the metric
     * @param histogramBuckets the bucket capacity of each series' histogram, which is what bounds its size
     */
    public DerivedMetricsSeriesTable(
        BigArrays bigArrays,
        boolean histogram,
        int histogramBuckets,
        ExponentialHistogramCircuitBreaker histogramBreaker
    ) {
        this.bigArrays = bigArrays;
        this.histogramBuckets = histogramBuckets;
        this.histogramBreaker = histogramBreaker;
        BytesRefHash hash = null;
        try {
            hash = new BytesRefHash(1, bigArrays);
            sum = bigArrays.newDoubleArray(1, true);
            min = bigArrays.newDoubleArray(1, false);
            max = bigArrays.newDoubleArray(1, false);
            first = bigArrays.newDoubleArray(1, true);
            last = bigArrays.newDoubleArray(1, true);
            count = bigArrays.newLongArray(1, true);
            histograms = histogram ? bigArrays.newObjectArray(1) : null;
            this.dimensions = hash;
            hash = null;
        } finally {
            // if any allocation above tripped the breaker, give back what we did take
            if (hash != null) {
                Releasables.close(hash, sum, min, max, first, last, count, histograms);
            }
        }
    }

    /**
     * Records one observation against the series identified by the encoded dimension tuple.
     *
     * @return the ordinal the observation landed on, which is negative when the series already existed. Callers use the sign to know
     *         whether a new series was created without a second lookup.
     */
    public long record(BytesRef encodedDimensions, double value) {
        long ordinal = dimensions.add(encodedDimensions);
        boolean created = ordinal >= 0;
        if (created == false) {
            ordinal = -1 - ordinal;
        } else {
            grow(ordinal);
            min.set(ordinal, Double.POSITIVE_INFINITY);
            max.set(ordinal, Double.NEGATIVE_INFINITY);
        }
        if (count.get(ordinal) == 0) {
            first.set(ordinal, value);
        }
        last.set(ordinal, value);
        count.increment(ordinal, 1);
        sum.increment(ordinal, value);
        min.set(ordinal, Math.min(min.get(ordinal), value));
        max.set(ordinal, Math.max(max.get(ordinal), value));
        if (histograms != null) {
            // The generator is created on first use rather than up front, so a series that is interned and then refused by a cap never
            // pays for one. It is charged against the same breaker as everything else here.
            ExponentialHistogramGenerator generator = histograms.get(ordinal);
            if (generator == null) {
                generator = ExponentialHistogramGenerator.create(histogramBuckets, histogramBreaker);
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
        Releasables.close(dimensions, sum, min, max, first, last, count, histograms);
    }
}
