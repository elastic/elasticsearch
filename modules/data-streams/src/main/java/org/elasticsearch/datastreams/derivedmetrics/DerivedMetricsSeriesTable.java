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
import org.elasticsearch.common.breaker.CircuitBreakingException;
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

import java.util.concurrent.atomic.LongAdder;

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
 * The critical section is a hash lookup and a handful of array writes.
 *
 * <p>That lock has been measured rather than assumed, and it does not scale. On the default configuration — built-in ingest metrics with
 * no dimensions, so one series per metric — aggregate throughput peaks at four threads and regresses below the single-thread figure by
 * eight. It is not a live problem: even collapsed the node observes over two million documents a second, roughly thirty times an
 * optimistic per-node write rate, and a real write thread spends about 0.32% of its time in here rather than the benchmark's hundred
 * percent, so continuous contention cannot arise at realistic rates.
 *
 * <p>The collapse belongs specifically to configurations that never read {@code _source}. With a parse in the path roughly half the work
 * is lock-free and throughput scales monotonically; without one this monitor is very nearly the whole of an observation, so threads
 * convoy on it.
 *
 * <p>Note what would <em>not</em> fix it. Striping by series hash does nothing here, because the shape that contends worst has a single
 * series and could never occupy more than one stripe. Striping per <em>thread</em> fixes exactly that case, and it is viable precisely
 * because the two failure modes are inverse: the configuration that contends worst is the cheapest to replicate per thread, while the
 * high-cardinality configuration that would be expensive to replicate barely contends at all. It still has to be bounded rather than
 * unconditional — see the contention section of the design note for the threshold that separates them.
 */
public class DerivedMetricsSeriesTable implements Releasable {

    private final BigArrays bigArrays;
    private final BytesRefHash dimensions;
    private DoubleArray sum;
    private DoubleArray min;
    private DoubleArray max;
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
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    /** How many series the columns can hold, cached so the common path does not re-check every column. See {@link #reserveOneMore}. */
    private long capacity;
    /**
     * Bytes this table's distributions currently hold. A {@link LongAdder} because it is written on the indexing thread as observations
     * arrive and again on the flush thread as distributions are handed away, and read by whoever is deciding what to flush next.
     */
    private final LongAdder histogramBytes = new LongAdder();
    private long histogramsRefused;
    /** Set when the dimension hash refused an insert and can therefore no longer be probed safely. See {@link #record}. */
    private boolean poisoned;
    private boolean sealed;
    private boolean closed;

    /**
     * @param reduction        what this table's metric reduces to, which decides whether it needs a distribution column
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
        // Distributions are the one part of a table whose size cannot be read back from the structure holding it, so it is tallied on
        // the way through. Everything else is Accountable and reports itself; see bytesHeld.
        LongAdder histogramTally = this.histogramBytes;
        ExponentialHistogramCircuitBreaker delegate = histogramBreaker;
        this.histogramBreaker = bytes -> {
            histogramTally.add(bytes);
            delegate.adjustBreaker(bytes);
        };
        boolean histogram = reduction.isHistogram();
        BytesRefHash hash = null;
        ExponentialHistogramMerger.Factory mergers = null;
        try {
            hash = new BytesRefHash(1, bigArrays);
            sum = bigArrays.newDoubleArray(1, true);
            min = bigArrays.newDoubleArray(1, false);
            max = bigArrays.newDoubleArray(1, false);
            count = bigArrays.newLongArray(1, true);
            histograms = histogram ? bigArrays.newObjectArray(1) : null;
            mergers = histogram ? ExponentialHistogramMerger.createFactory(histogramBuckets, this.histogramBreaker) : null;
            this.dimensions = hash;
            this.histogramMergers = mergers;
            hash = null;
        } finally {
            // if any allocation above tripped the breaker, give back what we did take
            if (hash != null) {
                Releasables.close(hash, sum, min, max, count, histograms, mergers);
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
        // Room for one more series is reserved before the tuple is interned, so that a breaker refusal leaves the table exactly as it
        // was. Interning first and growing second leaves a tuple in the hash with no columns behind it: the caller does not count the
        // series because the exception reached it, seal() gives it back anyway because it reports dimensions.size(), and emit walks
        // ordinals up to that same size and reads past the end of the columns — turning a refused observation into a failed flush.
        reserveOneMore();
        long ordinal;
        try {
            ordinal = dimensions.add(encodedDimensions);
        } catch (CircuitBreakingException e) {
            // BytesRefHash is not exception safe. add() points a hash slot at the new id, then appends the key, then increments its
            // size; and the append itself records the entry's end offset before copying the bytes. A refusal in between leaves a slot
            // referring to an entry whose bytes were never written, so any later add or find that probes that slot reads past the end
            // of the byte storage and throws. The series already interned are still intact — size was never incremented, so they are
            // exactly the ordinals below size() — but nothing may touch this hash again.
            poisoned = true;
            throw e;
        }
        boolean created = ordinal >= 0;
        if (created == false) {
            ordinal = -1 - ordinal;
        } else {
            min.set(ordinal, Double.POSITIVE_INFINITY);
            max.set(ordinal, Double.NEGATIVE_INFINITY);
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
                try {
                    generator = ExponentialHistogramGenerator.create(histogramBuckets, histogramMergers, histogramBreaker);
                } catch (CircuitBreakingException e) {
                    // The series itself is already interned and its scalar columns already updated, so throwing here would strand it in
                    // the same uncounted state the reservation above exists to prevent. The series keeps its place and reports an empty
                    // distribution; the refusal is counted rather than hidden, because an empty histogram is otherwise indistinguishable
                    // from a series that genuinely observed nothing.
                    histogramsRefused++;
                    return created ? ordinal : -1 - ordinal;
                }
                histograms.set(ordinal, generator);
            }
            generator.add(value);
        }
        return created ? ordinal : -1 - ordinal;
    }

    /**
     * Makes sure the columns can hold one more series than they currently do. Called before interning rather than after, so that the hash
     * and the columns can never disagree about how many series exist; see {@link #record}.
     *
     * <p>The capacity is cached so that the common path — an observation for a series that already exists — costs one comparison rather
     * than one growth check per column.
     */
    private void reserveOneMore() {
        long required = dimensions.size() + 1;
        if (required <= capacity) {
            return;
        }
        sum = bigArrays.grow(sum, required);
        min = bigArrays.grow(min, required);
        max = bigArrays.grow(max, required);
        count = bigArrays.grow(count, required);
        if (histograms != null) {
            histograms = bigArrays.grow(histograms, required);
        }
        // every column is grown to at least `required`, so the smallest of them is what the table can actually hold
        capacity = Math.min(Math.min(sum.size(), min.size()), Math.min(max.size(), count.size()));
        if (histograms != null) {
            capacity = Math.min(capacity, histograms.size());
        }
    }

    /** How many distributions this table could not accumulate because the breaker refused them. */
    long histogramsRefused() {
        return histogramsRefused;
    }

    /**
     * Whether this table's dimension hash has been left in a state that cannot be probed again. The series it already holds are still
     * readable and worth emitting; it simply must not receive another observation. See {@link #record}.
     */
    boolean poisoned() {
        return poisoned;
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
     * How many bytes this table is responsible for, which is what makes one metric comparable with another when the node has to decide
     * what to give up first. Series counts cannot do that job: a histogram series is roughly thirty times the size of a scalar one, so a
     * table with far fewer series can be the one actually filling the node.
     *
     * <p>Read from the structures themselves rather than tracked separately, so it cannot drift out of step with what is really held.
     */
    public long bytesHeld() {
        long bytes = dimensions.ramBytesUsed() + sum.ramBytesUsed() + min.ramBytesUsed() + max.ramBytesUsed() + count.ramBytesUsed();
        if (histograms != null) {
            bytes += histograms.ramBytesUsed() + histogramBytes.sum();
        }
        return bytes;
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
        Releasables.close(dimensions, sum, min, max, count, histograms, histogramMergers);
    }
}
