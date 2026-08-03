/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.util.List;

/**
 * How many distinct values one metric has been seen using for each of its dimensions, and which of those dimensions have grown so far
 * past their budget that the metric has given up breaking down by them.
 *
 * <p><b>Why this exists.</b> A metric's series count is the product of its dimensions' value counts, so a single runaway dimension — a
 * user id, a pod name, a trace id — is enough to spend the whole node budget and starve every other metric. Before this, the only thing
 * an operator could see was that <em>some</em> series had been refused; nothing said which dimension had done it, and nothing stopped it
 * from doing it again next interval. This answers the first question and {@link #observe} acts on the answer.
 *
 * <p><b>What it costs on the write path.</b> Nothing at all for an observation against a series that already exists, which is the
 * overwhelming majority of them. A dimension value that has never been seen before necessarily produces a dimension <em>tuple</em> that
 * has never been seen before, and therefore a new series — so feeding the sketches only when a series is interned misses no value while
 * skipping every repeat. That makes the count exact with respect to what it observes rather than sampled, and keeps the hot path to a
 * single volatile read of {@link #collapsedMask()}. The sketches are only ever touched from the already-expensive new-series path.
 *
 * <p><b>Sizing.</b> One {@link HyperLogLogPlusPlus} per metric, with one of its buckets per dimension, at precision 8: 256 registers, so
 * roughly 256 bytes per dimension once the sketch has left linear counting, and a relative error around 6.5%. That error is what makes
 * the budget below approximate, which is fine — a budget is a threshold, not a measurement, and being wrong by a few per cent about
 * where a runaway dimension crossed it changes nothing. All of it comes from the same {@link BigArrays} and {@link CircuitBreaker} the
 * rest of the buffer allocates against, so it is bounded and visible in {@code _nodes/stats/breakers} rather than being invisible
 * overhead.
 *
 * <p>Not thread safe. {@link #observe} takes this object's monitor, which it can afford because it only runs when a series is created;
 * {@link #collapsedMask()} is a plain volatile read and is the only part on the per-observation path.
 */
final class DerivedMetricsDimensionCardinality implements Releasable {

    /**
     * 256 registers per dimension. Chosen for size rather than accuracy: the estimate is read to decide whether a dimension has blown
     * past a budget of order a thousand, and 6.5% relative error is far inside what that decision cares about. Doubling the precision
     * would quarter the error and quadruple the memory, which is the wrong trade for a number nobody plots.
     */
    static final int PRECISION = 8;

    /**
     * How many of a metric's dimensions are tracked. The mask that carries the collapse decision to the write path is a single
     * {@code long}, because reading it is the only per-observation cost this class has and a word is the cheapest thing to read. A
     * metric with more dimensions than this has already lost the cardinality argument by configuration.
     */
    static final int MAX_TRACKED_DIMENSIONS = Long.SIZE;

    /**
     * How many new series may be interned between two reads of a dimension's estimate. Reading it means summing 256 registers, and
     * during exactly the cardinality explosion this exists to catch <em>every</em> observation creates a series — so checking on each
     * one would put a 256-iteration loop per dimension on the write path at the worst possible moment. The cost of the delay is that a
     * dimension may overshoot its budget by at most this many distinct values before it collapses, which is noise next to a budget of
     * order a thousand.
     */
    static final int CHECK_EVERY = 64;

    /**
     * A metric that is not tracked at all: one with no dimensions, one the node is already tracking its limit of, or one whose sketches
     * the breaker refused. Shared, immutable, and never collapses anything, so the write path needs no null check.
     */
    static final DerivedMetricsDimensionCardinality DISABLED = new DerivedMetricsDimensionCardinality(null, List.of(), 0);

    private final HyperLogLogPlusPlus sketches;
    /** The dimension names, in configuration order, so that what is reported names a dimension rather than an index into one. */
    private final List<String> dimensions;
    private final int tracked;
    /**
     * How many distinct values a dimension may have before the metric stops breaking down by it. Zero disables collapsing while leaving
     * the counting on, which is what an operator who wants the diagnosis without the degradation asks for.
     */
    private final long budget;
    /**
     * One bit per dimension the metric has given up. Volatile because the write path reads it on every observation and only ever needs
     * to see the decision promptly rather than atomically with anything else.
     */
    private volatile long collapsedMask;
    /** New series interned per dimension since that dimension's estimate was last read. See {@link #CHECK_EVERY}. */
    private final int[] sinceChecked;
    /**
     * The start of the most recent bucket this metric opened, so the buffer can drop what it knows about metrics whose streams have
     * stopped writing rather than remembering every metric the node has ever seen.
     */
    private volatile long lastBucketStartMillis;
    /**
     * Set when the breaker refused a sketch mid-collect. {@link HyperLogLogPlusPlus} is not exception safe, so the only safe response is
     * to stop feeding it; what it already holds is still readable and still releases correctly.
     */
    private boolean broken;
    private boolean closed;

    private DerivedMetricsDimensionCardinality(HyperLogLogPlusPlus sketches, List<String> dimensions, long budget) {
        this.sketches = sketches;
        this.dimensions = dimensions;
        this.tracked = Math.min(dimensions.size(), MAX_TRACKED_DIMENSIONS);
        this.budget = budget;
        this.sinceChecked = new int[tracked];
    }

    /**
     * Creates the sketches for one metric, or returns {@link #DISABLED} if the metric has no dimensions or the breaker will not pay for
     * them. Refusing here is deliberately silent and non-fatal: this is diagnostics, and a node too tight on memory to afford a quarter
     * of a kilobyte of sketch should spend what it has on the series themselves.
     *
     * @param budget how many distinct values a dimension may have before the metric collapses it, or zero to only count
     */
    static DerivedMetricsDimensionCardinality create(BigArrays bigArrays, CircuitBreaker breaker, List<String> dimensions, long budget) {
        int tracked = Math.min(dimensions.size(), MAX_TRACKED_DIMENSIONS);
        if (tracked == 0) {
            return DISABLED;
        }
        try {
            // One sketch object with one bucket per dimension rather than one object per dimension, and an initial bucket count of zero
            // so that the dense registers are only allocated for a dimension that actually outgrows linear counting. A metric whose
            // dimensions all have a handful of values — which is what a well-configured metric looks like — never pays for them.
            HyperLogLogPlusPlus sketches = new HyperLogLogPlusPlus(PRECISION, bigArrays, breaker, 0);
            return new DerivedMetricsDimensionCardinality(sketches, dimensions, budget);
        } catch (CircuitBreakingException e) {
            return DISABLED;
        }
    }

    /**
     * Which dimensions this metric has stopped breaking down by, as one bit per dimension index. The only thing this class contributes
     * to the per-observation path.
     *
     * <p><b>Why the collapse takes effect immediately rather than at the next bucket.</b> Deferring it would keep each bucket internally
     * consistent: a bucket would be either wholly broken out by a dimension or wholly collapsed, never both. Taking effect at once means
     * the bucket in which a dimension crosses its budget contains both the series that were already broken out and one placeholder
     * series carrying everything after, which is untidy — anyone filtering that bucket by the dimension sees only the values that
     * arrived before the crossing.
     *
     * <p>It is still the right way round, for two reasons. The first is that the artefact is bounded and the alternative is not: an
     * interval can be minutes long, and a dimension that has just been shown to take a new value per document would spend the whole
     * remainder of it doing exactly the thing this exists to stop — which is the starvation, not a cosmetic problem. The second is that
     * the artefact does not affect what the metric is <em>for</em>: the broken-out series and the placeholder series are disjoint sets of
     * observations, so the metric's total over the bucket is exactly right either way, and only the breakdown by the one dimension that
     * has already been declared unusable is mixed, for one bucket, once.
     */
    long collapsedMask() {
        return collapsedMask;
    }

    /**
     * Counts one newly created series' dimension values, and decides whether any dimension has now outgrown its budget.
     *
     * <p>A dimension that has already collapsed is not counted further. Its estimate then rests at roughly the budget, which is the
     * honest answer to "how many values did we see before we gave up" — and continuing to count values that no longer produce series
     * would only spend memory to sharpen a number nobody can act on.
     *
     * @param values the new series' dimension values, null where the document did not have one
     * @return how many dimensions collapsed as a result of this call, which is almost always zero
     */
    synchronized int observe(String[] values) {
        if (sketches == null || closed || broken) {
            return 0;
        }
        long mask = collapsedMask;
        int collapsed = 0;
        for (int dimension = 0; dimension < tracked; dimension++) {
            long bit = 1L << dimension;
            if ((mask & bit) != 0) {
                continue;
            }
            String value = values[dimension];
            if (value == null) {
                // an absent dimension is not a value: a document missing one forms its own series, which is a single series either way
                continue;
            }
            try {
                sketches.collect(dimension, hash(value));
            } catch (CircuitBreakingException e) {
                broken = true;
                return collapsed;
            }
            if (budget > 0 && ++sinceChecked[dimension] >= CHECK_EVERY) {
                sinceChecked[dimension] = 0;
                if (sketches.cardinality(dimension) > budget) {
                    mask |= bit;
                    collapsed++;
                }
            }
        }
        if (collapsed > 0) {
            collapsedMask = mask;
        }
        return collapsed;
    }

    /**
     * A 64 bit hash of a dimension value that allocates nothing, which is what rules out going through {@code String#getBytes} or a
     * {@link org.apache.lucene.util.BytesRef}. FNV-1a over the UTF-16 units is cheap and stable; the final avalanche is not optional,
     * because HyperLogLog reads the low bits as a register index and the leading zeroes of the rest as the run length, and FNV-1a's low
     * bits alone are not well enough distributed for that.
     */
    private static long hash(String value) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }
        return MurmurHash3.fmix(hash);
    }

    /** The estimated number of distinct values seen for one dimension, or zero once the metric has stopped tracking it. */
    synchronized long estimatedValues(int dimension) {
        if (sketches == null || closed || dimension >= tracked) {
            return 0;
        }
        return sketches.cardinality(dimension);
    }

    /** Whether this metric has given up breaking down by one dimension because it outgrew its budget. */
    boolean collapsed(int dimension) {
        return dimension < tracked && (collapsedMask & (1L << dimension)) != 0;
    }

    /** How many of the metric's dimensions are tracked, which is all of them unless it has more than {@link #MAX_TRACKED_DIMENSIONS}. */
    int tracked() {
        return tracked;
    }

    /** The name of one tracked dimension. */
    String dimension(int index) {
        return dimensions.get(index);
    }

    /** Notes that the metric is still producing buckets, so what is known about it is not swept as stale. */
    void markLive(long bucketStartMillis) {
        if (bucketStartMillis > lastBucketStartMillis) {
            lastBucketStartMillis = bucketStartMillis;
        }
    }

    long lastBucketStartMillis() {
        return lastBucketStartMillis;
    }

    /**
     * Idempotent, and takes this object's monitor so that a stream which resumes writing at the exact moment its sketches are being
     * swept records into a closed sketch rather than a released one — {@link #observe} sees {@code closed} and does nothing.
     */
    @Override
    public synchronized void close() {
        if (closed || sketches == null) {
            return;
        }
        closed = true;
        sketches.close();
    }
}
