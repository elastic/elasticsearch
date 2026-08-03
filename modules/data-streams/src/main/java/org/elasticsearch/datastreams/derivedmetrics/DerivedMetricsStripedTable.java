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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics.Reduction;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * One metric's accumulator state for one interval bucket, held as one or more per-thread stripes.
 *
 * <p>A {@link DerivedMetricsSeriesTable} is not thread safe, so recording into one costs its monitor. That monitor was measured and it does
 * not scale: on a configuration whose metrics have no dimensions — one series each, and no {@code _source} parse to do outside the lock —
 * aggregate throughput peaks at four threads and regresses below the single-thread figure at eight. Striping by series hash cannot fix
 * that, because the shape that contends worst has a single series and could never occupy more than one stripe. Striping per <em>thread</em>
 * fixes exactly it.
 *
 * <p>Per-thread striping is not free, though, and it cannot be unconditional: it replicates a series once per write thread, and the write
 * pool is sized to the processor count. What makes it viable is that <strong>the two failure modes are inverse</strong> — the configuration
 * that contends worst is the cheapest to replicate, and the high-cardinality configuration that is expensive to replicate barely contends,
 * because its observations already spread across many series. One threshold therefore separates them, and this class holds both sides of
 * it: a shared table is simply a striped one with a single stripe, so there is one code path rather than two.
 * {@link DerivedMetricsBuffer} decides which a bucket gets, from that metric's cardinality in the previous bucket.
 *
 * <p>The cost of getting the choice wrong is bounded. A metric whose cardinality spikes inside a bucket it entered striped replicates until
 * that bucket ends, and then flips to shared; it can never take more memory than the node and per-stream series budgets already allow,
 * because every stripe's copy of a series is charged to those budgets individually. What it does spend is <em>budget</em>: the caps bite
 * sooner in distinct-series terms, by up to the stripe count, for that one bucket.
 *
 * <p>Histogram metrics are never striped. A distribution costs roughly thirty times what a scalar series does, so replicating one is not in
 * the "free" half of the trade above, and a histogram observation is expensive enough that the monitor is a much smaller share of it.
 *
 * <p>A thread's stripe is its thread id masked to the stripe count, which is a field read and an {@code and} — this has to be cheaper than
 * the monitor it is avoiding, which rules out a thread-local lookup. Thread ids are handed out in sequence, so a pool's threads land on
 * distinct stripes in practice; two that do collide simply share a table, which is what every thread did before striping existed.
 */
final class DerivedMetricsStripedTable implements Releasable {

    private final BigArrays bigArrays;
    private final Reduction reduction;
    private final int histogramBuckets;
    private final ExponentialHistogramCircuitBreaker histogramBreaker;
    /**
     * One stripe is allocated eagerly and the rest on first use, so that a bucket always has somewhere to record — including when the
     * breaker refuses a further stripe — and so that a metric only one thread ever writes costs exactly what it did before striping.
     */
    private final AtomicReferenceArray<DerivedMetricsSeriesTable> stripes;
    /**
     * The stripe allocated with the bucket, which is the one belonging to the thread whose observation created it. Anything that needs a
     * stripe that certainly exists uses this one.
     */
    private final int primary;
    /**
     * What this metric's dimensions have been seen to hold, shared with every other bucket of the same metric and owned by the buffer
     * rather than by this object — it deliberately outlives any one bucket, because cardinality is a property of the metric.
     *
     * <p>It is held here so that the write path can read the collapse decision from a field of the bucket it has already looked up,
     * rather than paying for a second concurrent map probe per document per metric. This bucket must never close it.
     */
    private final DerivedMetricsDimensionCardinality dimensionCardinality;
    /**
     * Set once this bucket has been taken out of the buffer, to stop a writer from opening a stripe in a bucket that will never be emitted
     * again. Volatile rather than guarded by this object's monitor throughout {@link #seal()}, because a writer holding a stripe's monitor
     * may itself have to seal this bucket — see the ordering note on {@link #seal()}.
     */
    private volatile boolean sealed;
    private long seriesLostMerging;
    private boolean closed;

    DerivedMetricsStripedTable(
        BigArrays bigArrays,
        Reduction reduction,
        int histogramBuckets,
        ExponentialHistogramCircuitBreaker histogramBreaker,
        int stripes,
        DerivedMetricsDimensionCardinality dimensionCardinality
    ) {
        assert stripes >= 1 && Integer.bitCount(stripes) == 1 : "stripe counts are powers of two so that choosing one is a mask";
        assert stripes == 1 || reduction.isHistogram() == false : "a histogram bucket is too expensive to replicate per thread";
        this.dimensionCardinality = dimensionCardinality;
        this.bigArrays = bigArrays;
        this.reduction = reduction;
        this.histogramBuckets = histogramBuckets;
        this.histogramBreaker = histogramBreaker;
        this.stripes = new AtomicReferenceArray<>(stripes);
        // The creating thread is about to record into its own stripe, so that is the one to allocate: a bucket only ever written by one
        // thread then holds exactly one table. Eagerly, so that the breaker refusing a bucket outright surfaces where the buffer can
        // report it rather than on the first observation.
        this.primary = indexFor(Thread.currentThread());
        this.stripes.set(primary, newStripe());
    }

    /** What this metric's dimensions have been seen to hold. Shared across buckets and never closed by this object. */
    DerivedMetricsDimensionCardinality dimensionCardinality() {
        return dimensionCardinality;
    }

    /** The stripe a thread records into: its id masked, which is a field read and an {@code and}. */
    private int indexFor(Thread thread) {
        // the stripe count is a power of two, so this is a mask rather than the division a processor count would have made it
        return (int) thread.threadId() & (stripes.length() - 1);
    }

    private DerivedMetricsSeriesTable newStripe() {
        return new DerivedMetricsSeriesTable(bigArrays, reduction, histogramBuckets, histogramBreaker);
    }

    /**
     * The table this thread records into, which the caller must synchronize on exactly as it would an unstriped one — the stripe is
     * uncontended in the common case, not unshared: threads beyond the stripe count wrap around onto one another, and a stripe the breaker
     * refused falls back to the one allocated with the bucket.
     *
     * @return null when this bucket has been drained and the caller must look it up again, which is the same signal a sealed stripe gives
     */
    DerivedMetricsSeriesTable stripeForCurrentThread() {
        int index = indexFor(Thread.currentThread());
        DerivedMetricsSeriesTable stripe = stripes.get(index);
        return stripe != null ? stripe : openStripe(index);
    }

    /**
     * Opens the stripe a thread was assigned, under this bucket's monitor so that it cannot be created after the bucket has been sealed —
     * an observation recorded into such a stripe would be silently dropped, because nothing would ever emit it.
     */
    private synchronized DerivedMetricsSeriesTable openStripe(int index) {
        if (sealed) {
            return null;
        }
        DerivedMetricsSeriesTable stripe = stripes.get(index);
        if (stripe == null) {
            try {
                stripe = newStripe();
            } catch (CircuitBreakingException e) {
                // one more stripe is an optimisation, not a requirement: fall back to sharing the stripe that certainly exists rather
                // than dropping the observation, which is the same thing this bucket would have done had it been created shared
                return stripes.get(primary);
            }
            stripes.set(index, stripe);
        }
        return stripe;
    }

    /**
     * Marks this bucket and every stripe in it as drained, so a writer that is mid-observation finishes and then starts again on the
     * replacement bucket.
     *
     * <p>Only ever called by the thread that won the race to remove this bucket from the buffer, so it does not have to tolerate a
     * concurrent seal. It deliberately does <em>not</em> hold this object's monitor while it takes the stripes' monitors: a writer that
     * poisons a stripe seals the bucket while still holding that stripe's monitor, and the two orders together would deadlock.
     *
     * @return how many series the stripes held between them, which is what the buffer gives back to its budgets. Series that several
     *         stripes each interned count once per stripe, because that is how they were charged.
     */
    long seal() {
        synchronized (this) {
            sealed = true;
        }
        // past that point no stripe can be created, so the set below is final
        long held = 0;
        for (int index = 0; index < stripes.length(); index++) {
            DerivedMetricsSeriesTable stripe = stripes.get(index);
            if (stripe != null) {
                synchronized (stripe) {
                    held += stripe.seal();
                }
            }
        }
        return held;
    }

    /**
     * Folds the stripes into the single table the bucket is emitted from, transferring ownership of it to the caller — this object holds
     * nothing afterwards.
     *
     * <p>Merging rather than emitting each stripe separately is what keeps striping invisible downstream: the same series recorded by two
     * threads is one series in one document, exactly as it would have been on a shared table.
     *
     * <p>Called only after {@link #seal()}, which is what makes it safe to touch the stripes without their monitors: sealing takes every
     * stripe's monitor, so no observation is still in flight, and no further one can begin.
     */
    DerivedMetricsSeriesTable merge() {
        assert sealed : "a bucket must be sealed before it is merged";
        int target = largestMergeable();
        DerivedMetricsSeriesTable merged = stripes.getAndSet(target, null);
        BytesRef spare = new BytesRef();
        for (int index = 0; index < stripes.length(); index++) {
            DerivedMetricsSeriesTable stripe = stripes.getAndSet(index, null);
            if (stripe == null) {
                continue;
            }
            try {
                // merging allocates, so the breaker can refuse partway; what it refuses is reported rather than silently missing
                seriesLostMerging += merged.poisoned() ? stripe.size() : merged.mergeFrom(stripe, spare);
            } finally {
                stripe.close();
            }
        }
        return merged;
    }

    /**
     * The stripe everything else folds into, chosen to lose as little as possible when the breaker is already exhausted — which is the
     * only situation in which any of this is not simply "the biggest one".
     *
     * <p>Normally that is the largest stripe: merging into it is the least work, and a bucket whose only populated stripe is the largest
     * one is then handed on untouched. A poisoned stripe cannot be merged into at all, though, because its hash can no longer be probed.
     * When the largest stripe is the poisoned one the choice is between giving up what it holds and giving up everything else, so it goes
     * whichever way keeps more: this is exactly the case where a table was poisoned holding hundreds of series while its neighbours held
     * none, and merging it into an empty stripe would have re-interned every one of them against a breaker that had nothing left.
     */
    private int largestMergeable() {
        int largest = -1;
        int largestIntact = -1;
        long most = -1;
        long mostIntact = -1;
        long total = 0;
        for (int index = 0; index < stripes.length(); index++) {
            DerivedMetricsSeriesTable stripe = stripes.get(index);
            if (stripe == null) {
                continue;
            }
            long size = stripe.size();
            total += size;
            if (size > most) {
                most = size;
                largest = index;
            }
            if (stripe.poisoned() == false && size > mostIntact) {
                mostIntact = size;
                largestIntact = index;
            }
        }
        if (largest < 0 || stripes.get(largest).poisoned() == false || largestIntact < 0 || most >= total - most) {
            return largest < 0 ? primary : largest;
        }
        return largestIntact;
    }

    /** Series that could not be folded into the emitted table because the breaker refused the memory the merge needed. */
    long seriesLostMerging() {
        return seriesLostMerging;
    }

    /**
     * How many bytes this bucket is responsible for, summed across its stripes, so that cost-ranked shedding compares a striped bucket
     * with a shared one on the same terms.
     */
    long bytesHeld() {
        long bytes = 0;
        for (int index = 0; index < stripes.length(); index++) {
            DerivedMetricsSeriesTable stripe = stripes.get(index);
            if (stripe != null) {
                bytes += stripe.bytesHeld();
            }
        }
        return bytes;
    }

    /** How many stripes this bucket was created with. One means it is shared, which is the pre-striping behaviour. */
    int stripeCount() {
        return stripes.length();
    }

    /**
     * Idempotent, and a no-op once {@link #merge()} has handed the stripes away. It exists for the paths that give up on a bucket without
     * emitting it, since every stripe holds circuit breaker accounting that has to be returned exactly once.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (int index = 0; index < stripes.length(); index++) {
            Releasables.close(stripes.getAndSet(index, null));
        }
    }
}
