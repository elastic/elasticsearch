/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The state that all {@link SketchFilterOperator}s of one query share. It is the barrier of the whole scheme: no operator can
 * release a page until every operator has handed in its sketch, because only then the proof can say which keys can be in
 * the top N.
 * <p>
 * Each operator calls {@link #register()} when its factory builds it. The drivers of one plan are all built before any of them
 * runs, so when the first operator arrives, {@code registered} is the final number of operators. Each operator then calls
 * {@link #arrive} exactly once, with its sketch, or with {@code null} when it cannot take part (unsupported input, buffer
 * limit, or the operator is closed before it finished). The last operator to arrive computes the decision and completes
 * {@link #decided()}. An arrival with {@code null} makes the decision "do not filter".
 * <p>
 * The decision is "filter" only if the proof passes: let {@code lo(k)} be the sum over drivers of {@code c - e} for the
 * drivers that track {@code k}, and let {@code L} be the N-th largest {@code lo}. A key that no driver tracks has a count of at
 * most {@code B = max over buckets b of (sum over drivers of cmin_d(b))}. If {@code L > B}, the top N keys are all among the
 * keys with {@code hi(k) >= L}, where {@code hi(k)} adds {@code c} for the drivers that track {@code k} and
 * {@code cmin_d(bucket(k))} for the others. Those keys are the candidates. Keys with a total count equal to {@code L} stay
 * candidates, so ties are safe.
 */
public final class SketchFilterBarrier {
    private static final Logger logger = LogManager.getLogger(SketchFilterBarrier.class);

    private final int topN;
    private final int maxCandidates;
    private final long maxBufferedBytes;

    private final AtomicInteger registered = new AtomicInteger();
    private final AtomicInteger arrived = new AtomicInteger();
    private final List<CountSketch> sketches = new ArrayList<>();
    private boolean failed;

    private final AtomicLong bufferedBytes = new AtomicLong();
    private volatile boolean aborted;

    private final SubscribableListener<Void> decided = new SubscribableListener<>();
    private volatile LongSet candidates;

    /**
     * Timing, for the profile only. {@code t0} is when the first operator was registered, which is when the drivers are built, a few
     * milliseconds before they start. The other times are {@link System#nanoTime} values and are shown relative to {@code t0}.
     */
    private volatile long t0Nanos;
    private volatile long t0EpochMillis;
    private volatile long decideStartNanos;
    private volatile long decideEndNanos;

    /**
     * @param topN             the {@code N} of {@code LIMIT N}
     * @param maxCandidates    give up if more keys than this can be in the top N, because the filter would not help
     * @param maxBufferedBytes the most bytes of pages that all operators together can hold before the barrier gives up
     */
    public SketchFilterBarrier(int topN, int maxCandidates, long maxBufferedBytes) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be positive but was [" + topN + "]");
        }
        this.topN = topN;
        this.maxCandidates = maxCandidates;
        this.maxBufferedBytes = maxBufferedBytes;
    }

    /** Count one more operator that will arrive. Called when an operator is built. */
    void register() {
        if (t0Nanos == 0) {
            synchronized (this) {
                if (t0Nanos == 0) {
                    t0EpochMillis = System.currentTimeMillis();
                    t0Nanos = System.nanoTime();
                }
            }
        }
        registered.incrementAndGet();
    }

    /** Milliseconds from the registration of the first operator to {@code nanos}, or {@code -1} if {@code nanos} was never set. */
    double millisSinceStart(long nanos) {
        return nanos == 0 ? -1 : (nanos - t0Nanos) / 1_000_000.0;
    }

    /** The wall-clock time of the registration of the first operator, to line the timing up with a driver profile. */
    long startEpochMillis() {
        return t0EpochMillis;
    }

    long decideStartNanos() {
        return decideStartNanos;
    }

    long decideEndNanos() {
        return decideEndNanos;
    }

    /**
     * Hand in the sketch of one operator. Must be called exactly once per registered operator.
     *
     * @param sketch the sketch, or {@code null} if this operator cannot take part, which means that the decision is not to filter
     */
    void arrive(@Nullable CountSketch sketch) {
        boolean last;
        synchronized (this) {
            if (sketch == null) {
                failed = true;
            } else {
                sketches.add(sketch);
            }
            last = arrived.incrementAndGet() == registered.get();
        }
        if (last) {
            decideStartNanos = System.nanoTime();
            try {
                candidates = failed ? null : decide();
            } catch (RuntimeException e) {
                // A failed proof must never leave the operators waiting. Not filtering is always correct.
                logger.warn("sketch proof failed, not filtering", e);
                candidates = null;
            } finally {
                decideEndNanos = System.nanoTime();
                synchronized (this) {
                    sketches.clear();
                }
                decided.onResponse(null);
            }
        }
    }

    /** Completes when every operator has arrived and the decision is known. */
    SubscribableListener<Void> decided() {
        return decided;
    }

    boolean isDecided() {
        return decided.isDone();
    }

    /**
     * Whether the decision is to filter. Only valid after {@link #isDecided()}.
     */
    boolean filtering() {
        assert isDecided();
        return candidates != null;
    }

    /** The keys that can be in the top N. Only valid after {@link #isDecided()} and when {@link #filtering()}. */
    LongSet candidates() {
        assert filtering();
        return candidates;
    }

    /**
     * Try to reserve space for one more buffered page. Returns false, and aborts the whole barrier, when the limit is reached.
     */
    boolean tryBuffer(long bytes) {
        if (aborted) {
            return false;
        }
        if (bufferedBytes.addAndGet(bytes) > maxBufferedBytes) {
            aborted = true;
            return false;
        }
        return true;
    }

    void unbuffer(long bytes) {
        bufferedBytes.addAndGet(-bytes);
    }

    /** True once some operator hit the buffer limit. All operators then stop sketching and let their pages through. */
    boolean aborted() {
        return aborted;
    }

    /**
     * Merge the sketches and apply the proof. Returns the candidates, or {@code null} if the proof does not pass.
     * <p>
     * All sketches use the same bucket function, so a key is in the same bucket of every sketch, and everything the proof needs to
     * know about a key is in one bucket of each of the {@code drivers} tables. The proof therefore goes through the buckets, not
     * through a map of all keys, and it runs while every other driver waits, so it must be fast.
     * <ol>
     * <li>One sweep over all buckets records, for each bucket: {@code sumCmin}, the sum over the drivers of the bucket's
     * {@code c_min} (the bound for a key that no driver tracks is the largest one); {@code sumLo}, the sum of {@code c - e} of all
     * its slots, which is at least {@code lo} of any key in the bucket; and {@code maxHi}, {@code sumCmin} plus the sum of
     * {@code c - c_min} of all its slots, which is at least {@code hi} of any key in the bucket.</li>
     * <li>{@code L}, the N-th largest {@code lo}: take the buckets by decreasing {@code sumLo}, group their slots by key, and stop
     * when the next {@code sumLo} cannot beat the N-th largest found so far. The heavy keys are found first.</li>
     * <li>The candidates, the keys with {@code hi >= L}: only the buckets with {@code maxHi >= L} need to be grouped.</li>
     * </ol>
     */
    private LongSet decide() {
        final List<CountSketch> ds;
        synchronized (this) {
            ds = new ArrayList<>(sketches);
        }
        final int buckets = ds.get(0).buckets();
        final long[][] tables = new long[ds.size()][];
        for (int d = 0; d < tables.length; d++) {
            if (ds.get(d).buckets() != buckets) {
                throw new IllegalStateException("sketches have different sizes");
            }
            tables[d] = ds.get(d).table();
        }
        if (buckets >= (1 << BUCKET_BITS)) {
            throw new IllegalStateException("too many buckets [" + buckets + "]");
        }
        final BucketMerger merger = new BucketMerger(tables);
        final long[] sumLo = new long[buckets];
        final long[] maxHi = new long[buckets];
        long bound = 0;
        for (int b = 0; b < buckets; b++) {
            merger.summarize(b);
            sumLo[b] = merger.sumLo;
            maxHi[b] = merger.maxHi;
            bound = Math.max(bound, merger.sumCmin);
        }
        final long lowest = nthLargestLowerBound(merger, sumLo);
        if (lowest < 0) {
            logger.debug("sketch proof failed: fewer than [{}] tracked keys with a lower bound", topN);
            return null;
        }
        if (lowest <= bound) {
            logger.debug("sketch proof failed: lowest top lower bound [{}] is not above the bound [{}]", lowest, bound);
            return null;
        }
        final long[] found = candidates(merger, maxHi, lowest);
        if (found == null) {
            logger.debug("sketch proof failed: more than [{}] candidates", maxCandidates);
            return null;
        }
        logger.debug("sketch proof passed: [{}] candidates, lowest [{}], bound [{}]", found.length, lowest, bound);
        return new LongSet(found);
    }

    private static final int BUCKET_BITS = 21;

    /**
     * The exact N-th largest {@code lo} over all tracked keys, or {@code -1} if there are fewer than N keys with a positive one.
     */
    private long nthLargestLowerBound(BucketMerger merger, long[] sumLo) {
        final int buckets = sumLo.length;
        final long[] order = new long[buckets];
        for (int b = 0; b < buckets; b++) {
            if (sumLo[b] >= (1L << (63 - BUCKET_BITS))) {
                throw new IllegalStateException("lower bound too large [" + sumLo[b] + "]");
            }
            order[b] = (sumLo[b] << BUCKET_BITS) | b;
        }
        Arrays.sort(order);
        final long[] top = new long[topN]; // ascending: top[0] is the smallest of the N largest so far
        Arrays.fill(top, Long.MIN_VALUE);
        for (int i = buckets - 1; i >= 0; i--) {
            final long bucketSumLo = order[i] >>> BUCKET_BITS;
            if (bucketSumLo == 0 || (top[0] != Long.MIN_VALUE && bucketSumLo <= top[0])) {
                break; // no key in this bucket, or in any later one, can be among the N largest
            }
            merger.summarize((int) (order[i] & ((1L << BUCKET_BITS) - 1)));
            merger.group();
            for (int g = 0; g < merger.groups; g++) {
                final long lo = merger.groupLo[g];
                if (lo > top[0]) {
                    int j = 0;
                    while (j + 1 < topN && top[j + 1] < lo) {
                        top[j] = top[j + 1];
                        j++;
                    }
                    top[j] = lo;
                }
            }
            merger.reset();
        }
        return top[0] == Long.MIN_VALUE ? -1 : top[0];
    }

    /** The keys with {@code hi >= lowest}, or null if there are more than {@code maxCandidates}. */
    private long[] candidates(BucketMerger merger, long[] maxHi, long lowest) {
        long[] out = new long[16];
        int n = 0;
        for (int b = 0; b < maxHi.length; b++) {
            if (maxHi[b] < lowest) {
                continue;
            }
            merger.summarize(b);
            merger.group();
            for (int g = 0; g < merger.groups; g++) {
                if (merger.groupHiTracked[g] + merger.sumCmin - merger.groupTrackedCmin[g] >= lowest) {
                    if (n == maxCandidates) {
                        return null;
                    }
                    if (n == out.length) {
                        out = Arrays.copyOf(out, Math.min(maxCandidates, out.length * 2));
                    }
                    out[n++] = merger.groupKey[g];
                }
            }
            merger.reset();
        }
        return Arrays.copyOf(out, n);
    }

    /**
     * Reads one bucket from every sketch. {@link #summarize} computes the sums that bound the keys of the bucket without looking at
     * the keys. {@link #group} then combines the slots with the same key, with a small hash table that is cleared after use, and
     * fills the {@code group*} arrays: for each key the sum of {@code c - e} ({@code groupLo}), the sum of {@code c}
     * ({@code groupHiTracked}) and the sum of the bucket {@code c_min} of the drivers that track it ({@code groupTrackedCmin}).
     * Then {@code hi = groupHiTracked + sumCmin - groupTrackedCmin}: {@code c} where a driver tracks the key and {@code c_min}
     * where it does not.
     */
    private static final class BucketMerger {
        private static final long MIX = 0xC2B2AE3D27D4EB4FL;
        private static final long LOW_MASK = 0xFFFFFFFFL;
        private final long[][] tables;
        private final long[] cmin;
        private int base;

        long sumCmin;
        long sumLo;
        long maxHi;

        private final int[] cells; // group index + 1, or 0 for a free cell
        private final int[] used;
        private final int shift;
        private int usedCount;
        int groups;
        final long[] groupKey;
        final long[] groupLo;
        final long[] groupHiTracked;
        final long[] groupTrackedCmin;

        BucketMerger(long[][] tables) {
            this.tables = tables;
            this.cmin = new long[tables.length];
            int maxEntries = tables.length * CountSketch.SLOTS_PER_BUCKET;
            int cellCount = Integer.highestOneBit(Math.max(8, maxEntries * 2 - 1)) << 1;
            this.cells = new int[cellCount];
            this.used = new int[maxEntries];
            this.shift = 64 - Integer.numberOfTrailingZeros(cellCount);
            this.groupKey = new long[maxEntries];
            this.groupLo = new long[maxEntries];
            this.groupHiTracked = new long[maxEntries];
            this.groupTrackedCmin = new long[maxEntries];
        }

        void summarize(int bucket) {
            base = bucket * CountSketch.LONGS_PER_BUCKET;
            long sumC = 0;
            for (int d = 0; d < tables.length; d++) {
                final long[] t = tables[d];
                final long m0 = t[base + 1], m1 = t[base + 3], m2 = t[base + 5], m3 = t[base + 7];
                final long c = (m0 == 0 || m1 == 0 || m2 == 0 || m3 == 0)
                    ? 0
                    : Math.min(Math.min(m0 >>> 32, m1 >>> 32), Math.min(m2 >>> 32, m3 >>> 32));
                cmin[d] = c;
                sumC += c;
            }
            long lo = 0;
            long excess = 0;
            for (int d = 0; d < tables.length; d++) {
                final long[] t = tables[d];
                for (int i = base + 1; i < base + CountSketch.LONGS_PER_BUCKET; i += 2) {
                    final long meta = t[i];
                    if (meta != 0) {
                        final long c = meta >>> 32;
                        lo += c - (meta & LOW_MASK);
                        excess += c - cmin[d];
                    }
                }
            }
            sumCmin = sumC;
            sumLo = lo;
            maxHi = sumC + excess;
        }

        /** Combine the slots of the bucket last passed to {@link #summarize} by key. Call {@link #reset} when done with the groups. */
        void group() {
            groups = 0;
            for (int d = 0; d < tables.length; d++) {
                final long[] t = tables[d];
                for (int i = base; i < base + CountSketch.LONGS_PER_BUCKET; i += 2) {
                    final long meta = t[i + 1];
                    if (meta == 0) {
                        continue;
                    }
                    final long key = t[i];
                    final int mask = cells.length - 1;
                    int cell = (int) ((key * MIX) >>> shift);
                    int g;
                    while ((g = cells[cell]) != 0 && groupKey[g - 1] != key) {
                        cell = (cell + 1) & mask;
                    }
                    if (g == 0) {
                        g = ++groups;
                        cells[cell] = g;
                        used[usedCount++] = cell;
                        groupKey[g - 1] = key;
                        groupLo[g - 1] = 0;
                        groupHiTracked[g - 1] = 0;
                        groupTrackedCmin[g - 1] = 0;
                    }
                    final long c = meta >>> 32;
                    groupLo[g - 1] += c - (meta & LOW_MASK);
                    groupHiTracked[g - 1] += c;
                    groupTrackedCmin[g - 1] += cmin[d];
                }
            }
        }

        void reset() {
            for (int i = 0; i < usedCount; i++) {
                cells[used[i]] = 0;
            }
            usedCount = 0;
            groups = 0;
        }
    }
}
