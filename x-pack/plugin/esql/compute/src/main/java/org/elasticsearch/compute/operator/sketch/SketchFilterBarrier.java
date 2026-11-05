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
        registered.incrementAndGet();
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
            try {
                candidates = failed ? null : decide();
            } catch (RuntimeException e) {
                // A failed proof must never leave the operators waiting. Not filtering is always correct.
                logger.warn("sketch proof failed, not filtering", e);
                candidates = null;
            } finally {
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
     */
    private LongSet decide() {
        final List<CountSketch> ds;
        synchronized (this) {
            ds = new ArrayList<>(sketches);
        }
        final int drivers = ds.size();
        final int buckets = ds.get(0).buckets();
        long tracked = 0;
        for (CountSketch s : ds) {
            if (s.buckets() != buckets) {
                throw new IllegalStateException("sketches have different sizes");
            }
        }
        // sumCmin[b] = sum over drivers of cmin_d(b). cmin[d][b] is kept to subtract the drivers that track a key.
        final int[][] cmin = new int[drivers][buckets];
        final long[] sumCmin = new long[buckets];
        long bound = 0;
        for (int d = 0; d < drivers; d++) {
            for (int b = 0; b < buckets; b++) {
                int c = ds.get(d).cmin(b);
                cmin[d][b] = c;
                sumCmin[b] += c;
            }
        }
        for (int b = 0; b < buckets; b++) {
            bound = Math.max(bound, sumCmin[b]);
        }
        for (CountSketch s : ds) {
            tracked += (long) s.buckets() * CountSketch.SLOTS_PER_BUCKET;
        }
        final UnionMap union = new UnionMap(tracked);
        for (int d = 0; d < drivers; d++) {
            final int[] driverCmin = cmin[d];
            ds.get(d).forEachTracked((bucket, key, count, error) -> union.add(key, count - error, count, driverCmin[bucket]));
        }
        if (union.size < topN) {
            logger.debug("sketch proof failed: only [{}] tracked keys for top [{}]", union.size, topN);
            return null;
        }
        final long lowest = union.nthLargestLow(topN);
        if (lowest <= bound) {
            logger.debug("sketch proof failed: lowest top lower bound [{}] is not above the bound [{}]", lowest, bound);
            return null;
        }
        final long[] found = union.candidates(lowest, sumCmin, ds.get(0), maxCandidates);
        if (found == null) {
            logger.debug("sketch proof failed: more than [{}] candidates", maxCandidates);
            return null;
        }
        logger.debug("sketch proof passed: [{}] candidates, lowest [{}], bound [{}]", found.length, lowest, bound);
        return new LongSet(found);
    }

    /**
     * Open addressing map from a key to the sums over the drivers that track it. Zero is a valid key, so a separate
     * {@code used} array marks the cells.
     */
    private static final class UnionMap {
        private static final long MIX = 0x9E3779B97F4A7C15L;
        private final long[] keys;
        private final boolean[] used;
        private final long[] low;
        private final long[] highTracked;
        private final long[] trackedCmin;
        private final int shift;
        private int size;

        UnionMap(long expected) {
            long wanted = Math.max(16, expected + expected / 2);
            if (wanted > (1 << 30)) {
                throw new IllegalArgumentException("too many tracked keys [" + expected + "]");
            }
            int capacity = Integer.highestOneBit((int) wanted - 1) << 1;
            this.keys = new long[capacity];
            this.used = new boolean[capacity];
            this.low = new long[capacity];
            this.highTracked = new long[capacity];
            this.trackedCmin = new long[capacity];
            this.shift = 64 - Integer.numberOfTrailingZeros(capacity);
        }

        void add(long key, long lo, long hi, long cmin) {
            final int mask = keys.length - 1;
            int i = (int) (Long.rotateLeft(key * MIX, 17) >>> shift);
            while (used[i] && keys[i] != key) {
                i = (i + 1) & mask;
            }
            if (used[i] == false) {
                used[i] = true;
                keys[i] = key;
                size++;
            }
            low[i] += lo;
            highTracked[i] += hi;
            trackedCmin[i] += cmin;
        }

        long nthLargestLow(int n) {
            final long[] top = new long[n]; // ascending, top[0] is the smallest of the n largest
            Arrays.fill(top, Long.MIN_VALUE);
            for (int i = 0; i < keys.length; i++) {
                if (used[i] && low[i] > top[0]) {
                    int j = 0;
                    while (j + 1 < n && top[j + 1] < low[i]) {
                        top[j] = top[j + 1];
                        j++;
                    }
                    top[j] = low[i];
                }
            }
            return top[0];
        }

        @Nullable
        long[] candidates(long lowest, long[] sumCmin, CountSketch bucketFunction, int max) {
            long[] out = new long[Math.min(max, 64)];
            int n = 0;
            for (int i = 0; i < keys.length; i++) {
                if (used[i] == false) {
                    continue;
                }
                long hi = highTracked[i] + sumCmin[bucketFunction.bucketOf(keys[i])] - trackedCmin[i];
                if (hi >= lowest) {
                    if (n == max) {
                        return null;
                    }
                    if (n == out.length) {
                        out = Arrays.copyOf(out, Math.min(max, out.length * 2));
                    }
                    out[n++] = keys[i];
                }
            }
            return Arrays.copyOf(out, n);
        }
    }
}
