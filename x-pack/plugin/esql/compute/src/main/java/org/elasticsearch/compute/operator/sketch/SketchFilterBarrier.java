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
     * <p>
     * It never builds a map of all the tracked keys, which is too slow for 20 sketches of 64K slots, and it runs while every
     * other driver waits. It uses two facts instead.
     * <ul>
     * <li>{@code lo(k)} is at most {@code drivers * max c} over the slots of {@code k}. So all keys with {@code lo(k) >= drivers * c1}
     * have a slot with {@code c >= c1}. Taking the keys of the slots with {@code c >= c1} as seeds, and computing their exact
     * {@code lo}, gives the exact N-th largest {@code lo}, as soon as it is at least {@code drivers * c1}.</li>
     * <li>{@code hi(k) = sumCmin(b) + sum over the drivers that track k of (c - cmin_d(b))}, and {@code sumCmin(b) <= B}. A candidate
     * has {@code hi(k) >= L}, so the sum of the excesses {@code c - cmin_d(b)} of its slots is at least {@code L - B}, and one slot
     * has an excess of at least {@code (L - B) / drivers}. Such slots are rare.</li>
     * </ul>
     */
    private LongSet decide() {
        final List<CountSketch> ds;
        synchronized (this) {
            ds = new ArrayList<>(sketches);
        }
        final int drivers = ds.size();
        final int buckets = ds.get(0).buckets();
        for (CountSketch s : ds) {
            if (s.buckets() != buckets) {
                throw new IllegalStateException("sketches have different sizes");
            }
        }
        // The bound for a key that no driver tracks: the largest sum over the drivers of the c_min of a bucket.
        final long[] sumCmin = new long[buckets];
        for (CountSketch s : ds) {
            for (int b = 0; b < buckets; b++) {
                sumCmin[b] += s.cmin(b);
            }
        }
        long bound = 0;
        for (long v : sumCmin) {
            bound = Math.max(bound, v);
        }
        final long lowest = nthLargestLowerBound(ds);
        if (lowest < 0) {
            logger.debug("sketch proof failed: could not find the [{}]-th lower bound", topN);
            return null;
        }
        if (lowest <= bound) {
            logger.debug("sketch proof failed: lowest top lower bound [{}] is not above the bound [{}]", lowest, bound);
            return null;
        }
        final long minExcess = Math.max(1, (lowest - bound + drivers - 1) / drivers);
        final long[] found = candidates(ds, lowest, minExcess);
        if (found == null) {
            logger.debug("sketch proof failed: more than [{}] candidates", maxCandidates);
            return null;
        }
        logger.debug("sketch proof passed: [{}] candidates, lowest [{}], bound [{}]", found.length, lowest, bound);
        return new LongSet(found);
    }

    /** At most this many seed keys. More means the sketches are too flat for the proof to be worth it. */
    private static final int MAX_SEEDS = 1 << 20;

    /**
     * The exact N-th largest lower bound over all tracked keys, or {@code -1} if it cannot be found cheaply.
     */
    private long nthLargestLowerBound(List<CountSketch> ds) {
        final int drivers = ds.size();
        long c1 = 1024;
        for (int attempt = 0; attempt < 24; attempt++) {
            KeyMap seeds = collectSeeds(ds, c1);
            if (seeds == null) {
                c1 *= 2; // too many seeds
                continue;
            }
            if (seeds.size >= topN) {
                long lowest = seeds.nthLargestLow(topN);
                if (lowest >= drivers * c1 || c1 == 1) {
                    return lowest;
                }
            } else if (c1 == 1) {
                return -1;
            }
            c1 = Math.max(1, c1 / 2);
        }
        return -1;
    }

    /** The distinct keys of the slots with {@code c >= minCount}, with their exact lower bound. Null if there are too many. */
    private static KeyMap collectSeeds(List<CountSketch> ds, long minCount) {
        final KeyMap seeds = new KeyMap();
        for (CountSketch sketch : ds) {
            final long[] t = sketch.table();
            for (int i = 0; i < t.length; i += 2) {
                long meta = t[i + 1];
                if (meta != 0 && (meta >>> 32) >= minCount) {
                    long key = t[i];
                    if (seeds.contains(key) == false) {
                        seeds.add(key, lowerBound(ds, key));
                        if (seeds.size > MAX_SEEDS) {
                            return null;
                        }
                    }
                }
            }
        }
        return seeds;
    }

    /** The keys with {@code hi >= lowest}, found among the slots with an excess over their bucket bound of at least minExcess. */
    private long[] candidates(List<CountSketch> ds, long lowest, long minExcess) {
        final KeyMap seen = new KeyMap();
        long[] out = new long[16];
        int n = 0;
        for (CountSketch sketch : ds) {
            final long[] t = sketch.table();
            for (int base = 0; base < t.length; base += CountSketch.LONGS_PER_BUCKET) {
                final long cmin = bucketCmin(t, base);
                for (int i = base; i < base + CountSketch.LONGS_PER_BUCKET; i += 2) {
                    long meta = t[i + 1];
                    if (meta != 0 && (meta >>> 32) - cmin >= minExcess) {
                        long key = t[i];
                        if (seen.contains(key) == false) {
                            seen.add(key, 0);
                            if (upperBound(ds, key) >= lowest) {
                                if (n == maxCandidates) {
                                    return null;
                                }
                                if (n == out.length) {
                                    out = Arrays.copyOf(out, Math.min(maxCandidates, out.length * 2));
                                }
                                out[n++] = key;
                            }
                        }
                    }
                }
            }
        }
        return Arrays.copyOf(out, n);
    }

    private static long bucketCmin(long[] t, int base) {
        long min = Long.MAX_VALUE;
        for (int i = base; i < base + CountSketch.LONGS_PER_BUCKET; i += 2) {
            long meta = t[i + 1];
            if (meta == 0) {
                return 0;
            }
            min = Math.min(min, meta >>> 32);
        }
        return min;
    }

    /** The sum over the drivers that track {@code key} of {@code c - e}. */
    private static long lowerBound(List<CountSketch> ds, long key) {
        long lo = 0;
        for (CountSketch s : ds) {
            long meta = s.find(key);
            if (meta != 0) {
                lo += (meta >>> 32) - (meta & 0xFFFFFFFFL);
            }
        }
        return lo;
    }

    /** The sum over the drivers of {@code c} where the driver tracks {@code key}, and of the bucket bound where it does not. */
    private static long upperBound(List<CountSketch> ds, long key) {
        final int bucket = ds.get(0).bucketOf(key);
        long hi = 0;
        for (CountSketch s : ds) {
            long meta = s.find(key);
            hi += meta != 0 ? meta >>> 32 : s.cmin(bucket);
        }
        return hi;
    }

    /** A growing set of keys with a value each. Zero is a valid key, so a separate {@code used} array marks the cells. */
    private static final class KeyMap {
        private static final long MIX = 0x9E3779B97F4A7C15L;
        private long[] keys = new long[1024];
        private long[] lows = new long[1024];
        private boolean[] used = new boolean[1024];
        private int shift = 64 - 10;
        int size;

        boolean contains(long key) {
            return used[indexOf(key)];
        }

        void add(long key, long low) {
            int i = indexOf(key);
            if (used[i]) {
                return;
            }
            used[i] = true;
            keys[i] = key;
            lows[i] = low;
            if (++size * 2 > keys.length) {
                grow();
            }
        }

        private int indexOf(long key) {
            final int mask = keys.length - 1;
            int i = (int) (Long.rotateLeft(key * MIX, 17) >>> shift);
            while (used[i] && keys[i] != key) {
                i = (i + 1) & mask;
            }
            return i;
        }

        private void grow() {
            final long[] oldKeys = keys;
            final long[] oldLows = lows;
            final boolean[] oldUsed = used;
            keys = new long[oldKeys.length * 2];
            lows = new long[oldKeys.length * 2];
            used = new boolean[oldKeys.length * 2];
            shift--;
            for (int j = 0; j < oldKeys.length; j++) {
                if (oldUsed[j]) {
                    int i = indexOf(oldKeys[j]);
                    used[i] = true;
                    keys[i] = oldKeys[j];
                    lows[i] = oldLows[j];
                }
            }
        }

        long nthLargestLow(int n) {
            final long[] top = new long[n]; // ascending, top[0] is the smallest of the n largest
            Arrays.fill(top, Long.MIN_VALUE);
            for (int i = 0; i < keys.length; i++) {
                if (used[i] && lows[i] > top[0]) {
                    int j = 0;
                    while (j + 1 < n && top[j + 1] < lows[i]) {
                        top[j] = top[j + 1];
                        j++;
                    }
                    top[j] = lows[i];
                }
            }
            return top[0];
        }
    }
}
