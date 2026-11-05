/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

/**
 * A bucketed Space-Saving sketch of {@code long} keys.
 * <p>
 * The table has a power-of-two number of buckets, and each bucket has {@link #SLOTS_PER_BUCKET} slots. A slot holds a key, a
 * count {@code c} and an error {@code e}. A key that is not tracked and hashes to bucket {@code b} has a true count of at most
 * {@link #cmin(int) cmin(b)}, which is the smallest {@code c} in the bucket, or {@code 0} if the bucket has a free slot. A key
 * that is tracked has a true count in {@code [c - e, c]}.
 * <p>
 * On a miss in a full bucket the slot with the smallest count is evicted. The new key inherits that count as its error. This
 * keeps the bound above valid. Nothing is spilled: evicted keys are only counted through the bucket bound.
 * <p>
 * All sketches that are merged must use the same bucket function ({@link #bucketOf}) and the same number of buckets, so the
 * bounds can be added bucket by bucket.
 * <p>
 * Layout: one bucket uses eight {@code long}s. Slot {@code j} stores its key at {@code 8 * b + 2 * j} and its meta word at the
 * next index. The meta word is {@code (c << 32) | e}. A meta word of {@code 0} marks a free slot, because a tracked key has
 * {@code c >= 1}. This means that every {@code long} value, including {@code 0}, is a valid key.
 * This class is not thread safe.
 */
public final class CountSketch {
    public static final int SLOTS_PER_BUCKET = 4;
    private static final int LONGS_PER_BUCKET = 2 * SLOTS_PER_BUCKET;
    private static final long PHI = 0x9E3779B97F4A7C15L;
    private static final long ONE = 1L << 32;
    private static final long LOW_MASK = 0xFFFFFFFFL;

    private final int buckets;
    private final int shift;
    private final long[] table;

    /**
     * @param slots the target number of slots. The real number is rounded up to a power of two times {@link #SLOTS_PER_BUCKET},
     *              with at least two buckets.
     */
    public CountSketch(int slots) {
        int wanted = Math.max(2, (slots + SLOTS_PER_BUCKET - 1) / SLOTS_PER_BUCKET);
        this.buckets = Integer.highestOneBit(wanted - 1) << 1;
        this.shift = 64 - Integer.numberOfTrailingZeros(buckets);
        this.table = new long[buckets * LONGS_PER_BUCKET];
    }

    public int buckets() {
        return buckets;
    }

    public long ramBytes() {
        return (long) table.length * Long.BYTES;
    }

    /**
     * The bucket of a key. Every sketch with the same number of buckets maps a key to the same bucket.
     */
    public int bucketOf(long key) {
        return (int) ((key * PHI) >>> shift);
    }

    /** Count one row of {@code key}. */
    public void add(long key) {
        final long[] t = table;
        final int base = bucketOf(key) * LONGS_PER_BUCKET;
        int empty = -1;
        int min = -1;
        long minCount = Long.MAX_VALUE;
        for (int i = base; i < base + LONGS_PER_BUCKET; i += 2) {
            long meta = t[i + 1];
            if (meta == 0) {
                if (empty < 0) {
                    empty = i;
                }
            } else if (t[i] == key) {
                t[i + 1] = meta + ONE;
                return;
            } else {
                long count = meta >>> 32;
                if (count < minCount) {
                    minCount = count;
                    min = i;
                }
            }
        }
        if (empty >= 0) {
            t[empty] = key;
            t[empty + 1] = ONE;
        } else {
            t[min] = key;
            t[min + 1] = ((minCount + 1) << 32) | minCount;
        }
    }

    /**
     * The upper bound of the true count of any key in {@code bucket} that is not tracked.
     */
    public int cmin(int bucket) {
        final int base = bucket * LONGS_PER_BUCKET;
        long min = Long.MAX_VALUE;
        for (int i = base; i < base + LONGS_PER_BUCKET; i += 2) {
            long meta = table[i + 1];
            if (meta == 0) {
                return 0;
            }
            min = Math.min(min, meta >>> 32);
        }
        return (int) min;
    }

    /** Visit every tracked key. */
    public void forEachTracked(TrackedVisitor visitor) {
        for (int b = 0; b < buckets; b++) {
            final int base = b * LONGS_PER_BUCKET;
            for (int i = base; i < base + LONGS_PER_BUCKET; i += 2) {
                long meta = table[i + 1];
                if (meta != 0) {
                    visitor.visit(b, table[i], meta >>> 32, meta & LOW_MASK);
                }
            }
        }
    }

    @FunctionalInterface
    public interface TrackedVisitor {
        void visit(int bucket, long key, long count, long error);
    }
}
