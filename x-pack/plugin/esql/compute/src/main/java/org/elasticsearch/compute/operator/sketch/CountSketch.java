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
    static final int LONGS_PER_BUCKET = 2 * SLOTS_PER_BUCKET;
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

    /** The table, for the proof, which reads every sketch in a tight loop. See the class comment for the layout. */
    long[] table() {
        return table;
    }

    /**
     * The meta word {@code (count << 32) | error} of {@code key}, or {@code 0} if it is not tracked.
     */
    long find(long key) {
        final int base = bucketOf(key) * LONGS_PER_BUCKET;
        for (int i = base; i < base + LONGS_PER_BUCKET; i += 2) {
            long meta = table[i + 1];
            if (meta != 0 && table[i] == key) {
                return meta;
            }
        }
        return 0;
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

    /**
     * The slot of the key that was counted last, or {@code -1}. Rows with the same key are often next to each other, because the
     * index is sorted, and a row that repeats the previous key can be counted without hashing and without reading the bucket.
     * The slot still holds that key, because no other key was counted since.
     */
    private int lastIdx = -1;
    private long lastKey;

    /** Count one row of {@code key}. */
    public void add(long key) {
        if (lastIdx >= 0 && key == lastKey) {
            table[lastIdx + 1] += ONE;
            return;
        }
        lastIdx = insert(key);
        lastKey = key;
    }

    /** Count the first {@code count} keys of {@code keys}, in order. Same as calling {@link #add} for each. */
    public void addAll(long[] keys, int count) {
        addAll(keys, count, null, 0);
    }

    /**
     * Count the first {@code count} keys of {@code keys}, in order, and add every key of the page to {@code bloom} if it is not
     * {@code null}. A key that repeats the previous one is counted without being added again, because it was added with the previous
     * row. The exception is the first row of the page: it can repeat the last key of the previous page, so it is always added.
     */
    void addAll(long[] keys, int count, long[] bloom, int bloomShift) {
        final long[] t = table;
        long last = lastKey;
        int idx = lastIdx;
        if (bloom != null && count > 0) {
            PageBloom.add(bloom, bloomShift, keys[0]);
        }
        for (int r = 0; r < count; r++) {
            final long key = keys[r];
            if (idx >= 0 && key == last) {
                t[idx + 1] += ONE;
            } else {
                idx = insert(key);
                last = key;
                if (bloom != null) {
                    PageBloom.add(bloom, bloomShift, key);
                }
            }
        }
        lastKey = last;
        lastIdx = idx;
    }

    /**
     * Count one row of {@code key} and return the index of the slot that holds it.
     * <p>
     * The four slots are written out, not looped over: a loop with an early exit is not unrolled by the JIT, and the unrolled
     * code runs about 20% faster on the benchmark. It also reads the highest index first, so the bounds are checked once. The result is
     * the same as the loop: a hit adds one; otherwise the first free slot is used; if there is none, the first slot with the
     * smallest count is overwritten. A free slot has a count of 0, so the choice of the victim is one search for the first minimum.
     * This code is written for {@link #SLOTS_PER_BUCKET} slots of four.
     */
    private int insert(long key) {
        assert SLOTS_PER_BUCKET == 4 : "insert is written out for four slots";
        final long[] t = table;
        final int b = bucketOf(key) * LONGS_PER_BUCKET;
        final long m3 = t[b + 7];
        final long m0 = t[b + 1];
        final long m1 = t[b + 3];
        final long m2 = t[b + 5];
        if (m0 != 0 && t[b] == key) {
            t[b + 1] = m0 + ONE;
            return b;
        }
        if (m1 != 0 && t[b + 2] == key) {
            t[b + 3] = m1 + ONE;
            return b + 2;
        }
        if (m2 != 0 && t[b + 4] == key) {
            t[b + 5] = m2 + ONE;
            return b + 4;
        }
        if (m3 != 0 && t[b + 6] == key) {
            t[b + 7] = m3 + ONE;
            return b + 6;
        }
        int victim = 0;
        long count = m0 >>> 32;
        final long c1 = m1 >>> 32;
        if (c1 < count) {
            victim = 1;
            count = c1;
        }
        final long c2 = m2 >>> 32;
        if (c2 < count) {
            victim = 2;
            count = c2;
        }
        final long c3 = m3 >>> 32;
        if (c3 < count) {
            victim = 3;
            count = c3;
        }
        final int i = b + 2 * victim;
        t[i] = key;
        t[i + 1] = ((count + 1) << 32) | count;
        return i;
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
