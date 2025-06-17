/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.mvdedupe;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Removes duplicate values from multivalued positions, and keeps only the ones that pass the filters.
 * <p>
 *     Clone of {@link MultivalueDedupeLong}, but for it accepts a predicate and nulls flag to filter the values.
 * </p>
 */
public class TopNMultivalueDedupeLong {
    /**
     * The number of entries before we switch from and {@code n^2} strategy
     * with low overhead to an {@code n*log(n)} strategy with higher overhead.
     * The choice of number has been experimentally derived.
     */
    static final int ALWAYS_COPY_MISSING = 300;

    /**
     * The {@link Block} being deduplicated.
     */
    final LongBlock block;
    /**
     * Whether the hash expects nulls or not.
     */
    final boolean acceptNulls;
    /**
     * A predicate to test if a value is part of the top N or not.
     */
    final Predicate<Long> isAcceptable;
    /**
     * Oversized array of values that contains deduplicated values after
     * running {@link #copyMissing} and sorted values after calling
     * {@link #copyAndSort}
     */
    long[] work = new long[ArrayUtil.oversize(2, Long.BYTES)];
    /**
     * After calling {@link #copyMissing} or {@link #copyAndSort} this is
     * the number of values in {@link #work} for the current position.
     */
    int w;

    public TopNMultivalueDedupeLong(LongBlock block, boolean acceptNulls, Predicate<Long> isAcceptable) {
        this.block = block;
        this.acceptNulls = acceptNulls;
        this.isAcceptable = isAcceptable;
    }

    /**
     * Dedupe values, add them to the hash, and build an {@link IntBlock} of
     * their hashes. This block is suitable for passing as the grouping block
     * to a {@link GroupingAggregatorFunction}.
     */
    public MultivalueDedupe.HashResult hashAdd(BlockFactory blockFactory, LongHash hash) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(block.getPositionCount())) {
            boolean sawNull = false;
            for (int p = 0; p < block.getPositionCount(); p++) {
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 0 -> {
                        if (acceptNulls) {
                            sawNull = true;
                            builder.appendInt(0);
                        } else {
                            builder.appendNull();
                        }
                    }
                    case 1 -> {
                        long v = block.getLong(first);
                        hashAdd(builder, hash, v);
                    }
                    default -> {
                        if (count < ALWAYS_COPY_MISSING) {
                            copyMissing(first, count);
                            hashAddUniquedWork(hash, builder);
                        } else {
                            copyAndSort(first, count);
                            hashAddSortedWork(hash, builder);
                        }
                    }
                }
            }
            return new MultivalueDedupe.HashResult(builder.build(), sawNull);
        }
    }

    /**
     * Dedupe values and build an {@link IntBlock} of their hashes. This block is
     * suitable for passing as the grouping block to a {@link GroupingAggregatorFunction}.
     */
    public IntBlock hashLookup(BlockFactory blockFactory, LongHash hash) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(block.getPositionCount())) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 0 -> {
                        if (acceptNulls) {
                            builder.appendInt(0);
                        } else {
                            builder.appendNull();
                        }
                    }
                    case 1 -> {
                        long v = block.getLong(first);
                        hashLookupSingle(builder, hash, v);
                    }
                    default -> {
                        if (count < ALWAYS_COPY_MISSING) {
                            copyMissing(first, count);
                            hashLookupUniquedWork(hash, builder);
                        } else {
                            copyAndSort(first, count);
                            hashLookupSortedWork(hash, builder);
                        }
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * Copy all value from the position into {@link #work} and then
     * sorts it {@code n * log(n)}.
     */
    void copyAndSort(int first, int count) {
        grow(count);
        int end = first + count;

        w = 0;
        for (int i = first; i < end; i++) {
            long value = block.getLong(i);
            if (isAcceptable.test(value)) {
                work[w++] = value;
            }
        }

        Arrays.sort(work, 0, w);
    }

    /**
     * Fill {@link #work} with the unique values in the position by scanning
     * all fields already copied {@code n^2}.
     */
    void copyMissing(int first, int count) {
        grow(count);
        int end = first + count;

        // Find the first acceptable value
        for (; first < end; first++) {
            long v = block.getLong(first);
            if (isAcceptable.test(v)) {
                break;
            }
        }
        if (first == end) {
            w = 0;
            return;
        }

        work[0] = block.getLong(first);
        w = 1;
        i: for (int i = first + 1; i < end; i++) {
            long v = block.getLong(i);
            if (isAcceptable.test(v)) {
                for (int j = 0; j < w; j++) {
                    if (v == work[j]) {
                        continue i;
                    }
                }
                work[w++] = v;
            }
        }
    }

    /**
     * Writes an already deduplicated {@link #work} to a hash.
     */
    private void hashAddUniquedWork(LongHash hash, IntBlock.Builder builder) {
        if (w == 0) {
            builder.appendNull();
            return;
        }

        if (w == 1) {
            hashAddNoCheck(builder, hash, work[0]);
            return;
        }

        builder.beginPositionEntry();
        for (int i = 0; i < w; i++) {
            hashAddNoCheck(builder, hash, work[i]);
        }
        builder.endPositionEntry();
    }

    /**
     * Writes a sorted {@link #work} to a hash, skipping duplicates.
     */
    private void hashAddSortedWork(LongHash hash, IntBlock.Builder builder) {
        if (w == 0) {
            builder.appendNull();
            return;
        }

        if (w == 1) {
            hashAddNoCheck(builder, hash, work[0]);
            return;
        }

        builder.beginPositionEntry();
        long prev = work[0];
        hashAddNoCheck(builder, hash, prev);
        for (int i = 1; i < w; i++) {
            if (false == valuesEqual(prev, work[i])) {
                prev = work[i];
                hashAddNoCheck(builder, hash, prev);
            }
        }
        builder.endPositionEntry();
    }

    /**
     * Looks up an already deduplicated {@link #work} to a hash.
     */
    private void hashLookupUniquedWork(LongHash hash, IntBlock.Builder builder) {
        if (w == 0) {
            builder.appendNull();
            return;
        }
        if (w == 1) {
            hashLookupSingle(builder, hash, work[0]);
            return;
        }

        int i = 1;
        long firstLookup = hashLookup(hash, work[0]);
        while (firstLookup < 0) {
            if (i >= w) {
                // Didn't find any values
                builder.appendNull();
                return;
            }
            firstLookup = hashLookup(hash, work[i]);
            i++;
        }

        /*
         * Step 2 - find the next unique value in the hash
         */
        boolean foundSecond = false;
        while (i < w) {
            long nextLookup = hashLookup(hash, work[i]);
            if (nextLookup >= 0) {
                builder.beginPositionEntry();
                appendFound(builder, firstLookup);
                appendFound(builder, nextLookup);
                i++;
                foundSecond = true;
                break;
            }
            i++;
        }

        /*
         * Step 3a - we didn't find a second value, just emit the first one
         */
        if (false == foundSecond) {
            appendFound(builder, firstLookup);
            return;
        }

        /*
         * Step 3b - we found a second value, search for more
         */
        while (i < w) {
            long nextLookup = hashLookup(hash, work[i]);
            if (nextLookup >= 0) {
                appendFound(builder, nextLookup);
            }
            i++;
        }
        builder.endPositionEntry();
    }

    /**
     * Looks up a sorted {@link #work} to a hash, skipping duplicates.
     */
    private void hashLookupSortedWork(LongHash hash, IntBlock.Builder builder) {
        if (w == 1) {
            hashLookupSingle(builder, hash, work[0]);
            return;
        }

        /*
         * Step 1 - find the first unique value in the hash
         *   i will contain the next value to probe
         *   prev will contain the first value in the array contained in the hash
         *   firstLookup will contain the first value in the hash
         */
        int i = 1;
        long prev = work[0];
        long firstLookup = hashLookup(hash, prev);
        while (firstLookup < 0) {
            if (i >= w) {
                // Didn't find any values
                builder.appendNull();
                return;
            }
            prev = work[i];
            firstLookup = hashLookup(hash, prev);
            i++;
        }

        /*
         * Step 2 - find the next unique value in the hash
         */
        boolean foundSecond = false;
        while (i < w) {
            if (false == valuesEqual(prev, work[i])) {
                long nextLookup = hashLookup(hash, work[i]);
                if (nextLookup >= 0) {
                    prev = work[i];
                    builder.beginPositionEntry();
                    appendFound(builder, firstLookup);
                    appendFound(builder, nextLookup);
                    i++;
                    foundSecond = true;
                    break;
                }
            }
            i++;
        }

        /*
         * Step 3a - we didn't find a second value, just emit the first one
         */
        if (false == foundSecond) {
            appendFound(builder, firstLookup);
            return;
        }

        /*
         * Step 3b - we found a second value, search for more
         */
        while (i < w) {
            if (false == valuesEqual(prev, work[i])) {
                long nextLookup = hashLookup(hash, work[i]);
                if (nextLookup >= 0) {
                    prev = work[i];
                    appendFound(builder, nextLookup);
                }
            }
            i++;
        }
        builder.endPositionEntry();
    }

    private void grow(int size) {
        work = ArrayUtil.grow(work, size);
    }

    private void hashAdd(IntBlock.Builder builder, LongHash hash, long v) {
        if (isAcceptable.test(v)) {
            hashAddNoCheck(builder, hash, v);
        } else {
            builder.appendNull();
        }
    }

    private void hashAddNoCheck(IntBlock.Builder builder, LongHash hash, long v) {
        appendFound(builder, hash.add(v));
    }

    private long hashLookup(LongHash hash, long v) {
        return isAcceptable.test(v) ? hash.find(v) : -1;
    }

    private void hashLookupSingle(IntBlock.Builder builder, LongHash hash, long v) {
        long found = hashLookup(hash, v);
        if (found >= 0) {
            appendFound(builder, found);
        } else {
            builder.appendNull();
        }
    }

    private void appendFound(IntBlock.Builder builder, long found) {
        builder.appendInt(Math.toIntExact(BlockHash.hashOrdToGroupNullReserved(found)));
    }

    private static boolean valuesEqual(long lhs, long rhs) {
        return lhs == rhs;
    }
}
