/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.mvdedupe;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Removes duplicate values from multivalued positions, and keeps only the ones that pass the predicate.
 * <p>
 *     Clone of {@link MultivalueDedupeBytesRef}, but it accepts a predicate and nulls flag to filter the values
 *     in a top-N aggregation context. Values not in the top-N produce {@code null} group ids so the corresponding
 *     aggregator state is never allocated.
 * </p>
 */
public class TopNMultivalueDedupeBytesRef {
    /**
     * The number of entries before we switch from an {@code n^2} strategy
     * with low overhead to an {@code n*log(n)} strategy with higher overhead.
     * The choice of number has been experimentally derived.
     */
    static final int ALWAYS_COPY_MISSING = 20;

    /**
     * The {@link Block} being deduplicated.
     */
    final BytesRefBlock block;
    /**
     * Whether the hash expects nulls or not.
     */
    final boolean acceptNulls;
    /**
     * A predicate to test if a value is part of the top N or not.
     */
    final Predicate<BytesRef> isAcceptable;
    /**
     * Oversized array of values that contains deduplicated values after running {@link #copyMissing} and
     * sorted values after calling {@link #copyAndSort}.
     */
    BytesRef[] work = new BytesRef[ArrayUtil.oversize(2, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
    /**
     * After calling {@link #copyMissing} or {@link #copyAndSort} this is the number of values in {@link #work}
     * for the current position.
     */
    int w;

    public TopNMultivalueDedupeBytesRef(BytesRefBlock block, boolean acceptNulls, Predicate<BytesRef> isAcceptable) {
        this.block = block;
        this.acceptNulls = acceptNulls;
        this.isAcceptable = isAcceptable;
        fillWork(0, work.length);
    }

    /**
     * Dedupe values, add them to the hash, and build an {@link IntBlock} of their hashes. This block is
     * suitable for passing as the grouping block to a {@link GroupingAggregatorFunction}.
     */
    public MultivalueDedupe.HashResult hashAdd(BlockFactory blockFactory, BytesRefHashTable hash) {
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
                        BytesRef v = block.getBytesRef(first, work[0]);
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
     * Dedupe values and build an {@link IntBlock} of their hashes. This block is suitable for passing as the
     * grouping block to a {@link GroupingAggregatorFunction}.
     */
    public IntBlock hashLookup(BlockFactory blockFactory, BytesRefHashTable hash) {
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
                        BytesRef v = block.getBytesRef(first, work[0]);
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
     * Copy all values from the position into {@link #work} and then sort them in {@code n * log(n)}.
     * Only values passing {@link #isAcceptable} are kept.
     */
    void copyAndSort(int first, int count) {
        grow(count);
        int end = first + count;

        w = 0;
        for (int i = first; i < end; i++) {
            BytesRef candidate = block.getBytesRef(i, work[w]);
            if (isAcceptable.test(candidate)) {
                work[w] = candidate;
                w++;
            }
        }

        Arrays.sort(work, 0, w);
    }

    /**
     * Fill {@link #work} with the unique acceptable values in the position by scanning all fields already
     * copied {@code n^2}.
     */
    void copyMissing(int first, int count) {
        grow(count);
        int end = first + count;

        // Find the first acceptable value.
        int probe = first;
        for (; probe < end; probe++) {
            BytesRef candidate = block.getBytesRef(probe, work[0]);
            if (isAcceptable.test(candidate)) {
                work[0] = candidate;
                break;
            }
        }
        if (probe == end) {
            w = 0;
            return;
        }

        w = 1;
        i: for (int i = probe + 1; i < end; i++) {
            BytesRef v = block.getBytesRef(i, work[w]);
            if (isAcceptable.test(v)) {
                for (int j = 0; j < w; j++) {
                    if (v.equals(work[j])) {
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
    private void hashAddUniquedWork(BytesRefHashTable hash, IntBlock.Builder builder) {
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
    private void hashAddSortedWork(BytesRefHashTable hash, IntBlock.Builder builder) {
        if (w == 0) {
            builder.appendNull();
            return;
        }

        if (w == 1) {
            hashAddNoCheck(builder, hash, work[0]);
            return;
        }

        builder.beginPositionEntry();
        BytesRef prev = work[0];
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
     * Looks up an already deduplicated {@link #work} into a hash.
     */
    private void hashLookupUniquedWork(BytesRefHashTable hash, IntBlock.Builder builder) {
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
                builder.appendNull();
                return;
            }
            firstLookup = hashLookup(hash, work[i]);
            i++;
        }

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

        if (false == foundSecond) {
            appendFound(builder, firstLookup);
            return;
        }

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
     * Looks up a sorted {@link #work} into a hash, skipping duplicates.
     */
    private void hashLookupSortedWork(BytesRefHashTable hash, IntBlock.Builder builder) {
        if (w == 0) {
            builder.appendNull();
            return;
        }
        if (w == 1) {
            hashLookupSingle(builder, hash, work[0]);
            return;
        }

        int i = 1;
        BytesRef prev = work[0];
        long firstLookup = hashLookup(hash, prev);
        while (firstLookup < 0) {
            if (i >= w) {
                builder.appendNull();
                return;
            }
            prev = work[i];
            firstLookup = hashLookup(hash, prev);
            i++;
        }

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

        if (false == foundSecond) {
            appendFound(builder, firstLookup);
            return;
        }

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
        int prev = work.length;
        work = ArrayUtil.grow(work, size);
        fillWork(prev, work.length);
    }

    private void fillWork(int from, int to) {
        for (int i = from; i < to; i++) {
            work[i] = new BytesRef();
        }
    }

    private void hashAdd(IntBlock.Builder builder, BytesRefHashTable hash, BytesRef v) {
        if (isAcceptable.test(v)) {
            hashAddNoCheck(builder, hash, v);
        } else {
            builder.appendNull();
        }
    }

    private void hashAddNoCheck(IntBlock.Builder builder, BytesRefHashTable hash, BytesRef v) {
        appendFound(builder, hash.add(v));
    }

    private long hashLookup(BytesRefHashTable hash, BytesRef v) {
        return isAcceptable.test(v) ? hash.find(v) : -1;
    }

    private void hashLookupSingle(IntBlock.Builder builder, BytesRefHashTable hash, BytesRef v) {
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

    private static boolean valuesEqual(BytesRef lhs, BytesRef rhs) {
        return lhs.equals(rhs);
    }
}
