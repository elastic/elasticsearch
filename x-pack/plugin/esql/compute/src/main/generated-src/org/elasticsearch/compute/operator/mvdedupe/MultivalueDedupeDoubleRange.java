/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.mvdedupe;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder.DoubleRange;
import org.elasticsearch.compute.data.IntBlock;

import java.util.Arrays;

/**
 * Removes duplicate values from multivalued positions.
 * This class is generated. Edit {@code X-MultivalueDedupe.java.st} instead.
 */
public class MultivalueDedupeDoubleRange {
    /**
     * The number of entries before we switch from and {@code n^2} strategy
     * with low overhead to an {@code n*log(n)} strategy with higher overhead.
     * The choice of number has been experimentally derived.
     */
    static final int ALWAYS_COPY_MISSING = 20;
    /**
     * The {@link Block} being deduplicated.
     */
    final DoubleRangeBlock block;
    /**
     * Oversized array of values that contains deduplicated values after
     * running {@link #copyMissing} and sorted values after calling
     * {@link #copyAndSort}
     */
    DoubleRange[] work = new DoubleRange[ArrayUtil.oversize(2, DoubleRange.SIZE)];
    /**
     * After calling {@link #copyMissing} or {@link #copyAndSort} this is
     * the number of values in {@link #work} for the current position.
     */
    int w;

    public MultivalueDedupeDoubleRange(DoubleRangeBlock block) {
        this.block = block;
        fillWork(0, work.length);
    }

    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an adaptive algorithm based on the size of the input list.
     */
    public DoubleRangeBlock dedupeToBlockAdaptive(BlockFactory blockFactory) {
        if (block.mvDeduplicated()) {
            block.incRef();
            return block;
        }
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(block.getPositionCount())) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 1 -> builder.appendDoubleRange(block.getDoubleRange(first, work[0]));
                    default -> {
                        /*
                         * It's better to copyMissing when there are few unique values
                         * and better to copy and sort when there are many unique values.
                         * The more duplicate values there are the more comparatively worse
                         * copyAndSort is. But we don't know how many unique values there
                         * because our job is to find them. So we use the count of values
                         * as a proxy that is fast to test. It's not always going to be
                         * optimal but it has the nice property of being quite quick on
                         * short lists and not n^2 levels of terrible on long ones.
                         *
                         * It'd also be possible to make a truly hybrid mechanism that
                         * switches from copyMissing to copyUnique once it collects enough
                         * unique values. The trouble is that the switch is expensive and
                         * makes kind of a "hole" in the performance of that mechanism where
                         * you may as well have just gone with either of the two other
                         * strategies. So we just don't try it for now.
                         */
                        if (count < ALWAYS_COPY_MISSING) {
                            copyMissing(first, count);
                            writeUniquedWork(builder);
                        } else {
                            copyAndSort(first, count);
                            deduplicatedSortedWork(builder);
                        }
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an algorithm with very low overhead but {@code n^2}
     * case complexity for larger. Prefer {@link #dedupeToBlockAdaptive}
     * which picks based on the number of elements at each position.
     */
    public DoubleRangeBlock dedupeToBlockUsingCopyAndSort(BlockFactory blockFactory) {
        if (block.mvDeduplicated()) {
            block.incRef();
            return block;
        }
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(block.getPositionCount())) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 1 -> builder.appendDoubleRange(block.getDoubleRange(first, work[0]));
                    default -> {
                        copyAndSort(first, count);
                        deduplicatedSortedWork(builder);
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * Remove duplicate values from each position and write the results to a
     * {@link Block} using an algorithm that sorts all values. It has a higher
     * overhead for small numbers of values at each position than
     * {@link #dedupeToBlockUsingCopyMissing} for large numbers of values the
     * performance is dominated by the {@code n*log n} sort. Prefer
     * {@link #dedupeToBlockAdaptive} unless you need the results sorted.
     */
    public DoubleRangeBlock dedupeToBlockUsingCopyMissing(BlockFactory blockFactory) {
        if (block.mvDeduplicated()) {
            block.incRef();
            return block;
        }
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(block.getPositionCount())) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 1 -> builder.appendDoubleRange(block.getDoubleRange(first, work[0]));
                    default -> {
                        copyMissing(first, count);
                        writeUniquedWork(builder);
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * Sort values from each position and write the results to a {@link Block}.
     */
    public DoubleRangeBlock sortToBlock(BlockFactory blockFactory, boolean ascending) {
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(block.getPositionCount())) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 1 -> builder.appendDoubleRange(block.getDoubleRange(first, work[0]));
                    default -> {
                        copyAndSort(first, count);
                        writeSortedWork(builder, ascending);
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * Dedupe values, add them to the hash, and build an {@link IntBlock} of
     * their hashes. This block is suitable for passing as the grouping block
     * to a {@link GroupingAggregatorFunction}.
     */
    public MultivalueDedupe.HashResult hashAdd(BlockFactory blockFactory, LongLongHashTable hash) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(block.getPositionCount())) {
            boolean sawNull = false;
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    sawNull = true;
                    builder.appendInt(0);
                    continue;
                }
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 1 -> {
                        DoubleRange v = block.getDoubleRange(first, work[0]);
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
    public IntBlock hashLookup(BlockFactory blockFactory, LongLongHashTable hash) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(block.getPositionCount())) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (block.isNull(p)) {
                    builder.appendInt(0);
                    continue;
                }
                int count = block.getValueCount(p);
                int first = block.getFirstValueIndex(p);
                switch (count) {
                    case 1 -> {
                        DoubleRange v = block.getDoubleRange(first, work[0]);
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
     * Build a {@link BatchEncoder} which deduplicates values at each position
     * and then encodes the results into a {@code byte[]} which can be used for
     * things like hashing many fields together.
     */
    public BatchEncoder batchEncoder(int batchSize) {
        block.incRef();
        return new BatchEncoder.DoubleRanges(batchSize) {
            @Override
            protected void readNextBatch() {
                int position = firstPosition();
                if (w > 0) {
                    // The last block didn't fit so we have to *make* it fit
                    ensureCapacity(w);
                    startPosition();
                    encodeUniquedWork(this);
                    endPosition();
                    position++;
                }
                for (; position < block.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        encodeNull();
                        continue;
                    }
                    int count = block.getValueCount(position);
                    int first = block.getFirstValueIndex(position);
                    switch (count) {
                        case 1 -> {
                            DoubleRange v = block.getDoubleRange(first, work[0]);
                            if (hasCapacity(1)) {
                                startPosition();
                                encode(v);
                                endPosition();
                            } else {
                                work[0] = v;
                                w = 1;
                                return;
                            }
                        }
                        default -> {
                            if (count < ALWAYS_COPY_MISSING) {
                                copyMissing(first, count);
                            } else {
                                copyAndSort(first, count);
                                convertSortedWorkToUnique();
                            }
                            if (hasCapacity(w)) {
                                startPosition();
                                encodeUniquedWork(this);
                                endPosition();
                            } else {
                                return;
                            }
                        }
                    }
                }
            }

            @Override
            public void close() {
                block.decRef();
            }
        };
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
            block.getDoubleRange(i, work[w]);
            w++;
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

        block.getDoubleRange(first, work[0]);
        w = 1;
        i: for (int i = first + 1; i < end; i++) {
            DoubleRange v = block.getDoubleRange(i, work[w]);
            for (int j = 0; j < w; j++) {
                if (v.equals(work[j])) {
                    continue i;
                }
            }
            work[w++] = v;
        }
    }

    /**
     * Writes an already deduplicated {@link #work} to a {@link DoubleRangeBlock.Builder}.
     */
    private void writeUniquedWork(DoubleRangeBlock.Builder builder) {
        if (w == 1) {
            builder.appendDoubleRange(work[0]);
            return;
        }
        builder.beginPositionEntry();
        for (int i = 0; i < w; i++) {
            builder.appendDoubleRange(work[i]);
        }
        builder.endPositionEntry();
    }

    /**
     * Writes a sorted {@link #work} to a {@link DoubleRangeBlock.Builder}, skipping duplicates.
     */
    private void deduplicatedSortedWork(DoubleRangeBlock.Builder builder) {
        builder.beginPositionEntry();
        DoubleRange prev = work[0];
        builder.appendDoubleRange(prev);
        for (int i = 1; i < w; i++) {
            if (false == prev.equals(work[i])) {
                prev = work[i];
                builder.appendDoubleRange(prev);
            }
        }
        builder.endPositionEntry();
    }

    /**
     * Writes a {@link #work} to a {@link DoubleRangeBlock.Builder}.
     */
    private void writeSortedWork(DoubleRangeBlock.Builder builder, boolean ascending) {
        builder.beginPositionEntry();
        for (int i = 0; i < w; i++) {
            if (ascending) {
                builder.appendDoubleRange(work[i]);
            } else {
                builder.appendDoubleRange(work[w - i - 1]);
            }
        }
        builder.endPositionEntry();
    }

    /**
     * Writes an already deduplicated {@link #work} to a hash.
     */
    private void hashAddUniquedWork(LongLongHashTable hash, IntBlock.Builder builder) {
        if (w == 1) {
            hashAdd(builder, hash, work[0]);
            return;
        }
        builder.beginPositionEntry();
        for (int i = 0; i < w; i++) {
            hashAdd(builder, hash, work[i]);
        }
        builder.endPositionEntry();
    }

    /**
     * Writes a sorted {@link #work} to a hash, skipping duplicates.
     */
    private void hashAddSortedWork(LongLongHashTable hash, IntBlock.Builder builder) {
        if (w == 1) {
            hashAdd(builder, hash, work[0]);
            return;
        }
        builder.beginPositionEntry();
        DoubleRange prev = work[0];
        hashAdd(builder, hash, prev);
        for (int i = 1; i < w; i++) {
            if (false == valuesEqual(prev, work[i])) {
                prev = work[i];
                hashAdd(builder, hash, prev);
            }
        }
        builder.endPositionEntry();
    }

    /**
     * Looks up an already deduplicated {@link #work} to a hash.
     */
    private void hashLookupUniquedWork(LongLongHashTable hash, IntBlock.Builder builder) {
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
    private void hashLookupSortedWork(LongLongHashTable hash, IntBlock.Builder builder) {
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
        DoubleRange prev = work[0];
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

    /**
     * Writes a deduplicated {@link #work} to a {@link BatchEncoder.DoubleRanges}.
     */
    private void encodeUniquedWork(BatchEncoder.DoubleRanges encoder) {
        for (int i = 0; i < w; i++) {
            encoder.encode(work[i]);
        }
    }

    /**
     * Converts {@link #work} from sorted array to a deduplicated array.
     */
    private void convertSortedWorkToUnique() {
        DoubleRange prev = work[0];
        int end = w;
        w = 1;
        for (int i = 1; i < end; i++) {
            if (false == valuesEqual(prev, work[i])) {
                prev = work[i];
                work[w].reset(prev.from(), prev.to());
                w++;
            }
        }
    }

    private void grow(int size) {
        int prev = work.length;
        work = ArrayUtil.grow(work, size);
        fillWork(prev, work.length);
    }

    private void fillWork(int from, int to) {
        for (int i = from; i < to; i++) {
            work[i] = new DoubleRange();
        }
    }

    private void hashAdd(IntBlock.Builder builder, LongLongHashTable hash, DoubleRange v) {
        appendFound(builder, hash.add(Double.doubleToLongBits(v.from()), Double.doubleToLongBits(v.to())));
    }

    private long hashLookup(LongLongHashTable hash, DoubleRange v) {
        return hash.find(Double.doubleToLongBits(v.from()), Double.doubleToLongBits(v.to()));
    }

    private void hashLookupSingle(IntBlock.Builder builder, LongLongHashTable hash, DoubleRange v) {
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

    private static boolean valuesEqual(DoubleRange lhs, DoubleRange rhs) {
        return lhs.equals(rhs);
    }
}
