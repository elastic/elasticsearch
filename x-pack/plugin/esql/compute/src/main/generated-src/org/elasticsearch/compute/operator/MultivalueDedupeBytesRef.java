/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;

import java.util.Arrays;

/**
 * Removes duplicate values from multivalued positions.
 * This class is generated. Edit {@code X-MultivalueDedupe.java.st} instead.
 */
public class MultivalueDedupeBytesRef {
    /**
     * The number of entries before we switch from and {@code n^2} strategy
     * with low overhead to an {@code n*log(n)} strategy with higher overhead.
     * The choice of number has been experimentally derived.
     */
    private static final int ALWAYS_COPY_MISSING = 20;  // TODO BytesRef should try adding to the hash *first* and then comparing.
    private final BytesRefBlock block;
    private BytesRef[] work = new BytesRef[ArrayUtil.oversize(2, org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
    private int w;

    public MultivalueDedupeBytesRef(BytesRefBlock block) {
        this.block = block;
        // TODO very large numbers might want a hash based implementation - and for BytesRef that might not be that big
        fillWork(0, work.length);
    }

    /**
     * Dedupe values using an adaptive algorithm based on the size of the input list.
     */
    public BytesRefBlock dedupeToBlockAdaptive() {
        if (false == block.mayHaveMultivaluedFields()) {
            return block;
        }
        BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            int first = block.getFirstValueIndex(p);
            switch (count) {
                case 0 -> builder.appendNull();
                case 1 -> builder.appendBytesRef(block.getBytesRef(first, work[0]));
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
                        writeSortedWork(builder);
                    }
                }
            }
        }
        return builder.build();
    }

    /**
     * Dedupe values using an {@code n*log(n)} strategy with higher overhead. Prefer {@link #dedupeToBlockAdaptive}.
     * This is public for testing and performance testing.
     */
    public BytesRefBlock dedupeToBlockUsingCopyAndSort() {
        if (false == block.mayHaveMultivaluedFields()) {
            return block;
        }
        BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            int first = block.getFirstValueIndex(p);
            switch (count) {
                case 0 -> builder.appendNull();
                case 1 -> builder.appendBytesRef(block.getBytesRef(first, work[0]));
                default -> {
                    copyAndSort(first, count);
                    writeSortedWork(builder);
                }
            }
        }
        return builder.build();
    }

    /**
     * Dedupe values using an {@code n^2} strategy with low overhead. Prefer {@link #dedupeToBlockAdaptive}.
     * This is public for testing and performance testing.
     */
    public BytesRefBlock dedupeToBlockUsingCopyMissing() {
        if (false == block.mayHaveMultivaluedFields()) {
            return block;
        }
        BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            int first = block.getFirstValueIndex(p);
            switch (count) {
                case 0 -> builder.appendNull();
                case 1 -> builder.appendBytesRef(block.getBytesRef(first, work[0]));
                default -> {
                    copyMissing(first, count);
                    writeUniquedWork(builder);
                }
            }
        }
        return builder.build();
    }

    /**
     * Dedupe values and build a {@link LongBlock} suitable for passing
     * as the grouping block to a {@link GroupingAggregatorFunction}.
     */
    public LongBlock hash(BytesRefHash hash) {
        LongBlock.Builder builder = LongBlock.newBlockBuilder(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            int first = block.getFirstValueIndex(p);
            switch (count) {
                case 0 -> builder.appendNull();
                case 1 -> {
                    BytesRef v = block.getBytesRef(first, work[0]);
                    hash(builder, hash, v);
                }
                default -> {
                    if (count < ALWAYS_COPY_MISSING) {
                        copyMissing(first, count);
                        hashUniquedWork(hash, builder);
                    } else {
                        copyAndSort(first, count);
                        hashSortedWork(hash, builder);
                    }
                }
            }
        }
        return builder.build();
    }

    private void copyAndSort(int first, int count) {
        grow(count);
        int end = first + count;

        w = 0;
        for (int i = first; i < end; i++) {
            work[w] = block.getBytesRef(i, work[w]);
            w++;
        }

        Arrays.sort(work, 0, w);
    }

    private void copyMissing(int first, int count) {
        grow(count);
        int end = first + count;

        work[0] = block.getBytesRef(first, work[0]);
        w = 1;
        i: for (int i = first + 1; i < end; i++) {
            BytesRef v = block.getBytesRef(i, work[w]);
            for (int j = 0; j < w; j++) {
                if (v.equals(work[j])) {
                    continue i;
                }
            }
            work[w++] = v;
        }
    }

    private void writeUniquedWork(BytesRefBlock.Builder builder) {
        if (w == 1) {
            builder.appendBytesRef(work[0]);
            return;
        }
        builder.beginPositionEntry();
        for (int i = 0; i < w; i++) {
            builder.appendBytesRef(work[i]);
        }
        builder.endPositionEntry();
    }

    private void writeSortedWork(BytesRefBlock.Builder builder) {
        if (w == 1) {
            builder.appendBytesRef(work[0]);
            return;
        }
        builder.beginPositionEntry();
        BytesRef prev = work[0];
        builder.appendBytesRef(prev);
        for (int i = 1; i < w; i++) {
            if (false == prev.equals(work[i])) {
                prev = work[i];
                builder.appendBytesRef(prev);
            }
        }
        builder.endPositionEntry();
    }

    private void hashUniquedWork(BytesRefHash hash, LongBlock.Builder builder) {
        if (w == 1) {
            hash(builder, hash, work[0]);
            return;
        }
        builder.beginPositionEntry();
        for (int i = 0; i < w; i++) {
            hash(builder, hash, work[i]);
        }
        builder.endPositionEntry();
    }

    private void hashSortedWork(BytesRefHash hash, LongBlock.Builder builder) {
        if (w == 1) {
            hash(builder, hash, work[0]);
            return;
        }
        builder.beginPositionEntry();
        BytesRef prev = work[0];
        hash(builder, hash, prev);
        for (int i = 1; i < w; i++) {
            if (false == prev.equals(work[i])) {
                prev = work[i];
                hash(builder, hash, prev);
            }
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

    private void hash(LongBlock.Builder builder, BytesRefHash hash, BytesRef v) {
        builder.appendLong(BlockHash.hashOrdToGroup(hash.add(v)));
    }
}
