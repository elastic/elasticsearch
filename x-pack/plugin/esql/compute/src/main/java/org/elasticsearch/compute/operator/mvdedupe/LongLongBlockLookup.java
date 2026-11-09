/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.mvdedupe;

import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;

import java.util.Arrays;

/**
 * Looks up pairs of {@link LongBlock} values in a {@link LongLongHashTable} without adding them, enumerating the
 * combinations of a position's values the way {@link LongLongBlockAdd} adds them. A position whose combinations are
 * all absent is null, as a single absent key is; a position with several present combinations holds them all.
 */
public final class LongLongBlockLookup {
    private final BlockFactory blockFactory;
    private final LongLongHashTable hash;
    private final MultivalueDedupeLong block1;
    private final MultivalueDedupeLong block2;
    private int[] found = new int[4];
    private int foundCount;

    public LongLongBlockLookup(BlockFactory blockFactory, LongLongHashTable hash, LongBlock block1, LongBlock block2) {
        this.blockFactory = blockFactory;
        this.hash = hash;
        this.block1 = new MultivalueDedupeLong(block1);
        this.block2 = new MultivalueDedupeLong(block2);
    }

    public IntBlock lookup() {
        int positions = block1.block.getPositionCount();
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                lookup1(p, builder);
            }
            return builder.build();
        }
    }

    private void lookup1(int position, IntBlock.Builder builder) {
        int first = block1.block.getFirstValueIndex(position);
        int count = block1.block.getValueCount(position);
        switch (count) {
            case 0 -> builder.appendNull();
            case 1 -> {
                block1.w = 1;
                block1.work[0] = block1.block.getLong(first);
                lookup2(position, true, builder);
            }
            default -> {
                if (count < MultivalueDedupeLong.ALWAYS_COPY_MISSING) {
                    block1.copyMissing(first, count);
                    lookup2(position, true, builder);
                } else {
                    block1.copyAndSort(first, count);
                    lookup2(position, false, builder);
                }
            }
        }
    }

    private void lookup2(int position, boolean work1IsUnique, IntBlock.Builder builder) {
        int first = block2.block.getFirstValueIndex(position);
        int count = block2.block.getValueCount(position);
        switch (count) {
            case 0 -> builder.appendNull();
            case 1 -> {
                block2.w = 1;
                block2.work[0] = block2.block.getLong(first);
                finishLookup(work1IsUnique, true, builder);
            }
            default -> {
                if (count < MultivalueDedupeLong.ALWAYS_COPY_MISSING) {
                    block2.copyMissing(first, count);
                    finishLookup(work1IsUnique, true, builder);
                } else {
                    block2.copyAndSort(first, count);
                    finishLookup(work1IsUnique, false, builder);
                }
            }
        }
    }

    private void finishLookup(boolean work1IsUnique, boolean work2IsUnique, IntBlock.Builder builder) {
        foundCount = 0;
        if (work1IsUnique) {
            for (int i = 0; i < block1.w; i++) {
                findAll(block1.work[i], work2IsUnique);
            }
        } else {
            long prev1 = block1.work[0];
            findAll(prev1, work2IsUnique);
            for (int i = 1; i < block1.w; i++) {
                if (prev1 == block1.work[i]) {
                    continue;
                }
                prev1 = block1.work[i];
                findAll(prev1, work2IsUnique);
            }
        }
        switch (foundCount) {
            case 0 -> builder.appendNull();
            case 1 -> builder.appendInt(found[0]);
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < foundCount; i++) {
                    builder.appendInt(found[i]);
                }
                builder.endPositionEntry();
            }
        }
    }

    private void findAll(long v1, boolean work2IsUnique) {
        if (work2IsUnique) {
            for (int i = 0; i < block2.w; i++) {
                find(v1, block2.work[i]);
            }
            return;
        }
        long prev2 = block2.work[0];
        find(v1, prev2);
        for (int i = 1; i < block2.w; i++) {
            if (prev2 == block2.work[i]) {
                continue;
            }
            prev2 = block2.work[i];
            find(v1, prev2);
        }
    }

    private void find(long v1, long v2) {
        long ord = hash.find(v1, v2);
        if (ord < 0) {
            return;
        }
        if (foundCount == found.length) {
            found = Arrays.copyOf(found, found.length * 2);
        }
        found[foundCount++] = Math.toIntExact(BlockHash.hashOrdToGroup(ord));
    }
}
