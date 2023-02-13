/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

/**
 * Assigns group {@code 0} to the first of {@code true} or{@code false}
 * that it sees and {@code 1} to the second.
 */
final class BooleanBlockHash extends BlockHash {
    // TODO this isn't really a "hash" so maybe we should rename base class
    private final int[] buckets = { -1, -1 };
    private final int channel;

    BooleanBlockHash(int channel) {
        this.channel = channel;
    }

    @Override
    public LongBlock add(Page page) {
        BooleanBlock block = page.getBlock(channel);
        int positionCount = block.getPositionCount();
        BooleanVector vector = block.asVector();
        if (vector != null) {
            long[] groups = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                groups[i] = ord(vector.getBoolean(i));
            }
            return new LongArrayVector(groups, positionCount).asBlock();
        }
        LongBlock.Builder builder = LongBlock.newBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                builder.appendNull();
            } else {
                builder.appendLong(ord(block.getBoolean(i)));
            }
        }
        return builder.build();
    }

    private long ord(boolean b) {
        int pos = b ? 1 : 0;
        int ord = buckets[pos];
        if (ord == -1) {
            int otherPos = pos ^ 1; // 1 -> 0 and 0 -> 1 without branching
            ord = buckets[otherPos] + 1;
            buckets[pos] = ord;
        }
        return ord;
    }

    @Override
    public BooleanBlock[] getKeys() {
        BooleanVector.Builder builder = BooleanVector.newVectorBuilder(2);
        if (buckets[0] < buckets[1]) {
            if (buckets[0] >= 0) {
                builder.appendBoolean(false);
            }
            if (buckets[1] >= 0) {
                builder.appendBoolean(true);
            }
        } else {
            if (buckets[1] >= 0) {
                builder.appendBoolean(true);
            }
            if (buckets[0] >= 0) {
                builder.appendBoolean(false);
            }
        }
        return new BooleanBlock[] { builder.build().asBlock() };
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String toString() {
        return "BooleanBlockHash{channel="
            + channel
            + (buckets[1] == -1 ? "" : ", true=" + buckets[1])
            + (buckets[0] == -1 ? "" : ", false=" + buckets[0])
            + '}';
    }
}
