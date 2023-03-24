/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

/**
 * Assigns group {@code 0} to the first of {@code true} or{@code false}
 * that it sees and {@code 1} to the second.
 */
final class BooleanBlockHash extends BlockHash {
    private final int channel;

    private boolean seenFalse;
    private boolean seenTrue;

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
                builder.appendLong(ord(block.getBoolean(block.getFirstValueIndex(i))));
            }
        }
        return builder.build();
    }

    private long ord(boolean b) {
        if (b) {
            seenTrue = true;
            return 1;
        }
        seenFalse = true;
        return 0;
    }

    @Override
    public BooleanBlock[] getKeys() {
        BooleanVector.Builder builder = BooleanVector.newVectorBuilder(2);
        if (seenFalse) {
            builder.appendBoolean(false);
        }
        if (seenTrue) {
            builder.appendBoolean(true);
        }
        return new BooleanBlock[] { builder.build().asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        IntVector.Builder builder = IntVector.newVectorBuilder(2);
        if (seenFalse) {
            builder.appendInt(0);
        }
        if (seenTrue) {
            builder.appendInt(1);
        }
        return builder.build();
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String toString() {
        return "BooleanBlockHash{channel=" + channel + ", seenFalse=" + seenFalse + ", seenTrue=" + seenTrue + '}';
    }
}
