/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.MultivalueDedupeBoolean;

/**
 * Maps a {@link BooleanBlock} column to group ids. Assigns group
 * {@code 0} to {@code false} and group {@code 1} to {@code true}.
 */
final class BooleanBlockHash extends BlockHash {
    private final int channel;
    private final boolean[] everSeen = new boolean[2];

    BooleanBlockHash(int channel) {
        this.channel = channel;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BooleanBlock block = page.getBlock(channel);
        BooleanVector vector = block.asVector();
        if (vector == null) {
            addInput.add(0, add(block));
        } else {
            addInput.add(0, add(vector));
        }
    }

    private LongVector add(BooleanVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = MultivalueDedupeBoolean.hashOrd(everSeen, vector.getBoolean(i));
        }
        return new LongArrayVector(groups, groups.length);
    }

    private LongBlock add(BooleanBlock block) {
        return new MultivalueDedupeBoolean(block).hash(everSeen);
    }

    @Override
    public BooleanBlock[] getKeys() {
        BooleanVector.Builder builder = BooleanVector.newVectorBuilder(2);
        if (everSeen[0]) {
            builder.appendBoolean(false);
        }
        if (everSeen[1]) {
            builder.appendBoolean(true);
        }
        return new BooleanBlock[] { builder.build().asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        IntVector.Builder builder = IntVector.newVectorBuilder(2);
        if (everSeen[0]) {
            builder.appendInt(0);
        }
        if (everSeen[1]) {
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
        return "BooleanBlockHash{channel=" + channel + ", seenFalse=" + everSeen[0] + ", seenTrue=" + everSeen[1] + '}';
    }
}
