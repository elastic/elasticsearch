/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.MultivalueDedupeBoolean;

import static org.elasticsearch.compute.operator.MultivalueDedupeBoolean.FALSE_ORD;
import static org.elasticsearch.compute.operator.MultivalueDedupeBoolean.NULL_ORD;
import static org.elasticsearch.compute.operator.MultivalueDedupeBoolean.TRUE_ORD;

/**
 * Maps a {@link BooleanBlock} column to group ids. Assigns group
 * {@code 0} to {@code false} and group {@code 1} to {@code true}.
 */
final class BooleanBlockHash extends BlockHash {
    private final int channel;
    private final boolean[] everSeen = new boolean[TRUE_ORD + 1];

    BooleanBlockHash(int channel, DriverContext driverContext) {
        super(driverContext);
        this.channel = channel;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            everSeen[NULL_ORD] = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
        } else {
            BooleanBlock booleanBlock = page.getBlock(channel);
            BooleanVector booleanVector = booleanBlock.asVector();
            if (booleanVector == null) {
                try (IntBlock groupIds = add(booleanBlock)) {
                    addInput.add(0, groupIds);
                }
            } else {
                try (IntBlock groupIds = add(booleanVector).asBlock()) {
                    addInput.add(0, groupIds.asVector());
                }
            }
        }
    }

    private IntVector add(BooleanVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(MultivalueDedupeBoolean.hashOrd(everSeen, vector.getBoolean(i)));
            }
            return builder.build();
        }
    }

    private IntBlock add(BooleanBlock block) {
        return new MultivalueDedupeBoolean(block).hash(everSeen);
    }

    @Override
    public BooleanBlock[] getKeys() {
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(everSeen.length)) {
            if (everSeen[NULL_ORD]) {
                builder.appendNull();
            }
            if (everSeen[FALSE_ORD]) {
                builder.appendBoolean(false);
            }
            if (everSeen[TRUE_ORD]) {
                builder.appendBoolean(true);
            }
            return new BooleanBlock[] { builder.build() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        try (IntVector.Builder builder = blockFactory.newIntVectorBuilder(everSeen.length)) {
            for (int i = 0; i < everSeen.length; i++) {
                if (everSeen[i]) {
                    builder.appendInt(i);
                }
            }
            return builder.build();
        }
    }

    public BitArray seenGroupIds(BigArrays bigArrays) {
        BitArray seen = new BitArray(everSeen.length, bigArrays);
        for (int i = 0; i < everSeen.length; i++) {
            if (everSeen[i]) {
                seen.set(i);
            }
        }
        return seen;
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String toString() {
        return "BooleanBlockHash{channel="
            + channel
            + ", seenFalse="
            + everSeen[FALSE_ORD]
            + ", seenTrue="
            + everSeen[TRUE_ORD]
            + ", seenNull="
            + everSeen[NULL_ORD]
            + '}';
    }
}
