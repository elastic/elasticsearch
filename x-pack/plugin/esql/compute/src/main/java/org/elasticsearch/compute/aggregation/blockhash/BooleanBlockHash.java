/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean;
import org.elasticsearch.core.ReleasableIterator;

import static org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean.FALSE_ORD;
import static org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean.NULL_ORD;
import static org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean.TRUE_ORD;

/**
 * Maps a {@link BooleanBlock} column to group ids. Assigns
 * {@code 0} to {@code null}, {@code 1} to {@code false}, and
 * {@code 2} to {@code true}.
 */
final class BooleanBlockHash extends BlockHash {
    private final int channel;
    private final boolean[] everSeen = new boolean[TRUE_ORD + 1];

    BooleanBlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
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
                try (IntVector groupIds = add(booleanVector)) {
                    addInput.add(0, groupIds);
                }
            }
        }
    }

    private IntVector add(BooleanVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(i, MultivalueDedupeBoolean.hashOrd(everSeen, vector.getBoolean(i)));
            }
            return builder.build();
        }
    }

    private IntBlock add(BooleanBlock block) {
        return new MultivalueDedupeBoolean(block).hash(blockFactory, everSeen);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }
        BooleanBlock castBlock = page.getBlock(channel);
        BooleanVector vector = castBlock.asVector();
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntBlock lookup(BooleanVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                boolean v = vector.getBoolean(i);
                int ord = v ? TRUE_ORD : FALSE_ORD;
                if (everSeen[ord]) {
                    builder.appendInt(ord);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private IntBlock lookup(BooleanBlock block) {
        return new MultivalueDedupeBoolean(block).hash(blockFactory, new boolean[TRUE_ORD + 1]);
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

    @Override
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
