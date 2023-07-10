/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.MultivalueDedupeDouble;

/**
 * Maps a {@link DoubleBlock} column to group ids.
 */
final class DoubleBlockHash extends BlockHash {
    private final int channel;
    private final LongHash longHash;

    DoubleBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        DoubleBlock block = page.getBlock(channel);
        DoubleVector vector = block.asVector();
        if (vector == null) {
            addInput.add(0, add(block));
        } else {
            addInput.add(0, add(vector));
        }
    }

    private LongVector add(DoubleVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = hashOrdToGroup(longHash.add(Double.doubleToLongBits(vector.getDouble(i))));
        }
        return new LongArrayVector(groups, groups.length);
    }

    private LongBlock add(DoubleBlock block) {
        return new MultivalueDedupeDouble(block).hash(longHash);
    }

    @Override
    public DoubleBlock[] getKeys() {
        final int size = Math.toIntExact(longHash.size());
        final double[] keys = new double[size];
        for (int i = 0; i < size; i++) {
            keys[i] = Double.longBitsToDouble(longHash.get(i));
        }

        // TODO claim the array and wrap?
        return new DoubleBlock[] { new DoubleArrayVector(keys, keys.length).asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(longHash.size()));
    }

    @Override
    public void close() {
        longHash.close();
    }

    @Override
    public String toString() {
        return "DoubleBlockHash{channel=" + channel + ", entries=" + longHash.size() + '}';
    }
}
