/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;

final class DoubleBlockHash extends BlockHash {
    private final int channel;
    private final LongHash longHash;

    DoubleBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public LongBlock add(Page page) {
        DoubleBlock block = page.getBlock(channel);
        DoubleVector vector = block.asVector();
        if (vector == null) {
            return add(block);
        }
        return add(vector).asBlock();
    }

    private LongVector add(DoubleVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = hashOrdToGroup(longHash.add(Double.doubleToLongBits(vector.getDouble(i))));
        }
        return new LongArrayVector(groups, groups.length);
    }

    private static final double[] EMPTY = new double[0];

    private LongBlock add(DoubleBlock block) {
        double[] seen = EMPTY;
        LongBlock.Builder builder = LongBlock.newBlockBuilder(block.getTotalValueCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                builder.appendNull();
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int count = block.getValueCount(p);
            if (count == 1) {
                builder.appendLong(hashOrdToGroup(longHash.add(Double.doubleToLongBits(block.getDouble(start)))));
                continue;
            }
            if (seen.length < count) {
                seen = new double[ArrayUtil.oversize(count, Double.BYTES)];
            }
            builder.beginPositionEntry();
            // TODO if we know the elements were in sorted order we wouldn't need an array at all.
            // TODO we could also have an assertion that there aren't any duplicates on the block.
            // Lucene has them in ascending order without duplicates
            int end = start + count;
            int nextSeen = 0;
            for (int offset = start; offset < end; offset++) {
                nextSeen = add(builder, seen, nextSeen, block.getDouble(offset));
            }
            builder.endPositionEntry();
        }
        return builder.build();
    }

    protected int add(LongBlock.Builder builder, double[] seen, int nextSeen, double value) {
        for (int j = 0; j < nextSeen; j++) {
            if (seen[j] == value) {
                return nextSeen;
            }
        }
        seen[nextSeen] = value;
        builder.appendLong(hashOrdToGroup(longHash.add(Double.doubleToLongBits(value))));
        return nextSeen + 1;
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
