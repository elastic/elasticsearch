/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

final class IntBlockHash extends BlockHash {
    private final int channel;
    private final LongHash longHash;

    IntBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public LongBlock add(Page page) {
        IntBlock block = page.getBlock(channel);
        int positionCount = block.getPositionCount();
        IntVector vector = block.asVector();
        if (vector != null) {
            long[] groups = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                groups[i] = hashOrdToGroup(longHash.add(vector.getInt(i)));
            }
            return new LongArrayVector(groups, positionCount).asBlock();
        }
        LongBlock.Builder builder = LongBlock.newBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                builder.appendNull();
            } else {
                builder.appendLong(hashOrdToGroup(longHash.add(block.getInt(block.getFirstValueIndex(i)))));
            }
        }
        return builder.build();
    }

    @Override
    public IntBlock[] getKeys() {
        final int size = Math.toIntExact(longHash.size());
        final int[] keys = new int[size];
        for (int i = 0; i < size; i++) {
            keys[i] = (int) longHash.get(i);
        }
        return new IntBlock[] { new IntArrayVector(keys, keys.length).asBlock() };
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
        return "IntBlockHash{channel=" + channel + ", entries=" + longHash.size() + '}';
    }
}
