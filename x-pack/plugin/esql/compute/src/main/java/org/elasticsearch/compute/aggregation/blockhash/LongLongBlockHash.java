/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

/**
 * A specialized {@link BlockHash} implementation for two longs
 */
final class LongLongBlockHash extends BlockHash {
    private final int channel1;
    private final int channel2;
    private final LongLongHash hash;

    LongLongBlockHash(BigArrays bigArrays, int channel1, int channel2) {
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.hash = new LongLongHash(1, bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(hash);
    }

    @Override
    public LongBlock add(Page page) {
        LongBlock block1 = page.getBlock(channel1);
        LongBlock block2 = page.getBlock(channel2);
        int positions = block1.getPositionCount();
        LongVector vector1 = block1.asVector();
        LongVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            final long[] ords = new long[positions];
            for (int i = 0; i < positions; i++) {
                ords[i] = hashOrdToGroup(hash.add(vector1.getLong(i), vector2.getLong(i)));
            }
            return new LongArrayVector(ords, positions).asBlock();
        } else {
            LongBlock.Builder ords = LongBlock.newBlockBuilder(positions);
            for (int i = 0; i < positions; i++) {
                if (block1.isNull(i) || block2.isNull(i)) {
                    ords.appendNull();
                } else {
                    long h = hashOrdToGroup(
                        hash.add(block1.getLong(block1.getFirstValueIndex(i)), block2.getLong(block2.getFirstValueIndex(i)))
                    );
                    ords.appendLong(h);
                }
            }
            return ords.build();
        }
    }

    @Override
    public Block[] getKeys() {
        int positions = (int) hash.size();
        LongVector.Builder keys1 = LongVector.newVectorBuilder(positions);
        LongVector.Builder keys2 = LongVector.newVectorBuilder(positions);
        for (long i = 0; i < positions; i++) {
            keys1.appendLong(hash.getKey1(i));
            keys2.appendLong(hash.getKey2(i));
        }
        return new Block[] { keys1.build().asBlock(), keys2.build().asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(hash.size()));
    }

    @Override
    public String toString() {
        return "LongLongBlockHash{channels=[" + channel1 + "," + channel2 + "], entries=" + hash.size() + "}";
    }
}
