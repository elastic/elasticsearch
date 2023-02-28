/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

/**
 * A specialized {@link BlockHash} for a {@link BytesRef} and a long.
 */
final class BytesRefLongBlockHash extends BlockHash {
    private final int channel1;
    private final int channel2;
    private final BytesRefHash bytesHash;
    private final LongLongHash finalHash;
    private final boolean reverseOutput;

    BytesRefLongBlockHash(BigArrays bigArrays, int channel1, int channel2, boolean reverseOutput) {
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.reverseOutput = reverseOutput;

        boolean success = false;
        BytesRefHash bytesHash = null;
        LongLongHash longHash = null;
        try {
            bytesHash = new BytesRefHash(1, bigArrays);
            longHash = new LongLongHash(1, bigArrays);
            this.bytesHash = bytesHash;
            this.finalHash = longHash;
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(bytesHash);
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(bytesHash, finalHash);
    }

    @Override
    public LongBlock add(Page page) {
        BytesRefBlock block1 = page.getBlock(channel1);
        LongBlock block2 = page.getBlock(channel2);
        BytesRefVector vector1 = block1.asVector();
        LongVector vector2 = block2.asVector();
        BytesRef scratch = new BytesRef();
        int positions = page.getPositionCount();
        if (vector1 != null && vector2 != null) {
            final long[] ords = new long[positions];
            for (int i = 0; i < positions; i++) {
                long hash1 = hashOrdToGroup(bytesHash.add(vector1.getBytesRef(i, scratch)));
                ords[i] = hashOrdToGroup(finalHash.add(hash1, vector2.getLong(i)));
            }
            return new LongArrayVector(ords, positions).asBlock();
        } else {
            LongBlock.Builder ords = LongBlock.newBlockBuilder(positions);
            for (int i = 0; i < positions; i++) {
                if (block1.isNull(i) || block2.isNull(i)) {
                    ords.appendNull();
                } else {
                    long hash1 = hashOrdToGroup(bytesHash.add(block1.getBytesRef(i, scratch)));
                    long hash = hashOrdToGroup(finalHash.add(hash1, block2.getLong(i)));
                    ords.appendLong(hash);
                }
            }
            return ords.build();
        }
    }

    @Override
    public Block[] getKeys() {
        int positions = (int) finalHash.size();
        BytesRefVector.Builder keys1 = BytesRefVector.newVectorBuilder(positions);
        LongVector.Builder keys2 = LongVector.newVectorBuilder(positions);
        BytesRef scratch = new BytesRef();
        for (long i = 0; i < positions; i++) {
            keys2.appendLong(finalHash.getKey2(i));
            long h1 = finalHash.getKey1(i);
            keys1.appendBytesRef(bytesHash.get(h1, scratch));
        }
        if (reverseOutput) {
            return new Block[] { keys2.build().asBlock(), keys1.build().asBlock() };
        } else {
            return new Block[] { keys1.build().asBlock(), keys2.build().asBlock() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(finalHash.size()));
    }

    @Override
    public String toString() {
        return "BytesRefLongBlockHash{keys=[BytesRefKey[channel="
            + channel1
            + "], LongKey[channel="
            + channel2
            + "]], entries="
            + finalHash.size()
            + ", size="
            + bytesHash.ramBytesUsed()
            + "b}";
    }
}
