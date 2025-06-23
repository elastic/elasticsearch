/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Locale;

/**
 * Maps two {@link BytesRefBlock}s to group ids.
 */
final class BytesRef2BlockHash extends BlockHash {
    private final int emitBatchSize;
    private final int channel1;
    private final int channel2;
    private final BytesRefBlockHash hash1;
    private final BytesRefBlockHash hash2;
    private final LongHash finalHash;

    BytesRef2BlockHash(BlockFactory blockFactory, int channel1, int channel2, int emitBatchSize) {
        super(blockFactory);
        this.emitBatchSize = emitBatchSize;
        this.channel1 = channel1;
        this.channel2 = channel2;
        boolean success = false;
        try {
            this.hash1 = new BytesRefBlockHash(channel1, blockFactory);
            this.hash2 = new BytesRefBlockHash(channel2, blockFactory);
            this.finalHash = new LongHash(1, blockFactory.bigArrays());
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(hash1, hash2, finalHash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock b1 = page.getBlock(channel1);
        BytesRefBlock b2 = page.getBlock(channel2);
        BytesRefVector v1 = b1.asVector();
        BytesRefVector v2 = b2.asVector();
        if (v1 != null && v2 != null) {
            addVectors(v1, v2, addInput);
        } else {
            try (IntBlock k1 = hash1.add(b1); IntBlock k2 = hash2.add(b2)) {
                try (AddWork work = new AddWork(k1, k2, addInput)) {
                    work.add();
                }
            }
        }
    }

    private void addVectors(BytesRefVector v1, BytesRefVector v2, GroupingAggregatorFunction.AddInput addInput) {
        final int positionCount = v1.getPositionCount();
        try (IntVector.FixedBuilder ordsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            try (IntVector k1 = hash1.add(v1); IntVector k2 = hash2.add(v2)) {
                for (int p = 0; p < positionCount; p++) {
                    long ord = ord(k1.getInt(p), k2.getInt(p));
                    ordsBuilder.appendInt(p, Math.toIntExact(ord));
                }
            }
            try (IntVector ords = ordsBuilder.build()) {
                addInput.add(0, ords);
            }
        }
    }

    private class AddWork extends AddPage {
        final IntBlock b1;
        final IntBlock b2;

        AddWork(IntBlock b1, IntBlock b2, GroupingAggregatorFunction.AddInput addInput) {
            super(blockFactory, emitBatchSize, addInput);
            this.b1 = b1;
            this.b2 = b2;
        }

        void add() {
            int positionCount = b1.getPositionCount();
            for (int i = 0; i < positionCount; i++) {
                int v1 = b1.getValueCount(i);
                int v2 = b2.getValueCount(i);
                int first1 = b1.getFirstValueIndex(i);
                int first2 = b2.getFirstValueIndex(i);
                if (v1 == 1 && v2 == 1) {
                    long ord = ord(b1.getInt(first1), b2.getInt(first2));
                    appendOrdSv(i, Math.toIntExact(ord));
                    continue;
                }
                for (int i1 = 0; i1 < v1; i1++) {
                    int k1 = b1.getInt(first1 + i1);
                    for (int i2 = 0; i2 < v2; i2++) {
                        int k2 = b2.getInt(first2 + i2);
                        long ord = ord(k1, k2);
                        appendOrdInMv(i, Math.toIntExact(ord));
                    }
                }
                finishMv();
            }
            flushRemaining();
        }
    }

    private long ord(int k1, int k2) {
        return hashOrdToGroup(finalHash.add((long) k2 << 32 | k1));
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        // TODO Build Ordinals blocks #114010
        final int positions = (int) finalHash.size();
        final BytesRef scratch = new BytesRef();
        final BytesRefBlock[] outputBlocks = new BytesRefBlock[2];
        try {
            try (BytesRefBlock.Builder b1 = blockFactory.newBytesRefBlockBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    int k1 = (int) (finalHash.get(i) & 0xffffL);
                    if (k1 == 0) {
                        b1.appendNull();
                    } else {
                        b1.appendBytesRef(hash1.hash.get(k1 - 1, scratch));
                    }
                }
                outputBlocks[0] = b1.build();
            }
            try (BytesRefBlock.Builder b2 = blockFactory.newBytesRefBlockBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    int k2 = (int) (finalHash.get(i) >>> 32);
                    if (k2 == 0) {
                        b2.appendNull();
                    } else {
                        b2.appendBytesRef(hash2.hash.get(k2 - 1, scratch));
                    }
                }
                outputBlocks[1] = b2.build();
            }
            return outputBlocks;
        } finally {
            if (outputBlocks[outputBlocks.length - 1] == null) {
                Releasables.close(outputBlocks);
            }
        }
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new Range(0, Math.toIntExact(finalHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(finalHash.size()), blockFactory);
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "BytesRef2BlockHash{keys=[channel1=%d, channel2=%d], entries=%d}",
            channel1,
            channel2,
            finalHash.size()
        );
    }
}
