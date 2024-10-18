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
import org.elasticsearch.common.util.Int3Hash;
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
 * Maps three {@link BytesRefBlock}s to group ids.
 */
final class BytesRef3BlockHash extends BlockHash {
    private final int emitBatchSize;
    private final int channel1;
    private final int channel2;
    private final int channel3;
    private final BytesRefBlockHash hash1;
    private final BytesRefBlockHash hash2;
    private final BytesRefBlockHash hash3;
    private final Int3Hash finalHash;

    BytesRef3BlockHash(BlockFactory blockFactory, int channel1, int channel2, int channel3, int emitBatchSize) {
        super(blockFactory);
        this.emitBatchSize = emitBatchSize;
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.channel3 = channel3;
        boolean success = false;
        try {
            this.hash1 = new BytesRefBlockHash(channel1, blockFactory);
            this.hash2 = new BytesRefBlockHash(channel2, blockFactory);
            this.hash3 = new BytesRefBlockHash(channel3, blockFactory);
            this.finalHash = new Int3Hash(1, blockFactory.bigArrays());
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(hash1, hash2, hash3, finalHash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock b1 = page.getBlock(channel1);
        BytesRefBlock b2 = page.getBlock(channel2);
        BytesRefBlock b3 = page.getBlock(channel3);
        BytesRefVector v1 = b1.asVector();
        BytesRefVector v2 = b2.asVector();
        BytesRefVector v3 = b3.asVector();
        if (v1 != null && v2 != null && v3 != null) {
            addVectors(v1, v2, v3, addInput);
        } else {
            try (IntBlock k1 = hash1.add(b1); IntBlock k2 = hash2.add(b2); IntBlock k3 = hash3.add(b3)) {
                try (AddWork work = new AddWork(k1, k2, k3, addInput)) {
                    work.add();
                }
            }
        }
    }

    private void addVectors(BytesRefVector v1, BytesRefVector v2, BytesRefVector v3, GroupingAggregatorFunction.AddInput addInput) {
        final int positionCount = v1.getPositionCount();
        try (IntVector.FixedBuilder ordsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            try (IntVector k1 = hash1.add(v1); IntVector k2 = hash2.add(v2); IntVector k3 = hash3.add(v3)) {
                for (int p = 0; p < positionCount; p++) {
                    long ord = hashOrdToGroup(finalHash.add(k1.getInt(p), k2.getInt(p), k3.getInt(p)));
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
        final IntBlock b3;

        AddWork(IntBlock b1, IntBlock b2, IntBlock b3, GroupingAggregatorFunction.AddInput addInput) {
            super(blockFactory, emitBatchSize, addInput);
            this.b1 = b1;
            this.b2 = b2;
            this.b3 = b3;
        }

        void add() {
            int positionCount = b1.getPositionCount();
            for (int i = 0; i < positionCount; i++) {
                int v1 = b1.getValueCount(i);
                int v2 = b2.getValueCount(i);
                int v3 = b3.getValueCount(i);
                int first1 = b1.getFirstValueIndex(i);
                int first2 = b2.getFirstValueIndex(i);
                int first3 = b3.getFirstValueIndex(i);
                if (v1 == 1 && v2 == 1 && v3 == 1) {
                    long ord = hashOrdToGroup(finalHash.add(b1.getInt(first1), b2.getInt(first2), b3.getInt(first3)));
                    appendOrdSv(i, Math.toIntExact(ord));
                    continue;
                }
                for (int i1 = 0; i1 < v1; i1++) {
                    int k1 = b1.getInt(first1 + i1);
                    for (int i2 = 0; i2 < v2; i2++) {
                        int k2 = b2.getInt(first2 + i2);
                        for (int i3 = 0; i3 < v3; i3++) {
                            int k3 = b3.getInt(first3 + i3);
                            long ord = hashOrdToGroup(finalHash.add(k1, k2, k3));
                            appendOrdInMv(i, Math.toIntExact(ord));
                        }
                    }
                }
                finishMv();
            }
            flushRemaining();
        }
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
        final BytesRefBlock[] outputBlocks = new BytesRefBlock[3];
        try {
            try (BytesRefBlock.Builder b1 = blockFactory.newBytesRefBlockBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    int k1 = finalHash.getKey1(i);
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
                    int k2 = finalHash.getKey2(i);
                    if (k2 == 0) {
                        b2.appendNull();
                    } else {
                        b2.appendBytesRef(hash2.hash.get(k2 - 1, scratch));
                    }
                }
                outputBlocks[1] = b2.build();
            }
            try (BytesRefBlock.Builder b3 = blockFactory.newBytesRefBlockBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    int k3 = finalHash.getKey3(i);
                    if (k3 == 0) {
                        b3.appendNull();
                    } else {
                        b3.appendBytesRef(hash3.hash.get(k3 - 1, scratch));
                    }
                }
                outputBlocks[2] = b3.build();
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
            "BytesRef3BlockHash{keys=[channel1=%d, channel2=%d, channel3=%d], entries=%d}",
            channel1,
            channel2,
            channel3,
            finalHash.size()
        );
    }
}
