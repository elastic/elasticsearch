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
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.IntLongBlockAdd;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Maps a {@link LongBlock} column paired with a {@link BytesRefBlock} column to group ids.
 */
final class BytesRefLongBlockHash extends BlockHash {
    private final int bytesChannel;
    private final int longsChannel;
    private final boolean reverseOutput;
    private final int emitBatchSize;
    private final BytesRefBlockHash bytesHash;
    private final LongLongHash finalHash;

    BytesRefLongBlockHash(BlockFactory blockFactory, int bytesChannel, int longsChannel, boolean reverseOutput, int emitBatchSize) {
        super(blockFactory);
        this.bytesChannel = bytesChannel;
        this.longsChannel = longsChannel;
        this.reverseOutput = reverseOutput;
        this.emitBatchSize = emitBatchSize;

        boolean success = false;
        BytesRefBlockHash bytesHash = null;
        try {
            bytesHash = new BytesRefBlockHash(bytesChannel, blockFactory);
            this.bytesHash = bytesHash;
            this.finalHash = new LongLongHash(1, blockFactory.bigArrays());
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
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock bytesBlock = page.getBlock(bytesChannel);
        BytesRefVector bytesVector = bytesBlock.asVector();
        if (bytesVector != null) {
            try (IntVector bytesHashes = bytesHash.add(bytesVector)) {
                add(page, bytesHashes, addInput);
            }
        } else {
            try (IntBlock bytesHashes = bytesHash.add(bytesBlock)) {
                add(bytesHashes, page.getBlock(longsChannel), addInput);
            }
        }
    }

    public void add(Page page, IntVector bytesHashes, GroupingAggregatorFunction.AddInput addInput) {
        LongBlock longsBlock = page.getBlock(longsChannel);
        LongVector longsVector = longsBlock.asVector();
        if (longsVector != null) {
            try (IntVector ords = add(bytesHashes, longsVector)) {
                addInput.add(0, ords);
            }
        } else {
            add(bytesHashes.asBlock(), longsBlock, addInput);
        }
    }

    public void add(IntBlock bytesHashes, LongBlock longsBlock, GroupingAggregatorFunction.AddInput addInput) {
        try (IntLongBlockAdd work = new IntLongBlockAdd(blockFactory, emitBatchSize, addInput, finalHash, bytesHashes, longsBlock)) {
            work.add();
        }
    }

    public IntVector add(IntVector bytesHashes, LongVector longsVector) {
        int positions = bytesHashes.getPositionCount();
        final int[] ords = new int[positions];
        for (int i = 0; i < positions; i++) {
            ords[i] = Math.toIntExact(hashOrdToGroup(finalHash.add(bytesHashes.getInt(i), longsVector.getLong(i))));
        }
        return blockFactory.newIntArrayVector(ords, positions);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        int positions = (int) finalHash.size();
        BytesRefBlock k1 = null;
        LongVector k2 = null;
        try (
            BytesRefBlock.Builder keys1 = blockFactory.newBytesRefBlockBuilder(positions);
            LongVector.Builder keys2 = blockFactory.newLongVectorBuilder(positions)
        ) {
            BytesRef scratch = new BytesRef();
            for (long i = 0; i < positions; i++) {
                keys2.appendLong(finalHash.getKey2(i));
                long h1 = finalHash.getKey1(i);
                if (h1 == 0) {
                    keys1.appendNull();
                } else {
                    keys1.appendBytesRef(bytesHash.hash.get(h1 - 1, scratch));
                }
            }
            k1 = keys1.build();
            k2 = keys2.build();
        } finally {
            if (k2 == null) {
                Releasables.closeExpectNoException(k1);
            }
        }
        if (reverseOutput) {
            return new Block[] { k2.asBlock(), k1 };
        } else {
            return new Block[] { k1, k2.asBlock() };
        }
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(finalHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(finalHash.size()), blockFactory);
    }

    @Override
    public String toString() {
        return "BytesRefLongBlockHash{keys=[BytesRefKey[channel="
            + bytesChannel
            + "], LongKey[channel="
            + longsChannel
            + "]], entries="
            + finalHash.size()
            + ", size="
            + bytesHash.hash.ramBytesUsed()
            + "b}";
    }
}
