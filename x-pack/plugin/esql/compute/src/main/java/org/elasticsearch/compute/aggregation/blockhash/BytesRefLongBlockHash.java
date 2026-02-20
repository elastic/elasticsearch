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
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.LongLongHashTable;
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
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.IntLongBlockAdd;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Maps a {@link LongBlock} column paired with a {@link BytesRefBlock} column to group ids.
 */
public final class BytesRefLongBlockHash extends BlockHash {
    private final int bytesChannel;
    private final int longsChannel;
    private final boolean reverseOutput;
    private final int emitBatchSize;
    private final BytesRefBlockHash bytesHash;
    private final LongLongHashTable finalHash;
    private long minLongKey = Long.MAX_VALUE;
    private long maxLongKey = Long.MIN_VALUE;

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
            this.finalHash = HashImplFactory.newLongLongHash(blockFactory);
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
        int lastByte = bytesHashes.getInt(0);
        long lastLong = longsVector.getLong(0);
        ords[0] = Math.toIntExact(hashOrdToGroup(addGroup(lastByte, lastLong)));
        boolean constant = true;
        if (bytesHashes.isConstant()) {
            for (int i = 1; i < positions; i++) {
                final long nextLong = longsVector.getLong(i);
                if (nextLong == lastLong) {
                    ords[i] = ords[i - 1];
                } else {
                    ords[i] = Math.toIntExact(hashOrdToGroup(addGroup(lastByte, nextLong)));
                    lastLong = nextLong;
                    constant = false;
                }
            }
        } else {
            for (int i = 1; i < positions; i++) {
                final int nextByte = bytesHashes.getInt(i);
                final long nextLong = longsVector.getLong(i);
                if (nextByte == lastByte && nextLong == lastLong) {
                    ords[i] = ords[i - 1];
                } else {
                    ords[i] = Math.toIntExact(hashOrdToGroup(addGroup(nextByte, nextLong)));
                    lastByte = nextByte;
                    lastLong = nextLong;
                    constant = false;
                }
            }
        }
        if (constant) {
            return blockFactory.newConstantIntVector(ords[0], positions);
        } else {
            return blockFactory.newIntArrayVector(ords, positions);
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        BytesRefBlock k1 = null;
        LongVector k2 = null;
        int positions = (int) finalHash.size();
        if (OrdinalBytesRefBlock.isDense(positions, bytesHash.hash.size())) {
            try (var ordinals = blockFactory.newIntBlockBuilder(positions); var longs = blockFactory.newLongVectorBuilder(positions)) {
                for (long p = 0; p < positions; p++) {
                    long h1 = finalHash.getKey1(p);
                    if (h1 == 0) {
                        ordinals.appendNull();
                    } else {
                        ordinals.appendInt(Math.toIntExact(h1 - 1));
                    }
                    longs.appendLong(finalHash.getKey2(p));
                }
                BytesRefArray bytes = bytesHash.hash.getBytesRefs();
                bytes.incRef();
                BytesRefVector dict = null;

                try {
                    dict = blockFactory.newBytesRefArrayVector(bytes, Math.toIntExact(bytes.size()));
                    bytes = null; // transfer ownership to dict
                    k1 = new OrdinalBytesRefBlock(ordinals.build(), dict);
                    dict = null;  // transfer ownership to k1
                } finally {
                    Releasables.closeExpectNoException(bytes, dict);
                }
                k2 = longs.build();
            } finally {
                if (k2 == null) {
                    Releasables.closeExpectNoException(k1);
                }
            }
        } else {
            try (
                BytesRefBlock.Builder keys1 = blockFactory.newBytesRefBlockBuilder(positions);
                LongVector.Builder keys2 = blockFactory.newLongVectorBuilder(positions)
            ) {
                BytesRef scratch = new BytesRef();
                for (long i = 0; i < positions; i++) {
                    long h1 = finalHash.getKey1(i);
                    if (h1 == 0) {
                        keys1.appendNull();
                    } else {
                        keys1.appendBytesRef(bytesHash.hash.get(h1 - 1, scratch));
                    }
                    keys2.appendLong(finalHash.getKey2(i));
                }
                k1 = keys1.build();
                k2 = keys2.build();
            } finally {
                if (k2 == null) {
                    Releasables.closeExpectNoException(k1);
                }
            }
        }
        if (reverseOutput) {
            return new Block[] { k2.asBlock(), k1 };
        } else {
            return new Block[] { k1, k2.asBlock() };
        }
    }

    public long getBytesRefKeyFromGroup(long groupId) {
        return finalHash.getKey1(groupId);
    }

    public long getLongKeyFromGroup(long groupId) {
        return finalHash.getKey2(groupId);
    }

    public long getGroupId(long bytesKey, long longKey) {
        return finalHash.find(bytesKey, longKey);
    }

    public long addGroup(long bytesKey, long longKey) {
        minLongKey = Math.min(minLongKey, longKey);
        maxLongKey = Math.min(maxLongKey, longKey);
        return finalHash.add(bytesKey, longKey);
    }

    public long numGroups() {
        return finalHash.size();
    }

    public long getMinLongKey() {
        return minLongKey;
    }

    public long getMaxLongKey() {
        return maxLongKey;
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(finalHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, Math.toIntExact(finalHash.size()));
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(finalHash.size());
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
