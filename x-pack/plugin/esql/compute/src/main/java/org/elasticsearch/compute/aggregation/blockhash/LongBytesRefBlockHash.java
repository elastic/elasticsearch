/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.BytesLongPartitionedHash;
import org.elasticsearch.swisshash.BytesRefSwissHash;
import org.elasticsearch.swisshash.LongLongSwissHash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A specialized {@link BlockHash} for the two-key {@code (LONG, BYTES_REF)} (or {@code (BYTES_REF, LONG)})
 */
public final class LongBytesRefBlockHash extends PartitionedBlockHash {
    private final int longChannel;
    private final int bytesChannel;
    private final BytesRefHashTable bytesHash;
    private final LongIntBlockHash longIntHash;
    private AddBytesBatchWork addBytesBatchWork = null;
    private final boolean reverseOutput;

    public LongBytesRefBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize, boolean reverseOutput) {
        super(blockFactory);
        this.longChannel = reverseOutput ? specs.get(1).channel() : specs.get(0).channel();
        this.bytesChannel = reverseOutput ? specs.get(0).channel() : specs.get(1).channel();
        this.reverseOutput = reverseOutput;
        this.bytesHash = HashImplFactory.newBytesRefHash(blockFactory);
        boolean success = false;
        try {
            this.longIntHash = new LongIntBlockHash(specs, blockFactory, emitBatchSize, false);
            success = true;
        } finally {
            if (success == false) {
                bytesHash.close();
            }
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        final BytesRefBlock bytesBlock = page.getBlock(bytesChannel);
        final BytesRefVector bytesVector = bytesBlock.asVector();
        final LongBlock longBlock = page.getBlock(longChannel);
        final LongVector longVector = longBlock.asVector();
        if (bytesVector != null && longVector != null) {
            try (var ords = addBytesVector(bytesVector)) {
                longIntHash.addVector(longVector, ords, addInput);
            }
        } else {
            try (var ords = addBytesBlock(bytesBlock)) {
                longIntHash.addBlock(longBlock, ords, addInput);
            }
        }
    }

    private IntVector addBytesVector(BytesRefVector bytesVector) {
        if (bytesHash instanceof BytesRefSwissHash swiss && swiss.shouldPrefetch()) {
            if (addBytesBatchWork == null) {
                addBytesBatchWork = new AddBytesBatchWork(blockFactory);
            }
            return addBytesBatchWork.addBytesVectorWithPrefetch(bytesVector, swiss);
        }
        BytesRef scratch = new BytesRef();
        int positions = bytesVector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                BytesRef v = bytesVector.getBytesRef(i, scratch);
                builder.appendInt(Math.toIntExact(hashOrdToGroup(bytesHash.add(v))));
            }
            return builder.build();
        }
    }

    private IntBlock addBytesBlock(BytesRefBlock bytesBlock) {
        int positionCount = bytesBlock.getPositionCount();
        BytesRef scratch = new BytesRef();
        try (var builder = blockFactory.newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int start = bytesBlock.getFirstValueIndex(p);
                int valueCount = bytesBlock.getValueCount(p);
                int end = start + valueCount;
                switch (valueCount) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        var b = bytesBlock.getBytesRef(start, scratch);
                        builder.appendInt(Math.toIntExact(hashOrdToGroup(bytesHash.add(b))));
                    }
                    default -> {
                        builder.beginPositionEntry();
                        for (int v = start; v < end; v++) {
                            var b = bytesBlock.getBytesRef(v, scratch);
                            builder.appendInt(Math.toIntExact(hashOrdToGroup(bytesHash.add(b))));
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }

    @Override
    public void addAfterLimitReached(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock bytesBlock = page.getBlock(bytesChannel);
        BytesRefVector bytesVector = bytesBlock.asVector();
        LongBlock longBlock = page.getBlock(longChannel);
        LongVector longVector = longBlock.asVector();
        if (bytesVector == null || longVector == null) {
            add(page, addInput);
            return;
        }
        try (var intVector = lookupBytesVector(bytesVector)) {
            int position = longVector.getPositionCount();
            int offset = 0;
            LongLongHashTable hash = longIntHash.hash;
            while (offset < position) {
                int[] batchIds = longIntHash.batchIds;
                final int batchSize = Math.min(batchIds.length, position - offset);
                try (var groupIdsBuilder = blockFactory.newIntBlockBuilder(batchSize)) {
                    for (int i = 0; i < batchSize; i++) {
                        int bytesOrd = intVector.getInt(offset + i);
                        if (bytesOrd < 0) {
                            groupIdsBuilder.appendNull();
                            continue;
                        }
                        long intValue = bytesOrd & LongIntBlockHash.WIDEN;
                        long longKey = longVector.getLong(offset + i);
                        long ord = hash.find(longKey, intValue);
                        if (ord < 0) {
                            groupIdsBuilder.appendNull();
                        } else {
                            groupIdsBuilder.appendInt(Math.toIntExact(ord));
                        }
                    }
                    try (var groupIds = groupIdsBuilder.build()) {
                        addInput.add(offset, groupIds);
                    }
                }
                offset += batchSize;
            }
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        BytesRefBlock bytesBlock = page.getBlock(bytesChannel);
        BytesRefVector bytesVector = bytesBlock.asVector();
        if (bytesVector != null) {
            try (IntVector intVector = lookupBytesVector(bytesVector)) {
                return longIntHash.lookup(page.getBlock(longChannel), intVector.asBlock(), targetBlockSize);
            }
        } else {
            try (IntBlock intBlock = lookupBytesBlock(bytesBlock)) {
                return longIntHash.lookup(page.getBlock(longChannel), intBlock, targetBlockSize);
            }
        }
    }

    IntBlock lookupBytesBlock(BytesRefBlock bytes) {
        int positionCount = bytes.getPositionCount();
        BytesRef scratch = new BytesRef();
        try (var builder = blockFactory.newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int start = bytes.getFirstValueIndex(p);
                int valueCount = bytes.getValueCount(p);
                int end = start + valueCount;
                switch (valueCount) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        var b = bytes.getBytesRef(start, scratch);
                        builder.appendInt(Math.toIntExact(bytesHash.find(b)));
                    }
                    default -> {
                        builder.beginPositionEntry();
                        for (int v = start; v < end; v++) {
                            var b = bytes.getBytesRef(v, scratch);
                            builder.appendInt(Math.toIntExact(bytesHash.find(b)));
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }

    IntVector lookupBytesVector(BytesRefVector bytes) {
        int positionCount = bytes.getPositionCount();
        BytesRef scratch = new BytesRef();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                var v = bytes.getBytesRef(p, scratch);
                builder.appendInt(Math.toIntExact(bytesHash.find(v)));
            }
            return builder.build();
        }
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        Block[] keys = longIntHash.getKeys(selected);
        LongBlock longBlock = (LongBlock) keys[0];
        IntBlock intBlock = (IntBlock) keys[1];
        boolean success = false;
        try {
            IntVector intVector = intBlock.asVector();
            final BytesRefBlock bytesRefBlock;
            if (intVector != null) {
                if (OrdinalBytesRefBlock.isDense(selected.getPositionCount(), bytesHash.size())) {
                    bytesRefBlock = readBytesOrdinals(intVector);
                } else {
                    bytesRefBlock = readBytesVector(intVector);
                }
            } else {
                bytesRefBlock = readBytesBlock(intBlock);
            }
            intBlock.close();
            success = true;
            if (reverseOutput) {
                return new Block[] { bytesRefBlock, longBlock };
            } else {
                return new Block[] { longBlock, bytesRefBlock };
            }
        } finally {
            if (success == false) {
                Releasables.close(keys);
            }
        }
    }

    BytesRefBlock readBytesBlock(IntBlock ords) {
        BytesRef scratch = new BytesRef();
        try (var builder = blockFactory.newBytesRefBlockBuilder(ords.getPositionCount())) {
            for (int p = 0; p < ords.getPositionCount(); p++) {
                if (ords.isNull(p)) {
                    builder.appendNull();
                } else {
                    assert ords.getValueCount(p) == 1 : "expected single-valued ords; got " + ords;
                    final int ord = ords.getInt(ords.getFirstValueIndex(p));
                    builder.appendBytesRef(bytesHash.get(ord, scratch));
                }
            }
            return builder.build();
        }
    }

    BytesRefBlock readBytesVector(IntVector ords) {
        BytesRef scratch = new BytesRef();
        try (var builder = blockFactory.newBytesRefVectorBuilder(ords.getPositionCount())) {
            for (int p = 0; p < ords.getPositionCount(); p++) {
                builder.appendBytesRef(bytesHash.get(ords.getInt(p), scratch));
            }
            return builder.build().asBlock();
        }
    }

    BytesRefBlock readBytesOrdinals(IntVector oldOrds) {
        final long dictSize = bytesHash.size();
        blockFactory.adjustBreaker(dictSize * Integer.BYTES);
        IntVector newOrds = null;
        BytesRefVector dict = null;
        try {
            int nextOrd = 0;
            final int[] mappedOrds = new int[Math.toIntExact(dictSize)];
            Arrays.fill(mappedOrds, -1);
            try (var builder = blockFactory.newIntVectorFixedBuilder(oldOrds.getPositionCount())) {
                for (int i = 0; i < oldOrds.getPositionCount(); i++) {
                    int ord = oldOrds.getInt(i);
                    int newOrd = mappedOrds[ord];
                    if (newOrd == -1) {
                        newOrd = nextOrd++;
                        mappedOrds[ord] = newOrd;
                    }
                    builder.appendInt(i, newOrd);
                }
                newOrds = builder.build();
            }
            try (var builder = blockFactory.newBytesRefVectorBuilder(nextOrd)) {
                BytesRef scratch = new BytesRef();
                nextOrd = 0;
                for (int p = 0; p < oldOrds.getPositionCount(); p++) {
                    int ord = oldOrds.getInt(p);
                    if (mappedOrds[ord] == nextOrd) {
                        builder.appendBytesRef(bytesHash.get(ord, scratch));
                        nextOrd++;
                    }
                }
                dict = builder.build();
            }
            var result = new OrdinalBytesRefVector(newOrds, dict).asBlock();
            dict = null;
            newOrds = null;
            return result;
        } finally {
            blockFactory.adjustBreaker(-dictSize * Integer.BYTES);
            Releasables.close(newOrds, dict);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return longIntHash.nonEmpty();
    }

    @Override
    public int numKeys() {
        return longIntHash.numKeys();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return longIntHash.seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(bytesHash, longIntHash);
        if (addBytesBatchWork != null) {
            addBytesBatchWork.prefetchBarrier.flush();
        }
    }

    // for testing
    int effectiveEmitBatchSize() {
        return longIntHash.effectiveEmitBatchSize();
    }

    @Override
    public void ensureCapacity(int size) {
        // don't resize bytes
        longIntHash.ensureCapacity(size);
    }

    @Override
    public String toString() {
        return "LongBytesRefBlockHash{keys=[LongKey[channel="
            + longChannel
            + "], BytesRefKey[channel="
            + bytesChannel
            + "]], entries="
            + longIntHash.numKeys()
            + ", size="
            + (bytesHash.ramBytesUsed() + longIntHash.hash.ramBytesUsed())
            + "b}";
    }

    private static class AddBytesBatchWork {
        private static final int PREFETCH_BATCH = 64;
        private final PrefetchBarrier prefetchBarrier = new PrefetchBarrier();
        private final BlockFactory blockFactory;
        private final long[] batchHashes = new long[PREFETCH_BATCH];
        private final BytesRef[] batchKeys;

        AddBytesBatchWork(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            batchKeys = new BytesRef[PREFETCH_BATCH];
            for (int i = 0; i < PREFETCH_BATCH; i++) {
                batchKeys[i] = new BytesRef();
            }
        }

        private IntVector addBytesVectorWithPrefetch(BytesRefVector vector, BytesRefSwissHash swiss) {
            int positions = vector.getPositionCount();
            int dummy = 0;
            try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
                for (int offset = 0; offset < positions; offset += PREFETCH_BATCH) {
                    int batchSize = Math.min(PREFETCH_BATCH, positions - offset);
                    for (int i = 0; i < batchSize; i++) {
                        vector.getBytesRef(offset + i, batchKeys[i]);
                        batchHashes[i] = BytesRefSwissHash.hash64(batchKeys[i]);
                        dummy ^= swiss.prefetch(batchHashes[i]);
                    }
                    for (int i = 0; i < batchSize; i++) {
                        final long id = swiss.addWithHash(batchKeys[i], batchHashes[i]);
                        builder.appendInt(Math.toIntExact(hashOrdToGroup(id)));
                    }
                }
                for (int i = 0; i < PREFETCH_BATCH; i++) {
                    batchKeys[i].bytes = BytesRef.EMPTY_BYTES;
                    batchKeys[i].offset = 0;
                    batchKeys[i].length = 0;
                }
                prefetchBarrier.consume(dummy);
                return builder.build();
            }
        }
    }

    @Override
    public void clear() {
        bytesHash.clear();
        longIntHash.clear();
    }

    private BytesLongPartitionedHash partitioner() {
        if (longIntHash.hash instanceof LongLongSwissHash == false || bytesHash instanceof BytesRefSwissHash == false) {
            throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
        }
        return new BytesLongPartitionedHash((BytesRefSwissHash) bytesHash, (LongLongSwissHash) longIntHash.hash);
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        return new LongIntBlockHash.PartitionedHashKeysWithSeenBlocks(
            partitioner().splitPartition(breaker, partitionSplitter),
            longIntHash.seenBlocks
        );
    }

    @Override
    public boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        LongIntBlockHash.PartitionedHashKeysWithSeenBlocks withSeen = (LongIntBlockHash.PartitionedHashKeysWithSeenBlocks) keys;
        longIntHash.seenBlocks |= withSeen.seenBlocks();
        return partitioner().combinePartition(withSeen.delegate(), partitionIndex, resultIds);
    }

    @Override
    public boolean[] combinePartitions(List<? extends PartitionedHashKeys> keys, int partitionIndex, int[][] resultIds) {
        List<PartitionedHashKeys> delegates = new ArrayList<>(keys.size());
        for (PartitionedHashKeys k : keys) {
            LongIntBlockHash.PartitionedHashKeysWithSeenBlocks withSeen = (LongIntBlockHash.PartitionedHashKeysWithSeenBlocks) k;
            longIntHash.seenBlocks |= withSeen.seenBlocks();
            delegates.add(withSeen.delegate());
        }
        return partitioner().combinePartitions(delegates, partitionIndex, resultIds);
    }
}
