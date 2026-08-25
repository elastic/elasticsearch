/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeLong;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * Maps a {@link LongBlock} and an {@link IntBlock} to group ids, handling nulls and multivalued fields.
 */
public final class LongIntBlockHash extends BlockHash {
    private final int longChannel;
    private final int intChannel;
    private final int emitBatchSize;
    private final boolean reverseOutput;
    private final LongLongHashTable hash;

    private final long batchUsedBytes;
    private final long[] batchKeys1;
    private final long[] batchKeys2;
    private final int[] batchIds;
    // defaults to false, switch to true if we ever see input blocks
    private boolean seenBlocks = false;

    public LongIntBlockHash(List<GroupSpec> specs, BlockFactory blockFactory, int emitBatchSize, boolean reverseOutput) {
        super(blockFactory);
        this.longChannel = reverseOutput ? specs.get(1).channel() : specs.get(0).channel();
        this.intChannel = reverseOutput ? specs.get(0).channel() : specs.get(1).channel();
        // The emit size should be at least 256 so that prefetch taking effect.
        var emitVectorSize = Math.max(emitBatchSize, 256);
        this.reverseOutput = reverseOutput;
        final long bytes = (Integer.BYTES + Long.BYTES * 2) * (long) emitVectorSize;
        blockFactory.adjustBreaker(bytes);
        this.batchUsedBytes = bytes;
        boolean success = false;
        batchKeys1 = new long[emitVectorSize];
        batchKeys2 = new long[emitVectorSize];
        batchIds = new int[emitVectorSize];
        this.emitBatchSize = emitVectorSize;
        try {
            this.hash = HashImplFactory.newLongLongHash(blockFactory);
            success = true;
        } finally {
            if (success == false) {
                blockFactory.adjustBreaker(-bytes);
            }
        }
    }

    @Override
    public BlockHash resetOrCreate() {
        seenBlocks = false;
        hash.clear();
        return this;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        LongBlock longBlock = page.getBlock(longChannel);
        LongVector longVector = longBlock.asVector();
        IntBlock intBlock = page.getBlock(intChannel);
        IntVector intVector = intBlock.asVector();
        if (longVector != null && intVector != null) {
            addVector(longVector, intVector, addInput);
        } else {
            seenBlocks = true;
            try (
                LongBlock dedupedLongs = new MultivalueDedupeLong(longBlock).dedupeToBlockAdaptive(blockFactory);
                IntBlock dedupedInts = new MultivalueDedupeInt(intBlock).dedupeToBlockAdaptive(blockFactory);
                AddBlockWork work = new AddBlockWork(dedupedLongs, dedupedInts, addInput, emitBatchSize)
            ) {
                work.add();
            }
        }
    }

    private void addVector(LongVector longVector, IntVector intVector, GroupingAggregatorFunction.AddInput addInput) {
        if (hash.supportBulkAdd()) {
            addBatch(longVector, intVector, addInput);
        } else {
            addVectorOneAtTime(longVector, intVector, addInput);
        }
    }

    private void addBatch(LongVector longVector, IntVector intVector, GroupingAggregatorFunction.AddInput addInput) {
        final int position = longVector.getPositionCount();
        int offset = 0;
        while (offset < position) {
            final int batchSize = Math.min(batchIds.length, position - offset);
            longVector.copyTo(offset, batchKeys1, 0, batchSize);
            for (int i = 0; i < batchSize; i++) {
                batchKeys2[i] = intVector.getInt(offset + i) & WIDEN;
            }
            hash.bulkAdd(batchKeys1, batchKeys2, batchIds, batchSize);
            try (var groupIds = blockFactory.newIntArrayVector(batchIds, batchSize)) {
                addInput.add(offset, groupIds);
            }
            offset += batchSize;
        }
    }

    private void addVectorOneAtTime(LongVector longVector, IntVector intVector, GroupingAggregatorFunction.AddInput addInput) {
        int position = longVector.getPositionCount();
        int offset = 0;

        while (offset < position) {
            final int batchSize = Math.min(batchIds.length, position - offset);
            try (var groupIdsBuilder = blockFactory.newIntVectorFixedBuilder(batchSize)) {
                for (int i = 0; i < batchSize; i++) {
                    long longKey = longVector.getLong(offset + i);
                    long intValue = intVector.getInt(offset + i) & WIDEN;
                    long ord = hashOrdToGroup(hash.add(longKey, intValue));
                    groupIdsBuilder.appendInt(i, Math.toIntExact(ord));
                }
                try (var groupIds = groupIdsBuilder.build()) {
                    addInput.add(offset, groupIds);
                }
            }
            offset += batchSize;
        }
    }

    /*
     * longValue, intValue  -> longValue, intValue & WIDEN
     * null, intValue       -> 0, intValue & WIDEN | LONG_NULL_MASK
     * longValue, null      -> longValue, INT_NULL_MASK
     * null, null           -> 0, LONG_NULL_MASK | INT_NULL_MASK
     */
    static final long LONG_NULL_MASK = 0x00F0_0000_0000_0000L;
    static final long INT_NULL_MASK = 0x000F_0000_0000_0000L;
    static final long WIDEN = 0xFFFFFFFFL;

    private class AddBlockWork extends AddPage {
        final LongBlock longBlock;
        final IntBlock intBlock;

        AddBlockWork(LongBlock longBlock, IntBlock intBlock, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
            super(blockFactory, batchSize, addInput);
            this.longBlock = longBlock;
            this.intBlock = intBlock;
        }

        void add() {
            final int positionCount = longBlock.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                final int longCount = longBlock.getValueCount(p);
                final int intCount = intBlock.getValueCount(p);
                if (longCount == 0) {
                    addEmptyLong(p, intCount);
                } else if (intCount == 0) {
                    addEmptyInt(p, longCount);
                } else {
                    addLongInt(p, longCount, intCount);
                }
            }
            flushRemaining();
        }

        void addEmptyLong(int p, int intCount) {
            switch (intCount) {
                case 0 -> appendOrdSv(p, Math.toIntExact(hashOrdToGroup(hash.add(0, LONG_NULL_MASK | INT_NULL_MASK))));
                case 1 -> {
                    final long intValue = intBlock.getInt(intBlock.getFirstValueIndex(p)) & WIDEN;
                    appendOrdSv(p, Math.toIntExact(hashOrdToGroup(hash.add(0, intValue | LONG_NULL_MASK))));
                }
                default -> {
                    int start = intBlock.getFirstValueIndex(p);
                    int end = start + intCount;
                    for (int v = start; v < end; v++) {
                        final long intValue = intBlock.getInt(v) & WIDEN;
                        appendOrdInMv(p, Math.toIntExact(hashOrdToGroup(hash.add(0, intValue | LONG_NULL_MASK))));
                    }
                    finishMv();
                }
            }
        }

        void addEmptyInt(int p, int longCount) {
            switch (longCount) {
                case 0 -> appendOrdSv(p, Math.toIntExact(hashOrdToGroup(hash.add(0, LONG_NULL_MASK | INT_NULL_MASK))));
                case 1 -> {
                    final long longValue = longBlock.getLong(longBlock.getFirstValueIndex(p));
                    appendOrdSv(p, Math.toIntExact(hashOrdToGroup(hash.add(longValue, INT_NULL_MASK))));
                }
                default -> {
                    int start = longBlock.getFirstValueIndex(p);
                    int end = start + longCount;
                    for (int v = start; v < end; v++) {
                        final long longValue = longBlock.getLong(v);
                        appendOrdInMv(p, Math.toIntExact(hashOrdToGroup(hash.add(longValue, INT_NULL_MASK))));
                    }
                    finishMv();
                }
            }
        }

        void addLongInt(int p, int longCount, int intCount) {
            final int longStart = longBlock.getFirstValueIndex(p);
            final int intStart = intBlock.getFirstValueIndex(p);
            if (longCount == 1 && intCount == 1) {
                final long longValue = longBlock.getLong(longStart);
                final long intValue = intBlock.getInt(intStart) & WIDEN;
                appendOrdSv(p, Math.toIntExact(hashOrdToGroup(hash.add(longValue, intValue))));
                return;
            }
            final int longEnd = longStart + longCount;
            final int intEnd = intStart + intCount;
            for (int l = longStart; l < longEnd; l++) {
                final long longValue = longBlock.getLong(l);
                for (int i = intStart; i < intEnd; i++) {
                    final long intValue = intBlock.getInt(i) & WIDEN;
                    appendOrdInMv(p, Math.toIntExact(hashOrdToGroup(hash.add(longValue, intValue))));
                }
            }
            finishMv();
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        LongBlock longBlock = page.getBlock(longChannel);
        LongVector longVector = longBlock.asVector();
        IntBlock intBlock = page.getBlock(intChannel);
        IntVector intVector = intBlock.asVector();
        if (longVector != null && intVector != null) {
            return lookupVector(longVector, intVector, targetBlockSize);
        }
        return new LookupWork(longBlock, intBlock, targetBlockSize.getBytes());
    }

    private ReleasableIterator<IntBlock> lookupVector(LongVector longVector, IntVector intVector, ByteSizeValue targetBlockSize) {
        longVector.mustIncRef();
        intVector.mustIncRef();
        final long emitSize = Math.max(1L, targetBlockSize.getBytes() / Integer.BYTES);
        return new ReleasableIterator<>() {
            private int offset = 0;
            final int positionCount = longVector.getPositionCount();

            @Override
            public boolean hasNext() {
                return offset < positionCount;
            }

            @Override
            public IntBlock next() {
                int batchSize = (int) Math.min(emitSize, positionCount - offset);
                try (var groupIdsBuilder = blockFactory.newIntBlockBuilder(batchSize)) {
                    for (int i = 0; i < batchSize; i++) {
                        long longKey = longVector.getLong(offset + i);
                        long intKey = intVector.getInt(offset + i) & WIDEN;
                        long ord = hash.find(longKey, intKey);
                        if (ord < 0) {
                            groupIdsBuilder.appendNull();
                        } else {
                            groupIdsBuilder.appendInt(Math.toIntExact(ord));
                        }
                    }
                    offset += batchSize;
                    return groupIdsBuilder.build();
                }
            }

            @Override
            public void close() {
                Releasables.close(longVector::decRef, intVector::decRef);
            }
        };
    }

    class LookupWork implements ReleasableIterator<IntBlock> {
        private final LongBlock longBlock;
        private final IntBlock intBlock;
        private final long targetByteSize;
        private final int positionCount;
        private int position;

        LookupWork(LongBlock longBlock, IntBlock intBlock, long targetByteSize) {
            var dedupedLongs = new MultivalueDedupeLong(longBlock).dedupeToBlockAdaptive(blockFactory);
            try {
                this.longBlock = dedupedLongs;
                this.intBlock = new MultivalueDedupeInt(intBlock).dedupeToBlockAdaptive(blockFactory);
                dedupedLongs = null;
            } finally {
                Releasables.close(dedupedLongs);
            }
            this.positionCount = longBlock.getPositionCount();
            this.targetByteSize = targetByteSize;
        }

        @Override
        public boolean hasNext() {
            return position < positionCount;
        }

        @Override
        public IntBlock next() {
            int size = Math.toIntExact(Math.min(positionCount - position, targetByteSize / Integer.BYTES / 2));
            try (IntBlock.Builder ords = blockFactory.newIntBlockBuilder(size)) {
                if (ords.estimatedBytes() > targetByteSize) {
                    throw new IllegalStateException(
                        "initial builder overshot target [" + ords.estimatedBytes() + "] vs [" + targetByteSize + "]"
                    );
                }
                while (position < positionCount && ords.estimatedBytes() < targetByteSize) {
                    final int longCount = longBlock.getValueCount(position);
                    final int intCount = intBlock.getValueCount(position);
                    if (longCount == 0) {
                        lookupEmptyLong(ords, intCount);
                    } else if (intCount == 0) {
                        lookupEmptyInt(ords, longCount);
                    } else {
                        lookupLongInt(ords, longCount, intCount);
                    }
                    position++;
                }
                return ords.build();
            }
        }

        private void lookupEmptyLong(IntBlock.Builder ords, int intCount) {
            if (intCount == 0) {
                appendFound(ords, hash.find(0, LONG_NULL_MASK | INT_NULL_MASK));
            } else if (intCount == 1) {
                long intValue = intBlock.getInt(intBlock.getFirstValueIndex(position)) & WIDEN;
                appendFound(ords, hash.find(0, intValue | LONG_NULL_MASK));
            } else {
                int start = intBlock.getFirstValueIndex(position);
                int end = start + intCount;
                long firstFound = -1;
                boolean began = false;
                int count = 0;
                for (int v = start; v < end; v++) {
                    long intValue = intBlock.getInt(v) & WIDEN;
                    long found = hash.find(0, intValue | LONG_NULL_MASK);
                    if (found >= 0) {
                        if (firstFound < 0) {
                            firstFound = found;
                        } else {
                            if (began == false) {
                                began = true;
                                ords.beginPositionEntry();
                                ords.appendInt(Math.toIntExact(firstFound));
                                count++;
                            }
                            ords.appendInt(Math.toIntExact(found));
                            count++;
                            if (count > Block.MAX_LOOKUP) {
                                throw new IllegalArgumentException("Found a single entry with " + count + " entries");
                            }
                        }
                    }
                }
                finishMvLookup(ords, firstFound, began);
            }
        }

        private void lookupEmptyInt(IntBlock.Builder ords, int longCount) {
            if (longCount == 1) {
                long longValue = longBlock.getLong(longBlock.getFirstValueIndex(position));
                appendFound(ords, hash.find(longValue, INT_NULL_MASK));
            } else {
                int start = longBlock.getFirstValueIndex(position);
                int end = start + longCount;
                long firstFound = -1;
                boolean began = false;
                int count = 0;
                for (int v = start; v < end; v++) {
                    long longValue = longBlock.getLong(v);
                    long found = hash.find(longValue, INT_NULL_MASK);
                    if (found >= 0) {
                        if (firstFound < 0) {
                            firstFound = found;
                        } else {
                            if (began == false) {
                                began = true;
                                ords.beginPositionEntry();
                                ords.appendInt(Math.toIntExact(firstFound));
                                count++;
                            }
                            ords.appendInt(Math.toIntExact(found));
                            count++;
                            if (count > Block.MAX_LOOKUP) {
                                throw new IllegalArgumentException("Found a single entry with " + count + " entries");
                            }
                        }
                    }
                }
                finishMvLookup(ords, firstFound, began);
            }
        }

        private void lookupLongInt(IntBlock.Builder ords, int longCount, int intCount) {
            int longStart = longBlock.getFirstValueIndex(position);
            int intStart = intBlock.getFirstValueIndex(position);
            if (longCount == 1 && intCount == 1) {
                long longValue = longBlock.getLong(longStart);
                long intValue = intBlock.getInt(intStart) & WIDEN;
                appendFound(ords, hash.find(longValue, intValue));
                return;
            }
            int longEnd = longStart + longCount;
            int intEnd = intStart + intCount;
            long firstFound = -1;
            boolean began = false;
            int count = 0;
            for (int l = longStart; l < longEnd; l++) {
                long longValue = longBlock.getLong(l);
                for (int i = intStart; i < intEnd; i++) {
                    long intValue = intBlock.getInt(i) & WIDEN;
                    long found = hash.find(longValue, intValue);
                    if (found >= 0) {
                        if (firstFound < 0) {
                            firstFound = found;
                        } else {
                            if (began == false) {
                                began = true;
                                ords.beginPositionEntry();
                                ords.appendInt(Math.toIntExact(firstFound));
                                count++;
                            }
                            ords.appendInt(Math.toIntExact(found));
                            count++;
                            if (count > Block.MAX_LOOKUP) {
                                throw new IllegalArgumentException("Found a single entry with " + count + " entries");
                            }
                        }
                    }
                }
            }
            finishMvLookup(ords, firstFound, began);
        }

        private void appendFound(IntBlock.Builder ords, long found) {
            if (found < 0) {
                ords.appendNull();
            } else {
                ords.appendInt(Math.toIntExact(found));
            }
        }

        private void finishMvLookup(IntBlock.Builder ords, long firstFound, boolean began) {
            if (firstFound < 0) {
                ords.appendNull();
            } else if (began) {
                ords.endPositionEntry();
            } else {
                ords.appendInt(Math.toIntExact(firstFound));
            }
        }

        @Override
        public void close() {
            Releasables.close(longBlock::decRef, intBlock::decRef);
        }
    }

    void getKeysSlow(IntVector selected, Block[] blocks) {
        int positions = selected.getPositionCount();
        try (var longsBuilder = blockFactory.newLongBlockBuilder(positions); var intsBuilder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                int groupId = selected.getInt(i);
                long longValue = hash.getKey1(groupId);
                long intValue = hash.getKey2(groupId);
                if ((intValue & LONG_NULL_MASK) == 0) {
                    longsBuilder.appendLong(longValue);
                } else {
                    longsBuilder.appendNull();
                }
                if ((intValue & INT_NULL_MASK) == 0) {
                    intsBuilder.appendInt((int) intValue);
                } else {
                    intsBuilder.appendNull();
                }
            }
            blocks[0] = longsBuilder.build();
            blocks[1] = intsBuilder.build();
        }
    }

    void getKeysFast(IntVector selected, Block[] blocks) {
        int positions = selected.getPositionCount();
        try (
            var longsBuilder = blockFactory.newLongVectorFixedBuilder(positions);
            var intsBuilder = blockFactory.newIntVectorFixedBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                int groupId = selected.getInt(i);
                long longValue = hash.getKey1(groupId);
                long intValue = hash.getKey2(groupId);
                longsBuilder.appendLong(longValue);
                intsBuilder.appendInt((int) intValue);
            }
            blocks[0] = longsBuilder.build().asBlock();
            blocks[1] = intsBuilder.build().asBlock();
        }
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        Block[] blocks = new Block[2];
        boolean success = false;
        try {
            if (seenBlocks) {
                getKeysSlow(selected, blocks);
            } else {
                getKeysFast(selected, blocks);
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
        if (reverseOutput) {
            return new Block[] { blocks[1], blocks[0] };
        } else {
            return blocks;
        }
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(hash.size());
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, numKeys());
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, numKeys()).seenGroupIds(bigArrays);
    }

    // for testing
    int effectiveEmitBatchSize() {
        return emitBatchSize;
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-batchUsedBytes);
        Releasables.close(hash);
    }

    @Override
    public String toString() {
        return "LongIntBlockHash{keys=[LongKey[channel="
            + longChannel
            + "], IntKey[channel="
            + intChannel
            + "]], entries="
            + hash.size()
            + ", size="
            + hash.ramBytesUsed()
            + "b}";
    }
}
