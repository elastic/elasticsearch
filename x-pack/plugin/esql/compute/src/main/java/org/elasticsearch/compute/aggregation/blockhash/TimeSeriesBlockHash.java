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
import org.elasticsearch.common.util.BytesRefHashTable;
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
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * An optimized block hash that receives two blocks: tsid and timestamp, which are sorted.
 * Since the incoming data is sorted, this block hash checks tsid ordinals to avoid redundant
 * hash lookups for consecutive positions with the same tsid and timestamp.
 * Delegates to a {@link BytesRefLongBlockHash} for the actual hashing.
 */
public final class TimeSeriesBlockHash extends BlockHash {

    private final int tsidChannel;
    private final int timestampChannel;
    private final boolean reverseOutput;
    private final BytesRefHashTable tsidHash;
    private final LongLongHashTable finalHash;
    private final BytesRef scratch = new BytesRef();
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Long.MIN_VALUE;

    public TimeSeriesBlockHash(int tsidChannel, int timestampChannel, boolean reverseOutput, BlockFactory blockFactory) {
        super(blockFactory);
        this.tsidChannel = tsidChannel;
        this.timestampChannel = timestampChannel;
        this.reverseOutput = reverseOutput;
        boolean success = false;
        try {
            this.tsidHash = HashImplFactory.newBytesRefHash(blockFactory);
            this.finalHash = HashImplFactory.newLongLongHash(blockFactory);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(tsidHash, finalHash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        final BytesRefBlock tsidBlock = page.getBlock(tsidChannel);
        final BytesRefVector tsidVector = tsidBlock.asVector();
        if (tsidVector == null) {
            throw new IllegalStateException("Expected a vector for tsid");
        }
        final LongBlock timestampsBlock = page.getBlock(timestampChannel);
        final LongVector timestampVector = timestampsBlock.asVector();
        if (timestampVector == null) {
            throw new IllegalStateException("Expected a vector for timestamp");
        }
        if (tsidVector.isConstant()) {
            addConstant(tsidVector, timestampVector, addInput);
            return;
        }
        OrdinalBytesRefVector tsidOrdinals = tsidVector.asOrdinals();
        if (tsidOrdinals != null) {
            addOrdinals(tsidOrdinals, timestampVector, addInput);
            return;
        }
        addVector(tsidVector, timestampVector, addInput);
    }

    private void addConstant(BytesRefVector tsidVector, LongVector timestamps, GroupingAggregatorFunction.AddInput addInput) {
        final int tsid = Math.toIntExact(hashOrdToGroup(tsidHash.add(tsidVector.getBytesRef(0, scratch))));
        final int positionCount = timestamps.getPositionCount();
        long prevTimestamp = timestamps.getLong(0);
        trackTimestamp(prevTimestamp);
        int prevGroupId = (int) hashOrdToGroup(finalHash.add(tsid, prevTimestamp));
        if (timestamps.isConstant() || constantTimestamp(timestamps, prevTimestamp, positionCount)) {
            try (var groups = blockFactory.newConstantIntVector(prevGroupId, positionCount)) {
                addInput.add(0, groups);
            }
            return;
        }
        final IntVector groupsIds;
        try (var groupBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            groupBuilder.appendInt(0, prevGroupId);
            for (int p = 1; p < positionCount; p++) {
                long timestamp = timestamps.getLong(p);
                if (prevTimestamp != timestamp) {
                    trackTimestamp(timestamp);
                    prevGroupId = (int) hashOrdToGroup(finalHash.add(tsid, timestamp));
                    prevTimestamp = timestamp;
                }
                groupBuilder.appendInt(p, prevGroupId);
            }
            groupsIds = groupBuilder.build();
        }
        try (groupsIds) {
            addInput.add(0, groupsIds);
        }
    }

    private static boolean constantTimestamp(LongVector timestamps, long firstTimestamp, int positionCount) {
        final int lastPosition = positionCount - 1;
        if (timestamps.getLong(lastPosition) != firstTimestamp) {
            return false;
        }
        for (int i = 1; i < lastPosition; i++) {
            if (timestamps.getLong(i) != firstTimestamp) {
                return false;
            }
        }
        return true;
    }

    private void addOrdinals(OrdinalBytesRefVector tsidVector, LongVector timestamps, GroupingAggregatorFunction.AddInput addInput) {
        final int positionCount = tsidVector.getPositionCount();
        final IntVector groupIds;
        try (var tsidOrds = ordsForTsidDict(tsidVector)) {
            final long firstTimestamp = timestamps.getLong(0);
            if (timestamps.isConstant() || constantTimestamp(timestamps, firstTimestamp, positionCount)) {
                groupIds = groupIdsForOrdinalsWithConstantTimestamp(positionCount, tsidOrds, firstTimestamp);
            } else {
                groupIds = groupIdsForOrdinals(positionCount, tsidOrds, timestamps);
            }
        }
        try (groupIds) {
            addInput.add(0, groupIds);
        }
    }

    private IntVector groupIdsForOrdinals(int positionCount, IntVector tsidOrds, LongVector timestamps) {
        try (var groupIds = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            int prevTsid = tsidOrds.getInt(0);
            long prevTimestamp = timestamps.getLong(0);
            trackTimestamp(prevTimestamp);
            int prevGroupId = Math.toIntExact(hashOrdToGroup(finalHash.add(prevTsid, prevTimestamp)));
            groupIds.appendInt(0, prevGroupId);
            for (int p = 1; p < positionCount; p++) {
                final long timestamp = timestamps.getLong(p);
                trackTimestamp(timestamp);
                int tsid = tsidOrds.getInt(p);
                if (tsid != prevTsid || timestamp != prevTimestamp) {
                    prevTimestamp = timestamps.getLong(p);
                    prevGroupId = Math.toIntExact(hashOrdToGroup(finalHash.add(tsid, prevTimestamp)));
                    prevTsid = tsid;
                }
                groupIds.appendInt(p, prevGroupId);
            }
            return groupIds.build();
        }
    }

    private IntVector groupIdsForOrdinalsWithConstantTimestamp(int positionCount, IntVector tsidOrds, long timestamp) {
        trackTimestamp(timestamp);
        try (var groupIds = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            int prevTsid = tsidOrds.getInt(0);
            int prevGroupId = Math.toIntExact(hashOrdToGroup(finalHash.add(prevTsid, timestamp)));
            groupIds.appendInt(0, prevGroupId);

            for (int p = 1; p < positionCount; p++) {
                int tsid = tsidOrds.getInt(p);
                if (tsid != prevTsid) {
                    prevGroupId = Math.toIntExact(hashOrdToGroup(finalHash.add(tsid, timestamp)));
                    prevTsid = tsid;
                }
                groupIds.appendInt(p, prevGroupId);
            }
            return groupIds.build();
        }
    }

    private void addVector(BytesRefVector tsidVector, LongVector timestamps, GroupingAggregatorFunction.AddInput addInput) {
        final int positionCount = tsidVector.getPositionCount();
        long acquiredBytes = (long) Integer.BYTES * positionCount;
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "TimeSeriesBlockHash");
        final int[] ords = new int[positionCount];
        try {
            for (int p = 0; p < positionCount; p++) {
                BytesRef v = tsidVector.getBytesRef(p, scratch);
                ords[p] = Math.toIntExact(hashOrdToGroup(tsidHash.add(v)));
            }
            for (int p = 0; p < positionCount; p++) {
                long timestamp = timestamps.getLong(p);
                trackTimestamp(timestamp);
                ords[p] = Math.toIntExact(hashOrdToGroup(finalHash.add(ords[p], timestamp)));
            }
            try (var groupIds = blockFactory.newIntArrayVector(ords, positionCount, acquiredBytes)) {
                acquiredBytes = 0;
                addInput.add(0, groupIds);
            }
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
        }
    }

    private IntVector ordsForTsidDict(OrdinalBytesRefVector tsidVector) {
        final BytesRefVector dict = tsidVector.getDictionaryVector();
        final IntVector positions = tsidVector.getOrdinalsVector();
        long acquiredBytes = (long) Integer.BYTES * (positions.getPositionCount() + dict.getPositionCount());
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "TimeSeriesBlockHash");
        try {
            final int[] dictOrds = new int[dict.getPositionCount()];
            for (int p = 0; p < dict.getPositionCount(); p++) {
                BytesRef v = dict.getBytesRef(p, scratch);
                dictOrds[p] = Math.toIntExact(hashOrdToGroup(tsidHash.add(v)));
            }
            final int[] tsidOrds = new int[positions.getPositionCount()];
            for (int p = 0; p < positions.getPositionCount(); p++) {
                tsidOrds[p] = dictOrds[positions.getInt(p)];
            }
            final long positionBytes = (long) Integer.BYTES * positions.getPositionCount();
            final var result = blockFactory.newIntArrayVector(tsidOrds, positions.getPositionCount(), positionBytes);
            acquiredBytes -= positionBytes;
            return result;
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        final int positionCount = (int) finalHash.size();
        final Block[] blocks;
        if (OrdinalBytesRefBlock.isDense(positionCount, tsidHash.size())) {
            blocks = buildOrdinalKeys(positionCount);
        } else {
            blocks = buildNonOrdinalKeys(positionCount);
        }
        if (reverseOutput) {
            return new Block[] { blocks[1], blocks[0] };
        }
        return blocks;
    }

    private Block[] buildOrdinalKeys(int positionCount) {
        final Block[] blocks = new Block[2];
        try (
            var tsidOrds = blockFactory.newIntVectorFixedBuilder(positionCount);
            var timestamps = blockFactory.newLongVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                tsidOrds.appendInt(p, (int) finalHash.getKey1(p));
                timestamps.appendLong(p, finalHash.getKey2(p));
            }
            final BytesRefArray bytes = tsidHash.getBytesRefs();
            var dict = blockFactory.newBytesRefArrayVector(bytes, Math.toIntExact(bytes.size()));
            bytes.incRef();
            try {
                blocks[0] = new OrdinalBytesRefVector(tsidOrds.build(), dict).asBlock();
            } finally {
                if (blocks[0] == null) {
                    dict.close();
                }
            }
            blocks[1] = timestamps.build().asBlock();
        } finally {
            if (blocks[1] == null) {
                Releasables.close(blocks[0]);
            }
        }
        return blocks;
    }

    private Block[] buildNonOrdinalKeys(int positionCount) {
        final Block[] blocks = new Block[2];
        try (
            var ordsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount);
            var timestamps = blockFactory.newLongVectorFixedBuilder(positionCount)
        ) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                ordsBuilder.appendInt(p, (int) finalHash.getKey1(p));
                timestamps.appendLong(p, finalHash.getKey2(p));
            }
            try (var tsidBuilder = blockFactory.newBytesRefVectorBuilder(positionCount); var ords = ordsBuilder.build()) {
                for (int p = 0; p < positionCount; p++) {
                    tsidBuilder.appendBytesRef(tsidHash.get(ords.getInt(p), scratch));
                }
                blocks[0] = tsidBuilder.build().asBlock();
            }
            blocks[1] = timestamps.build().asBlock();
            return blocks;
        } finally {
            if (blocks[1] == null) {
                Releasables.close(blocks[0]);
            }
        }
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, (int) finalHash.size());
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(finalHash.size());
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(finalHash.size())).seenGroupIds(bigArrays);
    }

    public int tsidForGroup(long groupId) {
        return Math.toIntExact(finalHash.getKey1(groupId));
    }

    public long timestampForGroup(long groupId) {
        return finalHash.getKey2(groupId);
    }

    public long getGroupId(long tsid, long timestamp) {
        return finalHash.find(tsid, timestamp);
    }

    public long addGroup(int tsid, long timestamp) {
        trackTimestamp(timestamp);
        return finalHash.add(tsid, timestamp);
    }

    private void trackTimestamp(long timestamp) {
        minTimestamp = Math.min(minTimestamp, timestamp);
        maxTimestamp = Math.max(maxTimestamp, timestamp);
    }

    public long numGroups() {
        return finalHash.size();
    }

    public long minTimestamp() {
        return minTimestamp;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public String toString() {
        return "BytesRefLongBlockHash{keys=[tsid[channel="
            + tsidChannel
            + "], timestamp[channel="
            + timestampChannel
            + "]], entries="
            + finalHash.size()
            + ", size="
            + tsidHash.ramBytesUsed()
            + "b}";
    }
}
