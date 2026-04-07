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
    private final boolean trackTimestamp;
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Long.MIN_VALUE;

    public TimeSeriesBlockHash(
        int tsidChannel,
        int timestampChannel,
        boolean reverseOutput,
        boolean trackTimestamp,
        BlockFactory blockFactory
    ) {
        super(blockFactory);
        this.tsidChannel = tsidChannel;
        this.timestampChannel = timestampChannel;
        this.reverseOutput = reverseOutput;
        this.trackTimestamp = trackTimestamp;
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
        if (trackTimestamp) {
            trackTimestampFromVector(timestampVector);
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
        final IntVector ordinalsVector = tsidVector.getOrdinalsVector();
        final int ordinalsLength = ordinalsVector.getPositionCount();
        final BytesRefVector dictVector = tsidVector.getDictionaryVector();
        final int dictLength = dictVector.getPositionCount();
        long acquiredBytes = (long) Integer.BYTES * (ordinalsLength + dictLength);
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "TimeSeriesBlockHash");
        try {
            final int[] dictOrds = new int[dictLength];
            for (int p = 0; p < dictLength; p++) {
                BytesRef v = dictVector.getBytesRef(p, scratch);
                dictOrds[p] = Math.toIntExact(hashOrdToGroup(tsidHash.add(v)));
            }
            final int[] groupIds = new int[ordinalsLength];
            int prevTsid = dictOrds[ordinalsVector.getInt(0)];
            long prevTimestamp = timestamps.getLong(0);
            int prevGroupId = Math.toIntExact(hashOrdToGroup(finalHash.add(prevTsid, prevTimestamp)));
            groupIds[0] = prevGroupId;
            for (int p = 1; p < ordinalsLength; p++) {
                final long timestamp = timestamps.getLong(p);
                int tsid = dictOrds[ordinalsVector.getInt(p)];
                if (tsid != prevTsid || timestamp != prevTimestamp) {
                    prevTimestamp = timestamp;
                    prevGroupId = Math.toIntExact(hashOrdToGroup(finalHash.add(tsid, prevTimestamp)));
                    prevTsid = tsid;
                }
                groupIds[p] = prevGroupId;
            }
            try (var groupsVector = blockFactory.newIntArrayVector(groupIds, ordinalsLength, acquiredBytes)) {
                acquiredBytes = 0;
                addInput.add(0, groupsVector);
            }
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
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

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys(IntVector selected) {
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

    public long addExtraGroup(int tsid, long timestamp) {
        return finalHash.add(tsid, timestamp);
    }

    public long numGroups() {
        return finalHash.size();
    }

    private void maybeScanTimestampsFromFinalHash() {
        if (minTimestamp > maxTimestamp) {
            for (long i = 0; i < finalHash.size(); i++) {
                long t = finalHash.getKey2(i);
                minTimestamp = Math.min(t, minTimestamp);
                maxTimestamp = Math.max(t, maxTimestamp);
            }
        }
    }

    private void trackTimestampFromVector(LongVector timestampVector) {
        for (int p = 0; p < timestampVector.getPositionCount(); p++) {
            long t = timestampVector.getLong(p);
            minTimestamp = Math.min(t, minTimestamp);
            maxTimestamp = Math.max(t, maxTimestamp);
        }
    }

    public long minTimestamp() {
        maybeScanTimestampsFromFinalHash();
        return minTimestamp;
    }

    public long maxTimestamp() {
        maybeScanTimestampsFromFinalHash();
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
