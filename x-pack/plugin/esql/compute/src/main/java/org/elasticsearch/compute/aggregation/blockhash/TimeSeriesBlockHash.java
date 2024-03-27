/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

public final class TimeSeriesBlockHash extends BlockHash {

    private final AggregatorMode mode;
    private final int tsHashChannel;
    private final int timestampChannel;
    private final Rounding.Prepared preparedRounding;
    private final BytesRefHash tsidHashes;
    private final LongLongHash intervalHash;

    long groupOrdinal = -1;
    BytesRef previousTsidHash;
    long previousTimestamp;

    public TimeSeriesBlockHash(
        AggregatorMode mode,
        int tsHashChannel,
        int timestampChannel,
        Rounding rounding,
        DriverContext driverContext
    ) {
        super(driverContext.blockFactory());
        this.mode = mode;
        this.tsHashChannel = tsHashChannel;
        this.timestampChannel = timestampChannel;
        this.preparedRounding = rounding != null ? rounding.prepareForUnknown() : null;
        this.tsidHashes = new BytesRefHash(1, blockFactory.bigArrays());
        this.intervalHash = new LongLongHash(1, blockFactory.bigArrays());
    }

    @Override
    public void close() {
        Releasables.close(tsidHashes, intervalHash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock tsHashBlock = page.getBlock(tsHashChannel);
        BytesRefVector tsHashVector = Objects.requireNonNull(tsHashBlock.asVector());
        try (var ordsBuilder = blockFactory.newIntVectorBuilder(tsHashVector.getPositionCount())) {
            LongBlock timestampIntervalBlock = page.getBlock(timestampChannel);
            BytesRef spare = new BytesRef();
            if (preparedRounding != null) {
                for (int i = 0; i < tsHashVector.getPositionCount(); i++) {
                    BytesRef tsHash = tsHashVector.getBytesRef(i, spare);
                    long timestampInterval = preparedRounding.round(timestampIntervalBlock.getLong(i));
                    if (tsHash.equals(previousTsidHash) == false || timestampInterval != previousTimestamp) {
                        long tsidOrdinal = tsidHashes.add(tsHash);
                        if (tsidOrdinal < 0) {
                            tsidOrdinal = -1 - tsidOrdinal;
                        }
                        groupOrdinal = intervalHash.add(tsidOrdinal, timestampInterval);
                        if (groupOrdinal < 0) {
                            groupOrdinal = -1 - groupOrdinal;
                        }
                        previousTsidHash = BytesRef.deepCopyOf(tsHash);
                        previousTimestamp = timestampInterval;
                    }
                    ordsBuilder.appendInt(Math.toIntExact(groupOrdinal));
                }
            } else {
                for (int i = 0; i < tsHashVector.getPositionCount(); i++) {
                    BytesRef tsHash = tsHashVector.getBytesRef(i, spare);
                    if (tsHash.equals(previousTsidHash) == false) {
                        groupOrdinal = tsidHashes.add(tsHash);
                        if (groupOrdinal < 0) {
                            groupOrdinal = -1 - groupOrdinal;
                        }
                        previousTsidHash = BytesRef.deepCopyOf(tsHash);

                        // keep last time stamp around:
                        long timestamp = timestampIntervalBlock.getLong(i);
                        intervalHash.add(groupOrdinal, timestamp);
                    }
                    ordsBuilder.appendInt(Math.toIntExact(groupOrdinal));
                }
            }
            try (var ords = ordsBuilder.build()) {
                addInput.add(0, ords);
            }
        }
    }

    @Override
    public Block[] getKeys() {
        int positions = (int) intervalHash.size();
        BytesRefVector k1;
        LongVector k2;
        try (
            BytesRefVector.Builder tsidHashes = blockFactory.newBytesRefVectorBuilder(positions);
            LongVector.Builder timestampIntervals = blockFactory.newLongVectorBuilder(positions)
        ) {
            BytesRef scratch = new BytesRef();
            for (long i = 0; i < positions; i++) {
                BytesRef key1 = this.tsidHashes.get(intervalHash.getKey1(i), scratch);
                tsidHashes.appendBytesRef(key1);
                timestampIntervals.appendLong(intervalHash.getKey2(i));
            }
            k1 = tsidHashes.build();
            k2 = timestampIntervals.build();
        }
        return new Block[] { k1.asBlock(), k2.asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        long endExclusive = intervalHash.size();
        return IntVector.range(0, Math.toIntExact(endExclusive), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        long size = intervalHash.size();
        return new SeenGroupIds.Range(0, Math.toIntExact(size)).seenGroupIds(bigArrays);
    }

    public String toString() {
        return "TimeSeriesBlockHash{keys=[BytesRefKey[channel="
            + tsHashChannel
            + "], LongKey[channel="
            + timestampChannel
            + "]], entries="
            + groupOrdinal
            + "b}";
    }
}
