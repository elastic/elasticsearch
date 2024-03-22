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

import java.util.Objects;

public final class TimeSeriesBlockHash extends BlockHash {

    private final int tsHashChannel;
    private final int timestampChannel;
    private final Rounding.Prepared preparedRounding;
    private final DriverContext driverContext;

    long tsidOrdinal = -1;
    BytesRef previousTsidHash;
    long previousTimestamp;

    public TimeSeriesBlockHash(int tsHashChannel, int timestampChannel, Rounding rounding, DriverContext driverContext) {
        super(driverContext.blockFactory());
        this.tsHashChannel = tsHashChannel;
        this.timestampChannel = timestampChannel;
        if (rounding != null) {
            this.preparedRounding = rounding.prepareForUnknown();
        } else {
            this.preparedRounding = null;
        }
        this.driverContext = driverContext;
    }

    @Override
    public void close() {}

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock tsHashBlock = page.getBlock(tsHashChannel);
        BytesRefVector tsHashVector = Objects.requireNonNull(tsHashBlock.asVector());
        LongBlock timestampIntervalBlock = page.getBlock(timestampChannel);
        LongVector timestampIntervalVector = Objects.requireNonNull(timestampIntervalBlock.asVector());

        try (var ordsBuilder = driverContext.blockFactory().newIntVectorBuilder(tsHashVector.getPositionCount())) {
            BytesRef spare = new BytesRef();
            if (preparedRounding != null) {
                for (int i = 0; i < tsHashVector.getPositionCount(); i++) {
                    BytesRef tsHash = tsHashVector.getBytesRef(i, spare);
                    long timestampInterval = preparedRounding.round(timestampIntervalVector.getLong(i));
                    if (tsHash.equals(previousTsidHash) == false || timestampInterval != previousTimestamp) {
                        tsidOrdinal++;
                        previousTsidHash = BytesRef.deepCopyOf(tsHash);
                        previousTimestamp = timestampInterval;
                    }
                    ordsBuilder.appendInt(Math.toIntExact(tsidOrdinal));
                }
            } else {
                for (int i = 0; i < tsHashVector.getPositionCount(); i++) {
                    BytesRef tsHash = tsHashVector.getBytesRef(i, spare);
                    if (tsHash.equals(previousTsidHash) == false) {
                        tsidOrdinal++;
                        previousTsidHash = BytesRef.deepCopyOf(tsHash);
                    }
                    ordsBuilder.appendInt(Math.toIntExact(tsidOrdinal));
                }
            }
            try (var ords = ordsBuilder.build()) {
                addInput.add(0, ords);
            }
        }
    }

    @Override
    public Block[] getKeys() {
        // No keys for now... the tsid hash doesn't make for a good key, plus the planner doesn't know about this
        return new Block[0];
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(tsidOrdinal + 1), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(tsidOrdinal + 1)).seenGroupIds(bigArrays);
    }

    public String toString() {
        return "TimeSeriesBlockHash{keys=[BytesRefKey[channel="
            + tsHashChannel
            + "], LongKey[channel="
            + timestampChannel
            + "]], entries="
            + tsidOrdinal
            + "b}";
    }
}
