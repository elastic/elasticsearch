/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of doubles. It is created in a mode where it
 * won't track the {@code groupId}s that are sent to it and it is the
 * responsibility of the caller to only fetch values for {@code groupId}s
 * that it has sent using the {@code selected} parameter when building the
 * results. This is fine when there are no {@code null} values in the input
 * data. But once there are null values in the input data it is
 * <strong>much</strong> more convenient to only send non-null values and
 * the tracking built into the grouping code can't track that. In that case
 * call {@link #enableGroupIdTracking} to transition the state into a mode
 * where it'll track which {@code groupIds} have been written.
 * <p>
 * This class is generated. Edit {@code X-ArrayState.java.st} instead.
 * </p>
 */
final class DoubleArrayState extends AbstractArrayState implements GroupingAggregatorState {
    private final double init;

    private DoubleArray values;

    DoubleArrayState(BigArrays bigArrays, double init) {
        super(bigArrays);
        this.values = bigArrays.newDoubleArray(1, false);
        this.values.set(0, init);
        this.init = init;
    }

    double get(int groupId) {
        return values.get(groupId);
    }

    double getOrDefault(int groupId) {
        return groupId < values.size() ? values.get(groupId) : init;
    }

    void set(int groupId, double value) {
        ensureCapacity(groupId);
        values.set(groupId, value);
        trackGroupId(groupId);
    }

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
        if (false == trackingGroupIds()) {
            try (var builder = driverContext.blockFactory().newDoubleVectorFixedBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    builder.appendDouble(i, values.get(selected.getInt(i)));
                }
                return builder.build().asBlock();
            }
        }
        try (DoubleBlock.Builder builder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    builder.appendDouble(values.get(group));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private void ensureCapacity(int groupId) {
        if (groupId >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, groupId + 1);
            values.fill(prevSize, values.size(), init);
        }
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(
        Block[] blocks,
        int offset,
        IntVector selected,
        org.elasticsearch.compute.operator.DriverContext driverContext
    ) {
        assert blocks.length >= offset + 2;
        try (
            var valuesBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < values.size()) {
                    valuesBuilder.appendDouble(values.get(group));
                } else {
                    valuesBuilder.appendDouble(0); // TODO can we just use null?
                }
                hasValueBuilder.appendBoolean(i, hasValue(group));
            }
            blocks[offset + 0] = valuesBuilder.build();
            blocks[offset + 1] = hasValueBuilder.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.close(values, super::close);
    }
}
