/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of booleans, that also tracks failures.
 * It is created in a mode where it won't track
 * the {@code groupId}s that are sent to it and it is the
 * responsibility of the caller to only fetch values for {@code groupId}s
 * that it has sent using the {@code selected} parameter when building the
 * results. This is fine when there are no {@code null} values in the input
 * data. But once there are null values in the input data it is
 * <strong>much</strong> more convenient to only send non-null values and
 * the tracking built into the grouping code can't track that. In that case
 * call {@link #enableGroupIdTracking} to transition the state into a mode
 * where it'll track which {@code groupIds} have been written.
 * <p>
 * This class is generated. Edit {@code X-FallibleArrayState.java.st} instead.
 * </p>
 */
final class BooleanFallibleArrayState extends AbstractFallibleArrayState implements GroupingAggregatorState {
    private final boolean init;

    private BitArray values;
    private int size;

    BooleanFallibleArrayState(BigArrays bigArrays, boolean init) {
        super(bigArrays);
        this.values = new BitArray(1, bigArrays);
        this.size = 1;
        this.values.set(0, init);
        this.init = init;
    }

    boolean get(int groupId) {
        return values.get(groupId);
    }

    boolean getOrDefault(int groupId) {
        return groupId < size ? values.get(groupId) : init;
    }

    void set(int groupId, boolean value) {
        ensureCapacity(groupId);
        values.set(groupId, value);
        trackGroupId(groupId);
    }

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
        if (false == trackingGroupIds() && false == anyFailure()) {
            try (var builder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    builder.appendBoolean(i, values.get(selected.getInt(i)));
                }
                return builder.build().asBlock();
            }
        }
        try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group) && !hasFailed(group)) {
                    builder.appendBoolean(values.get(group));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private void ensureCapacity(int groupId) {
        if (groupId >= size) {
            values.fill(size, groupId + 1, init);
            size = groupId + 1;
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
        assert blocks.length >= offset + 3;
        try (
            var valuesBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount());
            var hasFailedBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < size) {
                    valuesBuilder.appendBoolean(values.get(group));
                } else {
                    valuesBuilder.appendBoolean(false); // TODO can we just use null?
                }
                hasValueBuilder.appendBoolean(i, hasValue(group));
                hasFailedBuilder.appendBoolean(i, hasFailed(group));
            }
            blocks[offset + 0] = valuesBuilder.build();
            blocks[offset + 1] = hasValueBuilder.build().asBlock();
            blocks[offset + 2] = hasFailedBuilder.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.close(values, super::close);
    }
}
