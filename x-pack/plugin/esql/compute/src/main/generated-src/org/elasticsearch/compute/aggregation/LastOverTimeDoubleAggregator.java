/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * A time-series aggregation function that collects the last value of each time series in each grouping
 * This class is generated. Edit `X-LastOverTimeAggregator.java.st` instead.
 */
@GroupingAggregator(
    timeseries = true,
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "DOUBLE_BLOCK") }
)
public class LastOverTimeDoubleAggregator {

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int groupId, long timestamp, double value) {
        current.maybeCollect(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps, // stylecheck
        DoubleBlock values,
        int otherPosition
    ) {
        int valueCount = values.getValueCount(otherPosition);
        if (valueCount > 0) {
            long timestamp = timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
            int firstIndex = values.getFirstValueIndex(otherPosition);
            for (int i = 0; i < valueCount; i++) {
                current.maybeCollect(groupId, timestamp, values.getDouble(firstIndex + i));
            }
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState otherState, int otherGroupId) {
        if (otherGroupId < otherState.timestamps.size() && otherState.hasValue(otherGroupId)) {
            var timestamp = otherState.timestamps.get(otherGroupId);
            var value = otherState.values.get(otherGroupId);
            current.maybeCollect(currentGroupId, timestamp, value);
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    public static final class GroupingState implements GroupingAggregatorState, Releasable {
        private final BigArrays bigArrays;
        private LongArray timestamps;
        private DoubleArray values;
        private BitArray hasValues = null;
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            boolean success = false;
            LongArray timestamps = null;
            DoubleArray values = null;
            try {
                timestamps = bigArrays.newLongArray(1, false);
                values = bigArrays.newDoubleArray(1, false);
                this.timestamps = timestamps;
                this.values = values;
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values);
                }
            }
        }

        void maybeCollect(int groupId, long timestamp, double value) {
            if (groupId > maxGroupId) {
                timestamps = bigArrays.grow(timestamps, groupId + 1);
                values = bigArrays.grow(values, groupId + 1);
                timestamps.set(groupId, timestamp);
                values.set(groupId, value);
            } else {
                // TODO: handle multiple values?
                if (hasValue(groupId) == false || timestamps.get(groupId) < timestamp) {
                    timestamps.set(groupId, timestamp);
                    values.set(groupId, value);
                }
            }
            maybeTrackGroup(groupId);
        }

        private void maybeTrackGroup(int groupId) {
            if (hasValues != null) {
                hasValues.set(groupId, true);
            } else {
                if (groupId > maxGroupId + 1) {
                    hasValues = new BitArray(groupId + 1, bigArrays);
                    if (maxGroupId >= 0) {
                        hasValues.fill(0, maxGroupId + 1, true);
                    }
                    hasValues.set(groupId, true);
                }
            }
            maxGroupId = Math.max(maxGroupId, groupId);
        }

        boolean hasValue(long groupId) {
            return groupId <= maxGroupId && (hasValues == null || hasValues.get(groupId));
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values, hasValues);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var valuesBuilder = driverContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (hasValue(group)) {
                        timestampsBuilder.appendLong(timestamps.get(group));
                        valuesBuilder.appendDouble(values.get(group));
                    } else {
                        timestampsBuilder.appendNull();
                        valuesBuilder.appendNull();
                    }
                }
                blocks[offset] = timestampsBuilder.build();
                blocks[offset + 1] = valuesBuilder.build();
            }
        }

        Block evaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
            try (var builder = evalContext.blockFactory().newDoubleBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (hasValue(group)) {
                        builder.appendDouble(values.get(group));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // tracking via hasValues
        }
    }
}
