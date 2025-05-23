/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * A time-series aggregation function that collects the First occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-ValueOverTimeAggregator.java.st` instead.
 */
@GroupingAggregator(
    timeseries = true,
    value = { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "LONG_BLOCK") }
)
public class FirstOverTimeLongAggregator {

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    // TODO: Since data in data_streams is sorted by `_tsid` and timestamp in descending order,
    // we can read the first encountered value for each group of `_tsid` and time bucket.
    public static void combine(GroupingState current, int groupId, long timestamp, long value) {
        current.collectValue(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps, // stylecheck
        LongBlock values,
        int otherPosition
    ) {
        int valueCount = values.getValueCount(otherPosition);
        if (valueCount > 0) {
            long timestamp = timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
            int firstIndex = values.getFirstValueIndex(otherPosition);
            for (int i = 0; i < valueCount; i++) {
                current.collectValue(groupId, timestamp, values.getLong(firstIndex + i));
            }
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState otherState, int otherGroupId) {
        if (otherGroupId < otherState.timestamps.size() && otherState.hasValue(otherGroupId)) {
            var timestamp = otherState.timestamps.get(otherGroupId);
            var value = otherState.values.get(otherGroupId);
            current.collectValue(currentGroupId, timestamp, value);
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        return state.evaluateFinal(selected, evalContext);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;
        private LongArray timestamps;
        private LongArray values;
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            LongArray timestamps = null;
            try {
                timestamps = bigArrays.newLongArray(1, false);
                this.timestamps = timestamps;
                this.values = bigArrays.newLongArray(1, false);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values, super::close);
                }
            }
        }

        void collectValue(int groupId, long timestamp, long value) {
            if (groupId < timestamps.size()) {
                // TODO: handle multiple values?
                if (groupId > maxGroupId || hasValue(groupId) == false || timestamps.get(groupId) > timestamp) {
                    timestamps.set(groupId, timestamp);
                    values.set(groupId, value);
                }
            } else {
                timestamps = bigArrays.grow(timestamps, groupId + 1);
                values = bigArrays.grow(values, groupId + 1);
                timestamps.set(groupId, timestamp);
                values.set(groupId, value);
            }
            maxGroupId = Math.max(maxGroupId, groupId);
            trackGroupId(groupId);
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValue(group)) {
                        timestampsBuilder.appendLong(timestamps.get(group));
                        valuesBuilder.appendLong(values.get(group));
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
            try (var builder = evalContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValue(group)) {
                        builder.appendLong(values.get(group));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }
}
