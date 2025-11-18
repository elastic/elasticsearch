/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * A time-series aggregation function that collects the First occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-ValueByTimestampAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG"),
        @IntermediateState(name = "values", type = "BYTES_REF"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator(
    { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "BYTES_REF_BLOCK") }
)
public class FirstBytesRefByTimestampAggregator {
    public static String describe() {
        return "first_BytesRef_by_timestamp";
    }

    public static LongBytesRefState initSingle(DriverContext driverContext) {
        return new LongBytesRefState(0, new BytesRef(), driverContext.breaker(), describe());
    }

    public static void first(LongBytesRefState current, BytesRef value, long timestamp) {
        current.v1(timestamp);
        current.v2(value);
    }

    public static void combine(LongBytesRefState current, BytesRef value, long timestamp) {
        if (timestamp < current.v1()) {
            current.v1(timestamp);
            current.v2(value);
        }
    }

    public static void combineIntermediate(LongBytesRefState current, long timestamp, BytesRef value, boolean seen) {
        if (seen) {
            if (current.seen()) {
                combine(current, value, timestamp);
            } else {
                first(current, value, timestamp);
                current.seen(true);
            }
        }
    }

    public static Block evaluateFinal(LongBytesRefState current, DriverContext ctx) {
        return ctx.blockFactory().newConstantBytesRefBlockWith(current.v2(), 1);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(GroupingState current, int groupId, BytesRef value, long timestamp) {
        current.collectValue(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps, // stylecheck
        BytesRefBlock values,
        int otherPosition
    ) {
        // TODO seen should probably be part of the intermediate representation
        int valueCount = values.getValueCount(otherPosition);
        if (valueCount > 0) {
            long timestamp = timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
            int firstIndex = values.getFirstValueIndex(otherPosition);
            BytesRef bytesScratch = new BytesRef();
            for (int i = 0; i < valueCount; i++) {
                current.collectValue(groupId, timestamp, values.getBytesRef(firstIndex + i, bytesScratch));
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;
        private LongArray timestamps;
        private ObjectArray<BreakingBytesRefBuilder> values;
        private final CircuitBreaker breaker;
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            this.breaker = breaker;
            LongArray timestamps = null;
            try {
                timestamps = bigArrays.newLongArray(1, false);
                this.timestamps = timestamps;
                this.values = bigArrays.newObjectArray(1);
                /*
                 * Enable group id tracking because we use has hasValue in the
                 * collection itself to detect the when a value first arrives.
                 */
                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values, super::close);
                }
            }
        }

        void collectValue(int groupId, long timestamp, BytesRef value) {
            boolean updated = false;
            if (groupId < timestamps.size()) {
                // TODO: handle multiple values?
                if (groupId > maxGroupId || hasValue(groupId) == false || timestamps.get(groupId) > timestamp) {
                    timestamps.set(groupId, timestamp);
                    updated = true;
                }
            } else {
                timestamps = bigArrays.grow(timestamps, groupId + 1);
                timestamps.set(groupId, timestamp);
                updated = true;
            }
            if (updated) {
                values = bigArrays.grow(values, groupId + 1);
                BreakingBytesRefBuilder builder = values.get(groupId);
                if (builder == null) {
                    builder = new BreakingBytesRefBuilder(breaker, "First", value.length);
                }
                builder.copyBytes(value);
                values.set(groupId, builder);
            }
            maxGroupId = Math.max(maxGroupId, groupId);
            trackGroupId(groupId);
        }

        @Override
        public void close() {
            for (long i = 0; i < values.size(); i++) {
                Releasables.close(values.get(i));
            }
            Releasables.close(timestamps, values, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var valuesBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValue(group)) {
                        timestampsBuilder.appendLong(timestamps.get(group));
                        valuesBuilder.appendBytesRef(values.get(group).bytesRefView());
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
            try (var builder = evalContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValue(group)) {
                        builder.appendBytesRef(values.get(group).bytesRefView());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }
}
