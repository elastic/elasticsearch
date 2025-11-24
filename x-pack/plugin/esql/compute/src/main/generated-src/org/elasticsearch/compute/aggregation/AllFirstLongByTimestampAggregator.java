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
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * A time-series aggregation function that collects the First occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-AllValueByTimestampAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG"),
        @IntermediateState(name = "values", type = "LONG"),
        @IntermediateState(name = "seen", type = "BOOLEAN"),
        @IntermediateState(name = "hasValue", type = "BOOLEAN") }
)
@GroupingAggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG_BLOCK"),
        @IntermediateState(name = "values", type = "LONG_BLOCK"),
        @IntermediateState(name = "hasValues", type = "BOOLEAN_BLOCK") }
)
public class AllFirstLongByTimestampAggregator {
    public static String describe() {
        return "all_first_long_by_timestamp";
    }

    public static AllLongLongState initSingle(DriverContext driverContext) {
        return new AllLongLongState(0, 0);
    }

    private static void first(AllLongLongState current, long timestamp, long value, boolean v2Seen) {
        current.seen(true);
        current.v1(timestamp);
        current.v2(v2Seen ? value : 0);
        current.v2Seen(v2Seen);
    }

    public static void combine(AllLongLongState current, @Position int position, LongBlock value, LongBlock timestamp) {
        if (current.seen() == false) {
            // We never observed a value before so we'll take this right in, no questions asked.
            first(current, timestamp.getLong(position), value.getLong(position), value.isNull(position) == false);
            return;
        }

        long ts = timestamp.getLong(position);
        if (ts < current.v1()) {
            // timestamp and seen flag are updated in all cases
            current.v1(ts);
            current.seen(true);
            if (value.isNull(position) == false) {
                // non-null value
                current.v2(value.getLong(position));
                current.v2Seen(true);
            } else {
                // null value
                current.v2Seen(false);
            }
        }
    }

    public static void combineIntermediate(AllLongLongState current, long timestamp, long value, boolean seen, boolean v2Seen) {
        if (seen) {
            if (current.seen()) {
                if (timestamp < current.v1()) {
                    // A newer timestamp has been observed in the reporting shard so we must update internal state
                    current.v1(timestamp);
                    current.v2(value);
                    current.v2Seen(v2Seen);
                }
            } else {
                current.v1(timestamp);
                current.v2(value);
                current.seen(true);
                current.v2Seen(v2Seen);
            }
        }
    }

    public static Block evaluateFinal(AllLongLongState current, DriverContext ctx) {
        if (current.v2Seen()) {
            return ctx.blockFactory().newConstantLongBlockWith(current.v2(), 1);
        } else {
            return ctx.blockFactory().newConstantNullBlock(1);
        }
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int groupId, @Position int position, LongBlock value, LongBlock timestamp) {
        boolean hasValue = value.isNull(position) == false;
        current.collectValue(groupId, timestamp.getLong(position), value.getLong(position), hasValue);
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps,
        LongBlock values,
        BooleanBlock hasValues,
        int otherPosition
    ) {
        // TODO seen should probably be part of the intermediate representation
        int valueCount = values.getValueCount(otherPosition);
        if (valueCount > 0) {
            long timestamp = timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
            int firstIndex = values.getFirstValueIndex(otherPosition);
            boolean hasValueFlag = hasValues.getBoolean(otherPosition);
            for (int i = 0; i < valueCount; i++) {
                current.collectValue(groupId, timestamp, values.getLong(firstIndex + i), hasValueFlag);
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;
        private LongArray timestamps;
        private LongArray values;
        private ByteArray hasValues;
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            LongArray timestamps = null;
            ByteArray hasValues = null;
            try {
                timestamps = bigArrays.newLongArray(1, false);
                this.timestamps = timestamps;
                this.values = bigArrays.newLongArray(1, false);
                hasValues = bigArrays.newByteArray(1, false);
                this.hasValues = hasValues;

                /*
                 * Enable group id tracking because we use has hasValue in the
                 * collection itself to detect the when a value first arrives.
                 */
                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(timestamps, values, hasValues, super::close);
                }
            }
        }

        void collectValue(int groupId, long timestamp, long value, boolean hasVal) {
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
                values.set(groupId, value);
                hasValues = bigArrays.grow(hasValues, groupId + 1);
                hasValues.set(groupId, (byte) (hasVal ? 1 : 0));
            }
            maxGroupId = Math.max(maxGroupId, groupId);
            trackGroupId(groupId);
        }

        @Override
        public void close() {
            Releasables.close(timestamps, values, hasValues, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            // Creates 3 intermediate state blocks (timestamps, values, hasValue)
            try (
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var valuesBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var hasValuesBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValues.get(group) == 1) {
                        timestampsBuilder.appendLong(timestamps.get(group));
                        valuesBuilder.appendLong(values.get(group));
                        hasValuesBuilder.appendBoolean(true);
                    } else {
                        timestampsBuilder.appendNull();
                        valuesBuilder.appendNull();
                        hasValuesBuilder.appendBoolean(false);
                    }
                }
                blocks[offset] = timestampsBuilder.build();
                blocks[offset + 1] = valuesBuilder.build();
                blocks[offset + 2] = hasValuesBuilder.build();
            }
        }

        Block evaluateFinal(IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
            try (var builder = evalContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValues.get(group) == 1) {
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
