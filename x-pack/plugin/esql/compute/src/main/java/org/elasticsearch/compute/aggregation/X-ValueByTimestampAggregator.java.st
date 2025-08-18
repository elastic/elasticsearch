/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.$Type$Array;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
// end generated imports

/**
 * A time-series aggregation function that collects the $Occurrence$ occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-ValueByTimestampAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG"),
        @IntermediateState(name = "values", type = "$TYPE$"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator(
    { @IntermediateState(name = "timestamps", type = "LONG_BLOCK"), @IntermediateState(name = "values", type = "$TYPE$_BLOCK") }
)
public class $Occurrence$$Type$ByTimestampAggregator {
    public static String describe() {
        return "$occurrence$_$type$_by_timestamp";
    }

    public static Long$Type$State initSingle(DriverContext driverContext) {
        return new Long$Type$State(0, 0);
    }

    public static void first(Long$Type$State current, $type$ value, long timestamp) {
        current.v1(timestamp);
        current.v2(value);
    }

    public static void combine(Long$Type$State current, $type$ value, long timestamp) {
        if (timestamp $if(First)$<$else$>$endif$ current.v1()) {
            current.v1(timestamp);
            current.v2(value);
        }
    }

    public static void combineIntermediate(Long$Type$State current, long timestamp, $type$ value, boolean seen) {
        if (seen) {
            if (current.seen()) {
                combine(current, value, timestamp);
            } else {
                first(current, value, timestamp);
                current.seen(true);
            }
        }
    }

    public static Block evaluateFinal(Long$Type$State current, DriverContext ctx) {
        return ctx.blockFactory().newConstant$Type$BlockWith(current.v2(), 1);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int groupId, $type$ value, long timestamp) {
        current.collectValue(groupId, timestamp, value);
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps, // stylecheck
        $Type$Block values,
        int otherPosition
    ) {
        // TODO seen should probably be part of the intermediate representation
        int valueCount = values.getValueCount(otherPosition);
        if (valueCount > 0) {
            long timestamp = timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
            int firstIndex = values.getFirstValueIndex(otherPosition);
            for (int i = 0; i < valueCount; i++) {
                current.collectValue(groupId, timestamp, values.get$Type$(firstIndex + i));
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;
        private LongArray timestamps;
        private $Type$Array values;
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            LongArray timestamps = null;
            try {
                timestamps = bigArrays.newLongArray(1, false);
                this.timestamps = timestamps;
                this.values = bigArrays.new$Type$Array(1, false);
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

        void collectValue(int groupId, long timestamp, $type$ value) {
            boolean updated = false;
            if (groupId < timestamps.size()) {
                // TODO: handle multiple values?
                if (groupId > maxGroupId || hasValue(groupId) == false || timestamps.get(groupId) $if(Last)$<$else$>$endif$ timestamp) {
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
                var valuesBuilder = driverContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValue(group)) {
                        timestampsBuilder.appendLong(timestamps.get(group));
                        valuesBuilder.append$Type$(values.get(group));
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
            try (var builder = evalContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValue(group)) {
                        builder.append$Type$(values.get(group));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }
}
