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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;
// end generated imports

/**
 * A time-series aggregation function that collects the Last occurrence value of a time series in a specified interval.
 * This class is generated. Edit `X-AllValueByTimestafmpAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "observed", type = "BOOLEAN"),
        @IntermediateState(name = "timestampPresent", type = "BOOLEAN"),
        @IntermediateState(name = "timestamp", type = "LONG"),
        @IntermediateState(name = "values", type = "FLOAT_BLOCK") }
)
@GroupingAggregator(
    {
        @IntermediateState(name = "observed", type = "BOOLEAN_BLOCK"),
        @IntermediateState(name = "timestampsPresent", type = "BOOLEAN_BLOCK"),
        @IntermediateState(name = "timestamps", type = "LONG_BLOCK"),
        @IntermediateState(name = "values", type = "FLOAT_BLOCK") }
)
public class AllLastFloatByTimestampAggregator {
    public static String describe() {
        return "all_last_float_by_timestamp";
    }

    public static AllLongFloatState initSingle(DriverContext driverContext) {
        return new AllLongFloatState(driverContext.bigArrays());
    }

    private static void overrideState(
        AllLongFloatState current,
        boolean timestampPresent,
        long timestamp,
        FloatBlock values,
        int position
    ) {
        current.observed(true);
        current.v1(timestampPresent ? timestamp : -1L);
        current.v1Seen(timestampPresent);
        if (values.isNull(position)) {
            Releasables.close(current.v2());
            current.v2(null);
        } else {
            int count = values.getValueCount(position);
            int offset = values.getFirstValueIndex(position);
            FloatArray a = null;
            boolean success = false;
            try {
                a = current.bigArrays().newFloatArray(count);
                for (int i = 0; i < count; ++i) {
                    a.set(i, values.getFloat(offset + i));
                }
                success = true;
                Releasables.close(current.v2());
                current.v2(a);
            } finally {
                if (success == false) {
                    Releasables.close(a);
                }
            }
        }
    }

    private static long dominantTimestampAtPosition(int position, LongBlock timestamps) {
        assert timestamps.isNull(position) == false : "The timestamp is null at this position";
        int lo = timestamps.getFirstValueIndex(position);
        int hi = lo + timestamps.getValueCount(position);
        long result = timestamps.getLong(lo++);

        for (int i = lo; i < hi; i++) {
            result = Math.max(result, timestamps.getLong(i));
        }

        return result;
    }

    public static void combine(AllLongFloatState current, @Position int position, FloatBlock values, LongBlock timestamps) {
        long timestamp = timestamps.isNull(position) ? -1 : dominantTimestampAtPosition(position, timestamps);
        boolean timestampPresent = timestamps.isNull(position) == false;

        if (current.observed() == false) {
            // We never saw a timestamp before, regardless of nullability.
            overrideState(current, timestampPresent, timestamp, values, position);
        } else if (timestampPresent && (current.v1Seen() == false || timestamp > current.v1())) {
            // The incoming timestamp wins against the current one because the latter was either null or older/newer.
            overrideState(current, true, timestamp, values, position);
        }
    }

    public static void combineIntermediate(
        AllLongFloatState current,
        boolean observed,
        boolean timestampPresent,
        long timestamp,
        FloatBlock values
    ) {
        if (observed == false) {
            // The incoming state hasn't observed anything. No work is needed.
            return;
        }

        if (current.observed()) {
            // Both the incoming shard and the current shard observed a value, so we must compare timestamps.
            if (current.v1Seen() == false && timestampPresent == false) {
                // Both observations have null timestamps. No work is needed.
                return;
            }
            if (timestampPresent && (current.v1Seen() == false || timestamp > current.v1())) {
                overrideState(current, timestampPresent, timestamp, values, 0);
            }
        } else {
            // The incoming state has observed a value, but we didn't. So we must update.
            overrideState(current, timestampPresent, timestamp, values, 0);
        }
    }

    public static Block evaluateFinal(AllLongFloatState current, DriverContext ctx) {
        return current.intermediateValuesBlockBuilder(ctx);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays());
    }

    public static void combine(GroupingState current, int group, @Position int position, FloatBlock values, LongBlock timestamps) {
        long timestamp = timestamps.isNull(position) ? 0L : dominantTimestampAtPosition(position, timestamps);
        current.collectValue(group, timestamps.isNull(position) == false, timestamp, position, values);
    }

    public static void combineIntermediate(
        GroupingState current,
        int group,
        BooleanBlock observed,
        BooleanBlock timestampPresent,
        LongBlock timestamps,
        FloatBlock values,
        int otherPosition
    ) {
        if (group < observed.getPositionCount() && observed.getBoolean(observed.getFirstValueIndex(otherPosition)) == false) {
            // The incoming state hasn't observed anything for this particular group. No work is needed.
            return;
        }
        long timestamp = timestamps.isNull(otherPosition) ? -1 : timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
        boolean hasTimestamp = timestampPresent.getBoolean(timestampPresent.getFirstValueIndex(otherPosition));
        current.collectValue(group, hasTimestamp, timestamp, otherPosition, values);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;

        /**
         * The group-indexed observed flags
         */
        private ByteArray observed;

        /**
         * The group-indexed timestamps seen flags
         */
        private ByteArray hasTimestamp;

        /**
         * The group-indexed timestamps
         */
        private LongArray timestamps;

        /**
         * The group-indexed values
         */
        private ObjectArray<FloatArray> values;

        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            ByteArray observed = null;
            ByteArray hasTimestamp = null;
            LongArray timestamps = null;
            try {
                // Initialize observed
                observed = bigArrays.newByteArray(1, false);
                observed.set(0, (byte) -1);
                this.observed = observed;

                // Initialize hasTimestamp
                hasTimestamp = bigArrays.newByteArray(1, false);
                hasTimestamp.set(0, (byte) -1);
                this.hasTimestamp = hasTimestamp;

                // Initialize timestamps
                timestamps = bigArrays.newLongArray(1, false);
                timestamps.set(0, -1L);
                this.timestamps = timestamps;

                // Initialize values
                this.values = bigArrays.newObjectArray(1);
                this.values.set(0, null);

                // Enable group id tracking because we use has hasValue in the
                // collection itself to detect when a value first arrives.
                enableGroupIdTracking(new SeenGroupIds.Empty());
                success = true;
            } finally {
                if (success == false) {
                    if (values != null) {
                        for (long i = 0; i < values.size(); i++) {
                            Releasables.close(values.get(i));
                        }
                    }
                    Releasables.close(observed, hasTimestamp, timestamps, values, super::close);
                }
            }
        }

        void collectValue(int group, boolean timestampPresent, long timestamp, int position, FloatBlock valuesBlock) {
            boolean updated = false;
            if (withinBounds(group)) {
                if (hasValue(group) == false
                    || (hasTimestamp.get(group) == 0 && timestampPresent)
                    || (timestampPresent && timestamp > timestamps.get(group))) {
                    // We never saw this group before, even if it's within bounds.
                    // Or, the incoming non-null timestamp wins against the null one in the state.
                    // Or, we found a better timestamp for this group.
                    updated = true;
                }
            } else {
                // We must grow all arrays to accommodate this group id
                observed = bigArrays.grow(observed, group + 1);
                hasTimestamp = bigArrays.grow(hasTimestamp, group + 1);
                timestamps = bigArrays.grow(timestamps, group + 1);
                values = bigArrays.grow(values, group + 1);
                updated = true;
            }
            if (updated) {
                observed.set(group, (byte) 1);
                hasTimestamp.set(group, (byte) (timestampPresent ? 1 : 0));
                timestamps.set(group, timestamp);
                boolean success = false;
                FloatArray groupValues = null;
                try {
                    if (valuesBlock.isNull(position) == false) {
                        int count = valuesBlock.getValueCount(position);
                        int offset = valuesBlock.getFirstValueIndex(position);
                        groupValues = bigArrays.newFloatArray(count);
                        for (int i = 0; i < count; ++i) {
                            groupValues.set(i, valuesBlock.getFloat(i + offset));
                        }
                    }
                    success = true;
                    Releasables.close(values.get(group));
                    values.set(group, groupValues);
                } finally {
                    if (success == false) {
                        Releasables.close(groupValues);
                    }
                }
            }
            maxGroupId = Math.max(maxGroupId, group);
            trackGroupId(group);
        }

        @Override
        public void close() {
            for (long i = 0; i < values.size(); i++) {
                Releasables.close(values.get(i));
            }
            Releasables.close(observed, hasTimestamp, timestamps, values, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            try (
                var observedBlockBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var hasTimestampBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount());
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (withinBounds(group)) {
                        // We must have seen this group before and saved its state
                        observedBlockBuilder.appendBoolean(observed.get(group) == 1);
                        hasTimestampBuilder.appendBoolean(hasTimestamp.get(group) == 1);
                        timestampsBuilder.appendLong(timestamps.get(group));
                    } else {
                        // Unknown group so we append nulls everywhere
                        observedBlockBuilder.appendBoolean(false);
                        hasTimestampBuilder.appendBoolean(false);
                        timestampsBuilder.appendNull();
                    }
                }

                // Create all intermediate state blocks
                blocks[offset + 0] = observedBlockBuilder.build();
                blocks[offset + 1] = hasTimestampBuilder.build();
                blocks[offset + 2] = timestampsBuilder.build();
                blocks[offset + 3] = intermediateValuesBlockBuilder(selected, driverContext.blockFactory());
            }
        }

        Block evaluateFinal(IntVector groups, GroupingAggregatorEvaluationContext evalContext) {
            return intermediateValuesBlockBuilder(groups, evalContext.blockFactory());
        }

        private boolean withinBounds(int group) {
            return group < Math.min(Math.min(timestamps.size(), values.size()), Math.min(observed.size(), hasTimestamp.size()));
        }

        private Block intermediateValuesBlockBuilder(IntVector groups, BlockFactory blockFactory) {
            try (var valuesBuilder = blockFactory.newFloatBlockBuilder(groups.getPositionCount())) {
                for (int p = 0; p < groups.getPositionCount(); p++) {
                    int group = groups.getInt(p);
                    int count = 0;
                    if (withinBounds(group) && observed.get(group) == 1 && values.get(group) != null) {
                        count = (int) values.get(group).size();
                    }
                    switch (count) {
                        case 0 -> valuesBuilder.appendNull();
                        case 1 -> valuesBuilder.appendFloat(values.get(group).get(0));
                        default -> {
                            valuesBuilder.beginPositionEntry();
                            for (int i = 0; i < count; ++i) {
                                valuesBuilder.appendFloat(values.get(group).get(i));
                            }
                            valuesBuilder.endPositionEntry();
                        }
                    }
                }
                return valuesBuilder.build();
            }
        }
    }
}
