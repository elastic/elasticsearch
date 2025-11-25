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
import org.elasticsearch.common.util.$Type$Array;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
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
 * This class is generated. Edit `X-AllValueByTimestampAggregator.java.st` instead.
 */
@Aggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG"),
        @IntermediateState(name = "values", type = "$TYPE$"),
        @IntermediateState(name = "seen", type = "BOOLEAN"),
        @IntermediateState(name = "hasValue", type = "BOOLEAN") }
)
@GroupingAggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG_BLOCK"),
        @IntermediateState(name = "values", type = "$TYPE$_BLOCK"),
        @IntermediateState(name = "hasValues", type = "BOOLEAN_BLOCK") }
)
public class $Prefix$$Occurrence$$Type$ByTimestampAggregator {
    public static String describe() {
        return "all_$occurrence$_$type$_by_timestamp";
    }

    public static $Prefix$Long$Type$State initSingle(DriverContext driverContext) {
        $if(BytesRef)$
        return new $Prefix$LongBytesRefState(0, new BytesRef(), driverContext.breaker(), describe());
        $else$
        return new $Prefix$Long$Type$State(0, 0);
        $endif$
    }

    private static void first($Prefix$Long$Type$State current, long timestamp, $type$ value, boolean v2Seen) {
        current.seen(true);
        current.v1(timestamp);
        $if(BytesRef)$
        current.v2(v2Seen ? value : new BytesRef());
        $else$
        current.v2(v2Seen ? value : 0);
        $endif$
        current.v2Seen(v2Seen);
    }

    public static void combine($Prefix$Long$Type$State current, @Position int position, $Type$Block value, LongBlock timestamp) {
        if (current.seen() == false) {
            // We never observed a value before so we'll take this right in, no questions asked.
            $if(BytesRef)$
            BytesRef bytesScratch = new BytesRef();
            first(current, timestamp.getLong(position), value.getBytesRef(position, bytesScratch), value.isNull(position) == false);
            $else$
            first(current, timestamp.getLong(position), value.get$Type$(position), value.isNull(position) == false);
            $endif$
            return;
        }

        long ts = timestamp.getLong(position);
        if (ts $if(First)$<$else$>$endif$ current.v1()) {
            // timestamp and seen flag are updated in all cases
            current.v1(ts);
            current.seen(true);
            if (value.isNull(position) == false) {
                // non-null value
                $if(BytesRef)$
                BytesRef bytesScratch = new BytesRef();
                current.v2(value.getBytesRef(position, bytesScratch));
                $else$
                current.v2(value.get$Type$(position));
                $endif$
                current.v2Seen(true);
            } else {
                // null value
                current.v2Seen(false);
            }
        }
    }

    public static void combineIntermediate($Prefix$Long$Type$State current, long timestamp, $type$ value, boolean seen, boolean v2Seen) {
        if (seen) {
            if (current.seen()) {
                if (timestamp $if(First)$<$else$>$endif$ current.v1()) {
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

    public static Block evaluateFinal($Prefix$Long$Type$State current, DriverContext ctx) {
        if (current.v2Seen()) {
            return ctx.blockFactory().newConstant$Type$BlockWith(current.v2(), 1);
        } else {
            return ctx.blockFactory().newConstantNullBlock(1);
        }
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        $if(BytesRef)$
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
        $else$
        return new GroupingState(driverContext.bigArrays());
        $endif$
    }

    public static void combine(GroupingState current, int groupId, @Position int position, $Type$Block value, LongBlock timestamp) {
        boolean hasValue = value.isNull(position) == false;
        $if(BytesRef)$
        BytesRef bytesScratch = new BytesRef();
        current.collectValue(groupId, timestamp.getLong(position), value.getBytesRef(position, bytesScratch), hasValue);
        $else$
        current.collectValue(groupId, timestamp.getLong(position), value.get$Type$(position), hasValue);
        $endif$
    }

    public static void combineIntermediate(
        GroupingState current,
        int groupId,
        LongBlock timestamps,
        $Type$Block values,
        BooleanBlock hasValues,
        int otherPosition
    ) {
        // TODO seen should probably be part of the intermediate representation
        int valueCount = values.getValueCount(otherPosition);
        if (valueCount > 0) {
            long timestamp = timestamps.getLong(timestamps.getFirstValueIndex(otherPosition));
            int firstIndex = values.getFirstValueIndex(otherPosition);
            boolean hasValueFlag = hasValues.getBoolean(otherPosition);
            $if(BytesRef)$
            BytesRef bytesScratch = new BytesRef();
            $endif$
            for (int i = 0; i < valueCount; i++) {
                $if(BytesRef)$
                current.collectValue(groupId, timestamp, values.getBytesRef(firstIndex + i, bytesScratch), hasValueFlag);
                $else$
                current.collectValue(groupId, timestamp, values.get$Type$(firstIndex + i), hasValueFlag);
                $endif$
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinal(selected, ctx);
    }

    public static final class GroupingState extends AbstractArrayState {
        private final BigArrays bigArrays;
        private LongArray timestamps;
        $if(BytesRef)$
        private ObjectArray<BreakingBytesRefBuilder> values;
        $else$
        private $Type$Array values;
        $endif$
        $if(BytesRef)$
        private final CircuitBreaker breaker;
        $endif$
        private ByteArray hasValues;
        private int maxGroupId = -1;

        GroupingState(BigArrays bigArrays$if(BytesRef)$, CircuitBreaker breaker$endif$) {
            super(bigArrays);
            this.bigArrays = bigArrays;
            boolean success = false;
            $if(BytesRef)$
            this.breaker = breaker;
            $endif$
            LongArray timestamps = null;
            ByteArray hasValues = null;
            try {
                timestamps = bigArrays.newLongArray(1, false);
                this.timestamps = timestamps;
                $if(BytesRef)$
                this.values = bigArrays.newObjectArray(1);
                $else$
                this.values = bigArrays.new$Type$Array(1, false);
                $endif$
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

        void collectValue(int groupId, long timestamp, $type$ value, boolean hasVal) {
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
                $if(BytesRef)$
                BreakingBytesRefBuilder builder = values.get(groupId);
                if (builder == null) {
                    builder = new BreakingBytesRefBuilder(breaker, "$Occurrence$", value.length);
                }
                builder.copyBytes(value);
                values.set(groupId, builder);
                $else$
                values.set(groupId, value);
                $endif$
                hasValues = bigArrays.grow(hasValues, groupId + 1);
                hasValues.set(groupId, (byte) (hasVal ? 1 : 0));
            }
            maxGroupId = Math.max(maxGroupId, groupId);
            trackGroupId(groupId);
        }

        @Override
        public void close() {
            $if(BytesRef)$
            for (long i = 0; i < values.size(); i++) {
                Releasables.close(values.get(i));
            }
            $endif$
            Releasables.close(timestamps, values, hasValues, super::close);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            // Creates 3 intermediate state blocks (timestamps, values, hasValue)
            try (
                var timestampsBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var valuesBuilder = driverContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount());
                var hasValuesBuilder = driverContext.blockFactory().newBooleanBlockBuilder(selected.getPositionCount())
            ) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValues.get(group) == 1) {
                        timestampsBuilder.appendLong(timestamps.get(group));
                        $if(BytesRef)$
                        valuesBuilder.append$Type$(values.get(group).bytesRefView());
                        $else$
                        valuesBuilder.append$Type$(values.get(group));
                        $endif$
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
            try (var builder = evalContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount())) {
                for (int p = 0; p < selected.getPositionCount(); p++) {
                    int group = selected.getInt(p);
                    if (group < timestamps.size() && hasValues.get(group) == 1) {
                        $if(BytesRef)$
                        builder.append$Type$(values.get(group).bytesRefView());
                        $else$
                        builder.append$Type$(values.get(group));
                        $endif$
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }
    }
}
