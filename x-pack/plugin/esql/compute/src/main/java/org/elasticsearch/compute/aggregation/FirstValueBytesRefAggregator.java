/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

@Aggregator(
    value = {
        @IntermediateState(name = "value", type = "BYTES_REF"),
        @IntermediateState(name = "by", type = "LONG"),
        @IntermediateState(name = "seen", type = "BOOLEAN") },
    includeTimestamps = true
)
@GroupingAggregator(includeTimestamps = true)
public class FirstValueBytesRefAggregator {

    // single

    public static FirstValueLongSingleState initSingle(DriverContext driverContext) {
        return new FirstValueLongSingleState(driverContext.breaker());
    }

    public static void combine(FirstValueLongSingleState state, BytesRef value) {
        state.add(value, /*TODO get timestamp value*/ 0L);
    }

    public static void combine(FirstValueLongSingleState state, long timestamp, BytesRef value) {
        state.add(value, timestamp);
    }

    public static void combineIntermediate(FirstValueLongSingleState current, BytesRef value, long by, boolean seen) {
        current.combine(value, by, seen);
    }

    public static Block evaluateFinal(FirstValueLongSingleState state, DriverContext driverContext) {
        return state.toFinal(driverContext);
    }

    // grouping

    public static FirstValueLongGroupingState initGrouping(DriverContext driverContext) {
        return new FirstValueLongGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(FirstValueLongGroupingState state, int groupId, BytesRef value) {
        state.add(groupId, value, /*TODO get timestamp value*/ 0L);
    }

    public static void combine(FirstValueLongGroupingState state, int groupId, long timestamp, BytesRef value) {
        state.add(groupId, value, timestamp);
    }

    public static void combineIntermediate(FirstValueLongGroupingState current, int groupId, BytesRef value, long by, boolean seen) {
        if (seen) {
            current.add(groupId, value, by);
        }
    }

    public static void combineStates(
        FirstValueLongGroupingState state,
        int groupId,
        FirstValueLongGroupingState otherState,
        int otherGroupId
    ) {
        if (otherState.byState.hasValue(otherGroupId)) {
            state.add(groupId, otherState.valueState.get(otherGroupId), otherState.byState.get(otherGroupId));
        }
    }

    public static Block evaluateFinal(FirstValueLongGroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toFinal(driverContext, selected);
    }

    public static class FirstValueLongSingleState implements AggregatorState {

        private final BreakingBytesRefBuilder value;
        private long by = Long.MIN_VALUE;
        private boolean seen = false;

        public FirstValueLongSingleState(CircuitBreaker breaker) {
            this.value = new BreakingBytesRefBuilder(breaker, "first_value_bytes_ref_aggregator");
        }

        public void add(BytesRef value, long by) {
            if (seen == false || by < this.by) {
                this.seen = true;
                this.value.grow(value.length);
                this.value.setLength(value.length);
                System.arraycopy(value.bytes, value.offset, this.value.bytes(), 0, value.length);
                this.by = by;
            }
        }

        public void combine(BytesRef value, long by, boolean seen) {
            if (this.seen == false || (seen && by < this.by)) {
                this.seen = true;
                this.value.grow(value.length);
                this.value.setLength(value.length);
                System.arraycopy(value.bytes, value.offset, this.value.bytes(), 0, value.length);
                this.by = by;
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(value.bytesRefView(), 1);
            blocks[offset + 1] = driverContext.blockFactory().newConstantLongBlockWith(by, 1);
            blocks[offset + 2] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
        }

        public Block toFinal(DriverContext driverContext) {
            return seen
                ? driverContext.blockFactory().newConstantBytesRefBlockWith(value.bytesRefView(), 1)
                : driverContext.blockFactory().newConstantNullBlock(1);
        }

        @Override
        public void close() {
            Releasables.close(value);
        }
    }

    public static class FirstValueLongGroupingState implements GroupingAggregatorState {

        private final BytesRefArrayState valueState;
        private final LongArrayState byState;

        public FirstValueLongGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.valueState = new BytesRefArrayState(bigArrays, breaker, "first_value_bytes_ref_grouping_aggregator");
            this.byState = new LongArrayState(bigArrays, Long.MIN_VALUE);
        }

        public void add(int groupId, BytesRef value, long byTimestamp) {
            if (byState.hasValue(groupId) == false || byTimestamp < byState.getOrDefault(groupId)) {
                valueState.set(groupId, value);
                byState.set(groupId, byTimestamp);
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            valueState.enableGroupIdTracking(seen);
            byState.enableGroupIdTracking(seen);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            valueState.toIntermediate(blocks, offset, selected, driverContext);
            byState.toIntermediate(blocks, offset + 1, selected, driverContext);
        }

        public Block toFinal(DriverContext driverContext, IntVector selected) {
            if (byState.trackingGroupIds()) {
                try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                    for (int i = 0; i < selected.getPositionCount(); i++) {
                        int group = selected.getInt(i);
                        if (byState.hasValue(group)) {
                            builder.appendBytesRef(valueState.get(group));
                        } else {
                            builder.appendNull();
                        }
                    }
                    return builder.build();
                }
            } else {
                try (var builder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount())) {
                    for (int i = 0; i < selected.getPositionCount(); i++) {
                        int group = selected.getInt(i);
                        builder.appendBytesRef(valueState.get(group));
                    }
                    return builder.build().asBlock();
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(valueState, byState);
        }
    }
}
