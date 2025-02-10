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
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
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
        @IntermediateState(name = "timestamp", type = "LONG"),
        @IntermediateState(name = "seen", type = "BOOLEAN") },
    includeTimestamps = true
)
@GroupingAggregator(includeTimestamps = true)
public class FirstValueBytesRefAggregator {

    // single

    public static FirstValueLongSingleState initSingle(DriverContext driverContext) {
        return new FirstValueLongSingleState(driverContext.breaker());
    }

    public static void combine(FirstValueLongSingleState state, long timestamp, BytesRef value) {
        state.add(value, timestamp);
    }

    public static void combineIntermediate(FirstValueLongSingleState current, BytesRef value, long timestamp, boolean seen) {
        if (seen) {
            current.add(value, timestamp);
        }
    }

    public static Block evaluateFinal(FirstValueLongSingleState state, DriverContext driverContext) {
        return state.toFinal(driverContext);
    }

    // grouping

    public static FirstValueLongGroupingState initGrouping(DriverContext driverContext) {
        return new FirstValueLongGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(FirstValueLongGroupingState state, int groupId, long timestamp, BytesRef value) {
        state.add(groupId, value, timestamp);
    }

    public static void combineIntermediate(FirstValueLongGroupingState current, int groupId, BytesRef value, long timestamp, boolean seen) {
        if (seen) {
            current.add(groupId, value, timestamp);
        }
    }

    public static void combineStates(
        FirstValueLongGroupingState state,
        int groupId,
        FirstValueLongGroupingState otherState,
        int otherGroupId
    ) {
        if (otherState.hasValue(otherGroupId)) {
            state.add(groupId, otherState.valueState.get(otherGroupId).bytesRefView(), otherState.timestampState.get(otherGroupId));
        }
    }

    public static Block evaluateFinal(FirstValueLongGroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toFinal(driverContext, selected);
    }

    public static class FirstValueLongSingleState implements AggregatorState {

        private final BreakingBytesRefBuilder value;
        private long timestamp = Long.MAX_VALUE;
        private boolean seen = false;

        public FirstValueLongSingleState(CircuitBreaker breaker) {
            this.value = new BreakingBytesRefBuilder(breaker, "first_value_bytes_ref_aggregator");
        }

        public void add(BytesRef value, long timestamp) {
            if (seen == false || timestamp < this.timestamp) {
                this.seen = true;
                this.value.grow(value.length);
                this.value.setLength(value.length);
                System.arraycopy(value.bytes, value.offset, this.value.bytes(), 0, value.length);
                this.timestamp = timestamp;
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(value.bytesRefView(), 1);
            blocks[offset + 1] = driverContext.blockFactory().newConstantLongBlockWith(timestamp, 1);
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

        private final BigArrays bigArrays;
        private final CircuitBreaker breaker;

        private ObjectArray<BreakingBytesRefBuilder> valueState;
        private LongArray timestampState;
        private BitArray seen;

        public FirstValueLongGroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            valueState = bigArrays.newObjectArray(0);
            timestampState = bigArrays.newLongArray(1, false);
            seen = null;
        }

        public void add(int groupId, BytesRef value, long timestamp) {
            if (hasValue(groupId) == false || timestamp < getTimestamp(groupId)) {
                ensureCapacity(groupId);
                var currentBuilder = valueState.get(groupId);
                if (currentBuilder == null) {
                    currentBuilder = new BreakingBytesRefBuilder(breaker, "values", value.length);
                    valueState.set(groupId, currentBuilder);
                }
                currentBuilder.copyBytes(value);
                timestampState.set(groupId, timestamp);
                if (seen != null) {
                    seen.set(groupId);
                }
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            if (this.seen == null) {
                this.seen = seen.seenGroupIds(bigArrays);
            }
        }

        public boolean hasValue(int groupId) {
            return groupId < valueState.size() && valueState.get(groupId) != null;
        }

        public long getTimestamp(int groupId) {
            return groupId < timestampState.size() ? timestampState.get(groupId) : Long.MAX_VALUE;
        }

        private void ensureCapacity(int groupId) {
            if (groupId >= timestampState.size()) {
                valueState = bigArrays.grow(valueState, groupId + 1);
                long prevSize = timestampState.size();
                timestampState = bigArrays.grow(timestampState, groupId + 1);
                timestampState.fill(prevSize, timestampState.size(), Long.MAX_VALUE);
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            assert blocks.length >= offset + 3;
            try (
                var valuesBuilder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount());
                var timestampBuilder = driverContext.blockFactory().newLongBlockBuilder(selected.getPositionCount());
                var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
            ) {
                var emptyBytesRef = new BytesRef();
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    if (hasValue(group)) {
                        valuesBuilder.appendBytesRef(valueState.get(group).bytesRefView());
                        timestampBuilder.appendLong(timestampState.get(group));
                    } else {
                        valuesBuilder.appendBytesRef(emptyBytesRef);
                        timestampBuilder.appendNull();
                    }
                    hasValueBuilder.appendBoolean(i, hasValue(group));
                }
                blocks[offset] = valuesBuilder.build().asBlock();
                blocks[offset + 1] = timestampBuilder.build();
                blocks[offset + 2] = hasValueBuilder.build().asBlock();
            }
        }

        public Block toFinal(DriverContext driverContext, IntVector selected) {
            if (seen != null) {
                try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
                    for (int i = 0; i < selected.getPositionCount(); i++) {
                        int group = selected.getInt(i);
                        if (seen.get(group)) {
                            builder.appendBytesRef(valueState.get(group).bytesRefView());
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
                        builder.appendBytesRef(valueState.get(group).bytesRefView());
                    }
                    return builder.build().asBlock();
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(valueState, timestampState, seen);
        }
    }
}
