/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorState;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.ann.RuntimeAggregator;
import org.elasticsearch.compute.ann.RuntimeIntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Runtime aggregator for MaxBytes2 function.
 * <p>
 * This aggregator finds the maximum BytesRef value (lexicographically).
 * It demonstrates runtime aggregator generation with non-primitive state type
 * and the first method pattern.
 * </p>
 */
@RuntimeAggregator(
    intermediateState = {
        @RuntimeIntermediateState(name = "max", type = "BYTES_REF"),
        @RuntimeIntermediateState(name = "seen", type = "BOOLEAN") },
    grouping = true
)
public class MaxBytes2Aggregator {

    private static boolean isBetter(BytesRef value, BytesRef otherValue) {
        return value.compareTo(otherValue) > 0;
    }

    /**
     * Initialize the aggregation state.
     */
    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.breaker());
    }

    /**
     * Combine the current state with a new BytesRef value.
     * Only updates if the new value is greater.
     */
    public static void combine(SingleState state, BytesRef value) {
        state.add(value);
    }

    /**
     * Combine intermediate states.
     */
    public static void combineIntermediate(SingleState state, BytesRef value, boolean seen) {
        if (seen) {
            combine(state, value);
        }
    }

    /**
     * Evaluate the final result.
     */
    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    // ========================================================================================
    // Grouping aggregator methods
    // ========================================================================================

    /**
     * Initialize the grouping aggregation state.
     */
    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    /**
     * Combine the current state with a new BytesRef value for a specific group.
     */
    public static void combine(GroupingState state, int groupId, BytesRef value) {
        state.add(groupId, value);
    }

    /**
     * Combine intermediate states for grouping.
     */
    public static void combineIntermediate(GroupingState state, int groupId, BytesRef value, boolean seen) {
        if (seen) {
            state.add(groupId, value);
        }
    }

    /**
     * Evaluate the final result for grouping.
     */
    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(selected, ctx.driverContext());
    }

    /**
     * State class for single (non-grouping) aggregation.
     */
    public static class SingleState implements AggregatorState {
        private final BreakingBytesRefBuilder internalState;
        private boolean seen;

        public SingleState(CircuitBreaker breaker) {
            this.internalState = new BreakingBytesRefBuilder(breaker, "max_bytes2_aggregator");
            this.seen = false;
        }

        public void add(BytesRef value) {
            if (seen == false || isBetter(value, internalState.bytesRefView())) {
                seen = true;
                internalState.grow(value.length);
                internalState.setLength(value.length);
                System.arraycopy(value.bytes, value.offset, internalState.bytes(), 0, value.length);
            }
        }

        public boolean seen() {
            return seen;
        }

        public void seen(boolean seen) {
            this.seen = seen;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
            blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
        }

        public Block toBlock(DriverContext driverContext) {
            if (seen == false) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            return driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
        }

        @Override
        public void close() {
            Releasables.close(internalState);
        }
    }

    /**
     * State class for grouping aggregation.
     * Uses a simple Map-based implementation for demonstration purposes.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final Map<Integer, BytesRef> values = new HashMap<>();
        private final Set<Integer> seenGroups = new HashSet<>();

        @SuppressWarnings("unused")
        private GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            // Simple implementation for testing - doesn't use BigArrays
        }

        public void add(int groupId, BytesRef value) {
            BytesRef current = values.get(groupId);
            if (current == null || isBetter(value, current)) {
                values.put(groupId, BytesRef.deepCopyOf(value));
            }
            seenGroups.add(groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            int count = selected.getPositionCount();
            try (
                BytesRefBlock.Builder maxBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(count);
                BooleanBlock.Builder seenBuilder = driverContext.blockFactory().newBooleanBlockBuilder(count)
            ) {
                for (int i = 0; i < count; i++) {
                    int groupId = selected.getInt(i);
                    BytesRef val = values.get(groupId);
                    if (val != null) {
                        maxBuilder.appendBytesRef(val);
                        seenBuilder.appendBoolean(true);
                    } else {
                        maxBuilder.appendNull();
                        seenBuilder.appendBoolean(false);
                    }
                }
                blocks[offset] = maxBuilder.build();
                blocks[offset + 1] = seenBuilder.build();
            }
        }

        Block toBlock(IntVector selected, DriverContext driverContext) {
            int count = selected.getPositionCount();
            try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(count)) {
                for (int i = 0; i < count; i++) {
                    int groupId = selected.getInt(i);
                    BytesRef val = values.get(groupId);
                    if (val != null) {
                        builder.appendBytesRef(val);
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // No-op for simple implementation
        }

        @Override
        public void close() {
            values.clear();
            seenGroups.clear();
        }
    }
}
