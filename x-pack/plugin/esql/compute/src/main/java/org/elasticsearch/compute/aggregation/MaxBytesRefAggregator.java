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

/**
 * Aggregator for `Max`, that works with BytesRef values.
 * Gets the biggest BytesRef value, based on its bytes natural order (Delegated to {@link BytesRef#compareTo}).
 */
@Aggregator({ @IntermediateState(name = "max", type = "BYTES_REF"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
class MaxBytesRefAggregator {
    private static boolean isBetter(BytesRef value, BytesRef otherValue) {
        return value.compareTo(otherValue) > 0;
    }

    public static SingleState initSingle(DriverContext driverContext) {
        return new SingleState(driverContext.breaker());
    }

    public static void combine(SingleState state, BytesRef value) {
        state.add(value);
    }

    public static void combineIntermediate(SingleState state, BytesRef value, boolean seen) {
        if (seen) {
            combine(state, value);
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(GroupingState state, int groupId, BytesRef value) {
        state.add(groupId, value);
    }

    public static void combineIntermediate(GroupingState state, int groupId, BytesRef value, boolean seen) {
        if (seen) {
            state.add(groupId, value);
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(selected, ctx.driverContext());
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final BytesRefArrayState internalState;

        private GroupingState(BigArrays bigArrays, CircuitBreaker breaker) {
            this.internalState = new BytesRefArrayState(bigArrays, breaker, "max_bytes_ref_grouping_aggregator");
        }

        public void add(int groupId, BytesRef value) {
            if (internalState.hasValue(groupId) == false || isBetter(value, internalState.get(groupId))) {
                internalState.set(groupId, value);
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            internalState.toIntermediate(blocks, offset, selected, driverContext);
        }

        Block toBlock(IntVector selected, DriverContext driverContext) {
            return internalState.toValuesBlock(selected, driverContext);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            internalState.enableGroupIdTracking(seen);
        }

        @Override
        public void close() {
            Releasables.close(internalState);
        }
    }

    public static class SingleState implements AggregatorState {
        private final BreakingBytesRefBuilder internalState;
        private boolean seen;

        private SingleState(CircuitBreaker breaker) {
            this.internalState = new BreakingBytesRefBuilder(breaker, "max_bytes_ref_aggregator");
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

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = driverContext.blockFactory().newConstantBytesRefBlockWith(internalState.bytesRefView(), 1);
            blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(seen, 1);
        }

        Block toBlock(DriverContext driverContext) {
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
}
