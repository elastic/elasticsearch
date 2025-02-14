/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregates field values for booleans.
 */
@Aggregator({ @IntermediateState(name = "values", type = "BOOLEAN_BLOCK") })
@GroupingAggregator
class ValuesBooleanAggregator {
    public static SingleState initSingle() {
        return new SingleState();
    }

    public static void combine(SingleState state, boolean v) {
        if (v) {
            state.seenTrue = true;
        } else {
            state.seenFalse = true;
        }
    }

    public static void combineIntermediate(SingleState state, BooleanBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getBoolean(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, boolean v) {
        long index = ((long) groupId) << 1 | (v ? 1 : 0);
        state.values.set(index);
    }

    public static void combineIntermediate(GroupingState state, int groupId, BooleanBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getBoolean(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        long stateOffset = ((long) statePosition) << 1;
        boolean seenFalse = state.values.get(stateOffset);
        boolean seenTrue = state.values.get(stateOffset | 1);

        if (seenFalse) {
            combine(current, currentGroupId, false);
        }
        if (seenTrue) {
            combine(current, currentGroupId, true);
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private boolean seenFalse;
        private boolean seenTrue;

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (seenFalse == false && seenTrue == false) {
                return blockFactory.newConstantNullBlock(1);
            }
            try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(2)) {
                builder.beginPositionEntry();
                if (seenFalse) {
                    builder.appendBoolean(false);
                }
                if (seenTrue) {
                    builder.appendBoolean(true);
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public void close() {}
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final BitArray values;

        private GroupingState(BigArrays bigArrays) {
            values = new BitArray(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    long index = ((long) selectedGroup) << 1;
                    boolean seenFalse = values.get(index);
                    boolean seenTrue = values.get(index | 1);
                    if (seenFalse) {
                        if (seenTrue) {
                            builder.beginPositionEntry();
                            builder.appendBoolean(false);
                            builder.appendBoolean(true);
                            builder.endPositionEntry();
                        } else {
                            builder.appendBoolean(false);
                        }
                    } else {
                        if (seenTrue) {
                            builder.appendBoolean(true);
                        } else {
                            builder.appendNull();
                        }
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we don't need to track which values have been seen because we don't do anything special for groups without values
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(values);
        }
    }
}
