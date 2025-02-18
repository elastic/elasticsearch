/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Aggregates field values for float.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "FLOAT_BLOCK") })
@GroupingAggregator
class ValuesFloatAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, float v) {
        state.values.add(Float.floatToIntBits(v));
    }

    public static void combineIntermediate(SingleState state, FloatBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getFloat(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, float v) {
        /*
         * Encode the groupId and value into a single long -
         * the top 32 bits for the group, the bottom 32 for the value.
         */
        state.values.add((((long) groupId) << Float.SIZE) | (Float.floatToIntBits(v) & 0xFFFFFFFFL));
    }

    public static void combineIntermediate(GroupingState state, int groupId, FloatBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getFloat(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        for (int id = 0; id < state.values.size(); id++) {
            long both = state.values.get(id);
            int group = (int) (both >>> Float.SIZE);
            if (group == statePosition) {
                float value = Float.intBitsToFloat((int) both);
                combine(current, currentGroupId, value);
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private final LongHash values;

        private SingleState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantFloatBlockWith(Float.intBitsToFloat((int) values.get(0)), 1);
            }
            try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendFloat(Float.intBitsToFloat((int) values.get(id)));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public void close() {
            values.close();
        }
    }

    /**
     * State for a grouped {@code VALUES} aggregation. This implementation
     * emphasizes collect-time performance over the performance of rendering
     * results. That's good, but it's a pretty intensive emphasis, requiring
     * an {@code O(n^2)} operation for collection to support a {@code O(1)}
     * collector operation. But at least it's fairly simple.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final LongHash values;

        private GroupingState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    /*
                     * Count can effectively be in three states - 0, 1, many. We use those
                     * states to buffer the first value, so we can avoid calling
                     * beginPositionEntry on single valued fields.
                     */
                    int count = 0;
                    float first = 0;
                    for (int id = 0; id < values.size(); id++) {
                        long both = values.get(id);
                        int group = (int) (both >>> Float.SIZE);
                        if (group == selectedGroup) {
                            float value = Float.intBitsToFloat((int) both);
                            switch (count) {
                                case 0 -> first = value;
                                case 1 -> {
                                    builder.beginPositionEntry();
                                    builder.appendFloat(first);
                                    builder.appendFloat(value);
                                }
                                default -> builder.appendFloat(value);
                            }
                            count++;
                        }
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendFloat(first);
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            values.close();
        }
    }
}
