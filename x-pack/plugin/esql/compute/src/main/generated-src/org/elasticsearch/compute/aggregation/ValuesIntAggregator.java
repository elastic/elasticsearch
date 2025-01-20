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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

/**
 * Aggregates field values for int.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "INT_BLOCK") })
@GroupingAggregator
class ValuesIntAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, int v) {
        state.values.add(v);
    }

    public static void combineIntermediate(SingleState state, IntBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getInt(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, int v) {
        /*
         * Encode the groupId and value into a single long -
         * the top 32 bits for the group, the bottom 32 for the value.
         */
        state.values.add((((long) groupId) << Integer.SIZE) | (v & 0xFFFFFFFFL));
    }

    public static void combineIntermediate(GroupingState state, int groupId, IntBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getInt(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        for (int id = 0; id < state.values.size(); id++) {
            long both = state.values.get(id);
            int group = (int) (both >>> Integer.SIZE);
            if (group == statePosition) {
                int value = (int) both;
                combine(current, currentGroupId, value);
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements Releasable {
        private final LongHash values;

        private SingleState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantIntBlockWith((int) values.get(0), 1);
            }
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendInt((int) values.get(id));
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
    public static class GroupingState implements Releasable {
        private final LongHash values;

        private GroupingState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    /*
                     * Count can effectively be in three states - 0, 1, many. We use those
                     * states to buffer the first value, so we can avoid calling
                     * beginPositionEntry on single valued fields.
                     */
                    int count = 0;
                    int first = 0;
                    for (int id = 0; id < values.size(); id++) {
                        long both = values.get(id);
                        int group = (int) (both >>> Integer.SIZE);
                        if (group == selectedGroup) {
                            int value = (int) both;
                            switch (count) {
                                case 0 -> first = value;
                                case 1 -> {
                                    builder.beginPositionEntry();
                                    builder.appendInt(first);
                                    builder.appendInt(value);
                                }
                                default -> builder.appendInt(value);
                            }
                            count++;
                        }
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendInt(first);
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            values.close();
        }
    }
}
