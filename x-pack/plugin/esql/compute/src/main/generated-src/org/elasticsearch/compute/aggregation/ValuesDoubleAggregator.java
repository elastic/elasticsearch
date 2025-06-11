/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Aggregates field values for double.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "DOUBLE_BLOCK") })
@GroupingAggregator
class ValuesDoubleAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, double v) {
        state.values.add(Double.doubleToLongBits(v));
    }

    public static void combineIntermediate(SingleState state, DoubleBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getDouble(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, double v) {
        state.values.add(groupId, Double.doubleToLongBits(v));
    }

    public static void combineIntermediate(GroupingState state, int groupId, DoubleBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getDouble(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        for (int id = 0; id < state.values.size(); id++) {
            if (state.values.getKey1(id) == statePosition) {
                double value = Double.longBitsToDouble(state.values.getKey2(id));
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
                return blockFactory.newConstantDoubleBlockWith(Double.longBitsToDouble(values.get(0)), 1);
            }
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendDouble(Double.longBitsToDouble(values.get(id)));
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
        private final LongLongHash values;

        private GroupingState(BigArrays bigArrays) {
            values = new LongLongHash(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        /**
         * Builds a {@link Block} with the unique values collected for the {@code #selected}
         * groups. This is the implementation of the final and intermediate results of the agg.
         */
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }

            long selectedCountsSize = 0;
            long idsSize = 0;
            try {
                /*
                 * Get a count of all groups less than the maximum selected group. Count
                 * *downwards* so that we can flip the sign on all of the actually selected
                 * groups. Negative values in this array are always unselected groups.
                 */
                int selectedCountsLen = selected.max() + 1;
                long adjust = RamUsageEstimator.alignObjectSize(
                    RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + selectedCountsLen * Integer.BYTES
                );
                blockFactory.adjustBreaker(adjust);
                selectedCountsSize = adjust;
                int[] selectedCounts = new int[selectedCountsLen];
                for (int id = 0; id < values.size(); id++) {
                    int group = (int) values.getKey1(id);
                    if (group < selectedCounts.length) {
                        selectedCounts[group]--;
                    }
                }

                /*
                 * Total the selected groups and turn the counts into the start index into a sort-of
                 * off-by-one running count. It's really the number of values that have been inserted
                 * into the results before starting on this group. Unselected groups will still
                 * have negative counts.
                 *
                 * For example, if
                 * | Group | Value Count | Selected |
                 * |-------|-------------|----------|
                 * |     0 | 3           | <-       |
                 * |     1 | 1           | <-       |
                 * |     2 | 2           |          |
                 * |     3 | 1           | <-       |
                 * |     4 | 4           | <-       |
                 *
                 * Then the total is 9 and the counts array will contain 0, 3, -2, 4, 5
                 */
                int total = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int count = -selectedCounts[group];
                    selectedCounts[group] = total;
                    total += count;
                }

                /*
                 * Build a list of ids to insert in order *and* convert the running
                 * count in selectedCounts[group] into the end index (exclusive) in
                 * ids for each group.
                 * Here we use the negative counts to signal that a group hasn't been
                 * selected and the id containing values for that group is ignored.
                 *
                 * For example, if
                 * | Group | Value Count | Selected |
                 * |-------|-------------|----------|
                 * |     0 | 3           | <-       |
                 * |     1 | 1           | <-       |
                 * |     2 | 2           |          |
                 * |     3 | 1           | <-       |
                 * |     4 | 4           | <-       |
                 *
                 * Then the total is 9 and the counts array will start with 0, 3, -2, 4, 5.
                 * The counts will end with 3, 4, -2, 5, 9.
                 */
                adjust = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + total * Integer.BYTES);
                blockFactory.adjustBreaker(adjust);
                idsSize = adjust;
                int[] ids = new int[total];
                for (int id = 0; id < values.size(); id++) {
                    int group = (int) values.getKey1(id);
                    if (group < selectedCounts.length && selectedCounts[group] >= 0) {
                        ids[selectedCounts[group]++] = id;
                    }
                }
                return buildOutputBlock(blockFactory, selected, selectedCounts, ids);
            } finally {
                blockFactory.adjustBreaker(-selectedCountsSize - idsSize);
            }
        }

        Block buildOutputBlock(BlockFactory blockFactory, IntVector selected, int[] selectedCounts, int[] ids) {
            /*
             * Insert the ids in order.
             */
            try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(selected.getPositionCount())) {
                int start = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int end = selectedCounts[group];
                    int count = end - start;
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> append(builder, ids[start]);
                        default -> {
                            builder.beginPositionEntry();
                            for (int i = start; i < end; i++) {
                                append(builder, ids[i]);
                            }
                            builder.endPositionEntry();
                        }
                    }
                    start = end;
                }
                return builder.build();
            }
        }

        private void append(DoubleBlock.Builder builder, int id) {
            double value = Double.longBitsToDouble(values.getKey2(id));
            builder.appendDouble(value);
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
