/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.RawBucketedSort;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Aggregates the top N field values for long.
 */
@Aggregator({ @IntermediateState(name = "topValuesList", type = "LONG_BLOCK") })
@GroupingAggregator
class TopValuesListLongAggregator {
    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, long v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, LongBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, long v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getLong(i));
        }
    }

    public static void combineStates(GroupingState current, int groupId, GroupingState state, int statePosition) {
        current.merge(groupId, state, statePosition);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class GroupingState implements Releasable {
        private final RawBucketedSort.ForLongs sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
            this.sort = new RawBucketedSort.ForLongs(bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
        }

        public void add(int groupId, long value) {
            sort.collect(groupId, value);
        }

        public void merge(int groupId, GroupingState other, int otherGroupId) {
            // TODO: Make a merge method in RawBucketedSort?
            other.sort.getValues(otherGroupId).forEach(value -> sort.collect(groupId, value));
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);

                    var values = sort.getValues(selectedGroup);

                    if (values.isEmpty()) {
                        builder.appendNull();
                        continue;
                    }

                    if (values.size() == 1) {
                        builder.appendLong(values.get(0));
                        continue;
                    }

                    builder.beginPositionEntry();

                    values.forEach(builder::appendLong);

                    builder.endPositionEntry();
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(sort);
        }
    }

    public static class SingleState implements Releasable {
        private GroupingState internalState;

        private SingleState(BigArrays bigArrays, int limit, boolean ascending) {
            this.internalState = new GroupingState(bigArrays, limit, ascending);
        }

        public void add(long value) {
            internalState.add(0, value);
        }

        public void merge(GroupingState other) {
            internalState.merge(0, other, 0);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
                var values = internalState.sort.getValues(0);

                if (values.isEmpty()) {
                    builder.appendNull();
                } else if (values.size() == 1) {
                    builder.appendLong(values.get(0));
                } else {
                    builder.beginPositionEntry();

                    values.forEach(builder::appendLong);

                    builder.endPositionEntry();
                }

                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(internalState);
        }
    }
}
