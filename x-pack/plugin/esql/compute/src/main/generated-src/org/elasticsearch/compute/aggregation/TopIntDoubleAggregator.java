/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.sort.IntDoubleBucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;
// end generated imports

/**
 * Aggregates the top N field values for int.
 * <p>
 *     This class is generated. Edit `X-TopAggregator.java.st` to edit this file.
 * </p>
 */
@Aggregator({ @IntermediateState(name = "top", type = "INT_BLOCK"), @IntermediateState(name = "output", type = "DOUBLE_BLOCK") })
@GroupingAggregator
class TopIntDoubleAggregator {
    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, int v, double outputValue) {
        state.add(v, outputValue);
    }

    public static void combineIntermediate(SingleState state, IntBlock values, DoubleBlock outputValues) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getInt(i), outputValues.getDouble(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, int v, double outputValue) {
        state.add(groupId, v, outputValue);
    }

    public static void combineIntermediate(GroupingState state, int groupId, IntBlock values, DoubleBlock outputValues, int position) {
        int start = values.getFirstValueIndex(position);
        int end = start + values.getValueCount(position);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getInt(i), outputValues.getDouble(i));
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final IntDoubleBucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
            this.sort = new IntDoubleBucketedSort(bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
        }

        public void add(int groupId, int value, double outputValue) {
            sort.collect(value, outputValue, groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            sort.toBlocks(driverContext.blockFactory(), blocks, offset, selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            Block[] blocks = new Block[2];
            sort.toBlocks(blockFactory, blocks, 0, selected);
            Releasables.close(blocks[0]);
            return blocks[1];
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(sort);
        }
    }

    public static class SingleState implements AggregatorState {
        private final GroupingState internalState;

        private SingleState(BigArrays bigArrays, int limit, boolean ascending) {
            this.internalState = new GroupingState(bigArrays, limit, ascending);
        }

        public void add(int value, double outputValue) {
            internalState.add(0, value, outputValue);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            try (var intValues = driverContext.blockFactory().newConstantIntVector(0, 1)) {
                internalState.toIntermediate(blocks, offset, intValues, driverContext);
            }
        }

        Block toBlock(BlockFactory blockFactory) {
            try (var intValues = blockFactory.newConstantIntVector(0, 1)) {
                return internalState.toBlock(blockFactory, intValues);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(internalState);
        }
    }
}
