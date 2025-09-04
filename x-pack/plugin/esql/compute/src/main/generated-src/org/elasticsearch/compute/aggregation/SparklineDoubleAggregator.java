/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.sort.DoubleIndirectBucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// end generated imports

/**
 * Aggregates field values for double.
 * This class is generated. Edit @{code X-SparklineAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "DOUBLE_BLOCK"), @IntermediateState(name = "timestamps", type = "LONG_BLOCK") })
@GroupingAggregator
class SparklineDoubleAggregator {

    private static class TimestampValueComparator implements Comparator<Tuple<Long, Double>> {

        private final SortOrder order;

        public TimestampValueComparator(SortOrder order) {
            this.order = order;
        }

        @Override
        public int compare(Tuple<Long, Double> lhs, Tuple<Long, Double> rhs) {
            if (Long.compare(lhs.v1(), rhs.v1()) != 0) {
                return order.reverseMul() * Long.compare(lhs.v1(), rhs.v1());
            }
            return order.reverseMul() * Double.compare(lhs.v2(), rhs.v2());
        }
    }

    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, double value, long timestamp) {
        state.add(timestamp, value);
    }

    public static void combineIntermediate(SingleState state, DoubleBlock values, LongBlock timestamps) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getDouble(i), timestamps.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, double value, long timestamp) {
        state.add(groupId, timestamp, value);
    }

    public static void combineIntermediate(GroupingState state, int groupId, DoubleBlock values, LongBlock timestamps, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getDouble(i), timestamps.getLong(i));
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final DoubleIndirectBucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
            Comparator<Tuple<Long, Double>> comparator = Comparator.<Tuple<Long, Double>>comparingLong(Tuple::v1).thenComparing(Tuple::v2);
            this.sort = new DoubleIndirectBucketedSort(bigArrays, comparator, limit);
        }

        public void add(int groupId, long sortValue, double indirectValue) {
            sort.collect(Tuple.tuple(sortValue, indirectValue), groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            sort.toBlocks(driverContext.blockFactory(), blocks, offset, selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            return sort.toIndirectBlock(blockFactory, selected);
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

        public void add(long sortValue, double indirectValue) {
            internalState.add(0, sortValue, indirectValue);
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
