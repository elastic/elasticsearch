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
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.sort.BytesRefBucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Aggregates the top N field values for BytesRef.
 * <p>
 *     This class is generated. Edit `X-TopAggregator.java.st` to edit this file.
 * </p>
 */
@Aggregator({ @IntermediateState(name = "top", type = "BYTES_REF_BLOCK") })
@GroupingAggregator
class TopBytesRefAggregator {
    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, BytesRef v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        var scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            combine(state, values.getBytesRef(i, scratch));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, BytesRef v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, BytesRefBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        var scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getBytesRef(i, scratch));
        }
    }

    public static void combineStates(GroupingState current, int groupId, GroupingState state, int statePosition) {
        current.merge(groupId, state, statePosition);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class GroupingState implements Releasable {
        private final BytesRefBucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
            // TODO pass the breaker in from the DriverContext
            CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
            this.sort = new BytesRefBucketedSort(breaker, "top", bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
        }

        public void add(int groupId, BytesRef value) {
            sort.collect(value, groupId);
        }

        public void merge(int groupId, GroupingState other, int otherGroupId) {
            sort.merge(groupId, other.sort, otherGroupId);
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            return sort.toBlock(blockFactory, selected);
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
        private final GroupingState internalState;

        private SingleState(BigArrays bigArrays, int limit, boolean ascending) {
            this.internalState = new GroupingState(bigArrays, limit, ascending);
        }

        public void add(BytesRef value) {
            internalState.add(0, value);
        }

        public void merge(GroupingState other) {
            internalState.merge(0, other, 0);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
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
