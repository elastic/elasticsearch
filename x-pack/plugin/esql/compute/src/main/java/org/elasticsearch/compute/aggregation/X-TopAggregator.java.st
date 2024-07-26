/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

$if(Ip)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
$if(!long)$
import org.elasticsearch.compute.data.$Type$Block;
$endif$
import org.elasticsearch.compute.data.IntVector;
$if(long)$
import org.elasticsearch.compute.data.$Type$Block;
$endif$
import org.elasticsearch.compute.data.sort.$Name$BucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Aggregates the top N field values for $type$.
 * <p>
 *     This class is generated. Edit `X-TopAggregator.java.st` to edit this file.
 * </p>
 */
@Aggregator({ @IntermediateState(name = "top", type = "$TYPE$_BLOCK") })
@GroupingAggregator
class Top$Name$Aggregator {
    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, $type$ v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, $Type$Block values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
$if(Ip)$
        var scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            combine(state, values.get$Type$(i, scratch));
        }
$else$
        for (int i = start; i < end; i++) {
            combine(state, values.get$Type$(i));
        }
$endif$
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, $type$ v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, $Type$Block values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
$if(Ip)$
        var scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.get$Type$(i, scratch));
        }
$else$
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.get$Type$(i));
        }
$endif$
    }

    public static void combineStates(GroupingState current, int groupId, GroupingState state, int statePosition) {
        current.merge(groupId, state, statePosition);
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class GroupingState implements Releasable {
        private final $Name$BucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
            this.sort = new $Name$BucketedSort(bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
        }

        public void add(int groupId, $type$ value) {
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

        public void add($type$ value) {
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
