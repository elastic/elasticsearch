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
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.sort.$Name$BucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;
// end generated imports

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
$if(BytesRef || Ip)$
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
$if(BytesRef || Ip)$
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

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final $Name$BucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
$if(BytesRef)$
            // TODO pass the breaker in from the DriverContext
            CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
            this.sort = new BytesRefBucketedSort(breaker, "top", bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
$else$
            this.sort = new $Name$BucketedSort(bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
$endif$
        }

        public void add(int groupId, $type$ value) {
            sort.collect(value, groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            return sort.toBlock(blockFactory, selected);
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

        public void add($type$ value) {
            internalState.add(0, value);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
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
