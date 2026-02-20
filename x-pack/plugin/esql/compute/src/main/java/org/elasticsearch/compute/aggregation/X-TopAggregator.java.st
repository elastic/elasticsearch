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
$if(hasOutputField)$
import org.elasticsearch.compute.data.$OutputFieldType$Block;
$endif$
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.sort.$Name$$OutputFieldName$BucketedSort;
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
$if(hasOutputField)$
@Aggregator({ @IntermediateState(name = "top", type = "$TYPE$_BLOCK"), @IntermediateState(name = "output", type = "$OutputFieldTYPE$_BLOCK") })
$else$
@Aggregator({ @IntermediateState(name = "top", type = "$TYPE$_BLOCK") })
$endif$
@GroupingAggregator
class Top$Name$$OutputFieldName$Aggregator {
    public static SingleState initSingle(BigArrays bigArrays, int limit, boolean ascending) {
        return new SingleState(bigArrays, limit, ascending);
    }

    public static void combine(SingleState state, $type$ v$if(hasOutputField)$, $OutputFieldtype$ outputValue$endif$) {
        state.add(v$if(hasOutputField)$, outputValue$endif$);
    }

    public static void combineIntermediate(SingleState state, $Type$Block values$if(hasOutputField)$, $OutputFieldType$Block outputValues$endif$) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
$if(BytesRef || Ip)$
        var scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            combine(state, values.get$Type$(i, scratch)$if(hasOutputField)$, outputValues.get$OutputFieldType$(i)$endif$);
        }
$else$
        for (int i = start; i < end; i++) {
            combine(state, values.get$Type$(i)$if(hasOutputField)$, outputValues.get$OutputFieldType$(i)$endif$);
        }
$endif$
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays, int limit, boolean ascending) {
        return new GroupingState(bigArrays, limit, ascending);
    }

    public static void combine(GroupingState state, int groupId, $type$ v$if(hasOutputField)$, $OutputFieldtype$ outputValue$endif$) {
        state.add(groupId, v$if(hasOutputField)$, outputValue$endif$);
    }

    public static void combineIntermediate(GroupingState state, int groupId, $Type$Block values, $if(hasOutputField)$$OutputFieldType$Block outputValues, $endif$int position) {
        int start = values.getFirstValueIndex(position);
        int end = start + values.getValueCount(position);
$if(BytesRef || Ip)$
        var scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.get$Type$(i, scratch)$if(hasOutputField)$, outputValues.get$OutputFieldType$(i)$endif$);
        }
$else$
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.get$Type$(i)$if(hasOutputField)$, outputValues.get$OutputFieldType$(i)$endif$);
        }
$endif$
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.blockFactory(), selected);
    }

    public static class GroupingState implements GroupingAggregatorState {
        private final $Name$$OutputFieldName$BucketedSort sort;

        private GroupingState(BigArrays bigArrays, int limit, boolean ascending) {
$if(BytesRef)$
            // TODO pass the breaker in from the DriverContext
            CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
            this.sort = new BytesRefBucketedSort(breaker, "top", bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
$else$
            this.sort = new $Name$$OutputFieldName$BucketedSort(bigArrays, ascending ? SortOrder.ASC : SortOrder.DESC, limit);
$endif$
        }

        public void add(int groupId, $type$ value$if(hasOutputField)$, $OutputFieldtype$ outputValue$endif$) {
            sort.collect(value, $if(hasOutputField)$outputValue, $endif$groupId);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
$if(hasOutputField)$
            sort.toBlocks(driverContext.blockFactory(), blocks, offset, selected);
$else$
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
$endif$
        }

$if(hasOutputField)$
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            Block[] blocks = new Block[2];
            sort.toBlocks(blockFactory, blocks, 0, selected);
            Releasables.close(blocks[0]);
            return blocks[1];
        }

$else$
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            return sort.toBlock(blockFactory, selected);
        }
$endif$

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

        public void add($type$ value$if(hasOutputField)$, $OutputFieldtype$ outputValue$endif$) {
            internalState.add(0, value$if(hasOutputField)$, outputValue$endif$);
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
