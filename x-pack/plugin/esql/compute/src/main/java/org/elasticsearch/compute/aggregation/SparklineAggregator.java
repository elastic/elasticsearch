/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.data.sort.LongLongBucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparklineAggregator {
    private static final int LIMIT = 1000; // TODO: What value should this be/where should it be set?

    public static SingleState initSingle(
        BigArrays bigArrays,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        AggregatorFunctionSupplier supplier
    ) {
        return new SingleState(bigArrays, dateBucketRounding, minDate, maxDate, supplier);
    }

    public static void combine(DriverContext driverContext, SingleState state, long trendValue, long dateValue) {
        state.add(driverContext, trendValue, dateValue);
    }

    public static void combineIntermediate(DriverContext driverContext, SingleState state, CompositeBlock aggregateIntermediateStates) {
        for (int i = 0; i < aggregateIntermediateStates.getBlockCount(); i++) {
            CompositeBlock compositeBlockForAggregate = aggregateIntermediateStates.getBlock(i);
            assert compositeBlockForAggregate.getBlockCount() > 1;
            LongVectorBlock dateBucketBlock = compositeBlockForAggregate.getBlock(0);
            long dateBucket = dateBucketBlock.getLong(0);

            // TODO: Add helper function to CompositeBlock to retrieve subset of block array to replace this
            Block[] aggregateIntermediateStateBlocks = new Block[compositeBlockForAggregate.getBlockCount() - 1];
            for (int j = 1; j < compositeBlockForAggregate.getBlockCount(); j++) {
                aggregateIntermediateStateBlocks[j - 1] = compositeBlockForAggregate.getBlock(j);
            }
            state.addIntermediate(driverContext, dateBucket, aggregateIntermediateStateBlocks);
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext, driverContext.blockFactory());
    }

    public static GroupingState initGrouping(
        BigArrays bigArrays,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        AggregatorFunctionSupplier supplier
    ) {
        return new GroupingState(bigArrays, dateBucketRounding, minDate, maxDate, supplier);
    }

    public static void combine(DriverContext driverContext, GroupingState state, int groupId, long trendValue, long dateValue) {
        state.add(driverContext, groupId, trendValue, dateValue);
    }

    public static void combineIntermediate(
        DriverContext driverContext,
        GroupingState state,
        int groupId,
        CompositeBlock aggregateIntermediateStates,
        int position
    ) {
        // TODO: Fix this implementation by including intermediate state data for all groups in toIntermediate below
        // Then adjust this function to properly retrieve the intermediate state data for each group
        /*int start = aggregateIntermediateStates.getFirstValueIndex(position);
        int end = start + aggregateIntermediateStates.getValueCount(position);
        for (int i = 0; i < end; i++) {
            CompositeBlock blockForAggregate = aggregateIntermediateStates.getBlock(i);
            assert blockForAggregate.getBlockCount() > 1;
            LongVectorBlock dateBucketBlock = blockForAggregate.getBlock(0);
            long dateBucket = dateBucketBlock.getLong(0);
            Block[] aggregateIntermediateStateBlocks = new Block[blockForAggregate.getBlockCount() - 1];
            for (int j = 1; j < blockForAggregate.getBlockCount(); j++) {
                aggregateIntermediateStateBlocks[j - 1] = blockForAggregate.getBlock(j);
            }
            state.addIntermediate(driverContext, groupId, dateBucket, aggregateIntermediateStateBlocks);
        }*/
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.driverContext(), ctx.blockFactory(), selected);
    }

    public static class GroupingState implements GroupingAggregatorState {
        // TODO: What datatypes should these actually be using?
        ObjectArray<Map<Long, AggregatorFunction>> aggregatorsByGroup;
        Rounding.Prepared dateBucketRounding;
        long minDate;
        long maxDate;
        AggregatorFunctionSupplier supplier;

        private GroupingState(
            BigArrays bigArrays,
            Rounding.Prepared dateBucketRounding,
            long minDate,
            long maxDate,
            AggregatorFunctionSupplier supplier
        ) {
            this.aggregatorsByGroup = bigArrays.newObjectArray(1);
            this.dateBucketRounding = dateBucketRounding;
            this.minDate = minDate;
            this.maxDate = maxDate;
            this.supplier = supplier;
        }

        public void add(DriverContext driverContext, int groupId, long trend, long date) {
            if (groupId >= aggregatorsByGroup.size()) {
                aggregatorsByGroup = driverContext.bigArrays().resize(aggregatorsByGroup, groupId + 1);
            }

            Map<Long, AggregatorFunction> aggregatorsForGroup = this.aggregatorsByGroup.get(groupId);
            AggregatorFunction aggregatorForDateBucket;
            if (aggregatorsForGroup == null) {
                aggregatorsForGroup = new HashMap<>();
                aggregatorForDateBucket = supplier.aggregator(driverContext, List.of(0, 1));
                aggregatorsForGroup.put(dateBucketRounding.round(date), aggregatorForDateBucket);
                this.aggregatorsByGroup.set(groupId, aggregatorsForGroup);
            } else {
                if (aggregatorsForGroup.containsKey(dateBucketRounding.round(date)) == false) {
                    aggregatorForDateBucket = supplier.aggregator(driverContext, List.of(0, 1));
                    aggregatorsForGroup.put(dateBucketRounding.round(date), aggregatorForDateBucket);
                } else {
                    aggregatorForDateBucket = aggregatorsForGroup.get(dateBucketRounding.round(date));
                }
            }
            aggregatorForDateBucket.addRawInput(
                new org.elasticsearch.compute.data.Page(1, driverContext.blockFactory().newLongBlockBuilder(1).appendLong(trend).build()),
                driverContext.blockFactory().newBooleanVectorBuilder(1).appendBoolean(true).build()
            );
        }

        public void addIntermediate(DriverContext driverContext, int groupId, long date, Block[] blocks) {
            if (groupId >= aggregatorsByGroup.size()) {
                aggregatorsByGroup = driverContext.bigArrays().resize(aggregatorsByGroup, groupId + 1);
            }

            Map<Long, AggregatorFunction> aggregatorsForGroup = this.aggregatorsByGroup.get(groupId);
            if (aggregatorsForGroup == null) {
                aggregatorsForGroup = new HashMap<>();
                this.aggregatorsByGroup.set(groupId, aggregatorsForGroup);
            }
            AggregatorFunction agg = aggregatorsForGroup.get(date);
            if (agg == null) {
                agg = supplier.aggregator(driverContext, List.of(0, 1));
                aggregatorsForGroup.put(date, agg);
            }

            agg.addIntermediateInput(new org.elasticsearch.compute.data.Page(1, blocks));
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            // TODO: Update this to handle grouping as this only handles single grouping for now.
            Map<Long, AggregatorFunction> aggregatorsForGroup = this.aggregatorsByGroup.get(0);
            if (aggregatorsForGroup == null || aggregatorsForGroup.isEmpty()) {
                throw new ElasticsearchStatusException("Missing data for group id 0", RestStatus.INTERNAL_SERVER_ERROR);
            }

            Block[] aggregatorBlocks = new Block[aggregatorsForGroup.size()];
            int aggregatorBlocksOffset = 0;
            for (Map.Entry<Long, AggregatorFunction> aggForDateBucket : aggregatorsForGroup.entrySet()) {
                Block[] aggBlocks = new Block[aggForDateBucket.getValue().intermediateBlockCount() + 1];
                aggBlocks[0] = driverContext.blockFactory().newConstantLongBlockWith(aggForDateBucket.getKey(), 1);
                aggForDateBucket.getValue().evaluateIntermediate(aggBlocks, 1, driverContext);
                CompositeBlock compositeBlock = new CompositeBlock(aggBlocks);
                aggregatorBlocks[aggregatorBlocksOffset] = compositeBlock;
                aggregatorBlocksOffset++;
            }
            CompositeBlock aggregatorBlocksComposite = new CompositeBlock(aggregatorBlocks);
            blocks[offset] = aggregatorBlocksComposite;
        }

        Block toBlock(DriverContext driverContext, BlockFactory blockFactory, IntVector selected) {
            LongLongBucketedSort sort = new LongLongBucketedSort(driverContext.blockFactory().bigArrays(), SortOrder.ASC, LIMIT);

            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                Map<Long, AggregatorFunction> aggregatorsForGroup = this.aggregatorsByGroup.get(groupId);
                if (aggregatorsForGroup == null) {
                    throw new ElasticsearchStatusException("Missing data for group id " + groupId, RestStatus.INTERNAL_SERVER_ERROR);
                }

                long currentDateBucket = dateBucketRounding.round(minDate);
                while (currentDateBucket <= dateBucketRounding.round(maxDate)) {
                    AggregatorFunction agg = aggregatorsForGroup.get(currentDateBucket);
                    if (agg == null) {
                        sort.collect(currentDateBucket, 0L, groupId);
                    } else {
                        Block[] blocks = new Block[agg.intermediateBlockCount()];
                        agg.evaluateFinal(blocks, 0, driverContext);
                        Block resultBlock = blocks[0];
                        LongVector resultVector = ((LongBlock) resultBlock).asVector();
                        if (resultVector == null) {
                            throw new ElasticsearchStatusException(
                                "Unexpected null result for group id " + groupId + " and date bucket " + currentDateBucket,
                                RestStatus.INTERNAL_SERVER_ERROR
                            );
                        } else {
                            sort.collect(currentDateBucket, resultVector.getLong(0), groupId);
                        }
                    }
                    currentDateBucket = dateBucketRounding.nextRoundingValue(currentDateBucket);
                }
            }

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
        public void close() {}
    }

    public static class SingleState implements AggregatorState {
        private final GroupingState internalState;

        private SingleState(
            BigArrays bigArrays,
            Rounding.Prepared dateBucketRounding,
            long minDate,
            long maxDate,
            AggregatorFunctionSupplier supplier
        ) {
            this.internalState = new GroupingState(bigArrays, dateBucketRounding, minDate, maxDate, supplier);
        }

        public void add(DriverContext driverContext, long trend, long date) {
            internalState.add(driverContext, 0, trend, date);
        }

        public void addIntermediate(DriverContext driverContext, long date, Block[] blocks) {
            internalState.addIntermediate(driverContext, 0, date, blocks);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            try (IntVector intValues = driverContext.blockFactory().newConstantIntVector(0, 1)) {
                internalState.toIntermediate(blocks, offset, intValues, driverContext);
            }
        }

        Block toBlock(DriverContext driverContext, BlockFactory blockFactory) {
            try (IntVector intValues = blockFactory.newConstantIntVector(0, 1)) {
                return internalState.toBlock(driverContext, blockFactory, intValues);
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(internalState);
        }
    }
}
