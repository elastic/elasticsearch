/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.sort.LongLongBucketedSort;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Aggregator({ @IntermediateState(name = "date", type = "LONG_BLOCK"), @IntermediateState(name = "trend", type = "LONG_BLOCK") })
@GroupingAggregator
class SparklineLongAggregator {
    private static final int LIMIT = 1000; // TODO: What value should this be/where should it be set?

    public static SingleState initSingle(
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        AggregatorFunctionSupplier supplier
    ) {
        return new SingleState(dateBucketRounding, minDate, maxDate, supplier);
    }

    public static void combine(SingleState state, long trendValue, long dateValue) {
        state.add(trendValue, dateValue);
    }

    public static void combineIntermediate(SingleState state, LongBlock dateValues, LongBlock trendValues) {
        int start = dateValues.getFirstValueIndex(0);
        int end = start + dateValues.getValueCount(0);
        for (int i = start; i < end; i++) {
            state.add(trendValues.getLong(i), dateValues.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext, driverContext.blockFactory());
    }

    public static GroupingState initGrouping(
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        AggregatorFunctionSupplier supplier
    ) {
        return new GroupingState(dateBucketRounding, minDate, maxDate, supplier);
    }

    public static void combine(GroupingState state, int groupId, long trendValue, long dateValue) {
        state.add(groupId, trendValue, dateValue);
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongBlock dateValues, LongBlock trendValues, int position) {
        int start = dateValues.getFirstValueIndex(position);
        int end = start + dateValues.getValueCount(position);
        for (int i = start; i < end; i++) {
            state.add(groupId, trendValues.getLong(i), dateValues.getLong(i));
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toBlock(ctx.driverContext(), ctx.blockFactory(), selected);
    }

    private record DateTrendPair(long date, long trend) {}

    public static class GroupingState implements GroupingAggregatorState {
        // TODO: What datatypes should these actually be using?
        // Maybe LongLongBucketedSort but this would require converting to blocks to read the data
        // when we want to run the logic in toBlock
        Map<Integer, List<DateTrendPair>> dateTrendPairs;
        Rounding.Prepared dateBucketRounding;
        long minDate;
        long maxDate;
        AggregatorFunctionSupplier supplier;

        private GroupingState(Rounding.Prepared dateBucketRounding, long minDate, long maxDate, AggregatorFunctionSupplier supplier) {
            this.dateTrendPairs = new HashMap<>();
            this.dateBucketRounding = dateBucketRounding;
            this.minDate = minDate;
            this.maxDate = maxDate;
            this.supplier = supplier;
        }

        public void add(int groupId, long trend, long date) {
            if (dateTrendPairs.containsKey(groupId)) {
                dateTrendPairs.get(groupId).add(new DateTrendPair(date, trend));
            } else {
                List<DateTrendPair> list = new ArrayList<>();
                list.add(new DateTrendPair(date, trend));
                dateTrendPairs.put(groupId, list);
            }
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            LongLongBucketedSort bucketedSort = new LongLongBucketedSort(driverContext.blockFactory().bigArrays(), SortOrder.ASC, LIMIT);
            for (Map.Entry<Integer, List<DateTrendPair>> entry : dateTrendPairs.entrySet()) {
                for (DateTrendPair dateTrendPair : entry.getValue()) {
                    bucketedSort.collect(dateTrendPair.date(), dateTrendPair.trend(), entry.getKey());
                }
            }
            bucketedSort.toBlocks(driverContext.blockFactory(), blocks, offset, selected);
        }

        Block toBlock(DriverContext driverContext, BlockFactory blockFactory, IntVector selected) {
            LongLongBucketedSort sort = new LongLongBucketedSort(driverContext.blockFactory().bigArrays(), SortOrder.ASC, LIMIT);

            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                if (dateTrendPairs.containsKey(groupId) == false) {
                    // TODO: Improve error handling here.
                    throw new ElasticsearchStatusException("Missing data for group id " + groupId, RestStatus.INTERNAL_SERVER_ERROR);
                }
                Map<Long, List<Long>> dateBucketedTrendValues = generateBucketData(dateTrendPairs.get(groupId));
                Map<Long, List<Long>> dateBucketedTrendValuesWithZeroBuckets = generateEmptyBuckets(dateBucketedTrendValues);
                addDateBucketedValuesToSort(sort, groupId, driverContext, blockFactory, dateBucketedTrendValuesWithZeroBuckets);
            }

            Block[] blocks = new Block[2];
            sort.toBlocks(blockFactory, blocks, 0, selected);
            Releasables.close(blocks[0]);
            return blocks[1];
        }

        private Map<Long, List<Long>> generateBucketData(List<DateTrendPair> dateTrendPairs) {
            Map<Long, List<Long>> result = new HashMap<>();
            for (DateTrendPair dateTrendPair : dateTrendPairs) {
                long dateBucket = dateBucketRounding.round(dateTrendPair.date());
                if (result.containsKey(dateBucket)) {
                    result.get(dateBucket).add(dateTrendPair.trend());
                } else {
                    List<Long> list = new ArrayList<>();
                    list.add(dateTrendPair.trend());
                    result.put(dateBucket, list);
                }
            }

            return result;
        }

        private Map<Long, List<Long>> generateEmptyBuckets(Map<Long, List<Long>> dateToTrendMap) {
            long currentBucket = dateBucketRounding.round(minDate);
            while (currentBucket <= dateBucketRounding.round(maxDate)) {
                if (dateToTrendMap.containsKey(currentBucket) == false) {
                    List<Long> zeroValueList = new ArrayList<>();
                    dateToTrendMap.put(currentBucket, zeroValueList);
                }
                currentBucket = dateBucketRounding.nextRoundingValue(currentBucket);
            }

            return dateToTrendMap;
        }

        private void addDateBucketedValuesToSort(
            LongLongBucketedSort sort,
            int groupId,
            DriverContext driverContext,
            BlockFactory blockFactory,
            Map<Long, List<Long>> dateToTrendMap
        ) {
            // TODO: What should booleanVector actually be set to?
            BooleanVector booleanVector = blockFactory.newBooleanVectorBuilder(1).appendBoolean(true).build();
            for (Map.Entry<Long, List<Long>> entry : dateToTrendMap.entrySet()) {
                // TODO: How many channels are actually needed?
                AggregatorFunction agg = supplier.aggregator(driverContext, List.of(0, 1));
                for (Long trendValue : entry.getValue()) {
                    // TODO: Is this the best way to pass values to the underlying aggregater?
                    agg.addRawInput(
                        new org.elasticsearch.compute.data.Page(1, blockFactory.newLongBlockBuilder(1).appendLong(trendValue).build()),
                        booleanVector
                    );
                }
                Block[] blocks = new Block[agg.intermediateBlockCount()];
                agg.evaluateFinal(blocks, 0, driverContext);
                Block resultBlock = blocks[0];
                LongVector resultVector = ((LongBlock) resultBlock).asVector();
                if (resultVector == null) {
                    sort.collect(entry.getKey(), 0L, groupId);
                } else {
                    sort.collect(entry.getKey(), resultVector.getLong(0), groupId);
                }
            }
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

        private SingleState(Rounding.Prepared dateBucketRounding, long minDate, long maxDate, AggregatorFunctionSupplier supplier) {
            this.internalState = new GroupingState(dateBucketRounding, minDate, maxDate, supplier);
        }

        public void add(long trend, long date) {
            internalState.add(0, trend, date);
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
