/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.TimeSeriesGroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.ValuesBooleanGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesBytesRefGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ValuesLongGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.WindowGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.BytesRefLongBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * A specialized version of {@link HashAggregationOperator} that aggregates time-series aggregations from time-series sources.
 */
public class TimeSeriesAggregationOperator extends HashAggregationOperator {

    public record Factory(
        Rounding.Prepared timeBucket,
        boolean dateNanos,
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            // TODO: use TimeSeriesBlockHash when possible
            return new TimeSeriesAggregationOperator(
                timeBucket,
                dateNanos ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
                aggregators,
                () -> BlockHash.build(
                    groups,
                    driverContext.blockFactory(),
                    maxPageSize,
                    true // we can enable optimizations as the inputs are vectors
                ),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "TimeSeriesAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private final Rounding.Prepared timeBucket;
    private final DateFieldMapper.Resolution timeResolution;
    private ExpandingGroups expandingGroups = null;

    public TimeSeriesAggregationOperator(
        Rounding.Prepared timeBucket,
        DateFieldMapper.Resolution timeResolution,
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        super(aggregators, blockHash, driverContext);
        this.timeBucket = timeBucket;
        this.timeResolution = timeResolution;
    }

    @Override
    public void finish() {
        expandWindowBuckets();
        super.finish();
    }

    private long largestWindowMillis() {
        long largestWindow = Long.MIN_VALUE;
        for (GroupingAggregator aggregator : aggregators) {
            if (aggregator.aggregatorFunction() instanceof WindowGroupingAggregatorFunction aggregatorFunction) {
                largestWindow = Math.max(largestWindow, aggregatorFunction.window().toMillis());
            }
        }
        return largestWindow;
    }

    private void expandWindowBuckets() {
        for (GroupingAggregator aggregator : aggregators) {
            if (aggregator.mode().isOutputPartial()) {
                return;
            }
        }
        long windowMillis = largestWindowMillis();
        if (windowMillis <= 0) {
            return;
        }
        BytesRefLongBlockHash tsBlockHash = (BytesRefLongBlockHash) blockHash;
        long startingGroupId = tsBlockHash.numGroups();
        if (startingGroupId == 0) {
            return;
        }
        this.expandingGroups = new ExpandingGroups(driverContext.bigArrays(), startingGroupId);
        for (long groupId = 0; groupId < startingGroupId; groupId++) {
            long tsid = tsBlockHash.getBytesRefKeyFromGroup(groupId);
            long endTimestamp = tsBlockHash.getLongKeyFromGroup(groupId);
            long bucket = Math.max(endTimestamp - timeResolution.convert(largestWindowMillis()), tsBlockHash.getMinLongKey());
            while ((bucket = timeBucket.nextRoundingValue(bucket)) < endTimestamp) {
                if (tsBlockHash.addGroup(tsid, bucket) >= 0) {
                    expandingGroups.addGroup(Math.toIntExact(groupId));
                }
            }
        }
    }

    @Override
    protected void evaluateAggregator(
        GroupingAggregator aggregator,
        Block[] blocks,
        int offset,
        IntVector selected,
        GroupingAggregatorEvaluationContext evaluationContext
    ) {
        if (expandingGroups != null && expandingGroups.count > 0 && isValuesAggregator(aggregator)) {
            try (var valuesSelected = selectedForValuesAggregator(driverContext.blockFactory(), selected, expandingGroups)) {
                super.evaluateAggregator(aggregator, blocks, offset, valuesSelected, evaluationContext);
            }
        } else {
            super.evaluateAggregator(aggregator, blocks, offset, selected, evaluationContext);
        }
    }

    private static IntVector selectedForValuesAggregator(BlockFactory blockFactory, IntVector selected, ExpandingGroups expandingGroups) {
        try (var builder = blockFactory.newIntVectorFixedBuilder(selected.getPositionCount())) {
            int first = selected.getPositionCount() - expandingGroups.count;
            for (int i = 0; i < first; i++) {
                builder.appendInt(i, selected.getInt(i));
            }
            for (int i = 0; i < expandingGroups.count; i++) {
                builder.appendInt(first + i, expandingGroups.getGroup(i));
            }
            return builder.build();
        }
    }

    private static boolean isValuesAggregator(GroupingAggregator aggregator) {
        GroupingAggregatorFunction fn = aggregator.aggregatorFunction();
        return fn instanceof ValuesBooleanGroupingAggregatorFunction
            || fn instanceof ValuesBytesRefGroupingAggregatorFunction
            || fn instanceof ValuesIntGroupingAggregatorFunction
            || fn instanceof ValuesLongGroupingAggregatorFunction
            || fn instanceof ValuesDoubleGroupingAggregatorFunction
            || fn instanceof DimensionValuesByteRefGroupingAggregatorFunction;
    }

    @Override
    protected GroupingAggregatorEvaluationContext evaluationContext(BlockHash blockHash, Block[] keys) {
        if (keys.length < 2) {
            return super.evaluationContext(blockHash, keys);
        }
        final BytesRefLongBlockHash hash = (BytesRefLongBlockHash) blockHash;

        final LongBlock timestamps = keys[0].elementType() == ElementType.LONG ? (LongBlock) keys[0] : (LongBlock) keys[1];
        // block hash so that we can look key
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return timeResolution.roundDownToMillis(timestamps.getLong(groupId));
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return timeResolution.roundDownToMillis(timeBucket.nextRoundingValue(timestamps.getLong(groupId)));
            }

            @Override
            public List<Integer> groupIdsFromWindow(int startingGroupId, Duration window) {
                long ordinal = hash.getBytesRefKeyFromGroup(startingGroupId);
                long startTimestamp = hash.getLongKeyFromGroup(startingGroupId);
                List<Integer> results = new ArrayList<>();
                results.add(startingGroupId);
                long endTimestamp = startTimestamp + timeResolution.convert(window.toMillis());
                long nextTimestamp = startTimestamp;
                while ((nextTimestamp = timeBucket.nextRoundingValue(nextTimestamp)) < endTimestamp) {
                    long nextGroupId = hash.getGroupId(ordinal, nextTimestamp);
                    if (nextGroupId != -1) {
                        results.add(Math.toIntExact(nextGroupId));
                    }
                }
                assert nextTimestamp == endTimestamp : "expected to end at the original timestamp bucket";
                return results;
            }
        };
    }

    static class ExpandingGroups extends AbstractRefCounted implements Releasable {
        private final BigArrays bigArrays;
        private IntArray newGroups;
        final long startingGroupId;
        private int count;

        ExpandingGroups(BigArrays bigArrays, long startingGroupId) {
            this.bigArrays = bigArrays;
            this.startingGroupId = startingGroupId;
            this.newGroups = bigArrays.newIntArray(128);
        }

        void addGroup(int groupId) {
            newGroups = bigArrays.grow(newGroups, count + 1);
            newGroups.set(count++, groupId);
        }

        int getGroup(int index) {
            return newGroups.get(index);
        }

        @Override
        protected void closeInternal() {
            newGroups.close();
        }

        @Override
        public void close() {
            decRef();
        }
    }

    @Override
    public void close() {
        Releasables.close(expandingGroups, super::close);
    }
}
