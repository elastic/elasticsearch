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
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.TimeSeriesGroupingAggregatorEvaluationContext;
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
import java.util.Set;
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

    /*
     * Expands window buckets to ensure all required time buckets are present for time-series aggregations.
     * This is equivalent to sliding the window over the raw input.
     *
     * For example, given these two data points:
     * ```
     * |_tsid| cluster| host | timestamp            | metric |
     * | t1  | prod   | h1   | 2025-04-15T01:12:00Z | 100    |
     * | t2  | prod   | h2   | 2025-04-15T01:14:00Z | 200    |
     * ```
     * Without expanding, the within time-series aggregation yields:
     * ```
     * _tsid | VALUES(cluster) | BUCKET                 | SUM_OVER_TIME |
     * t1    | prod            | 2025-04-15T01:12:00Z   | 100           |
     * t2    | prod            | 2025-04-15T01:14:00Z   | 200           |
     * ```
     * And the final result is:
     * ```
     * cluster | bucket                 | SUM  |
     * prod    | 2025-04-15T01:12:00Z   | 100  |
     * prod    | 2025-04-15T01:14:00Z   | 200  |
     * ```
     *
     * While `bucket=5s` and no window:
     * ```
     * TS ...
     * | WHERE TRANGE('2025-04-15T01:10:00Z', '2025-04-15T01:15:00Z')
     * | STATS sum(sum_over_time(metric)) BY host, TBUCKET(5s)
     * ```
     * Yields:
     * ```
     * cluster | bucket                 | SUM  |
     * prod    | 2025-04-15T01:10:00Z   | 300  |
     * ```
     *
     * The correct result should be as if we slide over the raw input:
     * ```
     * cluster | bucket                 | SUM  |
     * prod    | 2025-04-15T01:10:00Z   | 300  |
     * prod    | 2025-04-15T01:11:00Z   | 300  |
     * prod    | 2025-04-15T01:12:00Z   | 300  |
     * prod    | 2025-04-15T01:13:00Z   | 200  |
     * prod    | 2025-04-15T01:14:00Z   | 200  |
     * ```
     *
     * In order to achieve this, we need to fill in the missing buckets between (timestamp-window, timestamp)
     * during the aggregation phase, so that the within time-series aggregation produces:
     * ```
     * _tsid |VALUES(cluster)  | BUCKET                 | SUM_OVER_TIME |
     * t1    | prod            | 2025-04-15T01:10:00Z   | 100           |
     * t1    | prod            | 2025-04-15T01:11:00Z   | 100           |
     * t1    | prod            | 2025-04-15T01:12:00Z   | 100           |
     * t2    | prod            | 2025-04-15T01:10:00Z   | 200           |
     * t2    | prod            | 2025-04-15T01:11:00Z   | 200           |
     * t2    | prod            | 2025-04-15T01:12:00Z   | 200           |
     * t2    | prod            | 2025-04-15T01:13:00Z   | 200           |
     * t2    | prod            | 2025-04-15T01:14:00Z   | 200           |
     * ```
     */
    private void expandWindowBuckets() {
        for (GroupingAggregator aggregator : aggregators) {
            if (aggregator.mode().isOutputPartial()) {
                return;
            }
        }
        final long windowMillis = largestWindowMillis();
        if (windowMillis <= 0) {
            return;
        }
        BytesRefLongBlockHash tsBlockHash = (BytesRefLongBlockHash) blockHash;
        final long numGroups = tsBlockHash.numGroups();
        if (numGroups == 0) {
            return;
        }
        this.expandingGroups = new ExpandingGroups(driverContext.bigArrays());
        for (long groupId = 0; groupId < numGroups; groupId++) {
            long tsid = tsBlockHash.getBytesRefKeyFromGroup(groupId);
            long endTimestamp = tsBlockHash.getLongKeyFromGroup(groupId);
            long bucket = timeBucket.nextRoundingValue(endTimestamp - timeResolution.convert(largestWindowMillis()));
            bucket = Math.max(bucket, tsBlockHash.getMinLongKey());
            // Fill the missing buckets between (timestamp-window, timestamp)
            while (bucket < endTimestamp) {
                if (tsBlockHash.addGroup(tsid, bucket) >= 0) {
                    expandingGroups.addGroup(Math.toIntExact(groupId));
                }
                bucket = timeBucket.nextRoundingValue(bucket);
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
        if (expandingGroups != null && expandingGroups.count > 0 && isValuesAggregator(aggregator.aggregatorFunction())) {
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

    // generated classes are not available during javadoc
    private static final Set<String> VALUES_CLASSES = Set.of(
        "org.elasticsearch.compute.aggregation.ValuesBooleanGroupingAggregatorFunction",
        "org.elasticsearch.compute.aggregation.ValuesBytesRefGroupingAggregatorFunction",
        "org.elasticsearch.compute.aggregation.ValuesIntGroupingAggregatorFunction",
        "org.elasticsearch.compute.aggregation.ValuesLongGroupingAggregatorFunction",
        "org.elasticsearch.compute.aggregation.ValuesDoubleGroupingAggregatorFunction",
        "org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction"
    );

    static boolean isValuesAggregator(GroupingAggregatorFunction aggregatorFunction) {
        return VALUES_CLASSES.contains(aggregatorFunction.getClass().getName());
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
                return timeBucket.nextRoundingValue(timeResolution.roundDownToMillis(timestamps.getLong(groupId)));
            }

            @Override
            public List<Integer> groupIdsFromWindow(int startingGroupId, Duration window) {
                long tsid = hash.getBytesRefKeyFromGroup(startingGroupId);
                long bucket = hash.getLongKeyFromGroup(startingGroupId);
                List<Integer> results = new ArrayList<>();
                results.add(startingGroupId);
                long endTimestamp = bucket + timeResolution.convert(window.toMillis());
                while ((bucket = timeBucket.nextRoundingValue(bucket)) < endTimestamp) {
                    long nextGroupId = hash.getGroupId(tsid, bucket);
                    if (nextGroupId != -1) {
                        results.add(Math.toIntExact(nextGroupId));
                    }
                }
                return results;
            }

            @Override
            public int previousGroupId(int currentGroupId) {
                long tsid = hash.getBytesRefKeyFromGroup(currentGroupId);
                long currentBucketTs = timestamps.getLong(currentGroupId);
                long nextBucketTs = timeBucket.nextRoundingValue(currentBucketTs);
                long previousBucketTS = currentBucketTs - (nextBucketTs - currentBucketTs);
                return Math.toIntExact(hash.getGroupId(tsid, previousBucketTS));
            }

            @Override
            public int nextGroupId(int currentGroupId) {
                long tsid = hash.getBytesRefKeyFromGroup(currentGroupId);
                long currentBucketTs = timestamps.getLong(currentGroupId);
                long nextBucketTs = timeBucket.nextRoundingValue(currentBucketTs);
                return Math.toIntExact(hash.getGroupId(tsid, nextBucketTs));
            }
        };
    }

    static class ExpandingGroups extends AbstractRefCounted implements Releasable {
        private final BigArrays bigArrays;
        private IntArray newGroups;
        private int count;

        ExpandingGroups(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
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
