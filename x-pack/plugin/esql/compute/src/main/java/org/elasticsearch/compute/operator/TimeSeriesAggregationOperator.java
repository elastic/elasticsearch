/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.hppc.LongLongHashMap;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.TimeSeriesGroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.WindowGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * A specialized version of {@link HashAggregationOperator} that aggregates time-series aggregations from time-series sources.
 */
public class TimeSeriesAggregationOperator extends HashAggregationOperator {

    /**
     * Default target rows per output page when chunking the operator's output, i.e. the default of the
     * {@code esql.time_series.target_chunk_rows} setting. The operator slices its single emitted result into pages of
     * about this many rows, bounding the size of each page. The same target applies to both partial/intermediate output
     * (bounding pages sent to the coordinator) and final output (bounding the coordinator's peak memory during final
     * evaluation and the page sizes handed to downstream operators). A {@code _tsid} may straddle a page boundary; the
     * coordinator re-merges groups by key.
     */
    public static final int DEFAULT_TARGET_CHUNK_ROWS = 100_000;

    /**
     * @param targetChunkRows target number of rows per output page when chunking output. The operator slices its emitted
     *        result into pages of about this many rows. The same target applies to both partial/intermediate and final
     *        output.
     */
    public record Factory(
        Rounding.Prepared timeBucket,
        boolean dateNanos,
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int aggregationBatchSize,
        int targetChunkRows
    ) implements OperatorFactory {

        public Factory(
            Rounding.Prepared timeBucket,
            boolean dateNanos,
            List<BlockHash.GroupSpec> groups,
            AggregatorMode aggregatorMode,
            List<GroupingAggregator.Factory> aggregators,
            int aggregationBatchSize
        ) {
            this(timeBucket, dateNanos, groups, aggregatorMode, aggregators, aggregationBatchSize, Integer.MAX_VALUE);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            final boolean outputFinal = aggregatorMode.isOutputPartial() == false;
            return new TimeSeriesAggregationOperator(
                timeBucket,
                dateNanos ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
                aggregatorMode,
                aggregators,
                () -> {
                    // Use TimeSeriesBlockHash for groups over the [tsid, timestamp] pair, to reduce the group overhead.
                    if (groups.size() == 2) {
                        var g1 = groups.get(0);
                        var g2 = groups.get(1);
                        if (g1.elementType() == ElementType.BYTES_REF && g2.elementType() == ElementType.LONG) {
                            return new TimeSeriesBlockHash(g1.channel(), g2.channel(), false, outputFinal, driverContext.blockFactory());
                        } else if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.BYTES_REF) {
                            return new TimeSeriesBlockHash(g2.channel(), g1.channel(), true, outputFinal, driverContext.blockFactory());
                        }
                    }
                    // Broken optimizations are allowed as the inputs are vectors.
                    return BlockHash.build(groups, driverContext.blockFactory(), aggregationBatchSize, true);
                },
                targetChunkRows,
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
    private int numGroupsBeforeExpanding = -1;

    public TimeSeriesAggregationOperator(
        Rounding.Prepared timeBucket,
        DateFieldMapper.Resolution timeResolution,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        int targetChunkRows,
        DriverContext driverContext
    ) {
        super(aggregatorMode, aggregators, blockHash, Integer.MAX_VALUE, 1.0, targetChunkRows, null, driverContext);
        this.timeBucket = timeBucket;
        this.timeResolution = timeResolution;
    }

    @Override
    public void finish() {
        expandWindowBuckets();
        super.finish();
    }

    @Override
    protected boolean shouldEmitPartialResultsPeriodically() {
        return false;
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
     * While `bucket=1m` and `window=3m` without expanding:
     * ```
     * TS ...
     * | WHERE TRANGE('2025-04-15T01:12:00Z', '2025-04-15T01:17:00Z')
     * | STATS sum(sum_over_time(metric, 3m)) BY cluster, TBUCKET(1minute)
     * ```
     * Yields:
     * ```
     * cluster | bucket                 | SUM  |
     * prod    | 2025-04-15T01:12:00Z   | 100  |
     * prod    | 2025-04-15T01:14:00Z   | 200  |
     * ```
     *
     * The correct result should be as if we evaluate each bucket over the raw input:
     * ```
     * cluster | bucket                 | SUM  |
     * prod    | 2025-04-15T01:12:00Z   | 100  |
     * prod    | 2025-04-15T01:13:00Z   | 100  |
     * prod    | 2025-04-15T01:14:00Z   | 300  |
     * prod    | 2025-04-15T01:15:00Z   | 200  |
     * prod    | 2025-04-15T01:16:00Z   | 200  |
     * ```
     *
     * In order to achieve this, we need to materialize the later buckets whose window still includes
     * `timestamp` during the aggregation phase, so that the within time-series aggregation produces:
     * ```
     * _tsid | VALUES(cluster) | BUCKET                 | SUM_OVER_TIME |
     * t1    | prod            | 2025-04-15T01:12:00Z   | 100           |
     * t1    | prod            | 2025-04-15T01:13:00Z   | 100           |
     * t1    | prod            | 2025-04-15T01:14:00Z   | 100           |
     * t2    | prod            | 2025-04-15T01:14:00Z   | 200           |
     * t2    | prod            | 2025-04-15T01:15:00Z   | 200           |
     * t2    | prod            | 2025-04-15T01:16:00Z   | 200           |
     * ```
     */
    private void expandWindowBuckets() {
        if (aggregatorMode.isOutputPartial()) {
            return;
        }
        final long windowMillis = largestWindowMillis();
        if (windowMillis <= 0) {
            return;
        }
        if (blockHash instanceof TimeSeriesBlockHash == false) {
            return;
        }
        TimeSeriesBlockHash tsBlockHash = (TimeSeriesBlockHash) blockHash;
        final long numGroups = tsBlockHash.numGroups();
        if (numGroups == 0) {
            return;
        }
        Rounding.Prepared optimizedTimeBucket = optimizeRoundingForTimeRange(tsBlockHash.minTimestamp(), tsBlockHash.maxTimestamp());
        // The hash keys are in the time resolution of the timestamp field (nanoseconds for date_nanos), while the
        // roundings operate on milliseconds. Iterate the buckets in the millisecond domain and convert the resulting
        // labels back to the hash resolution when creating groups.
        long maxBoundMillis = timeResolution.roundDownToMillis(tsBlockHash.maxTimestamp());
        this.numGroupsBeforeExpanding = Math.toIntExact(numGroups);
        // the extra groups have not been seen by any aggregator; tell them so a group whose window turns out to be
        // completely empty evaluates to null instead of the aggregator's default value
        var seenGroups = new SeenGroupIds.Range(0, numGroupsBeforeExpanding);
        for (GroupingAggregator aggregator : aggregators) {
            aggregator.aggregatorFunction().selectedMayContainUnseenGroups(seenGroups);
        }
        this.expandingGroups = new ExpandingGroups(driverContext.bigArrays());
        for (long groupId = 0; groupId < numGroups; groupId++) {
            int tsid = tsBlockHash.tsidForGroup(groupId);
            long startMillis = timeResolution.roundDownToMillis(tsBlockHash.timestampForGroup(groupId));
            long effectiveEndMillis = Math.min(startMillis + windowMillis, maxBoundMillis + 1);
            long bucketMillis = optimizedTimeBucket.nextRoundingValue(startMillis);
            // Fill the missing buckets between (timestamp - window, timestamp).
            while (bucketMillis < effectiveEndMillis) {
                if (tsBlockHash.addExtraGroup(tsid, timeResolution.convert(bucketMillis)) >= 0) {
                    expandingGroups.addGroup(Math.toIntExact(groupId));
                }
                bucketMillis = optimizedTimeBucket.nextRoundingValue(bucketMillis);
            }
        }
    }

    @Override
    protected IntVector customizeSelected(GroupingAggregator aggregator, IntVector selected) {
        var function = aggregator.aggregatorFunction();
        if (expandingGroups != null && expandingGroups.count > 0 && isValuesAggregator(function)) {
            return selectedForValuesAggregator(driverContext.blockFactory(), selected);
        }
        return super.customizeSelected(aggregator, selected);
    }

    private IntVector selectedForValuesAggregator(BlockFactory blockFactory, IntVector selected) {
        try (var builder = blockFactory.newIntVectorFixedBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                if (groupId < numGroupsBeforeExpanding) {
                    builder.appendInt(i, groupId);
                } else {
                    builder.appendInt(i, expandingGroups.getGroup(groupId - numGroupsBeforeExpanding));
                }
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
        "org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction",
        "org.elasticsearch.compute.aggregation.PackDimsGroupingAggregatorFunction"
    );

    static boolean isValuesAggregator(GroupingAggregatorFunction aggregatorFunction) {
        return VALUES_CLASSES.contains(aggregatorFunction.getClass().getName());
    }

    @Override
    protected GroupingAggregatorEvaluationContext evaluationContext(BlockHash blockHash) {
        if (blockHash instanceof TimeSeriesBlockHash tsBlockHash) {
            return evaluationContext(tsBlockHash);
        }
        return super.evaluationContext(blockHash);
    }

    private GroupingAggregatorEvaluationContext evaluationContext(TimeSeriesBlockHash tsBlockHash) {
        Rounding.Prepared fastRounding = optimizeRoundingForTimeRange(tsBlockHash.minTimestamp(), tsBlockHash.maxTimestamp());
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            IntArray prevGroupIds;
            IntArray nextGroupIds;

            @Override
            public long rangeStartInMillis(int groupId) {
                return fastRounding.roundingFloor(timeResolution.roundDownToMillis(tsBlockHash.timestampForGroup(groupId)));
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return fastRounding.roundingCeiling(timeResolution.roundDownToMillis(tsBlockHash.timestampForGroup(groupId)));
            }

            @Override
            public void forEachGroupInRange(int startingGroupId, long rangeStartMillis, long rangeEndMillis, IntConsumer action) {
                int tsid = tsBlockHash.tsidForGroup(startingGroupId);
                long minMillis = timeResolution.roundDownToMillis(tsBlockHash.minTimestamp());
                var iterator = fastRounding.iterator(Math.max(rangeStartMillis, minMillis), rangeEndMillis);
                while (iterator.next()) {
                    if (iterator.getRoundedFloor() >= rangeStartMillis) {
                        // the hash keys are in the resolution of the timestamp field, the iterator yields milliseconds
                        long groupId = tsBlockHash.getGroupId(tsid, timeResolution.convert(iterator.getRounded()));
                        if (groupId != -1 && groupId != startingGroupId) {
                            action.accept(Math.toIntExact(groupId));
                        }
                    }
                }
            }

            @Override
            public int previousGroupId(int currentGroupId) {
                return prevGroupIds.get(currentGroupId);
            }

            @Override
            public int nextGroupId(int currentGroupId) {
                return nextGroupIds.get(currentGroupId);
            }

            @Override
            public void computeAdjacentGroupIds() {
                if (nextGroupIds != null) {
                    return;
                }
                long numGroups = tsBlockHash.numGroups();
                nextGroupIds = driverContext.bigArrays().newIntArray(numGroups);
                nextGroupIds.fill(0, numGroups, -1);
                prevGroupIds = driverContext.bigArrays().newIntArray(numGroups);
                prevGroupIds.fill(0, numGroups, -1);
                LongLongHashMap nextTimestamps = new LongLongHashMap(); // cached the rounded up timestamps
                for (int groupId = 0; groupId < numGroups; groupId++) {
                    long tsid = tsBlockHash.tsidForGroup(groupId);
                    long bucketTs = tsBlockHash.timestampForGroup(groupId);
                    int cacheIndex = nextTimestamps.indexOf(bucketTs);
                    long nextBucketTs;
                    if (cacheIndex >= 0) {
                        nextBucketTs = nextTimestamps.indexGet(cacheIndex);
                    } else {
                        // both the map and the hash keys are in the resolution of the timestamp field, the rounding
                        // operates on milliseconds
                        nextBucketTs = timeResolution.convert(fastRounding.nextRoundingValue(timeResolution.roundDownToMillis(bucketTs)));
                        nextTimestamps.put(bucketTs, nextBucketTs);
                    }
                    int nextGroupId = Math.toIntExact(tsBlockHash.getGroupId(tsid, nextBucketTs));
                    if (nextGroupId >= 0) {
                        // https://github.com/elastic/elasticsearch/issues/152758
                        assert tsBlockHash.tsidForGroup(nextGroupId) == tsid
                            : "adjacent groups must share the same tsid: group "
                                + groupId
                                + " (tsid="
                                + tsid
                                + ") -> nextGroup "
                                + nextGroupId
                                + " (tsid="
                                + tsBlockHash.tsidForGroup(nextGroupId)
                                + ")";
                        assert tsBlockHash.timestampForGroup(nextGroupId) == nextBucketTs
                            : "next group timestamp mismatch: expected "
                                + nextBucketTs
                                + " but group "
                                + nextGroupId
                                + " has "
                                + tsBlockHash.timestampForGroup(nextGroupId);
                        assert prevGroupIds.get(nextGroupId) == -1
                            : "prevGroupIds["
                                + nextGroupId
                                + "] already set to "
                                + prevGroupIds.get(nextGroupId)
                                + " when linking from group "
                                + groupId;
                        nextGroupIds.set(groupId, nextGroupId);
                        prevGroupIds.set(nextGroupId, groupId);
                    }
                }
            }

            @Override
            public void close() {
                Releasables.close(nextGroupIds, prevGroupIds, super::close);
            }
        };
    }

    /**
     * When running queries from timezones with daylight savings, we by default use the slow JavaTime-based rounding,
     * because when collecting the partial results we initially have no information about the time range covered.
     * As soon as we have the actual populated groups and their timestamps, we can optimize the rounding in case
     * it does not intersect with any daylight savings transition.
     */
    private Rounding.Prepared optimizeRoundingForTimeRange(long minTimestamp, long maxTimestamp) {
        if (minTimestamp <= maxTimestamp) {
            long startMillis = timeResolution.roundDownToMillis(minTimestamp);
            long endMillis = timeResolution.roundUpToMillis(maxTimestamp);
            return timeBucket.getUnprepared().prepare(startMillis, endMillis);
        } else {
            return timeBucket;
        }
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
