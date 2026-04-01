/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FirstDocIdGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.TimeSeriesGroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.WindowGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        int aggregationBatchSize,
        Rounding.Prepared outputTimeBucket
    ) implements OperatorFactory {

        public Factory(
            Rounding.Prepared timeBucket,
            boolean dateNanos,
            List<BlockHash.GroupSpec> groups,
            AggregatorMode aggregatorMode,
            List<GroupingAggregator.Factory> aggregators,
            int aggregationBatchSize
        ) {
            this(timeBucket, dateNanos, groups, aggregatorMode, aggregators, aggregationBatchSize, null);
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
                outputTimeBucket,
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
    private final Rounding.Prepared outputTimeBucket;
    private ExpandingGroups expandingGroups = null;
    private int numGroupsBeforeExpanding = -1;

    public TimeSeriesAggregationOperator(
        Rounding.Prepared timeBucket,
        DateFieldMapper.Resolution timeResolution,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        Rounding.Prepared outputTimeBucket,
        DriverContext driverContext
    ) {
        super(aggregatorMode, aggregators, blockHash, Integer.MAX_VALUE, 1.0, Integer.MAX_VALUE, driverContext);
        this.timeBucket = timeBucket;
        this.timeResolution = timeResolution;
        this.outputTimeBucket = outputTimeBucket;
    }

    @Override
    public void finish() {
        expandWindowBuckets();
        super.finish();
    }

    @Override
    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        if (needsOutputFiltering() == false) {
            super.emit();
            return;
        }
        TimeSeriesBlockHash tsBlockHash = (TimeSeriesBlockHash) blockHash;
        int[] alignedPositions = computeOutputAlignedPositions(tsBlockHash);
        if (alignedPositions == null) {
            super.emit();
            return;
        }
        Block[] blocks = null;
        IntVector allSelected = null;
        IntVector outputSelected = null;
        long startInNanos = System.nanoTime();
        boolean success = false;
        try {
            allSelected = blockHash.nonEmpty();

            Block[] fullKeys = blockHash.getKeys(allSelected);
            Block[] outputKeys = new Block[fullKeys.length];
            boolean keysFiltered = false;
            try {
                for (int b = 0; b < fullKeys.length; b++) {
                    outputKeys[b] = fullKeys[b].filter(false, alignedPositions);
                }
                keysFiltered = true;
            } finally {
                Releasables.close(fullKeys);
                if (keysFiltered == false) {
                    Releasables.closeExpectNoException(outputKeys);
                }
            }

            try (var builder = driverContext.blockFactory().newIntVectorFixedBuilder(alignedPositions.length)) {
                for (int i = 0; i < alignedPositions.length; i++) {
                    builder.appendInt(i, alignedPositions[i]);
                }
                outputSelected = builder.build();
            }

            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            blocks = new Block[outputKeys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(outputKeys, 0, blocks, 0, outputKeys.length);
            int offset = outputKeys.length;

            try (var ctx = evaluationContext(blockHash)) {
                if (ctx instanceof TimeSeriesGroupingAggregatorEvaluationContext tsCtx) {
                    tsCtx.setAllGroupIds(allSelected);
                }
                for (int i = 0; i < aggregators.size(); i++) {
                    try (
                        var customizeSelected = customizeSelected(aggregators.get(i), outputSelected);
                        var prepared = aggregators.get(i).prepareForEvaluate(customizeSelected, ctx)
                    ) {
                        prepared.evaluate(blocks, offset, customizeSelected);
                    }
                    offset += aggBlockCounts[i];
                }
                output = ReleasableIterator.single(new Page(blocks));
                success = true;
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            Releasables.close(allSelected, outputSelected);
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
            emitNanos += System.nanoTime() - startInNanos;
            emitCount++;
        }
    }

    private boolean needsOutputFiltering() {
        return aggregatorMode.isOutputPartial() == false && outputTimeBucket != null && blockHash instanceof TimeSeriesBlockHash;
    }

    /**
     * Computes the group IDs that are aligned to the output bucket boundary.
     *
     * @return aligned group IDs, or {@code null} when every group is already aligned (no filtering needed)
     */
    private int[] computeOutputAlignedPositions(TimeSeriesBlockHash tsBlockHash) {
        Rounding.Prepared optimizedOutput = optimizeOutputRoundingForTimeRange(tsBlockHash.minTimestamp(), tsBlockHash.maxTimestamp());
        int totalGroups = Math.toIntExact(tsBlockHash.numGroups());
        int[] positions = new int[Math.min(totalGroups, 16)];
        int idx = 0;
        for (int p = 0; p < totalGroups; p++) {
            long millis = timeResolution.roundDownToMillis(tsBlockHash.timestampForGroup(p));
            if (optimizedOutput.round(millis) == millis) {
                positions = ArrayUtil.grow(positions, idx + 1);
                positions[idx++] = p;
            }
        }
        if (idx == totalGroups) {
            return null;
        }
        return ArrayUtil.copyOfSubArray(positions, 0, idx);
    }

    private Rounding.Prepared optimizeOutputRoundingForTimeRange(long minTimestamp, long maxTimestamp) {
        if (minTimestamp <= maxTimestamp) {
            long startMillis = timeResolution.roundDownToMillis(minTimestamp);
            long endMillis = timeResolution.roundUpToMillis(maxTimestamp);
            return outputTimeBucket.getUnprepared().prepare(startMillis, endMillis);
        } else {
            return outputTimeBucket;
        }
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
        long minBound = tsBlockHash.minTimestamp();
        if (outputTimeBucket != null) {
            minBound = optimizeOutputRoundingForTimeRange(tsBlockHash.minTimestamp(), tsBlockHash.maxTimestamp()).round(minBound);
        }
        this.numGroupsBeforeExpanding = Math.toIntExact(numGroups);
        this.expandingGroups = new ExpandingGroups(driverContext.bigArrays());
        for (long groupId = 0; groupId < numGroups; groupId++) {
            int tsid = tsBlockHash.tsidForGroup(groupId);
            long endTimestamp = tsBlockHash.timestampForGroup(groupId);
            long bucket = optimizedTimeBucket.nextRoundingValue(endTimestamp - timeResolution.convert(largestWindowMillis()));
            bucket = Math.max(bucket, minBound);
            // Fill the missing buckets between (timestamp-window, timestamp)
            while (bucket < endTimestamp) {
                if (tsBlockHash.addExtraGroup(tsid, bucket) >= 0) {
                    expandingGroups.addGroup(Math.toIntExact(groupId));
                }
                bucket = optimizedTimeBucket.nextRoundingValue(bucket);
            }
        }
    }

    @Override
    protected IntVector customizeSelected(GroupingAggregator aggregator, IntVector selected) {
        if (expandingGroups != null && expandingGroups.count > 0 && isValuesAggregator(aggregator.aggregatorFunction())) {
            return selectedForValuesAggregator(driverContext.blockFactory(), selected);
        }
        if (aggregator.aggregatorFunction() instanceof FirstDocIdGroupingAggregatorFunction) {
            return selectedForDocIdsAggregator(driverContext.blockFactory(), selected);
        }
        return super.customizeSelected(aggregator, selected);
    }

    /*
     * The {@link FirstDocIdGroupingAggregatorFunction} collects the first doc id for each group. With time-buckets, the same
     * tsid can appear in multiple groups. When loading the dimension field, this may result in loading multiple documents for
     * the same tsid several times.
     *
     * There are two options:
     * 1. Load only one document per tsid and apply tsid ordinals to the dimension values. This requires a separate value source
     *    reader for dimension fields, or modifying the current reader to understand ordinals and apply them to the dictionary.
     * 2. Remap the group ids in the selected vector so that all groups with the same tsid use a single group id. This means we
     *    may load the same document multiple times for the same tsid, but not different documents. The overhead of loading the
     *    same document multiple times is small compared to loading different documents for the same tsid.

     * This method uses the second option as it is a more contained change.
     *
     * Example:
     *   _tsid key:     [t1, t2, t1, t3, t2]
     *   selected:      [0,  1,  2,  3,  4]
     *   first doc ids: [10, 20, 30, 40, 50]
     *
     *   re-mapped selected:            [0, 1, 0, 3, 1]
     *   first doc ids with re-mapped : [10, 20, 10, 40, 20]
     *   Loading docs: [10, 10, 20, 20, 40], which is not much more expensive than [10, 20, 40].
     */
    private IntVector selectedForDocIdsAggregator(BlockFactory blockFactory, IntVector selected) {
        final TimeSeriesBlockHash tsBlockHash;
        if (blockHash instanceof TimeSeriesBlockHash ts) {
            tsBlockHash = ts;
        } else {
            // Without time bucket, one tsid per group already; no need to re-map
            selected.incRef();
            return selected;
        }
        try (var builder = blockFactory.newIntVectorFixedBuilder(selected.getPositionCount())) {
            int[] firstGroups = new int[selected.getPositionCount() + 1];
            Arrays.fill(firstGroups, -1);
            for (int p = 0; p < selected.getPositionCount(); p++) {
                int groupId = selected.getInt(p);
                int tsidOrdinal = tsBlockHash.tsidForGroup(groupId);
                if (firstGroups.length <= tsidOrdinal) {
                    int prevSize = firstGroups.length;
                    firstGroups = ArrayUtil.grow(firstGroups, tsidOrdinal);
                    Arrays.fill(firstGroups, prevSize, firstGroups.length, -1);
                }
                int first = firstGroups[tsidOrdinal];
                if (first == -1) {
                    first = firstGroups[tsidOrdinal] = groupId;
                }
                builder.appendInt(p, first);
            }
            return builder.build();
        }
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
        "org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction"
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
        Rounding.Prepared optimizedTimeBucket = optimizeRoundingForTimeRange(tsBlockHash.minTimestamp(), tsBlockHash.maxTimestamp());
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            IntArray prevGroupIds;
            IntArray nextGroupIds;

            @Override
            public long rangeStartInMillis(int groupId) {
                return timeResolution.roundDownToMillis(tsBlockHash.timestampForGroup(groupId));
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return optimizedTimeBucket.nextRoundingValue(timeResolution.roundDownToMillis(tsBlockHash.timestampForGroup(groupId)));
            }

            @Override
            public List<Integer> groupIdsFromWindow(int startingGroupId, Duration window) {
                int tsid = tsBlockHash.tsidForGroup(startingGroupId);
                long bucket = tsBlockHash.timestampForGroup(startingGroupId);
                List<Integer> results = new ArrayList<>();
                results.add(startingGroupId);
                long endTimestamp = bucket + timeResolution.convert(window.toMillis());
                while ((bucket = optimizedTimeBucket.nextRoundingValue(bucket)) < endTimestamp) {
                    long nextGroupId = tsBlockHash.getGroupId(tsid, bucket);
                    if (nextGroupId != -1) {
                        results.add(Math.toIntExact(nextGroupId));
                    }
                }
                return results;
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
                Map<Long, Long> nextTimestamps = new HashMap<>(); // cached the rounded up timestamps
                for (int groupId = 0; groupId < numGroups; groupId++) {
                    long tsid = tsBlockHash.tsidForGroup(groupId);
                    long bucketTs = tsBlockHash.timestampForGroup(groupId);
                    long nextBucketTs = nextTimestamps.computeIfAbsent(bucketTs, optimizedTimeBucket::nextRoundingValue);
                    int nextGroupId = Math.toIntExact(tsBlockHash.getGroupId(tsid, nextBucketTs));
                    if (nextGroupId >= 0) {
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
