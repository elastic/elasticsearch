/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.ParallelHashAggregationOperator;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntConsumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RateDoubleGroupingAggregatorFunctionTests extends ComputeTestCase {
    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    public void testAdjacentGroupsAtLookbackLimitAreUsed() {
        DriverContext driverContext = driverContext();
        try (var context = lookbackContext(driverContext, new long[] { 0, 360_000 }, new long[] { 60_000, 420_000 })) {
            assertTrue(AbstractRateGroupingFunction.isPreviousGroupWithinLookback(context, 0, 1));
            assertTrue(AbstractRateGroupingFunction.isNextGroupWithinLookback(context, 0, 1));
            assertEquals(60.0, AbstractRateGroupingFunction.interpolationBoundaryInSeconds(60_000, 60_000), 0.0);
            assertEquals(210.0, AbstractRateGroupingFunction.interpolationBoundaryInSeconds(60_000, 360_000), 0.0);
            assertEquals(0.0005, AbstractRateGroupingFunction.interpolationBoundaryInSeconds(0, 1), 0.0);
        } finally {
            driverContext.finish();
        }
    }

    public void testAdjacentGroupsPastLookbackLimitAreIgnored() {
        DriverContext driverContext = driverContext();
        try (var context = lookbackContext(driverContext, new long[] { 0, 360_001 }, new long[] { 60_000, 420_001 })) {
            assertFalse(AbstractRateGroupingFunction.isPreviousGroupWithinLookback(context, 0, 1));
            assertFalse(AbstractRateGroupingFunction.isNextGroupWithinLookback(context, 0, 1));
            assertFalse(AbstractRateGroupingFunction.isPreviousGroupWithinLookback(context, -1, 1));
            assertFalse(AbstractRateGroupingFunction.isNextGroupWithinLookback(context, 0, -1));
        } finally {
            driverContext.finish();
        }
    }

    public void testFlushOnSliceChanged() {
        DriverContext driverContext = driverContext();
        List<Page> pages = new ArrayList<>();
        int numIntervals = between(1, 10);
        record Interval(long t1, double v1, long t2, double v2) {}
        List<Interval> intervals = new ArrayList<>();
        for (int interval = 0; interval < numIntervals; interval++) {
            int positions = between(1, 100);
            long timestamp = between(1, 1000);
            long value = between(1, 10);
            long[] values = new long[positions];
            long[] timestamps = new long[positions];
            for (int p = 0; p < positions; p++) {
                values[p] = value;
                timestamps[p] = timestamp;
                value += between(1, 10);
                timestamp += between(1, 10);
            }
            intervals.add(new Interval(timestamps[positions - 1], values[positions - 1], timestamps[0], values[0]));
            BlockFactory blockFactory = blockFactory();
            try (
                var valuesBuilder = blockFactory.newDoubleBlockBuilder(positions);
                var timestampsBuilder = blockFactory.newLongBlockBuilder(positions);
            ) {
                var temporalities = blockFactory.newConstantNullBlock(positions);
                for (int p = 0; p < positions; p++) {
                    valuesBuilder.appendDouble(values[positions - p - 1]);
                    timestampsBuilder.appendLong(timestamps[positions - p - 1]);
                }
                pages.add(
                    new Page(
                        blockFactory.newConstantIntBlockWith(0, positions),
                        valuesBuilder.build(),
                        timestampsBuilder.build(),
                        temporalities,
                        blockFactory.newConstantIntBlockWith(interval, positions),
                        blockFactory.newConstantLongBlockWith(Long.MAX_VALUE, positions)
                    )
                );
            }
        }
        // values, timestamps, temporality, slice, future_timestamps
        AggregatorMode aggregatorMode = AggregatorMode.INITIAL;
        var aggregatorFactory = new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, false, TestWarningsSource.INSTANCE)
            .groupingAggregatorFactory(aggregatorMode, List.of(1, 2, 3, 4, 5));
        final List<BlockHash.GroupSpec> groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.INT));
        HashAggregationOperator hashAggregationOperator = new HashAggregationOperator.Builder().mode(aggregatorMode)
            .aggregators(List.of(aggregatorFactory))
            .groups(groupSpecs)
            .aggregationBatchSize(randomIntBetween(1, 1024))
            .partialEmit(Integer.MAX_VALUE, 1.0)
            .maxPageSize(Integer.MAX_VALUE)
            .build()
            .get(driverContext);
        List<Page> outputPages = new ArrayList<>();
        Driver driver = TestDriverFactory.create(
            driverContext,
            new CannedSourceOperator(pages.iterator()),
            List.of(hashAggregationOperator),
            new PageConsumerOperator(outputPages::add)
        );
        new TestDriverRunner().run(driver);
        for (Page out : outputPages) {
            assertThat(out.getPositionCount(), equalTo(1));
            LongBlock timestamps = out.getBlock(1);
            DoubleBlock values = out.getBlock(2);
            assertThat(values.getValueCount(0), equalTo(numIntervals * 2));
            assertThat(timestamps.getValueCount(0), equalTo(numIntervals * 2));
            for (int i = 0; i < numIntervals; i++) {
                Interval interval = intervals.get(i);
                assertThat(timestamps.getLong(2 * i), equalTo(interval.t1));
                assertThat(values.getDouble(2 * i), equalTo(interval.v1));
                assertThat(timestamps.getLong(2 * i + 1), equalTo(interval.t2));
                assertThat(values.getDouble(2 * i + 1), equalTo(interval.v2));
            }
            out.close();
        }
    }

    public void testInvalidTemporalityWarning() {
        BlockFactory blockFactory = blockFactory();
        int positions = 10;

        var values = blockFactory.newConstantDoubleBlockWith(10.0, positions);
        var timestamps = blockFactory.newConstantLongBlockWith(1000, positions);
        var temporalities = blockFactory.newConstantBytesRefBlockWith(new BytesRef("invalid_temporality"), positions);
        var groupIds = blockFactory.newConstantIntBlockWith(0, positions);
        var sliceIndices = blockFactory.newConstantIntBlockWith(0, positions);
        var futureMaxTimestamps = blockFactory.newConstantLongBlockWith(Long.MAX_VALUE, positions);

        Page page = new Page(groupIds, values, timestamps, temporalities, sliceIndices, futureMaxTimestamps);

        var source = new TestWarningsSource("rate(field)");
        DriverContext driverContext = driverContext();
        var aggregator = new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, false, source).groupingAggregator(
            driverContext,
            List.of(1, 2, 3, 4, 5)
        );
        try {
            var addInput = aggregator.prepareProcessRawInputPage(null, page);
            try (var groups = blockFactory.newConstantIntBlockWith(0, positions).asVector()) {
                addInput.add(0, groups);
            }
            addInput.close();
        } finally {
            aggregator.close();
            page.releaseBlocks();
        }

        driverContext.finish();
        assertThat(
            driverContext.warnings(),
            containsInAnyOrder(
                "Line 1:1: evaluation of [rate(field)] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.compute.aggregation.InvalidTemporalityException: "
                    + "Invalid temporality value: [invalid_temporality], expected [cumulative] or [delta]"
            )
        );
    }

    public void testRawTimestampMustBeWithinItsGroupBucket() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = driverContext();
        var aggregator = new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, false, TestWarningsSource.INSTANCE)
            .groupingAggregator(driverContext, List.of(0, 1, 2, 3, 4));
        Page page = new Page(
            blockFactory.newConstantDoubleBlockWith(10.0, 1),
            blockFactory.newConstantLongBlockWith(201, 1),
            blockFactory.newConstantNullBlock(1),
            blockFactory.newConstantIntBlockWith(0, 1),
            blockFactory.newConstantLongBlockWith(Long.MAX_VALUE, 1)
        );
        try (
            var groupIds = blockFactory.newConstantIntBlockWith(0, 1).asVector();
            var context = evaluationContext(driverContext, 100, 200)
        ) {
            try (var addInput = aggregator.prepareProcessRawInputPage(null, page)) {
                addInput.add(0, groupIds);
            }
            AssertionError error = expectThrows(AssertionError.class, () -> aggregator.prepareEvaluateIntermediate(groupIds, context));
            assertThat(error.getMessage(), containsString("raw timestamp 201"));
            assertThat(error.getMessage(), containsString("was assigned to group 0 outside bucket [100, 200]"));
        } finally {
            aggregator.close();
            page.releaseBlocks();
            driverContext.finish();
        }
    }

    public void testReducedStateTimestampsMustBeWithinTheirGroupBucket() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = driverContext();
        var aggregator = new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, false, TestWarningsSource.INSTANCE)
            .groupingAggregator(driverContext, List.of(0, 1, 2, 3));
        final Page page;
        try (var timestamps = blockFactory.newLongBlockBuilder(2); var values = blockFactory.newDoubleBlockBuilder(2)) {
            timestamps.beginPositionEntry();
            timestamps.appendLong(201);
            timestamps.appendLong(150);
            timestamps.endPositionEntry();
            values.beginPositionEntry();
            values.appendDouble(20);
            values.appendDouble(10);
            values.endPositionEntry();
            page = new Page(
                timestamps.build(),
                values.build(),
                blockFactory.newConstantLongBlockWith(2, 1),
                blockFactory.newConstantDoubleBlockWith(0, 1)
            );
        }
        try (
            var groupIds = blockFactory.newConstantIntBlockWith(0, 1).asVector();
            var context = evaluationContext(driverContext, 100, 200)
        ) {
            aggregator.addIntermediateInput(0, groupIds, page);
            AssertionError error = expectThrows(AssertionError.class, () -> aggregator.prepareEvaluateIntermediate(groupIds, context));
            assertThat(error.getMessage(), containsString("lastTs 201 is after bucket end"));
        } finally {
            aggregator.close();
            page.releaseBlocks();
            driverContext.finish();
        }
    }

    public void testSupportPartitioningOnlyWithIntermediateInput() {
        DriverContext driverContext = driverContext();
        try (var agg = supplier(randomBoolean()).groupingAggregator(driverContext, List.of(2, 3, 4, 5))) {
            assertTrue(agg.supportPartitioning(AggregatorMode.FINAL));
            assertTrue(agg.supportPartitioning(AggregatorMode.INTERMEDIATE));
            assertFalse(agg.supportPartitioning(AggregatorMode.INITIAL));
            assertFalse(agg.supportPartitioning(AggregatorMode.SINGLE));
        }
    }

    public void testTimeSeriesPartitioning() {
        assumeTrue("partitioned block hashes are not supported on this platform", PartitionedBlockHash.supportPartitioning());
        int numTsids = between(1024, 4096);
        int bucketsPerTsid = between(1, 4);
        int totalBuckets = numTsids * bucketsPerTsid;
        List<Page> pages = intermediatePages(blockFactory(), numTsids, bucketsPerTsid);
        boolean isRateOverTime = randomBoolean();
        try {
            List<RateResult> singlePass = runFinalAggregation(driverContext(), pages, isRateOverTime, Integer.MAX_VALUE);
            assertThat(singlePass, not(empty()));
            List<RateResult> partitioned = runFinalAggregation(driverContext(), pages, isRateOverTime, between(256, totalBuckets / 2));
            assertThat(partitioned, equalTo(singlePass));
        } finally {
            Releasables.close(pages);
        }
    }

    private static final String WORKER_EXECUTOR = "esql_partition_worker";
    private static final long BUCKET_MILLIS = 60_000;

    private static RateDoubleGroupingAggregatorFunction.FunctionSupplier supplier(boolean isRateOverTime) {
        return new RateDoubleGroupingAggregatorFunction.FunctionSupplier(isRateOverTime, false, TestWarningsSource.INSTANCE);
    }

    record RateResult(String tsid, long timestamp, Object rate) {}

    private List<Page> intermediatePages(BlockFactory blockFactory, int numTsids, int bucketsPerTsid) {
        record Row(int tsid, int bucket, int seq, int intervals) {}
        List<Row> rows = new ArrayList<>();
        for (int tsid = 0; tsid < numTsids; tsid++) {
            for (int bucket = 0; bucket < bucketsPerTsid; bucket++) {
                int intervals = between(1, 3);
                for (int s = 0; s < intervals; s++) {
                    rows.add(new Row(tsid, bucket, s, between(1, 3)));
                }
                // boundary buckets
                if (randomInt(100) < 5) {
                    intervals = between(1, 2);
                    for (int s = 0; s < intervals; s++) {
                        rows.add(new Row(tsid, bucket, s, between(1, 3)));
                    }
                }
            }
        }
        Randomness.shuffle(rows);
        List<Page> pages = new ArrayList<>();
        int from = 0;
        while (from < rows.size()) {
            int to = Math.min(rows.size(), from + between(128, 512));
            int positionCount = to - from;
            try (
                var tsids = blockFactory.newBytesRefVectorBuilder(positionCount);
                var buckets = blockFactory.newLongVectorFixedBuilder(positionCount);
                var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
                var values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
                var sampleCounts = blockFactory.newLongVectorFixedBuilder(positionCount);
                var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
            ) {
                for (int p = 0; p < positionCount; p++) {
                    Row row = rows.get(from + p);
                    tsids.appendBytesRef(new BytesRef("tsid-" + row.tsid + (char) ('!' + row.tsid % 90)));
                    long bucketStart = row.bucket * BUCKET_MILLIS;
                    buckets.appendLong(p, bucketStart);
                    timestamps.beginPositionEntry();
                    values.beginPositionEntry();
                    long samples = 0;
                    for (int i = 0; i < row.intervals; i++) {
                        long firstTs = bucketStart + (row.seq * 3 + i) * 2000L;
                        long lastTs = firstTs + between(0, 1999);
                        timestamps.appendLong(lastTs);
                        timestamps.appendLong(firstTs);
                        values.appendDouble(between(0, 1000));
                        values.appendDouble(between(0, 1000));
                        samples += between(1, 10);
                    }
                    timestamps.endPositionEntry();
                    values.endPositionEntry();
                    sampleCounts.appendLong(p, samples);
                    resets.appendDouble(p, between(0, 100));
                }
                pages.add(
                    new Page(
                        tsids.build().asBlock(),
                        buckets.build().asBlock(),
                        timestamps.build(),
                        values.build(),
                        sampleCounts.build().asBlock(),
                        resets.build().asBlock()
                    )
                );
            }
            from = to;
        }
        return pages;
    }

    private List<RateResult> runFinalAggregation(
        DriverContext driverContext,
        List<Page> inputPages,
        boolean isRateOverTime,
        int partitionKeysThreshold
    ) {
        final TestThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                WORKER_EXECUTOR,
                between(1, 8),
                between(1, 1024),
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        HashAggregationOperator.ParallelConfig parallelConfig = null;
        if (partitionKeysThreshold != Integer.MAX_VALUE) {
            parallelConfig = new HashAggregationOperator.ParallelConfig(
                threadPool.executor(WORKER_EXECUTOR),
                between(1, 8),
                between(1, 10),
                partitionKeysThreshold
            );
        }
        final List<Page> outputPages = new ArrayList<>();
        try {
            var factory = new TimeSeriesAggregationOperator.Factory(
                Rounding.builder(TimeValue.timeValueMillis(BUCKET_MILLIS)).build().prepareForUnknown(),
                false,
                List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
                AggregatorMode.FINAL,
                List.of(supplier(isRateOverTime).groupingAggregatorFactory(AggregatorMode.FINAL, List.of(2, 3, 4, 5))),
                randomIntBetween(128, 4096),
                randomIntBetween(128, 4096),
                TimeSeriesAggregationOperator.DEFAULT_PARTIAL_EMIT_KEYS_THRESHOLD,
                parallelConfig
            );
            try (
                var source = new CannedSourceOperator(CannedSourceOperator.deepCopyOf(driverContext.blockFactory(), inputPages).iterator());
                Driver driver = TestDriverFactory.create(
                    driverContext,
                    source,
                    List.of(factory.get(driverContext)),
                    new TestResultPageSinkOperator(outputPages::add)
                )
            ) {
                new TestDriverRunner().run(driver);
                boolean promoted = driver.profile()
                    .operators()
                    .stream()
                    .anyMatch(status -> status.operator().equals(ParallelHashAggregationOperator.class.getSimpleName()));
                assertThat("promoted to the parallel operator", promoted, equalTo(partitionKeysThreshold < Integer.MAX_VALUE));
            }
            List<RateResult> rows = new ArrayList<>();
            BytesRef scratch = new BytesRef();
            for (Page page : outputPages) {
                assertThat(page.getBlockCount(), equalTo(3));
                BytesRefBlock tsids = page.getBlock(0);
                LongBlock timestamps = page.getBlock(1);
                Block rates = page.getBlock(2);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    rows.add(
                        new RateResult(
                            tsids.getBytesRef(p, scratch).utf8ToString(),
                            timestamps.getLong(p),
                            BlockUtils.toJavaObject(rates, p)
                        )
                    );
                }
            }
            rows.sort(Comparator.comparing(RateResult::tsid).thenComparingLong(RateResult::timestamp));
            return rows;
        } finally {
            Releasables.close(outputPages);
            terminate(threadPool);
        }
    }

    private static TimeSeriesGroupingAggregatorEvaluationContext evaluationContext(
        DriverContext driverContext,
        long rangeStart,
        long rangeEnd
    ) {
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return rangeStart;
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return rangeEnd;
            }

            @Override
            public void forEachGroupInRange(int startingGroupId, long rangeStartMillis, long rangeEndMillis, IntConsumer action) {}

            @Override
            public int previousGroupId(int currentGroupId) {
                return -1;
            }

            @Override
            public int nextGroupId(int currentGroupId) {
                return -1;
            }

            @Override
            public void computeAdjacentGroupIds() {}
        };
    }

    private static TimeSeriesGroupingAggregatorEvaluationContext lookbackContext(
        DriverContext driverContext,
        long[] rangeStarts,
        long[] rangeEnds
    ) {
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return rangeStarts[groupId];
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return rangeEnds[groupId];
            }

            @Override
            public void forEachGroupInRange(int startingGroupId, long rangeStartMillis, long rangeEndMillis, IntConsumer action) {}

            @Override
            public int previousGroupId(int currentGroupId) {
                return currentGroupId - 1;
            }

            @Override
            public int nextGroupId(int currentGroupId) {
                return currentGroupId + 1;
            }

            @Override
            public void computeAdjacentGroupIds() {}
        };
    }
}
