/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestWarningsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RateDoubleGroupingAggregatorFunctionTests extends ComputeTestCase {
    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
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
}
