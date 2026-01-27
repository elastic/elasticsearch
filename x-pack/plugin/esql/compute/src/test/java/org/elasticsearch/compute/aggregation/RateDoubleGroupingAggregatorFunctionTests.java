/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

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
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;

import java.util.ArrayList;
import java.util.List;

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
                var timestampsBuilder = blockFactory.newLongBlockBuilder(positions)
            ) {
                for (int p = 0; p < positions; p++) {
                    valuesBuilder.appendDouble(values[positions - p - 1]);
                    timestampsBuilder.appendLong(timestamps[positions - p - 1]);
                }
                pages.add(
                    new Page(
                        blockFactory.newConstantIntBlockWith(0, positions),
                        valuesBuilder.build(),
                        timestampsBuilder.build(),
                        blockFactory.newConstantIntBlockWith(interval, positions),
                        blockFactory.newConstantLongBlockWith(Long.MAX_VALUE, positions)
                    )
                );
            }
        }
        // values, timestamps, slice, future_timestamps
        var aggregatorFactory = new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false, false).groupingAggregatorFactory(
            AggregatorMode.INITIAL,
            List.of(1, 2, 3, 4)
        );
        final List<BlockHash.GroupSpec> groupSpecs = List.of(new BlockHash.GroupSpec(0, ElementType.INT));
        HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
            List.of(aggregatorFactory),
            () -> BlockHash.build(groupSpecs, driverContext.blockFactory(), randomIntBetween(1, 1024), randomBoolean()),
            driverContext
        );
        List<Page> outputPages = new ArrayList<>();
        Driver driver = TestDriverFactory.create(
            driverContext,
            new CannedSourceOperator(pages.iterator()),
            List.of(hashAggregationOperator),
            new PageConsumerOperator(outputPages::add)
        );
        OperatorTestCase.runDriver(driver);
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
}
