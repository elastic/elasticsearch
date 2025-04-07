/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class SumLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLongBetween(-max, max)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumLongAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of longs";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        long sum = input.stream().flatMapToLong(b -> allLongs(b)).sum();
        assertThat(((LongBlock) result).getLong(0), equalTo(sum));
    }

    public void testOverflowFails() {
        DriverContext driverContext = driverContext();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceLongBlockSourceOperator(driverContext.blockFactory(), LongStream.of(Long.MAX_VALUE - 1, 2)),
                List.of(simple().get(driverContext)),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far"))
            )
        ) {
            Exception e = expectThrows(ArithmeticException.class, () -> runDriver(d));
            assertThat(e.getMessage(), equalTo("long overflow"));
        }
    }

    public void testRejectsDouble() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(Iterators.single(new Page(blockFactory.newDoubleArrayVector(new double[] { 1.0 }, 1).asBlock()))),
                List.of(simple().get(driverContext)),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far"))
            )
        ) {
            expectThrows(Exception.class, () -> runDriver(d));  // ### find a more specific exception type
        }
    }
}
