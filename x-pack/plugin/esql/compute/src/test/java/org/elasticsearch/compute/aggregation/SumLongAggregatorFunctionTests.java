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
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TestResultPageSinkOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SumLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLongBetween(-max, max)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new SumLongAggregatorFunctionSupplier(-1, -2, "", inputChannels);
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
        List<Page> results = new ArrayList<>();
        DriverContext driverContext = driverContext();
        List<String> warnings = new ArrayList<>();
        try (
            Driver d = new Driver(
                driverContext,
                new SequenceLongBlockSourceOperator(driverContext.blockFactory(), LongStream.of(Long.MAX_VALUE - 1, 2)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add),
                () -> {
                    warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
                }
            )
        ) {
            runDriver(d);
        }

        assertDriverContext(driverContext);

        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0).getBlockCount(), equalTo(1));
        assertThat(results.get(0).getPositionCount(), equalTo(1));
        assertThat(results.get(0).getBlock(0).isNull(0), equalTo(true));

        assertThat(
            warnings,
            contains(
                containsString("\"Line -1:-2: evaluation of [] failed, treating result as null. Only first 20 failures recorded.\""),
                containsString("\"Line -1:-2: java.lang.ArithmeticException: long overflow\"")
            )
        );
    }

    public void testRejectsDouble() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            Driver d = new Driver(
                driverContext,
                new CannedSourceOperator(Iterators.single(new Page(blockFactory.newDoubleArrayVector(new double[] { 1.0 }, 1).asBlock()))),
                List.of(simple().get(driverContext)),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far")),
                () -> {}
            )
        ) {
            expectThrows(Exception.class, () -> runDriver(d));  // ### find a more specific exception type
        }
    }
}
