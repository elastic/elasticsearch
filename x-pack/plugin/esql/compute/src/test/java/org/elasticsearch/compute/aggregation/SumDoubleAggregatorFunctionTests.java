/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SumDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        return new SequenceDoubleBlockSourceOperator(LongStream.range(0, size).mapToDouble(l -> ESTestCase.randomDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, int inputChannel) {
        return new SumDoubleAggregatorFunctionSupplier(bigArrays, inputChannel);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of doubles";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        double sum = input.stream().flatMapToDouble(b -> allDoubles(b)).sum();
        assertThat(((DoubleBlock) result).getDouble(0), closeTo(sum, .0001));
    }

    public void testOverflowSucceeds() {
        DriverContext driverContext = new DriverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                driverContext,
                new SequenceDoubleBlockSourceOperator(DoubleStream.of(Double.MAX_VALUE - 1, 2)),
                List.of(simple(nonBreakingBigArrays()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertThat(results.get(0).<DoubleBlock>getBlock(0).getDouble(0), equalTo(Double.MAX_VALUE + 1));
        assertDriverContext(driverContext);
    }

    public void testSummationAccuracy() {
        DriverContext driverContext = new DriverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                driverContext,
                new SequenceDoubleBlockSourceOperator(
                    DoubleStream.of(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7)
                ),
                List.of(simple(nonBreakingBigArrays()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertEquals(15.3, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), Double.MIN_NORMAL);
        assertDriverContext(driverContext);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        results.clear();
        int n = randomIntBetween(5, 10);
        double[] values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        driverContext = new DriverContext();
        try (
            Driver d = new Driver(
                driverContext,
                new SequenceDoubleBlockSourceOperator(DoubleStream.of(values)),
                List.of(simple(nonBreakingBigArrays()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertEquals(sum, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 1e-10);
        assertDriverContext(driverContext);

        // Summing up some big double values and expect infinity result
        results.clear();
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        driverContext = new DriverContext();
        try (
            Driver d = new Driver(
                driverContext,
                new SequenceDoubleBlockSourceOperator(DoubleStream.of(largeValues)),
                List.of(simple(nonBreakingBigArrays()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertEquals(Double.POSITIVE_INFINITY, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 0d);
        assertDriverContext(driverContext);

        results.clear();
        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        driverContext = new DriverContext();
        try (
            Driver d = new Driver(
                driverContext,
                new SequenceDoubleBlockSourceOperator(DoubleStream.of(largeValues)),
                List.of(simple(nonBreakingBigArrays()).get(driverContext)),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertEquals(Double.NEGATIVE_INFINITY, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 0d);
        assertDriverContext(driverContext);
    }
}
