/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SequenceFloatBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SumFloatAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceFloatBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToObj(l -> ESTestCase.randomFloat()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumFloatAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of floats";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        double sum = input.stream().flatMap(AggregatorFunctionTestCase::allFloats).mapToDouble(f -> (double) f).sum();
        assertThat(((DoubleBlock) result).getDouble(0), closeTo(sum, .0001));
    }

    public void testOverflowSucceeds() {
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceFloatBlockSourceOperator(driverContext.blockFactory(), Stream.of(Float.MAX_VALUE - 1, 2f)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertThat(results.get(0).<DoubleBlock>getBlock(0).getDouble(0), equalTo((double) Float.MAX_VALUE + 1));
        assertDriverContext(driverContext);
    }

    public void testSummationAccuracy() {
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceFloatBlockSourceOperator(
                    driverContext.blockFactory(),
                    Stream.of(0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f, 1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.6f, 1.7f)
                ),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertEquals(15.3, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 0.001);
        assertDriverContext(driverContext);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        results.clear();
        int n = randomIntBetween(5, 10);
        Float[] values = new Float[n];
        float sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)
                : randomFloatBetween(Float.MIN_VALUE, Float.MAX_VALUE, true);
            sum += values[i];
        }
        driverContext = driverContext();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceFloatBlockSourceOperator(driverContext.blockFactory(), Stream.of(values)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertEquals(sum, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 1e-10);
        assertDriverContext(driverContext);

        // Summing up some big float values and expect a big double result
        results.clear();
        n = randomIntBetween(5, 10);
        Float[] largeValues = new Float[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Float.MAX_VALUE;
        }
        driverContext = driverContext();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceFloatBlockSourceOperator(driverContext.blockFactory(), Stream.of(largeValues)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertEquals((double) Float.MAX_VALUE * n, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 0d);
        assertDriverContext(driverContext);

        results.clear();
        for (int i = 0; i < n; i++) {
            largeValues[i] = -Float.MAX_VALUE;
        }
        driverContext = driverContext();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceFloatBlockSourceOperator(driverContext.blockFactory(), Stream.of(largeValues)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertEquals((double) -Float.MAX_VALUE * n, results.get(0).<DoubleBlock>getBlock(0).getDouble(0), 0d);
        assertDriverContext(driverContext);
    }
}
