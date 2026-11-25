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
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceDoubleBlockSourceOperator;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MaxDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> randomDoubleWithEdgeCases()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MaxDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of doubles";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        double[] values = input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).toArray();
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(skipNaNMax(values)));
    }

    public void testAllNaNProducesNaN() {
        assertMax(List.of(Double.NaN, Double.NaN, Double.NaN), Double.NaN);
    }

    public void testNaNSkippedWhenFinitePresent() {
        assertMax(List.of(Double.NaN, 3.0, Double.NaN, 1.0, Double.NaN), 3.0);
    }

    public void testSignedZeroOrdering() {
        assertMax(List.of(-0.0, Double.NaN, 0.0), 0.0);
    }

    public void testNegativeInfinityIsAnOrdinaryValue() {
        assertMax(List.of(Double.NaN, Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
    }

    private void assertMax(List<Double> values, double expected) {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new SequenceDoubleBlockSourceOperator(runner.blockFactory(), values));
        List<Page> results = runner.run(simple());
        assertThat(results, hasSize(1));
        Block result = results.get(0).getBlock(0);
        assertFalse(result.isNull(0));
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(expected));
    }

    private static double randomDoubleWithEdgeCases() {
        return switch (randomIntBetween(0, 7)) {
            case 0 -> Double.NaN;
            case 1 -> 0.0;
            case 2 -> -0.0;
            default -> randomDouble();
        };
    }

    private static double skipNaNMax(double[] values) {
        OptionalDouble max = Arrays.stream(values).filter(v -> Double.isNaN(v) == false).max();
        return max.isPresent() ? max.getAsDouble() : Double.NaN;
    }
}
