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

public class MinDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> randomDoubleWithEdgeCases()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of doubles";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        double[] values = input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).toArray();
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(skipNaNMin(values)));
    }

    public void testAllNaNProducesNaN() {
        assertMin(List.of(Double.NaN, Double.NaN, Double.NaN), Double.NaN);
    }

    public void testNaNSkippedWhenFinitePresent() {
        assertMin(List.of(Double.NaN, 3.0, Double.NaN, 1.0, Double.NaN), 1.0);
    }

    public void testSignedZeroOrdering() {
        assertMin(List.of(0.0, Double.NaN, -0.0), -0.0);
    }

    public void testPositiveInfinityIsAnOrdinaryValue() {
        assertMin(List.of(Double.NaN, Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    }

    private void assertMin(List<Double> values, double expected) {
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

    private static double skipNaNMin(double[] values) {
        OptionalDouble min = Arrays.stream(values).filter(v -> Double.isNaN(v) == false).min();
        return min.isPresent() ? min.getAsDouble() : Double.NaN;
    }
}
