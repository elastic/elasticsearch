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

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The aggregator kept for clusters with a node that predates NaN skipping: a NaN input makes the result NaN.
 */
public class MinNanPropagatingDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> randomDoubleWithEdgeCases()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinNanPropagatingDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min_nan_propagating of doubles";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        double expected = input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).reduce(Math::min).getAsDouble();
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(expected));
    }

    public void testNaNAmongFiniteValuesMakesTheResultNaN() {
        assertResult(List.of(3.0, Double.NaN, -6.0, 2.0), Double.NaN);
    }

    public void testFiniteValuesAreUnaffected() {
        assertResult(List.of(3.0, -6.0, 2.0), Math.min(Math.min(3.0, -6.0), 2.0));
    }

    private void assertResult(List<Double> values, double expected) {
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
}
