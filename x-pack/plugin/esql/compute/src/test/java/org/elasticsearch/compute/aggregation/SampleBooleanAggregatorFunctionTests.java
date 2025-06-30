/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.SequenceBooleanBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.test.MixWithIncrement;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@SeedDecorators(MixWithIncrement.class)
public class SampleBooleanAggregatorFunctionTests extends AggregatorFunctionTestCase {
    private static final int LIMIT = 50;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBooleanBlockSourceOperator(blockFactory, IntStream.range(0, size).mapToObj(l -> randomBoolean()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SampleBooleanAggregatorFunctionSupplier(LIMIT);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sample of booleans";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        List<Boolean> inputValues = input.stream().flatMap(AggregatorFunctionTestCase::allBooleans).collect(Collectors.toList());
        Boolean[] resultValues = AggregatorFunctionTestCase.allBooleans(result).toArray(Boolean[]::new);
        assertThat(resultValues, arrayWithSize(Math.min(inputValues.size(), LIMIT)));
    }

    public void testDistribution() {
        // Sample from the numbers 50x true and 50x false.
        int N = 100;
        Aggregator.Factory aggregatorFactory = aggregatorFunction().aggregatorFactory(AggregatorMode.SINGLE, List.of(0));
        AggregationOperator.AggregationOperatorFactory operatorFactory = new AggregationOperator.AggregationOperatorFactory(
            List.of(aggregatorFactory),
            AggregatorMode.SINGLE
        );

        // Repeat 1000x, count how often each value is sampled.
        int trueCount = 0;
        int falseCount = 0;
        for (int iteration = 0; iteration < 1000; iteration++) {
            List<Page> input = CannedSourceOperator.collectPages(
                new SequenceBooleanBlockSourceOperator(driverContext().blockFactory(), IntStream.range(0, N).mapToObj(i -> i % 2 == 0))
            );
            List<Page> results = drive(operatorFactory.get(driverContext()), input.iterator(), driverContext());
            for (Page page : results) {
                BooleanBlock block = page.getBlock(0);
                for (int i = 0; i < block.getTotalValueCount(); i++) {
                    if (block.getBoolean(i)) {
                        trueCount++;
                    } else {
                        falseCount++;
                    }
                }
            }
            MixWithIncrement.next();
        }

        // On average, both boolean values should be sampled 25000x.
        // The interval [23000,27000] is at least 10 sigma, so this should never fail.
        assertThat(trueCount, both(greaterThan(23000)).and(lessThan(27000)));
        assertThat(falseCount, both(greaterThan(23000)).and(lessThan(27000)));
    }
}
