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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.test.MixWithIncrement;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.lessThan;

@SeedDecorators(MixWithIncrement.class)
public class SampleLongAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private static final int LIMIT = 50;

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SampleLongAggregatorFunctionSupplier(LIMIT);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sample of longs";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        Set<Long> inputValues = input.stream().flatMapToLong(AggregatorFunctionTestCase::allLongs).boxed().collect(Collectors.toSet());
        Long[] resultValues = AggregatorFunctionTestCase.allLongs(result).boxed().toArray(Long[]::new);
        assertThat(resultValues, arrayWithSize(Math.min(inputValues.size(), LIMIT)));
        assertThat(inputValues, hasItems(resultValues));
    }

    public void testDistribution() {
        // Sample from the numbers 0...99.
        int N = 100;
        Aggregator.Factory aggregatorFactory = aggregatorFunction().aggregatorFactory(AggregatorMode.SINGLE, List.of(0));
        AggregationOperator.AggregationOperatorFactory operatorFactory = new AggregationOperator.AggregationOperatorFactory(
            List.of(aggregatorFactory),
            AggregatorMode.SINGLE
        );

        // Repeat 1000x, count how often each number is sampled.
        int[] sampledCounts = new int[N];
        for (int iteration = 0; iteration < 1000; iteration++) {
            List<Page> input = CannedSourceOperator.collectPages(
                new SequenceLongBlockSourceOperator(driverContext().blockFactory(), LongStream.range(0, N))
            );
            List<Page> results = drive(operatorFactory.get(driverContext()), input.iterator(), driverContext());
            for (Page page : results) {
                LongBlock block = page.getBlock(0);
                for (int i = 0; i < block.getTotalValueCount(); i++) {
                    sampledCounts[(int) block.getLong(i)]++;
                }
            }
            MixWithIncrement.next();
        }

        // On average, each number should be sampled 500x.
        // The interval [300,700] is approx. 10 sigma, so this should never fail.
        for (int i = 0; i < N; i++) {
            assertThat(sampledCounts[i], both(greaterThan(300)).and(lessThan(700)));
        }
    }
}
