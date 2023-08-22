/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionTests;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class AggregationOperatorTests extends ForkingOperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new SequenceLongBlockSourceOperator(LongStream.range(0, size).map(l -> randomLongBetween(-max, max)));
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        List<Integer> sumChannels, maxChannels;
        if (mode.isInputPartial()) {
            int sumInterChannelCount = SumLongAggregatorFunction.intermediateStateDesc().size();
            int maxInterChannelCount = MaxLongAggregatorFunction.intermediateStateDesc().size();
            sumChannels = IntStream.range(0, sumInterChannelCount).boxed().toList();
            maxChannels = IntStream.range(sumInterChannelCount, sumInterChannelCount + maxInterChannelCount).boxed().toList();
        } else {
            sumChannels = maxChannels = List.of(0);
        }

        return new AggregationOperator.AggregationOperatorFactory(
            List.of(
                new SumLongAggregatorFunctionSupplier(bigArrays, sumChannels).aggregatorFactory(mode),
                new MaxLongAggregatorFunctionSupplier(bigArrays, maxChannels).aggregatorFactory(mode)
            ),
            mode
        );
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "AggregationOperator[mode = SINGLE, aggs = sum of longs, max of longs]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "AggregationOperator[aggregators=["
            + "Aggregator[aggregatorFunction=SumLongAggregatorFunction[channels=[0]], mode=SINGLE], "
            + "Aggregator[aggregatorFunction=MaxLongAggregatorFunction[channels=[0]], mode=SINGLE]]]";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(2));
        assertThat(results.get(0).getPositionCount(), equalTo(1));

        SumLongAggregatorFunctionTests sum = new SumLongAggregatorFunctionTests();
        MaxLongAggregatorFunctionTests max = new MaxLongAggregatorFunctionTests();

        Block sums = results.get(0).getBlock(0);
        Block maxs = results.get(0).getBlock(1);
        sum.assertSimpleOutput(input.stream().map(p -> p.<Block>getBlock(0)).toList(), sums);
        max.assertSimpleOutput(input.stream().map(p -> p.<Block>getBlock(0)).toList(), maxs);
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big array so never breaks", false);
        return null;
    }
}
