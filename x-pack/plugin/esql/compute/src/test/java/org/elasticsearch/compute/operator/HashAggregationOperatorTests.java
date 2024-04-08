/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongGroupingAggregatorFunctionTests;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongGroupingAggregatorFunctionTests;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class HashAggregationOperatorTests extends ForkingOperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new TupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(l % 5, randomLongBetween(-max, max)))
        );
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(AggregatorMode mode) {
        List<Integer> sumChannels, maxChannels;
        if (mode.isInputPartial()) {
            int sumChannelCount = SumLongAggregatorFunction.intermediateStateDesc().size();
            int maxChannelCount = MaxLongAggregatorFunction.intermediateStateDesc().size();
            sumChannels = range(1, 1 + sumChannelCount).boxed().toList();
            maxChannels = range(1 + sumChannelCount, 1 + sumChannelCount + maxChannelCount).boxed().toList();
        } else {
            sumChannels = maxChannels = List.of(1);
        }

        return new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new HashAggregationOperator.GroupSpec(0, ElementType.LONG)),
            List.of(
                new SumLongAggregatorFunctionSupplier(sumChannels).groupingAggregatorFactory(mode),
                new MaxLongAggregatorFunctionSupplier(maxChannels).groupingAggregatorFactory(mode)
            ),
            randomPageSize()
        );
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "HashAggregationOperator[mode = <not-needed>, aggs = sum of longs, max of longs]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "HashAggregationOperator[blockHash=LongBlockHash{channel=0, entries=0, seenNull=false}, aggregators=["
            + "GroupingAggregator[aggregatorFunction=SumLongGroupingAggregatorFunction[channels=[1]], mode=SINGLE], "
            + "GroupingAggregator[aggregatorFunction=MaxLongGroupingAggregatorFunction[channels=[1]], mode=SINGLE]]]";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlockCount(), equalTo(3));
        assertThat(results.get(0).getPositionCount(), equalTo(5));

        SumLongGroupingAggregatorFunctionTests sum = new SumLongGroupingAggregatorFunctionTests();
        MaxLongGroupingAggregatorFunctionTests max = new MaxLongGroupingAggregatorFunctionTests();

        LongBlock groups = results.get(0).getBlock(0);
        Block sums = results.get(0).getBlock(1);
        Block maxs = results.get(0).getBlock(2);
        for (int i = 0; i < 5; i++) {
            long group = groups.getLong(i);
            sum.assertSimpleGroup(input, sums, i, group);
            max.assertSimpleGroup(input, maxs, i, group);
        }
    }
}
