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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class HashAggregationOperatorTests extends ForkingOperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new TupleBlockSourceOperator(LongStream.range(0, size).mapToObj(l -> Tuple.tuple(l % 5, randomLongBetween(-max, max))));
    }

    @Override
    protected Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode) {
        return operatorFactory(bigArrays, mode, randomPageSize());
    }

    private Operator.OperatorFactory operatorFactory(BigArrays bigArrays, AggregatorMode mode, int maxPageSize) {
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
                new SumLongAggregatorFunctionSupplier(bigArrays, sumChannels).groupingAggregatorFactory(mode),
                new MaxLongAggregatorFunctionSupplier(bigArrays, maxChannels).groupingAggregatorFactory(mode)
            ),
            maxPageSize,
            bigArrays
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

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        return ByteSizeValue.ofBytes(between(1, 32));
    }

    public void testChunkIntermediateOutput() {
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        int maxPageSize = randomIntBetween(2, 100);
        var partialHashFactory = operatorFactory(BigArrays.NON_RECYCLING_INSTANCE, AggregatorMode.INITIAL, maxPageSize);
        var finalHashFactory = operatorFactory(BigArrays.NON_RECYCLING_INSTANCE, AggregatorMode.FINAL, maxPageSize);
        var partialHashOperator = partialHashFactory.get(new DriverContext(bigArrays, BlockFactory.getNonBreakingInstance()));
        var finalHashOperator = finalHashFactory.get(new DriverContext(bigArrays, BlockFactory.getNonBreakingInstance()));
        final Set<Long> partialKeys = new HashSet<>();
        final Map<Long, Long> expectedSums = new HashMap<>();

        int iters = randomIntBetween(1, 100);
        for (int i = 0; i < iters; i++) {
            int positions = between(1, maxPageSize * 2);
            LongBlock.Builder b1 = LongBlock.newBlockBuilder(positions);
            LongBlock.Builder b2 = LongBlock.newBlockBuilder(positions);
            for (int p = 0; p < positions; p++) {
                long k = randomIntBetween(1, 1000);
                long v = randomIntBetween(1, 1000);
                b1.appendLong(k);
                b2.appendLong(v);
                partialKeys.add(k);
                expectedSums.merge(k, v, Long::sum);
            }
            Page p = new Page(b1.build(), b2.build());
            partialHashOperator.addInput(p);
            assertThat(partialHashOperator.needsInput(), equalTo(partialKeys.size() < maxPageSize));
            Page intermediateOutput = partialHashOperator.getOutput();
            assertTrue(partialHashOperator.needsInput());
            if (partialKeys.size() >= maxPageSize) {
                assertNotNull(intermediateOutput);
                partialKeys.clear();
                finalHashOperator.addInput(intermediateOutput);
            } else {
                assertNull(intermediateOutput);
            }
            assertFalse(partialHashOperator.isFinished());
        }
        partialHashOperator.finish();
        assertFalse(partialHashOperator.isFinished());
        finalHashOperator.addInput(partialHashOperator.getOutput());
        assertTrue(partialHashOperator.isFinished());
        finalHashOperator.finish();
        Page finalOutput = finalHashOperator.getOutput();
        LongBlock b0 = finalOutput.getBlock(0);
        LongBlock b1 = finalOutput.getBlock(1);
        Map<Long, Long> actualSums = new HashMap<>();
        for (int i = 0; i < finalOutput.getPositionCount(); i++) {
            actualSums.put(b0.getLong(i), b1.getLong(i));
        }
        assertThat(actualSums, equalTo(expectedSums));
    }
}
