/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class OverflowingSumLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new OverflowingSumLongAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "overflowing_sum of longs";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size / 5);
        return new TupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLongBetween(-max, max)))
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        long sum = input.stream().flatMapToLong(p -> allLongs(p, group)).sum();
        assertThat(((LongBlock) result).getLong(position), equalTo(sum));
    }

    public void testOverflowFails() {
        DriverContext driverContext = driverContext();

        assertThrows(ArithmeticException.class, () -> {
            Operator.OperatorFactory factory = simpleWithMode(AggregatorMode.SINGLE);
            List<Page> input = CannedSourceOperator.collectPages(
                new TupleBlockSourceOperator(
                    driverContext.blockFactory(),
                    LongStream.range(0, 10).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), Long.MAX_VALUE - 1))
                )
            );
            drive(factory.get(driverContext), input.iterator(), driverContext);
        });

        assertDriverContext(driverContext);
    }
}
