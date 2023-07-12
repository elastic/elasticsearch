/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CannedSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class SumIntAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        int max = between(1, (int) Math.min(Integer.MAX_VALUE, Long.MAX_VALUE / size));
        return new SequenceIntBlockSourceOperator(LongStream.range(0, size).mapToInt(l -> between(-max, max)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, List<Integer> inputChannels) {
        return new SumIntAggregatorFunctionSupplier(bigArrays, inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of ints";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        long sum = input.stream().flatMapToInt(b -> allInts(b)).asLongStream().sum();
        assertThat(((LongBlock) result).getLong(0), equalTo(sum));
    }

    public void testRejectsDouble() {
        DriverContext driverContext = new DriverContext();
        try (
            Driver d = new Driver(
                driverContext,
                new CannedSourceOperator(Iterators.single(new Page(new DoubleArrayVector(new double[] { 1.0 }, 1).asBlock()))),
                List.of(simple(nonBreakingBigArrays()).get(driverContext)),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far")),
                () -> {}
            )
        ) {
            expectThrows(Exception.class, () -> runDriver(d));  // ### find a more specific exception type
        }
        assertDriverContext(driverContext);
    }
}
