/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class SumDoubleAggregatorTests extends AggregatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new SequenceDoubleBlockSourceOperator(LongStream.range(0, end).asDoubleStream());
    }

    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.SUM_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of doubles";
    }

    @Override
    protected void assertSimpleResult(int end, Block result) {
        double expected = LongStream.range(0, end).mapToDouble(Double::valueOf).sum();
        assertThat(result.getDouble(0), equalTo(expected));
    }

    public void testOverflowSucceeds() {
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                new SequenceDoubleBlockSourceOperator(DoubleStream.of(Double.MAX_VALUE - 1, 2)),
                List.of(simple(nonBreakingBigArrays()).get()),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertThat(results.get(0).getBlock(0).getDouble(0), equalTo(Double.MAX_VALUE + 1));
    }
}
