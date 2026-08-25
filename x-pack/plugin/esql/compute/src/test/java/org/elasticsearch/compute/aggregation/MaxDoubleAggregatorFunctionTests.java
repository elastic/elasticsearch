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
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MaxDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> ESTestCase.randomDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MaxDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of doubles";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        double max = input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).max().getAsDouble();
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(max));
    }

    /**
     * {@code -Infinity} is the identity element of {@code max}: seeding the accumulator with any greater value makes the
     * seed win over every observation, so the aggregation reports the seed instead of the data. It is an ordinary
     * observation wherever IEEE-754 arithmetic applies, so the seed must not be observable in the result.
     */
    public void testNegativeInfinityIsPreserved() {
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceDoubleBlockSourceOperator(driverContext.blockFactory(), DoubleStream.of(Double.NEGATIVE_INFINITY)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }
        assertThat(results.get(0).<DoubleBlock>getBlock(0).getDouble(0), equalTo(Double.NEGATIVE_INFINITY));
        assertDriverContext(driverContext);
    }
}
