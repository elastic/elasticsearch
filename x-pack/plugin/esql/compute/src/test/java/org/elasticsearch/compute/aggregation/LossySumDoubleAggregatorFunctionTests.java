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

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class LossySumDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> ESTestCase.randomDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new LossySumDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "lossy_sum of doubles";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        double sum = input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).sum();
        assertThat(((DoubleBlock) result).getDouble(0), closeTo(sum, .0001));
    }

    public void testOverflowSucceeds() {
        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new SequenceDoubleBlockSourceOperator(driverContext.blockFactory(), DoubleStream.of(Double.MAX_VALUE - 1, 2)),
                List.of(simple().get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            new TestDriverRunner().run(d);
        }
        assertThat(results.get(0).<DoubleBlock>getBlock(0).getDouble(0), equalTo(Double.MAX_VALUE + 1));
        assertDriverContext(driverContext);
    }
}
