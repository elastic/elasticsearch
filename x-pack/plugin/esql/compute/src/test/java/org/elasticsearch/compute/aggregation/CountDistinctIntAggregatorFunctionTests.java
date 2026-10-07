/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceIntBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.equalTo;

public class CountDistinctIntAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int max = between(1, Math.min(Integer.MAX_VALUE, Integer.MAX_VALUE / size));
        return new SequenceIntBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToInt(l -> between(-max, max)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new CountDistinctIntAggregatorFunctionSupplier(CountDistinctTestUtils.PRECISION);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of ints";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        long expected = CountDistinctTestUtils.expectedCount(
            state -> input.stream().flatMapToInt(p -> allInts(p.getBlock(0))).forEach(state::collect)
        );
        assertThat(((LongBlock) result).getLong(0), equalTo(expected));
    }

    @Override
    protected void assertOutputFromEmpty(Block b) {
        assertThat(b.getPositionCount(), equalTo(1));
        assertThat(valuesAtPositions(b, 0, 1), equalTo(List.of(List.of(0L))));
    }

    public void testRejectsDouble() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(Iterators.single(new Page(blockFactory.newDoubleArrayVector(new double[] { 1.0 }, 1).asBlock()))),
                List.of(simple().get(driverContext)),
                new PageConsumerOperator(page -> fail("shouldn't have made it this far"))
            )
        ) {
            expectThrows(ClassCastException.class, () -> new TestDriverRunner().run(d));
        }
    }
}
