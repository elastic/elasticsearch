/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.equalTo;

public class PresentAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return PresentAggregatorFunction.supplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "present";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        boolean present = input.stream().flatMapToLong(p -> allLongs(p.getBlock(0))).findAny().isPresent();
        assertThat(((BooleanBlock) result).getBoolean(0), equalTo(present));
    }

    @Override
    protected void assertOutputFromEmpty(Block b) {
        assertThat(b.getPositionCount(), equalTo(1));
        assertThat(valuesAtPositions(b, 0, 1), equalTo(List.of(List.of(false))));
    }

    public void testWithNonNullAndConstantNullPages() {
        Aggregator.Factory aggregatorFactory = aggregatorFunction().aggregatorFactory(AggregatorMode.SINGLE, List.of(0));

        AggregationOperator.AggregationOperatorFactory operatorFactory = new AggregationOperator.AggregationOperatorFactory(
            List.of(aggregatorFactory),
            AggregatorMode.SINGLE
        );

        List<Page> input = CannedSourceOperator.collectPages(
            new SequenceLongBlockSourceOperator(driverContext().blockFactory(), LongStream.range(0, 100).map(l -> randomLong()))
        );

        try (Page page = new Page(blockFactory().newConstantNullBlock(10))) {
            // randomly add the null page before or after the non-null page
            input.add(randomFrom(0, 1), page);

            List<Page> results = new TestDriverRunner().builder(driverContext()).input(input).run(operatorFactory);

            assertThat(results.size(), equalTo(1));
            var firstPage = results.get(0);
            assertThat(firstPage.getPositionCount(), equalTo(1));
            var block = firstPage.getBlock(0);
            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(valuesAtPositions(block, 0, 1), equalTo(List.of(List.of(true))));
        }
    }
}
