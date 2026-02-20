/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.GroundTruthFirstLastAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.TupleLongLongBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.processPages;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class AllFirstLongByTimestampAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen tsgen = randomFrom(
            FirstLongByTimestampGroupingAggregatorFunctionTests.TimestampGen.values()
        );
        return new TupleLongLongBlockSourceOperator(
            blockFactory,
            IntStream.range(0, size).mapToObj(l -> Tuple.tuple(randomBoolean() ? null : randomLong(), tsgen.gen()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllFirstLongByTimestampAggregatorFunctionSupplier();
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_first_long_by_timestamp";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        GroundTruthFirstLastAggregator work = new GroundTruthFirstLastAggregator(true);
        processPages(work, input);
        work.check(BlockUtils.toJavaObject(result, 0));
    }

    /**
     * Reproduces a scenario where null timestamps incorrectly win over valid timestamps when combineIntermediate is called. We don't need
     * multivalues anywhere to reproduce the bug.
     *
     * See https://github.com/elastic/elasticsearch-serverless/issues/5348
     */
    public void testNullTimestampDoesNotWinOverValidTimestamp() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        Page page1 = new Page(
            // value
            blockFactory.newLongBlockBuilder(1).beginPositionEntry().appendLong(2).build(),
            // valid and earliest timestamp
            blockFactory.newLongBlockBuilder(1).beginPositionEntry().appendLong(1).build()
        );

        Page page2 = new Page(
            // value
            blockFactory.newLongBlockBuilder(1).beginPositionEntry().appendLong(3).build(),
            // null timestamp
            blockFactory.newLongBlockBuilder(1).appendNull().build()
        );

        // Run each page through separate aggregators
        List<Page> intermediatePages1 = new TestDriverRunner().builder(driverContext)
            .input(page1)
            .run(simpleWithMode(AggregatorMode.INITIAL));
        List<Page> intermediatePages2 = new TestDriverRunner().builder(driverContext)
            .input(page2)
            .run(simpleWithMode(AggregatorMode.INITIAL));

        Iterator<Page> finalInput = Iterators.concat(intermediatePages1.iterator(), intermediatePages2.iterator());

        // Combine intermediate results (this calls combineIntermediate to expose the bug)
        List<Page> results = new TestDriverRunner().builder(driverContext).input(finalInput).run(simpleWithMode(AggregatorMode.FINAL));

        assertThat(results, hasSize(1));
        Block result = results.get(0).getBlock(0);
        assertThat(BlockUtils.toJavaObject(result, 0), equalTo(2L));
    }
}
