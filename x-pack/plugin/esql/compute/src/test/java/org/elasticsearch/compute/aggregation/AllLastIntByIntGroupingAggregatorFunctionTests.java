/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.GroundTruthFirstLastAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.processPages;

public class AllLastIntByIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.INT, ElementType.INT),
            IntStream.range(0, size).mapToObj(l -> List.of(randomLongBetween(0, 4), randomInt(), randomInt())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllLastIntByIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_last_int_by_int";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        GroundTruthFirstLastAggregator work = new GroundTruthFirstLastAggregator(false);
        processPages(work, input, group);
        work.check(BlockUtils.toJavaObject(result, position));
    }

    /** Tests that groups arriving out of order (key 1 before null key) are handled correctly. */
    public void testTwoBlocksOneKeyNull() {
        var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
        BlockFactory blockFactory = runner.blockFactory();
        Page page1 = new Page(
            blockFactory.newConstantLongBlockWith(1L, 1),
            blockFactory.newConstantIntBlockWith(randomInt(), 1),
            blockFactory.newConstantIntBlockWith(randomInt(), 1)
        );
        Page page2 = new Page(
            blockFactory.newConstantNullBlock(1),
            blockFactory.newConstantIntBlockWith(randomInt(), 1),
            blockFactory.newConstantIntBlockWith(randomInt(), 1)
        );
        runner.input(List.of(page1, page2));
        assertSimpleOutput(runner.deepCopy(), runner.run(simple()));
    }
}
