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
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.processPages;

public class AllLastLongByIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.LONG, ElementType.INT),
            IntStream.range(0, size).mapToObj(l -> List.of(randomLongBetween(0, 4), randomLong(), randomInt())).toList()
        );
    }

    @Override
    protected int inputCount() {
        return 2;
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new AllLastLongByIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_last_long_by_int";
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
            blockFactory.newConstantLongBlockWith(randomLong(), 1),
            blockFactory.newConstantIntBlockWith(randomInt(), 1)
        );
        Page page2 = new Page(
            blockFactory.newConstantNullBlock(1),
            blockFactory.newConstantLongBlockWith(randomLong(), 1),
            blockFactory.newConstantIntBlockWith(randomInt(), 1)
        );
        runner.input(List.of(page1, page2));
        assertSimpleOutput(runner.deepCopy(), runner.run(simple()));
    }

    /**
     * A shard that never saw a group still emits a partial for it when another aggregate created the group.
     * That partial carries the unset key (-1), which is later than any pre-epoch timestamp, so LAST must not
     * keep it over the real row.
     */
    public void testUnobservedIntermediateRowDoesNotReplaceEarlierLast() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            var agg = new AllLastLongByIntGroupingAggregatorFunction(List.of(0, 1, 2, 3), driverContext);
            var groups = blockFactory.newConstantIntVector(3, 1);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext);
            Page rawPage = new Page(blockFactory.newConstantLongBlockWith(10046L, 1), blockFactory.newConstantIntBlockWith(-50_064, 1));
            Page unobserved = new Page(
                blockFactory.newBooleanArrayVector(new boolean[] { false }, 1).asBlock(),
                blockFactory.newConstantBooleanVector(true, 1).asBlock(),
                blockFactory.newIntArrayVector(new int[] { -1 }, 1).asBlock(),
                blockFactory.newConstantNullBlock(1)
            )
        ) {
            try (var add = agg.prepareProcessRawInputPage(new SeenGroupIds.Empty(), rawPage)) {
                add.add(0, groups);
            }
            agg.addIntermediateInput(0, groups, unobserved);
            Block[] resultBlocks = new Block[1];
            try (var prepared = agg.prepareEvaluateFinal(groups, evalContext)) {
                prepared.evaluate(resultBlocks, 0, groups);
            }
            try (LongBlock result = (LongBlock) resultBlocks[0]) {
                assertFalse(result.isNull(0));
                assertEquals(10046L, result.getLong(0));
            }
        }
    }
}
