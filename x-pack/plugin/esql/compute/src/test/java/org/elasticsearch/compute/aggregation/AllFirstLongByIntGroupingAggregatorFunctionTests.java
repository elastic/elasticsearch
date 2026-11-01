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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.compute.aggregation.FirstLastAggregatorTestingUtils.processPages;

public class AllFirstLongByIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
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
        return new AllFirstLongByIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "all_first_long_by_int";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        GroundTruthFirstLastAggregator work = new GroundTruthFirstLastAggregator(true);
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
     * That partial is one row whose group id is the reducer's id, which is often larger than the page length.
     * Folding it in used the unset key (-1 or 0) and a null value, and FIRST kept that over the real row.
     */
    public void testUnobservedIntermediateRowDoesNotReplaceEarlierFirst() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            var agg = new AllFirstLongByIntGroupingAggregatorFunction(List.of(0, 1, 2, 3), driverContext);
            var groups = blockFactory.newConstantIntVector(3, 1);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext);
            Page rawPage = new Page(blockFactory.newConstantLongBlockWith(10046L, 1), blockFactory.newConstantIntBlockWith(50_064, 1));
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

    /**
     * A group id below {@code maxGroupId} was not necessarily collected. Its intermediate state has to say
     * "not observed" with no timestamp, rather than presenting the unset key as a real one.
     */
    public void testIntermediateForNeverCollectedGroupHasNoTimestamp() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        try (
            var agg = new AllFirstLongByIntGroupingAggregatorFunction(List.of(0, 1, 2, 3), driverContext);
            var collected = blockFactory.newConstantIntVector(1, 1);
            var missing = blockFactory.newConstantIntVector(0, 1);
            var evalContext = new GroupingAggregatorEvaluationContext(driverContext);
            Page rawPage = new Page(blockFactory.newConstantLongBlockWith(10046L, 1), blockFactory.newConstantIntBlockWith(50_064, 1))
        ) {
            try (var add = agg.prepareProcessRawInputPage(new SeenGroupIds.Empty(), rawPage)) {
                add.add(0, collected);
            }
            Block[] intermediate = new Block[agg.intermediateBlockCount()];
            try (var prepared = agg.prepareEvaluateIntermediate(missing, evalContext)) {
                prepared.evaluate(intermediate, 0, missing);
            }
            try (Page page = new Page(intermediate)) {
                BooleanBlock observed = page.getBlock(0);
                BooleanBlock timestampPresent = page.getBlock(1);
                IntBlock timestamp = page.getBlock(2);
                LongBlock values = page.getBlock(3);
                assertFalse(observed.getBoolean(observed.getFirstValueIndex(0)));
                assertFalse(timestampPresent.getBoolean(timestampPresent.getFirstValueIndex(0)));
                assertTrue(timestamp.isNull(0));
                assertTrue(values.isNull(0));
            }
        }
    }
}
