/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Contracts for {@link ColumnarRowDropHelper}: per-batch failure accumulation, idempotent
 * multi-column marking, all-dropped batches, multi-value block filtering, and budget enforcement.
 */
public class ColumnarRowDropHelperTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    // ---- factory contract ----

    public void testNullForNonSkipRowPolicy() {
        assertNull(ColumnarRowDropHelper.forPolicy(null, null, "f.parquet"));
        assertNull(ColumnarRowDropHelper.forPolicy(ErrorPolicy.STRICT, null, "f.parquet"));
        assertNull(ColumnarRowDropHelper.forPolicy(ErrorPolicy.PERMISSIVE, null, "f.parquet"));
    }

    public void testNonNullForSkipRowPolicy() {
        assertNotNull(ColumnarRowDropHelper.forPolicy(new ErrorPolicy(10, true), null, "f.parquet"));
        assertNotNull(ColumnarRowDropHelper.forPolicy(ErrorPolicy.LENIENT, null, "f.parquet"));
    }

    // ---- single failure ----

    public void testSingleFailureDropsRow() {
        ColumnarRowDropHelper helper = helper(100);
        helper.beginBatch(3);
        helper.markFailed(1);

        assertTrue(helper.hasFailures());
        assertThat(helper.failedCount(), equalTo(1));

        Block[] blocks = { blockFactory.newConstantIntBlockWith(7, 3) };
        blocks = helper.filterBlocks(blocks, blockFactory);
        try {
            assertThat(blocks[0].getPositionCount(), equalTo(2)); // position 1 dropped
        } finally {
            for (Block b : blocks)
                b.close();
        }
    }

    // ---- one row failing two columns counts once (idempotent markFailed) ----

    public void testOneRowFailingTwoColumnsCountsOnce() {
        ColumnarRowDropHelper helper = helper(100);
        helper.beginBatch(4);
        helper.markFailed(2); // column A fails for row 2
        helper.markFailed(2); // column B also fails for row 2 — must not double-count
        helper.markFailed(3);

        assertThat(helper.failedCount(), equalTo(2)); // 2 distinct rows, not 3

        Block[] col1 = { blockFactory.newConstantIntBlockWith(1, 4) };
        Block[] col2 = { blockFactory.newConstantIntBlockWith(2, 4) };
        col1 = helper.filterBlocks(col1, blockFactory);
        col2 = helper.filterBlocks(col2, blockFactory);
        try {
            assertThat(col1[0].getPositionCount(), equalTo(2));
            assertThat(col2[0].getPositionCount(), equalTo(2));
        } finally {
            for (Block b : col1)
                b.close();
            for (Block b : col2)
                b.close();
        }
    }

    // ---- all-dropped batch ----

    public void testAllDroppedBatchProducesEmptyBlocks() {
        ColumnarRowDropHelper helper = helper(100);
        helper.beginBatch(3);
        helper.markFailed(0);
        helper.markFailed(1);
        helper.markFailed(2);

        assertThat(helper.failedCount(), equalTo(3));

        Block[] blocks = { blockFactory.newConstantIntBlockWith(99, 3) };
        blocks = helper.filterBlocks(blocks, blockFactory);
        try {
            assertThat(blocks[0].getPositionCount(), equalTo(0));
        } finally {
            for (Block b : blocks)
                b.close();
        }
    }

    // ---- multi-value positions ----

    public void testMvPositionDroppedByRowNotByValue() {
        // Row 1 has multi-value [20, 21]; dropping row 1 must remove both values as one position.
        ColumnarRowDropHelper helper = helper(100);
        helper.beginBatch(3);
        helper.markFailed(1);

        Block mvBlock;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(4)) {
            builder.appendInt(10);
            builder.beginPositionEntry();
            builder.appendInt(20);
            builder.appendInt(21);
            builder.endPositionEntry();
            builder.appendInt(30);
            mvBlock = builder.build();
        }
        Block[] blocks = { mvBlock };
        blocks = helper.filterBlocks(blocks, blockFactory);
        try {
            IntBlock filtered = (IntBlock) blocks[0];
            assertThat(filtered.getPositionCount(), equalTo(2));          // rows 0 and 2 remain
            assertThat(filtered.getValueCount(0), equalTo(1));            // row 0: single value 10
            assertThat(filtered.getInt(filtered.getFirstValueIndex(0)), equalTo(10));
            assertThat(filtered.getValueCount(1), equalTo(1));            // row 2: single value 30
            assertThat(filtered.getInt(filtered.getFirstValueIndex(1)), equalTo(30));
        } finally {
            for (Block b : blocks)
                b.close();
        }
    }

    // ---- no failures ----

    public void testNoFailuresLeavesBlocksUnchanged() {
        ColumnarRowDropHelper helper = helper(100);
        helper.beginBatch(3);

        assertFalse(helper.hasFailures());
        assertThat(helper.failedCount(), equalTo(0));

        Block original = blockFactory.newConstantIntBlockWith(5, 3);
        Block[] blocks = { original };
        Block[] filtered = helper.filterBlocks(blocks, blockFactory);
        try {
            assertSame(original, filtered[0]); // no copy made when there are no failures
            assertThat(filtered[0].getPositionCount(), equalTo(3));
        } finally {
            for (Block b : filtered)
                b.close();
        }
    }

    // ---- budget enforcement ----

    public void testBudgetExceededThrows() {
        ColumnarRowDropHelper helper = helper(1); // maxErrors = 1
        helper.beginBatch(3);
        helper.markFailed(0);
        helper.markFailed(1); // 2 errors, budget is 1
        helper.addToTotals(3, 2);
        expectThrows(ParsingException.class, helper::checkBudget);
    }

    public void testBudgetNotExceededDoesNotThrow() {
        ColumnarRowDropHelper helper = helper(5);
        helper.beginBatch(3);
        helper.markFailed(0);
        helper.addToTotals(3, 1);
        helper.checkBudget(); // should not throw
    }

    // ---- beginBatch resets state between batches ----

    public void testBeginBatchResetsBetweenBatches() {
        ColumnarRowDropHelper helper = helper(100);

        helper.beginBatch(2);
        helper.markFailed(0);
        assertThat(helper.failedCount(), equalTo(1));

        helper.beginBatch(3); // reset
        assertFalse(helper.hasFailures());
        assertThat(helper.failedCount(), equalTo(0));
    }

    private ColumnarRowDropHelper helper(long maxErrors) {
        return ColumnarRowDropHelper.forPolicy(new ErrorPolicy(maxErrors, true), null, "test.parquet");
    }
}
