/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.WarningsTests.TestWarningsSource;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ChangePointOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // size must be in [25, 1000] for ChangePoint to function correctly
        // and detect the step change.
        size = Math.clamp(size, 25, 1000);
        List<Long> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (i <= size / 2) {
                data.add(0L);
            } else {
                data.add(1L);
            }
        }
        return new SequenceLongBlockSourceOperator(blockFactory, data);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        boolean seenOne = false;
        assertThat(results, hasSize(input.size()));
        for (int i = 0; i < results.size(); i++) {
            Page inputPage = input.get(i);
            Page resultPage = results.get(i);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(3));
            for (int j = 0; j < resultPage.getPositionCount(); j++) {
                long inputValue = ((LongBlock) resultPage.getBlock(0)).getLong(j);
                long resultValue = ((LongBlock) resultPage.getBlock(0)).getLong(j);
                assertThat(resultValue, equalTo(inputValue));
                if (seenOne == false && resultValue == 1L) {
                    BytesRef type = ((BytesRefBlock) resultPage.getBlock(1)).getBytesRef(j, new BytesRef());
                    double pvalue = ((DoubleBlock) resultPage.getBlock(2)).getDouble(j);
                    assertThat(type.utf8ToString(), equalTo("step_change"));
                    assertThat(pvalue, equalTo(0.0));
                    seenOne = true;
                } else {
                    assertThat(resultPage.getBlock(1).isNull(j), equalTo(true));
                    assertThat(resultPage.getBlock(2).isNull(j), equalTo(true));
                }
            }
        }
        assertThat(seenOne, equalTo(true));
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ChangePointOperator.Factory(0, null, new TestWarningsSource(null));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ChangePointOperator[channel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("ChangePointOperator[channel=0]");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    @Override
    public void testCanProduceMoreDataWithoutExtraInput() {
        // Change point cannot work with empty input, so skip this test
    }

    /**
     * Two groups of 30 rows each, spread across three pages of 20 rows.
     * The A/B group boundary falls in the middle of page 1, testing that the operator
     * correctly detects independent change points per group and annotates the right rows
     * even when a page contains rows from more than one group.
     * <p>
     * Layout:
     * <ul>
     *   <li>Page 0 (rows  0-19): group A — values 0L×15, 1L×5  → change point at position 15</li>
     *   <li>Page 1 (rows 20-39): group A×10 (all 1L), group B×10 (all 0L) — no change points</li>
     *   <li>Page 2 (rows 40-59): group B — values 0L×5, 1L×15  → change point at position 5</li>
     * </ul>
     * Column layout per page: block 0 = group key (BytesRef), block 1 = value (Long).
     * Output appends block 2 = change type (BytesRef), block 3 = p-value (Double).
     */
    public void testGroupedTwoGroupsThreePages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        BytesRef groupA = new BytesRef("A");
        BytesRef groupB = new BytesRef("B");

        Page page0, page1, page2;
        try (
            BytesRefBlock.Builder g = blockFactory.newBytesRefBlockBuilder(20);
            LongBlock.Builder v = blockFactory.newLongBlockBuilder(20)
        ) {
            for (int i = 0; i < 20; i++) {
                g.appendBytesRef(groupA);
                v.appendLong(i < 15 ? 0L : 1L);
            }
            page0 = new Page(g.build(), v.build());
        }
        try (
            BytesRefBlock.Builder g = blockFactory.newBytesRefBlockBuilder(20);
            LongBlock.Builder v = blockFactory.newLongBlockBuilder(20)
        ) {
            for (int i = 0; i < 10; i++) {
                g.appendBytesRef(groupA);
                v.appendLong(1L);
            }
            for (int i = 0; i < 10; i++) {
                g.appendBytesRef(groupB);
                v.appendLong(0L);
            }
            page1 = new Page(g.build(), v.build());
        }
        try (
            BytesRefBlock.Builder g = blockFactory.newBytesRefBlockBuilder(20);
            LongBlock.Builder v = blockFactory.newLongBlockBuilder(20)
        ) {
            for (int i = 0; i < 20; i++) {
                g.appendBytesRef(groupB);
                v.appendLong(i < 5 ? 0L : 1L);
            }
            page2 = new Page(g.build(), v.build());
        }

        // group key is block 0, value is block 1; groupChannels[0] = 0 (hardcoded in operator)
        List<Page> outputPages = new ArrayList<>();
        try (ChangePointOperator op = new ChangePointOperator(ctx, 1, 0, new TestWarningsSource(null))) {
            op.addInput(page0);
            op.addInput(page1);
            op.addInput(page2);
            op.finish();
            Page out;
            while ((out = op.getOutput()) != null) {
                outputPages.add(out);
            }
        }

        try {
            assertThat(outputPages, hasSize(3));
            assertChangePointAt(outputPages.get(0), 15);   // group A change point on page 0
            assertNoChangePoints(outputPages.get(1));       // boundary page: no change points
            assertChangePointAt(outputPages.get(2), 5);    // group B change point on page 2
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    private void assertChangePointAt(Page page, int position) {
        BytesRefBlock typeBlock = page.getBlock(2);
        DoubleBlock pvalueBlock = page.getBlock(3);
        for (int j = 0; j < page.getPositionCount(); j++) {
            if (j == position) {
                assertThat("expected change type at " + j, typeBlock.isNull(j), equalTo(false));
                assertThat("expected pvalue at " + j, pvalueBlock.isNull(j), equalTo(false));
                assertThat(typeBlock.getBytesRef(j, new BytesRef()).utf8ToString(), equalTo("step_change"));
            } else {
                assertThat("unexpected change type at " + j, typeBlock.isNull(j), equalTo(true));
                assertThat("unexpected pvalue at " + j, pvalueBlock.isNull(j), equalTo(true));
            }
        }
    }

    private void assertNoChangePoints(Page page) {
        Block typeBlock = page.getBlock(2);
        Block pvalueBlock = page.getBlock(3);
        for (int j = 0; j < page.getPositionCount(); j++) {
            assertThat("unexpected change type at " + j, typeBlock.isNull(j), equalTo(true));
            assertThat("unexpected pvalue at " + j, pvalueBlock.isNull(j), equalTo(true));
        }
    }
}
