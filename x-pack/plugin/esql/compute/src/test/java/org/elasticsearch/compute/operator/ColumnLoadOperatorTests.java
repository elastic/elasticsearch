/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceIntBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class ColumnLoadOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceIntBlockSourceOperator(blockFactory, IntStream.range(0, size).map(l -> between(0, 4)));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int count = input.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(count));
        int keysIdx = 0;
        int loadedIdx = 0;
        IntBlock keys = null;
        int keysOffset = 0;
        LongBlock loaded = null;
        int loadedOffset = 0;
        int p = 0;
        while (p < count) {
            if (keys == null) {
                keys = input.get(keysIdx++).getBlock(0);
            }
            if (loaded == null) {
                loaded = results.get(loadedIdx++).getBlock(1);
            }
            int valueCount = keys.getValueCount(p - keysOffset);
            assertThat(loaded.getValueCount(p - loadedOffset), equalTo(valueCount));
            int keysStart = keys.getFirstValueIndex(p - keysOffset);
            int loadedStart = loaded.getFirstValueIndex(p - loadedOffset);
            for (int k = keysStart, l = loadedStart; k < keysStart + valueCount; k++, l++) {
                assertThat(loaded.getLong(l), equalTo(3L * keys.getInt(k)));
            }
            p++;
            if (p - keysOffset == keys.getPositionCount()) {
                keysOffset += keys.getPositionCount();
                keys = null;
            }
            if (p - loadedOffset == loaded.getPositionCount()) {
                loadedOffset += loaded.getPositionCount();
                loaded = null;
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ColumnLoadOperator.Factory(
            new ColumnLoadOperator.Values(
                "values",
                TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 0, 3, 6, 9, 12 }, 5).asBlock()
            ),
            0
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ColumnLoad[values=values:LONG, positions=0]");
    }

    public void testNullHeavyPageStaysWhole() {
        assertPageStaysWhole(3333, p -> p % 10 == 0);
    }

    public void testDensePageStaysWhole() {
        assertPageStaysWhole(3333, p -> true);
    }

    private void assertPageStaysWhole(int positionCount, IntPredicate hit) {
        DriverContext driverContext = driverContext();
        Page input;
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (hit.test(p)) {
                    builder.appendInt(between(0, 4));
                } else {
                    builder.appendNull();
                }
            }
            input = new Page(builder.build());
        }
        List<Page> origInput = BlockTestUtils.deepCopyOf(List.of(input), TestBlockFactory.getNonBreakingInstance());
        List<Page> results = new TestDriverRunner().builder(driverContext).input(input).run(simple());
        try {
            assertThat(results, hasSize(1));
            assertSimpleOutput(origInput, results);
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
            }
        }
    }

    public void testBlownUpPageSplits() {
        int valuesPerPosition = 1000;
        int positionCount = 100;
        LongBlock values;
        try (LongBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(valuesPerPosition)) {
            builder.beginPositionEntry();
            for (int v = 0; v < valuesPerPosition; v++) {
                builder.appendLong(v);
            }
            builder.endPositionEntry();
            values = builder.build();
        }
        DriverContext driverContext = driverContext();
        Page input = new Page(driverContext.blockFactory().newConstantIntBlockWith(0, positionCount));
        List<Page> results = new TestDriverRunner().builder(driverContext)
            .input(input)
            .run(new ColumnLoadOperator.Factory(new ColumnLoadOperator.Values("values", values), 0));
        try {
            // each position copies about 8KB, so a page of 100 positions is well over the target
            assertThat(results, hasSize(greaterThan(1)));
            assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(positionCount));
            for (Page page : results) {
                LongBlock loaded = page.getBlock(1);
                for (int p = 0; p < loaded.getPositionCount(); p++) {
                    assertThat(loaded.getValueCount(p), equalTo(valuesPerPosition));
                }
            }
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
            }
        }
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
