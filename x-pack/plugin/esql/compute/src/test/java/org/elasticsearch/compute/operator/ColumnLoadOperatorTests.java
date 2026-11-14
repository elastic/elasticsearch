/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceIntBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
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

    public void testTargetBlockSize() {
        assertThat(ColumnLoadOperator.targetBlockSize(4).getBytes(), equalTo((long) Operator.TARGET_PAGE_SIZE / 5));
        assertThat(ColumnLoadOperator.targetBlockSize(1000), equalTo(ByteSizeValue.ofKb(10)));
    }

    public void testSparsePositionsKeepPageWhole() {
        DriverContext driverContext = driverContext();
        int positionCount = 3333;
        Page input;
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                if (p % 10 == 0) {
                    builder.appendInt(between(0, 4));
                } else {
                    builder.appendNull();
                }
            }
            input = new Page(builder.build());
        }
        List<Page> results = new TestDriverRunner().builder(driverContext).input(input).run(simple().get(driverContext));
        try {
            assertThat(results, hasSize(1));
            assertThat(results.get(0).getPositionCount(), equalTo(positionCount));
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
            }
        }
    }

    public void testDensePageNearTargetKeepsWhole() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int longColumns = 3;
        // A builder that grows to fit the loaded column crosses the target before it is full.
        int positionCount = (int) (ColumnLoadOperator.targetBlockSize(longColumns + 1).getBytes() * 95 / 100 / Long.BYTES);
        Block[] blocks = new Block[longColumns + 1];
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                builder.appendInt(between(0, 4));
            }
            blocks[0] = builder.build().asBlock();
        }
        for (int c = 1; c <= longColumns; c++) {
            try (LongVector.FixedBuilder builder = blockFactory.newLongVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    builder.appendLong(randomLong());
                }
                blocks[c] = builder.build().asBlock();
            }
        }
        List<Page> results = new TestDriverRunner().builder(driverContext).input(new Page(blocks)).run(simple().get(driverContext));
        try {
            assertThat(results, hasSize(1));
            assertThat(results.get(0).getPositionCount(), equalTo(positionCount));
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
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

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
