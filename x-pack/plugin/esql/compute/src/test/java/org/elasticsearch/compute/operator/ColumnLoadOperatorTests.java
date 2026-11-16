/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceIntBlockSourceOperator;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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

    public void testMultiColumnDensePageUnderTargetKeepsWhole() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = 3333;
        Block[] blocks = new Block[10];
        try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                builder.appendInt(p % 5);
            }
            blocks[0] = builder.build().asBlock();
        }
        for (int c = 1; c < blocks.length; c++) {
            try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    builder.appendInt(p);
                }
                blocks[c] = builder.build().asBlock();
            }
        }
        BlockFactory nonBreaking = TestBlockFactory.getNonBreakingInstance();
        List<ColumnLoadOperator.Values> values = List.of(
            new ColumnLoadOperator.Values("v0", nonBreaking.newLongArrayVector(new long[] { 0, 1, 2, 3, 4 }, 5).asBlock()),
            new ColumnLoadOperator.Values("v1", nonBreaking.newLongArrayVector(new long[] { 100, 101, 102, 103, 104 }, 5).asBlock()),
            new ColumnLoadOperator.Values("v2", nonBreaking.newLongArrayVector(new long[] { 200, 201, 202, 203, 204 }, 5).asBlock())
        );
        ColumnLoadOperator.Factory factory = new ColumnLoadOperator.Factory(values, 0);
        assertThat(factory, equalTo(new ColumnLoadOperator.Factory(new ArrayList<>(values), 0)));
        List<Page> results = new TestDriverRunner().builder(driverContext).input(new Page(blocks)).run(factory);
        try {
            assertThat(results, hasSize(1));
            Page out = results.get(0);
            assertThat(out.getPositionCount(), equalTo(positionCount));
            assertThat(out.getBlockCount(), equalTo(blocks.length + 3));
            for (int c = 0; c < 3; c++) {
                LongBlock col = out.getBlock(blocks.length + c);
                for (int p = 0; p < positionCount; p++) {
                    assertThat(col.getLong(p), equalTo(c * 100L + (p % 5)));
                }
            }
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
            }
        }
    }

    public void testMultiColumnCombinedSizeSplitsOversizedLoad() {
        DriverContext driverContext = driverContext();
        int positionCount = 8192;
        Page input;
        try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                builder.appendInt(p % 5);
            }
            input = new Page(builder.build().asBlock());
        }
        BlockFactory nonBreaking = TestBlockFactory.getNonBreakingInstance();
        List<ColumnLoadOperator.Values> values = new ArrayList<>(10);
        for (int c = 0; c < 10; c++) {
            long base = c * 1000L;
            Block col = nonBreaking.newLongArrayVector(new long[] { base, base + 3, base + 6, base + 9, base + 12 }, 5).asBlock();
            values.add(new ColumnLoadOperator.Values("v" + c, col));
        }
        List<Page> results = new TestDriverRunner().builder(driverContext).input(input).run(new ColumnLoadOperator.Factory(values, 0));
        try {
            assertThat(results.size(), greaterThan(1));
            int rowOffset = 0;
            for (Page p : results) {
                long loadedBytes = 0;
                for (int c = 0; c < 10; c++) {
                    LongBlock col = p.getBlock(1 + c);
                    loadedBytes += col.ramBytesUsed();
                    for (int r = 0; r < p.getPositionCount(); r++) {
                        int key = (rowOffset + r) % 5;
                        assertThat(col.getLong(r), equalTo(c * 1000L + 3L * key));
                    }
                }
                assertThat(loadedBytes, lessThanOrEqualTo(ColumnLoadOperator.TARGET_LOADED_BYTES.getBytes() + 4096));
                rowOffset += p.getPositionCount();
            }
            assertThat(rowOffset, equalTo(positionCount));
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
            }
        }
    }

    public void testConstantColumnsKeepDensePageWhole() {
        DriverContext driverContext = driverContext();
        int positionCount = 8192;
        Page input;
        try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                builder.appendInt(0);
            }
            input = new Page(builder.build().asBlock());
        }
        BlockFactory nonBreaking = TestBlockFactory.getNonBreakingInstance();
        List<ColumnLoadOperator.Values> values = new ArrayList<>(10);
        for (int c = 0; c < 10; c++) {
            values.add(new ColumnLoadOperator.Values("c" + c, nonBreaking.newConstantLongBlockWith(10L + c, 1)));
        }
        List<Page> results = new TestDriverRunner().builder(driverContext).input(input).run(new ColumnLoadOperator.Factory(values, 0));
        try {
            assertThat(results, hasSize(1));
            Page out = results.get(0);
            assertThat(out.getPositionCount(), equalTo(positionCount));
            for (int c = 0; c < 10; c++) {
                LongBlock col = out.getBlock(1 + c);
                assertThat(col.getLong(0), equalTo(10L + c));
                assertThat(col.getLong(positionCount - 1), equalTo(10L + c));
            }
        } finally {
            for (Page p : results) {
                p.releaseBlocks();
            }
        }
    }

    public void testBytesRefPageUnderTargetKeepsWhole() {
        for (int nonNullEvery : new int[] { 1, 10 }) {
            DriverContext driverContext = driverContext();
            int positionCount = 20_000;
            Page input;
            try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    if (p % nonNullEvery == 0) {
                        builder.appendInt(p / nonNullEvery % 2);
                    } else {
                        builder.appendNull();
                    }
                }
                input = new Page(builder.build());
            }
            BytesRefBlock v0;
            BytesRefBlock v1;
            try (
                BytesRefBlock.Builder b0 = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(2);
                BytesRefBlock.Builder b1 = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(2)
            ) {
                v0 = b0.appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("bb")).build();
                v1 = b1.appendBytesRef(new BytesRef("x")).appendBytesRef(new BytesRef("yy")).build();
            }
            List<Page> results = new TestDriverRunner().builder(driverContext)
                .input(input)
                .run(
                    new ColumnLoadOperator.Factory(
                        List.of(new ColumnLoadOperator.Values("v0", v0), new ColumnLoadOperator.Values("v1", v1)),
                        0
                    )
                );
            try {
                assertThat(results, hasSize(1));
                Page out = results.get(0);
                assertThat(out.getPositionCount(), equalTo(positionCount));
                BytesRefBlock out0 = out.getBlock(1);
                BytesRefBlock out1 = out.getBlock(2);
                BytesRef scratch = new BytesRef();
                assertThat(out0.getBytesRef(0, scratch), equalTo(new BytesRef("a")));
                assertThat(out1.getBytesRef(0, scratch), equalTo(new BytesRef("x")));
            } finally {
                for (Page p : results) {
                    p.releaseBlocks();
                }
                v0.close();
                v1.close();
            }
        }
    }

    public void testMultiColumnCranky() {
        DriverContext inputContext = driverContext();
        List<Page> input = new ArrayList<>();
        for (int pageIdx = 0; pageIdx < 4; pageIdx++) {
            try (IntBlock.Builder builder = inputContext.blockFactory().newIntBlockBuilder(500)) {
                for (int p = 0; p < 500; p++) {
                    if (p % 7 == 0) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(between(0, 4));
                    }
                }
                input.add(new Page(builder.build()));
            }
        }
        DriverContext crankyContext = crankyDriverContext();
        Block v0 = TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 0, 3, 6, 9, 12 }, 5).asBlock();
        BytesRefBlock v1;
        try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(5)) {
            for (int i = 0; i < 5; i++) {
                builder.appendBytesRef(new BytesRef("val" + i));
            }
            v1 = builder.build();
        }
        boolean driverStarted = false;
        try {
            ColumnLoadOperator.Factory factory = new ColumnLoadOperator.Factory(
                List.of(new ColumnLoadOperator.Values("v0", v0), new ColumnLoadOperator.Values("v1", v1)),
                0
            );
            Operator operator = factory.get(crankyContext);
            driverStarted = true;
            List<Page> results = new TestDriverRunner().builder(crankyContext).input(input).run(operator);
            for (Page p : results) {
                p.releaseBlocks();
            }
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        } finally {
            if (driverStarted == false) {
                for (Page p : input) {
                    p.releaseBlocks();
                }
            }
            v1.close();
        }
        assertThat(inputContext.breaker().getUsed(), equalTo(0L));
        assertThat(crankyContext.breaker().getUsed(), equalTo(0L));
    }

    public void testUnsupportedLookupBlockDelegatesToLookup() {
        BlockFactory nonBreaking = TestBlockFactory.getNonBreakingInstance();
        LongBlock from = nonBreaking.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock();
        LongBlock to = nonBreaking.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock();
        LongRangeArrayBlock rangeValues = new LongRangeArrayBlock(from, to);
        for (ColumnLoadOperator.Factory factory : List.of(
            new ColumnLoadOperator.Factory(new ColumnLoadOperator.Values("v1", rangeValues), 0),
            new ColumnLoadOperator.Factory(
                List.of(new ColumnLoadOperator.Values("v0", from), new ColumnLoadOperator.Values("v1", rangeValues)),
                0
            )
        )) {
            DriverContext emptyContext = driverContext();
            List<Page> emptyResults = new TestDriverRunner().builder(emptyContext).input(List.<Page>of()).run(factory);
            assertThat(emptyResults, hasSize(0));

            DriverContext nonEmptyContext = driverContext();
            Page input;
            try (IntVector.FixedBuilder builder = nonEmptyContext.blockFactory().newIntVectorFixedBuilder(2)) {
                builder.appendInt(0).appendInt(1);
                input = new Page(builder.build().asBlock());
            }
            expectThrows(
                UnsupportedOperationException.class,
                () -> new TestDriverRunner().builder(nonEmptyContext).input(input).run(factory)
            );
        }
    }

    public void testMultiColumnNegativePositionThrows() {
        BlockFactory nonBreaking = TestBlockFactory.getNonBreakingInstance();
        ColumnLoadOperator.Factory factory = new ColumnLoadOperator.Factory(
            List.of(
                new ColumnLoadOperator.Values("c0", nonBreaking.newConstantLongBlockWith(1L, 1)),
                new ColumnLoadOperator.Values("c1", nonBreaking.newConstantLongBlockWith(2L, 1))
            ),
            0
        );
        DriverContext context = driverContext();
        Page input = new Page(context.blockFactory().newIntArrayVector(new int[] { 0, -2 }, 2).asBlock());
        var e = expectThrows(IllegalArgumentException.class, () -> new TestDriverRunner().builder(context).input(input).run(factory));
        assertThat(e.getMessage(), containsString("invalid position [-2]"));
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
        return equalTo("ColumnLoad[values=[values:LONG], positions=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
