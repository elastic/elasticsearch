/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Every case here is about the one thing the shape is meant to make impossible to get wrong: a block whose type came
 * from anywhere other than the declaration that produced its handle.
 */
public class LocalRelationBuilderTests extends ESTestCase {

    private static final ByteSizeValue LIMIT = ByteSizeValue.ofKb(256);

    private LocalRelationBuilder rows() {
        return new LocalRelationBuilder("f", TestBlockFactory.getNonBreakingInstance());
    }

    /** The declaration decides the block, so there is no second derivation to disagree with it. */
    public void testEachHandleBuildsItsOwnBlockType() {
        LocalRelationBuilder rows = rows();
        LocalRelationBuilder.Keyword name = rows.keyword("name");
        LocalRelationBuilder.Longs count = rows.longs("count");

        name.append("a");
        count.append(7L);

        try (Page page = rows.page()) {
            assertThat(page.getPositionCount(), equalTo(1));
            assertThat(((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()), equalTo(new BytesRef("a")));
            assertThat(((LongBlock) page.getBlock(1)).getLong(0), equalTo(7L));
        }
    }

    /** The column types a client is told, which a handle cannot contradict. */
    public void testTheDeclaredTypesAreTheColumnTypes() {
        LocalRelationBuilder rows = rows();
        rows.keyword("k");
        rows.text("t");
        rows.longs("l");
        rows.datetime("d");
        rows.ints("i");
        rows.doubles("f");
        rows.booleans("b");

        assertThat(
            rows.columns().stream().map(c -> c.dataType()).toList(),
            equalTo(
                List.of(
                    DataType.KEYWORD,
                    DataType.TEXT,
                    DataType.LONG,
                    DataType.DATETIME,
                    DataType.INTEGER,
                    DataType.DOUBLE,
                    DataType.BOOLEAN
                )
            )
        );
        rows.close();
    }

    /**
     * Every handle's {@code append} and {@code appendNull}, with the block each produces asserted rather than just
     * the absence of a throw: a handle wired to the wrong builder is a wrong block type, not an exception.
     */
    public void testEveryHandleAppendsItsOwnTypeAndItsOwnNull() {
        LocalRelationBuilder rows = rows();
        LocalRelationBuilder.Keyword keyword = rows.keyword("k");
        LocalRelationBuilder.Keyword text = rows.text("t");
        LocalRelationBuilder.Longs longs = rows.longs("l");
        LocalRelationBuilder.Longs datetime = rows.datetime("d");
        LocalRelationBuilder.Ints ints = rows.ints("i");
        LocalRelationBuilder.Doubles doubles = rows.doubles("f");
        LocalRelationBuilder.Booleans booleans = rows.booleans("b");

        keyword.append("a");
        // The BytesRef overload, which a caller already holding one uses to avoid a copy.
        text.append(new BytesRef("b"));
        longs.append(1L);
        datetime.append(1736000000000L);
        ints.append(2);
        doubles.append(0.5);
        booleans.append(true);

        keyword.appendNull();
        text.appendNull();
        longs.appendNull();
        datetime.appendNull();
        ints.appendNull();
        doubles.appendNull();
        booleans.appendNull();

        try (Page page = rows.page()) {
            assertThat(page.getPositionCount(), equalTo(2));
            assertThat(
                IntStream.range(0, page.getBlockCount()).mapToObj(i -> page.getBlock(i).elementType()).toList(),
                equalTo(
                    List.of(
                        ElementType.BYTES_REF,
                        ElementType.BYTES_REF,
                        ElementType.LONG,
                        ElementType.LONG,
                        ElementType.INT,
                        ElementType.DOUBLE,
                        ElementType.BOOLEAN
                    )
                )
            );
            assertThat(((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()), equalTo(new BytesRef("a")));
            assertThat(((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()), equalTo(new BytesRef("b")));
            assertThat(((LongBlock) page.getBlock(2)).getLong(0), equalTo(1L));
            assertThat(((LongBlock) page.getBlock(3)).getLong(0), equalTo(1736000000000L));
            assertThat(((IntBlock) page.getBlock(4)).getInt(0), equalTo(2));
            assertThat(((DoubleBlock) page.getBlock(5)).getDouble(0), equalTo(0.5));
            assertThat(((BooleanBlock) page.getBlock(6)).getBoolean(0), equalTo(true));
            for (int i = 0; i < page.getBlockCount(); i++) {
                assertThat("the null appended to column [" + i + "]", page.getBlock(i).isNull(1), equalTo(true));
            }
        }
    }

    public void testANullIsAnEmptyCellRatherThanAMissingOne() {
        LocalRelationBuilder rows = rows();
        LocalRelationBuilder.Keyword k = rows.keyword("k");
        k.appendNull();
        k.append("b");

        try (Page page = rows.page()) {
            assertThat(page.getPositionCount(), equalTo(2));
            assertThat(page.getBlock(0).isNull(0), equalTo(true));
            assertThat(page.getBlock(0).isNull(1), equalTo(false));
        }
    }

    /** {@code appendOrNull} is the shape most callers have: a value that may or may not be there. */
    public void testAppendOrNullTakesEither() {
        LocalRelationBuilder rows = rows();
        LocalRelationBuilder.Keyword k = rows.keyword("k");
        k.appendOrNull(null);
        k.appendOrNull("b");

        try (Page page = rows.page()) {
            assertThat(page.getBlock(0).isNull(0), equalTo(true));
            assertThat(((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()), equalTo(new BytesRef("b")));
        }
    }

    /** A function that matched nothing still owes one block per column, or the page is not the shape of its output. */
    public void testNoRowsStillCarriesOneBlockPerColumn() {
        LocalRelationBuilder rows = rows();
        rows.keyword("a");
        rows.longs("b");

        try (Page page = rows.page()) {
            assertThat(page.getBlockCount(), equalTo(2));
            assertThat(page.getPositionCount(), equalTo(0));
        }
    }

    /** A relation with no output is a plan the rest of the language has no answer for. */
    public void testNoColumnsIsRefused() {
        LocalRelationBuilder rows = rows();
        IllegalStateException e = expectThrows(IllegalStateException.class, rows::page);
        assertThat(e.getMessage(), containsString("declared no columns"));
    }

    /** One name twice makes a reference to it ambiguous, with nothing downstream positioned to say which. */
    public void testTheSameNameTwiceIsRefused() {
        LocalRelationBuilder rows = rows();
        rows.keyword("a");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> rows.keyword("a"));
        assertThat(e.getMessage(), containsString("declared the column [a] more than once"));
        rows.close();
    }

    /**
     * A column short of another is a function that stopped appending part way through. Refused rather than padded,
     * because which column it meant to leave empty is not something this can know.
     */
    public void testAColumnShortOfAnotherIsRefused() {
        LocalRelationBuilder rows = rows();
        LocalRelationBuilder.Keyword a = rows.keyword("a");
        LocalRelationBuilder.Longs b = rows.longs("b");
        a.append("x");
        a.append("y");
        b.append(1L);

        IllegalStateException e = expectThrows(IllegalStateException.class, rows::page);
        assertThat(e.getMessage(), containsString("wrote [1] values to the column [b] and [2] to [a]"));
    }

    public void testDeclaringAfterThePageIsBuiltIsRefused() {
        LocalRelationBuilder rows = rows();
        rows.keyword("a");
        rows.page().releaseBlocks();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> rows.keyword("b"));
        assertThat(e.getMessage(), containsString("after finishing"));
    }

    public void testBuildingThePageTwiceIsRefused() {
        LocalRelationBuilder rows = rows();
        rows.keyword("a");
        rows.page().releaseBlocks();

        IllegalStateException e = expectThrows(IllegalStateException.class, rows::page);
        assertThat(e.getMessage(), containsString("built its page twice"));
    }

    /** Closing without building releases what was written; the breaker is what notices if it does not. */
    public void testCloseReleasesWhatWasWritten() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, LIMIT).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory factory = new BlockFactory(breaker, bigArrays);

        LocalRelationBuilder rows = new LocalRelationBuilder("f", factory);
        LocalRelationBuilder.Keyword k = rows.keyword("k");
        for (int i = 0; i < 1000; i++) {
            k.append("some text that takes a few bytes " + i);
        }
        assertThat(breaker.getUsed() > 0, equalTo(true));

        rows.close();

        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** And the page's blocks are the caller's from there, so closing after handing them over releases nothing twice. */
    public void testCloseAfterThePageIsHandedOverIsANoOp() {
        LocalRelationBuilder rows = rows();
        rows.keyword("k").append("a");

        Page page = rows.page();
        rows.close();

        assertThat(((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()), equalTo(new BytesRef("a")));
        page.releaseBlocks();
    }

    public void testAValueTooLargeForTheBreakerFailsRatherThanLeaks() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofKb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory factory = new BlockFactory(breaker, bigArrays);

        LocalRelationBuilder rows = new LocalRelationBuilder("f", factory);
        LocalRelationBuilder.Keyword k = rows.keyword("k");
        expectThrows(CircuitBreakingException.class, () -> {
            for (int i = 0; i < 100_000; i++) {
                k.append("some text that takes a few bytes " + i);
            }
        });
        rows.close();

        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
