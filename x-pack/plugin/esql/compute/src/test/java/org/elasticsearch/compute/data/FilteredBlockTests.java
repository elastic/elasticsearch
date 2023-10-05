/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilteredBlockTests extends ESTestCase {

    final CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
    final BlockFactory blockFactory = BlockFactory.getInstance(breaker, bigArrays);

    @Before
    @After
    public void checkBreaker() {
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testFilterAllPositions() {
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var block = vector.asBlock();
        var filteredBlock = block.filter();
        var filteredVector = filteredBlock.asVector();

        assertEquals(0, filteredVector.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredVector.getInt(0));

        assertEquals(0, filteredBlock.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredBlock.getInt(0));
        releaseAndAssertBreaker(filteredBlock);
    }

    public void testKeepAllPositions() {
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var positions = IntStream.range(0, positionCount).toArray();
        var block = vector.asBlock();
        var filteredBlock = block.filter(positions);
        var filteredVector = filteredBlock.asVector();

        assertEquals(positionCount, filteredVector.getPositionCount());
        var anyPosition = randomPosition(positionCount);
        assertEquals(anyPosition, filteredVector.getInt(anyPosition));

        assertEquals(positionCount, filteredBlock.getPositionCount());
        assertEquals(anyPosition, filteredBlock.getInt(anyPosition));
        releaseAndAssertBreaker(filteredBlock);
    }

    public void testKeepSomePositions() {
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var positions = IntStream.range(0, positionCount).filter(i -> i % 2 == 0).toArray();
        var block = vector.asBlock();
        var filteredBlock = block.filter(positions);
        var filteredVector = filteredBlock.asVector();

        assertEquals(positionCount / 2, filteredVector.getPositionCount());
        var anyPosition = randomIntBetween(0, (positionCount / 2) - 1);
        assertEquals(anyPosition * 2, filteredVector.getInt(anyPosition));
        assertEquals(anyPosition * 2, filteredBlock.getInt(anyPosition));

        assertEquals(positionCount / 2, filteredBlock.getPositionCount());
        assertEquals(anyPosition * 2, filteredBlock.getInt(anyPosition));
        releaseAndAssertBreaker(filteredBlock);
    }

    public void testFilterOnFilter() {  // TODO: tired of this sv / mv block here. do more below
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var block = vector.asBlock();
        var filteredBlock = block.filter(IntStream.range(0, positionCount).filter(i1 -> i1 % 2 == 0).toArray());
        var filteredTwice = filteredBlock.filter(IntStream.range(0, positionCount / 2).filter(i -> i % 2 == 0).toArray());

        assertEquals(positionCount / 4, filteredTwice.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount / 4 - 1);
        assertEquals(anyPosition * 4, filteredTwice.getInt(anyPosition));
        releaseAndAssertBreaker(filteredTwice);
    }

    public void testFilterOnNull() {
        IntBlock block;
        if (randomBoolean()) {
            var nulls = new BitSet();
            nulls.set(1);
            block = blockFactory.newIntArrayBlock(new int[] { 10, 0, 30, 40 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        } else {
            var blockBuilder = blockFactory.newIntBlockBuilder(4);
            blockBuilder.appendInt(10);
            blockBuilder.appendNull();
            blockBuilder.appendInt(30);
            blockBuilder.appendInt(40);
            block = blockBuilder.build();
        }

        var filtered = block.filter(1, 2, 3);

        assertTrue(filtered.isNull(0));
        assertTrue(filtered.mayHaveNulls());
        assertFalse(filtered.areAllValuesNull());
        assertEquals(1, filtered.nullValuesCount());
        assertEquals(2, filtered.getTotalValueCount());
        assertFalse(filtered.isNull(1));
        assertEquals(30, filtered.getInt(filtered.getFirstValueIndex(1)));
        releaseAndAssertBreaker(filtered);
    }

    public void testFilterOnAllNullsBlock() {
        Block block;
        if (randomBoolean()) {
            var nulls = new BitSet();
            nulls.set(0, 4);
            block = blockFactory.newIntArrayBlock(new int[] { 0, 0, 0, 0 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        } else {
            var blockBuilder = blockFactory.newIntBlockBuilder(4);
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            block = blockBuilder.build();
        }

        var filtered = block.filter(1, 2, 3);

        assertTrue(filtered.isNull(0));
        assertTrue(filtered.mayHaveNulls());
        assertTrue(filtered.areAllValuesNull());
        assertEquals(3, filtered.nullValuesCount());
        assertEquals(0, filtered.getTotalValueCount());
        releaseAndAssertBreaker(filtered);
    }

    public void testFilterOnNoNullsBlock() {
        IntBlock block;
        if (randomBoolean()) {
            block = blockFactory.newIntArrayVector(new int[] { 10, 20, 30, 40 }, 4).asBlock();
        } else {
            var blockBuilder = blockFactory.newIntBlockBuilder(4);
            blockBuilder.appendInt(10);
            blockBuilder.appendInt(20);
            blockBuilder.appendInt(30);
            blockBuilder.appendInt(40);
            block = blockBuilder.build();
        }
        var filtered = block.filter(1, 2, 3);

        assertFalse(filtered.isNull(0));
        assertFalse(filtered.mayHaveNulls());
        assertFalse(filtered.areAllValuesNull());
        assertEquals(0, filtered.nullValuesCount());
        assertEquals(3, filtered.getTotalValueCount());

        assertEquals(20, filtered.asVector().getInt(0));
        assertEquals(30, filtered.asVector().getInt(1));
        assertEquals(40, filtered.asVector().getInt(2));
        releaseAndAssertBreaker(filtered);
    }

    public void testFilterToStringSimple() {
        BitSet nulls = BitSet.valueOf(new byte[] { 0x08 });  // any non-empty bitset, that does not affect the filter, should suffice

        var boolVector = new BooleanArrayVector(new boolean[] { true, false, false, true }, 4);
        var boolBlock = new BooleanArrayBlock(
            new boolean[] { true, false, false, true },
            4,
            null,
            nulls,
            randomFrom(Block.MvOrdering.values())
        );
        for (Object obj : List.of(boolVector.filter(0, 2), boolVector.asBlock().filter(0, 2), boolBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[true, false]"));
            assertThat(s, containsString("positions=2"));
        }

        var intVector = new IntArrayVector(new int[] { 10, 20, 30, 40 }, 4);
        var intBlock = new IntArrayBlock(new int[] { 10, 20, 30, 40 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        for (Object obj : List.of(intVector.filter(0, 2), intVector.asBlock().filter(0, 2), intBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[10, 30]"));
            assertThat(s, containsString("positions=2"));
        }

        var longVector = new LongArrayVector(new long[] { 100L, 200L, 300L, 400L }, 4);
        var longBlock = new LongArrayBlock(new long[] { 100L, 200L, 300L, 400L }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        for (Object obj : List.of(longVector.filter(0, 2), longVector.asBlock().filter(0, 2), longBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[100, 300]"));
            assertThat(s, containsString("positions=2"));
        }

        var doubleVector = new DoubleArrayVector(new double[] { 1.1, 2.2, 3.3, 4.4 }, 4);
        var doubleBlock = new DoubleArrayBlock(new double[] { 1.1, 2.2, 3.3, 4.4 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        for (Object obj : List.of(doubleVector.filter(0, 2), doubleVector.asBlock().filter(0, 2), doubleBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[1.1, 3.3]"));
            assertThat(s, containsString("positions=2"));
        }

        assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("3c").toString().equals("[33 63]");
        try (var bytesRefArray = arrayOf("1a", "2b", "3c", "4d")) {
            var bytesRefVector = new BytesRefArrayVector(bytesRefArray, 4);
            var bytesRefBlock = new BytesRefArrayBlock(bytesRefArray, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
            for (Object obj : List.of(bytesRefVector.filter(0, 2), bytesRefVector.asBlock().filter(0, 2), bytesRefBlock.filter(0, 2))) {
                String s = obj.toString();
                assertThat(s, containsString("[[31 61], [33 63]]"));
                assertThat(s, containsString("positions=2"));
            }
        }
    }

    public void testFilterToStringMultiValue() {
        var bb = blockFactory.newBooleanBlockBuilder(6);
        bb.beginPositionEntry().appendBoolean(true).appendBoolean(true).endPositionEntry();
        bb.beginPositionEntry().appendBoolean(false).appendBoolean(false).endPositionEntry();
        bb.beginPositionEntry().appendBoolean(false).appendBoolean(false).endPositionEntry();
        Block filter = bb.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[true, true], [false, false]]"));
        assertThat(filter.toString(), containsString("positions=2"));
        releaseAndAssertBreaker(filter);

        var ib = blockFactory.newIntBlockBuilder(6);
        ib.beginPositionEntry().appendInt(0).appendInt(10).endPositionEntry();
        ib.beginPositionEntry().appendInt(20).appendInt(50).endPositionEntry();
        ib.beginPositionEntry().appendInt(90).appendInt(1000).endPositionEntry();
        filter = ib.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[0, 10], [20, 50]]"));
        assertThat(filter.toString(), containsString("positions=2"));
        releaseAndAssertBreaker(filter);

        var lb = blockFactory.newLongBlockBuilder(6);
        lb.beginPositionEntry().appendLong(0).appendLong(10).endPositionEntry();
        lb.beginPositionEntry().appendLong(20).appendLong(50).endPositionEntry();
        lb.beginPositionEntry().appendLong(90).appendLong(1000).endPositionEntry();
        filter = lb.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[0, 10], [20, 50]]"));
        assertThat(filter.toString(), containsString("positions=2"));
        releaseAndAssertBreaker(filter);

        var db = blockFactory.newDoubleBlockBuilder(6);
        db.beginPositionEntry().appendDouble(0).appendDouble(10).endPositionEntry();
        db.beginPositionEntry().appendDouble(0.002).appendDouble(10e8).endPositionEntry();
        db.beginPositionEntry().appendDouble(90).appendDouble(1000).endPositionEntry();
        filter = db.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[0.0, 10.0], [0.002, 1.0E9]]"));
        assertThat(filter.toString(), containsString("positions=2"));
        releaseAndAssertBreaker(filter);

        assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("3c").toString().equals("[33 63]");
        assert new BytesRef("cat").toString().equals("[63 61 74]") && new BytesRef("dog").toString().equals("[64 6f 67]");
        var bytesBlock = blockFactory.newBytesRefBlockBuilder(6);
        bytesBlock.beginPositionEntry().appendBytesRef(new BytesRef("1a")).appendBytesRef(new BytesRef("3c")).endPositionEntry();
        bytesBlock.beginPositionEntry().appendBytesRef(new BytesRef("cat")).appendBytesRef(new BytesRef("dog")).endPositionEntry();
        bytesBlock.beginPositionEntry().appendBytesRef(new BytesRef("pig")).appendBytesRef(new BytesRef("chicken")).endPositionEntry();
        filter = bytesBlock.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[[31 61], [33 63]], [[63 61 74], [64 6f 67]]"));
        assertThat(filter.toString(), containsString("positions=2"));
        releaseAndAssertBreaker(filter);
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }

    BytesRefArray arrayOf(String... values) {
        var array = new BytesRefArray(values.length, bigArrays);
        Arrays.stream(values).map(BytesRef::new).forEach(array::append);
        return array;
    }

    void releaseAndAssertBreaker(Block... blocks) {
        assertThat(breaker.getUsed(), greaterThan(0L));
        Page[] pages = Arrays.stream(blocks).map(Page::new).toArray(Page[]::new);
        Releasables.closeExpectNoException(blocks);
        Arrays.stream(blocks).forEach(block -> assertThat(block.isReleased(), is(true)));
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotDoubleRelease);
        Arrays.stream(pages).forEach(BasicBlockTests::assertCannotReadFromPage);
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotAddToPage);
        assertThat(breaker.getUsed(), is(0L));
    }

    void releaseAndAssertBreaker(Vector vector) {
        assertThat(breaker.getUsed(), greaterThan(0L));
        Releasables.closeExpectNoException(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }
}
