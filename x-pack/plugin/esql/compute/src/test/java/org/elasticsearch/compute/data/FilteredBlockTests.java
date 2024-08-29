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
import org.elasticsearch.core.Releasable;
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
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
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
        var filteredVector = vector.filter();

        assertEquals(0, filteredVector.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredVector.getInt(0));
        filteredVector.close();

        var filteredBlock = vector.asBlock().filter();
        assertEquals(0, filteredBlock.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredBlock.getInt(0));
        vector.close();
        releaseAndAssertBreaker(filteredBlock);
    }

    public void testKeepAllPositions() {
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var positions = IntStream.range(0, positionCount).toArray();

        var filteredVector = vector.filter(positions);
        assertEquals(positionCount, filteredVector.getPositionCount());
        var anyPosition = randomPosition(positionCount);
        assertEquals(anyPosition, filteredVector.getInt(anyPosition));
        filteredVector.close();

        var filteredBlock = vector.filter(positions).asBlock();
        assertEquals(positionCount, filteredBlock.getPositionCount());
        assertEquals(anyPosition, filteredBlock.getInt(anyPosition));
        Releasables.close(vector);
        releaseAndAssertBreaker(filteredBlock);
    }

    public void testKeepSomePositions() {
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var positions = IntStream.range(0, positionCount).filter(i -> i % 2 == 0).toArray();

        var filteredVector = vector.filter(positions);
        assertEquals(positionCount / 2, filteredVector.getPositionCount());
        var anyPosition = randomIntBetween(0, (positionCount / 2) - 1);
        assertEquals(anyPosition * 2, filteredVector.getInt(anyPosition));
        assertEquals(anyPosition * 2, filteredVector.asBlock().getInt(anyPosition));
        filteredVector.close();

        var filteredBlock = vector.asBlock().filter(positions);
        assertEquals(positionCount / 2, filteredBlock.getPositionCount());
        assertEquals(anyPosition * 2, filteredBlock.getInt(anyPosition));
        vector.close();
        releaseAndAssertBreaker(filteredBlock);
    }

    public void testFilterOnFilter() {  // TODO: tired of this sv / mv block here. do more below
        var positionCount = 100;
        var vector = blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);

        var filteredVector = vector.filter(IntStream.range(0, positionCount).filter(i1 -> i1 % 2 == 0).toArray());
        var filteredTwice = filteredVector.filter(IntStream.range(0, positionCount / 2).filter(i -> i % 2 == 0).toArray());

        assertEquals(positionCount / 4, filteredTwice.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount / 4 - 1);
        assertEquals(anyPosition * 4, filteredTwice.getInt(anyPosition));
        Releasables.close(vector, filteredVector);
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
        assertEquals(2, filtered.getTotalValueCount());
        assertFalse(filtered.isNull(1));
        assertEquals(30, filtered.getInt(filtered.getFirstValueIndex(1)));
        Releasables.closeExpectNoException(block);
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
        assertEquals(0, filtered.getTotalValueCount());
        block.close();
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
        assertEquals(3, filtered.getTotalValueCount());

        assertEquals(20, filtered.asVector().getInt(0));
        assertEquals(30, filtered.asVector().getInt(1));
        assertEquals(40, filtered.asVector().getInt(2));
        block.close();
        releaseAndAssertBreaker(filtered);
    }

    public void testFilterToStringSimple() {
        BitSet nulls = BitSet.valueOf(new byte[] { 0x08 });  // any non-empty bitset, that does not affect the filter, should suffice

        var boolVector = blockFactory.newBooleanArrayVector(new boolean[] { true, false, false, true }, 4);
        var boolBlock = blockFactory.newBooleanArrayBlock(
            new boolean[] { true, false, false, true },
            4,
            null,
            nulls,
            randomFrom(Block.MvOrdering.values())
        );
        for (Releasable obj : List.of(boolVector.filter(0, 2), boolVector.asBlock().filter(0, 2), boolBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[true, false]"));
            assertThat(s, containsString("positions=2"));
            Releasables.close(obj);
        }
        Releasables.close(boolVector, boolBlock);

        var intVector = blockFactory.newIntArrayVector(new int[] { 10, 20, 30, 40 }, 4);
        var intBlock = blockFactory.newIntArrayBlock(new int[] { 10, 20, 30, 40 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        for (Releasable obj : List.of(intVector.filter(0, 2), intVector.asBlock().filter(0, 2), intBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[10, 30]"));
            assertThat(s, containsString("positions=2"));
            Releasables.close(obj);
        }
        Releasables.close(intVector, intBlock);

        var longVector = blockFactory.newLongArrayVector(new long[] { 100L, 200L, 300L, 400L }, 4);
        var longBlock = blockFactory.newLongArrayBlock(
            new long[] { 100L, 200L, 300L, 400L },
            4,
            null,
            nulls,
            randomFrom(Block.MvOrdering.values())
        );
        for (Releasable obj : List.of(longVector.filter(0, 2), longVector.asBlock().filter(0, 2), longBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[100, 300]"));
            assertThat(s, containsString("positions=2"));
            Releasables.close(obj);
        }

        Releasables.close(longVector, longBlock);

        var doubleVector = blockFactory.newDoubleArrayVector(new double[] { 1.1, 2.2, 3.3, 4.4 }, 4);
        var doubleBlock = blockFactory.newDoubleArrayBlock(
            new double[] { 1.1, 2.2, 3.3, 4.4 },
            4,
            null,
            nulls,
            randomFrom(Block.MvOrdering.values())
        );
        for (Releasable obj : List.of(doubleVector.filter(0, 2), doubleVector.asBlock().filter(0, 2), doubleBlock.filter(0, 2))) {
            String s = obj.toString();
            assertThat(s, containsString("[1.1, 3.3]"));
            assertThat(s, containsString("positions=2"));
            Releasables.close(obj);
        }

        Releasables.close(doubleVector, doubleBlock);

        assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("3c").toString().equals("[33 63]");
        var bytesRefVector = blockFactory.newBytesRefArrayVector(arrayOf("1a", "2b", "3c", "4d"), 4);
        var bytesRefBlock = blockFactory.newBytesRefArrayBlock(
            arrayOf("1a", "2b", "3c", "4d"),
            4,
            null,
            nulls,
            randomFrom(Block.MvOrdering.values())
        );
        for (Releasable obj : List.of(bytesRefVector.filter(0, 2), bytesRefVector.asBlock().filter(0, 2), bytesRefBlock.filter(0, 2))) {
            assertThat(
                obj.toString(),
                either(equalTo("BytesRefArrayVector[positions=2]")).or(
                    equalTo("BytesRefVectorBlock[vector=BytesRefArrayVector[positions=2]]")
                )
            );
            Releasables.close(obj);
        }
        Releasables.close(bytesRefVector, bytesRefBlock);
    }

    public void testFilterToStringMultiValue() {
        {
            var builder = blockFactory.newBooleanBlockBuilder(6);
            builder.beginPositionEntry().appendBoolean(true).appendBoolean(true).endPositionEntry();
            builder.beginPositionEntry().appendBoolean(false).appendBoolean(false).endPositionEntry();
            builder.beginPositionEntry().appendBoolean(false).appendBoolean(false).endPositionEntry();
            BooleanBlock block = builder.build();
            var filter = block.filter(0, 1);
            assertThat(
                filter.toString(),
                containsString(
                    "BooleanArrayBlock[positions=2, mvOrdering=UNORDERED, "
                        + "vector=BooleanArrayVector[positions=4, values=[true, true, false, false]]]"
                )
            );
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newIntBlockBuilder(6);
            builder.beginPositionEntry().appendInt(0).appendInt(10).endPositionEntry();
            builder.beginPositionEntry().appendInt(20).appendInt(50).endPositionEntry();
            builder.beginPositionEntry().appendInt(90).appendInt(1000).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(0, 1);
            assertThat(
                filter.toString(),
                containsString(
                    "IntArrayBlock[positions=2, mvOrdering=UNORDERED, vector=IntArrayVector[positions=4, values=[0, 10, 20, 50]]]"
                )
            );
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newLongBlockBuilder(6);
            builder.beginPositionEntry().appendLong(0).appendLong(10).endPositionEntry();
            builder.beginPositionEntry().appendLong(20).appendLong(50).endPositionEntry();
            builder.beginPositionEntry().appendLong(90).appendLong(1000).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(0, 1);
            assertThat(
                filter.toString(),
                containsString(
                    "LongArrayBlock[positions=2, mvOrdering=UNORDERED, vector=LongArrayVector[positions=4, values=[0, 10, 20, 50]]]"
                )
            );
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newDoubleBlockBuilder(6);
            builder.beginPositionEntry().appendDouble(0).appendDouble(10).endPositionEntry();
            builder.beginPositionEntry().appendDouble(0.002).appendDouble(10e8).endPositionEntry();
            builder.beginPositionEntry().appendDouble(90).appendDouble(1000).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(0, 1);
            assertThat(
                filter.toString(),
                containsString(
                    "DoubleArrayBlock[positions=2, mvOrdering=UNORDERED, "
                        + "vector=DoubleArrayVector[positions=4, values=[0.0, 10.0, 0.002, 1.0E9]]]"
                )
            );
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("3c").toString().equals("[33 63]");
            assert new BytesRef("cat").toString().equals("[63 61 74]") && new BytesRef("dog").toString().equals("[64 6f 67]");
            var builder = blockFactory.newBytesRefBlockBuilder(6);
            builder.beginPositionEntry().appendBytesRef(new BytesRef("1a")).appendBytesRef(new BytesRef("3c")).endPositionEntry();
            builder.beginPositionEntry().appendBytesRef(new BytesRef("cat")).appendBytesRef(new BytesRef("dog")).endPositionEntry();
            builder.beginPositionEntry().appendBytesRef(new BytesRef("pig")).appendBytesRef(new BytesRef("chicken")).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(0, 1);
            assertThat(
                filter.toString(),
                containsString("BytesRefArrayBlock[positions=2, mvOrdering=UNORDERED, vector=BytesRefArrayVector[positions=4]]")
            );
            assertThat(filter.getPositionCount(), equalTo(2));
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
    }

    /** Tests filtering on the last position of a block with multi-values. */
    public void testFilterOnLastPositionWithMultiValues() {
        {
            var builder = blockFactory.newBooleanBlockBuilder(0);
            builder.beginPositionEntry().appendBoolean(true).appendBoolean(false).endPositionEntry();
            builder.beginPositionEntry().appendBoolean(false).appendBoolean(true).endPositionEntry();
            BooleanBlock block = builder.build();
            var filter = block.filter(1);
            assertThat(filter.getPositionCount(), is(1));
            assertThat(filter.getValueCount(0), is(2));
            assertThat(filter.getBoolean(filter.getFirstValueIndex(0)), is(false));
            assertThat(filter.getBoolean(filter.getFirstValueIndex(0) + 1), is(true));
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newIntBlockBuilder(6);
            builder.beginPositionEntry().appendInt(0).appendInt(10).endPositionEntry();
            builder.beginPositionEntry().appendInt(20).appendInt(50).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(1);
            assertThat(filter.getPositionCount(), is(1));
            assertThat(filter.getInt(filter.getFirstValueIndex(0)), is(20));
            assertThat(filter.getInt(filter.getFirstValueIndex(0) + 1), is(50));
            assertThat(filter.getValueCount(0), is(2));
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newLongBlockBuilder(6);
            builder.beginPositionEntry().appendLong(0).appendLong(10).endPositionEntry();
            builder.beginPositionEntry().appendLong(20).appendLong(50).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(1);
            assertThat(filter.getPositionCount(), is(1));
            assertThat(filter.getValueCount(0), is(2));
            assertThat(filter.getLong(filter.getFirstValueIndex(0)), is(20L));
            assertThat(filter.getLong(filter.getFirstValueIndex(0) + 1), is(50L));
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newDoubleBlockBuilder(6);
            builder.beginPositionEntry().appendDouble(0).appendDouble(10).endPositionEntry();
            builder.beginPositionEntry().appendDouble(0.002).appendDouble(10e8).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(1);
            assertThat(filter.getPositionCount(), is(1));
            assertThat(filter.getValueCount(0), is(2));
            assertThat(filter.getDouble(filter.getFirstValueIndex(0)), is(0.002));
            assertThat(filter.getDouble(filter.getFirstValueIndex(0) + 1), is(10e8));
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
        {
            var builder = blockFactory.newBytesRefBlockBuilder(6);
            builder.beginPositionEntry().appendBytesRef(new BytesRef("cat")).appendBytesRef(new BytesRef("dog")).endPositionEntry();
            builder.beginPositionEntry().appendBytesRef(new BytesRef("pig")).appendBytesRef(new BytesRef("chicken")).endPositionEntry();
            var block = builder.build();
            var filter = block.filter(1);
            assertThat(filter.getPositionCount(), is(1));
            assertThat(filter.getValueCount(0), is(2));
            assertThat(filter.getBytesRef(filter.getFirstValueIndex(0), new BytesRef()), equalTo(new BytesRef("pig")));
            assertThat(filter.getBytesRef(filter.getFirstValueIndex(0) + 1, new BytesRef()), equalTo(new BytesRef("chicken")));
            Releasables.close(builder, block);
            releaseAndAssertBreaker(filter);
        }
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
