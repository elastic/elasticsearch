/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

public class FilteredBlockTests extends ESTestCase {

    public void testFilterAllPositions() {
        var positionCount = 100;
        var vector = new IntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var filteredVector = vector.filter();

        assertEquals(0, filteredVector.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredVector.getInt(0));

        var filteredBlock = vector.asBlock().filter();
        assertEquals(0, filteredBlock.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredBlock.getInt(0));
    }

    public void testKeepAllPositions() {
        var positionCount = 100;
        var vector = new IntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var positions = IntStream.range(0, positionCount).toArray();

        var filteredVector = vector.filter(positions);
        assertEquals(positionCount, filteredVector.getPositionCount());
        var anyPosition = randomPosition(positionCount);
        assertEquals(anyPosition, filteredVector.getInt(anyPosition));

        var filteredBlock = vector.filter(positions).asBlock();
        assertEquals(positionCount, filteredBlock.getPositionCount());
        assertEquals(anyPosition, filteredBlock.getInt(anyPosition));
    }

    public void testKeepSomePositions() {
        var positionCount = 100;
        var vector = new IntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var positions = IntStream.range(0, positionCount).filter(i -> i % 2 == 0).toArray();

        var filteredVector = vector.filter(positions);
        assertEquals(positionCount / 2, filteredVector.getPositionCount());
        var anyPosition = randomIntBetween(0, (positionCount / 2) - 1);
        assertEquals(anyPosition * 2, filteredVector.getInt(anyPosition));
        assertEquals(anyPosition * 2, filteredVector.asBlock().getInt(anyPosition));

        var filteredBlock = vector.asBlock().filter(positions);
        assertEquals(positionCount / 2, filteredBlock.getPositionCount());
        assertEquals(anyPosition * 2, filteredBlock.getInt(anyPosition));
    }

    public void testFilterOnFilter() {  // TODO: tired of this sv / mv block here. do more below
        var positionCount = 100;
        var vector = new IntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);

        var filteredVector = vector.filter(IntStream.range(0, positionCount).filter(i1 -> i1 % 2 == 0).toArray());
        var filteredTwice = filteredVector.filter(IntStream.range(0, positionCount / 2).filter(i -> i % 2 == 0).toArray());

        assertEquals(positionCount / 4, filteredTwice.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount / 4 - 1);
        assertEquals(anyPosition * 4, filteredTwice.getInt(anyPosition));
    }

    public void testFilterOnNull() {
        IntBlock block;
        if (randomBoolean()) {
            var nulls = new BitSet();
            nulls.set(1);
            block = new IntArrayBlock(new int[] { 10, 0, 30, 40 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        } else {
            var blockBuilder = IntBlock.newBlockBuilder(4);
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
    }

    public void testFilterOnAllNullsBlock() {
        Block block;
        if (randomBoolean()) {
            var nulls = new BitSet();
            nulls.set(0, 4);
            block = new IntArrayBlock(new int[] { 0, 0, 0, 0 }, 4, null, nulls, randomFrom(Block.MvOrdering.values()));
        } else {
            var blockBuilder = IntBlock.newBlockBuilder(4);
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
    }

    public void testFilterOnNoNullsBlock() {
        IntBlock block;
        if (randomBoolean()) {
            block = new IntArrayVector(new int[] { 10, 20, 30, 40 }, 4).asBlock();
        } else {
            var blockBuilder = IntBlock.newBlockBuilder(4);
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
        var bb = BooleanBlock.newBlockBuilder(6);
        bb.beginPositionEntry().appendBoolean(true).appendBoolean(true).endPositionEntry();
        bb.beginPositionEntry().appendBoolean(false).appendBoolean(false).endPositionEntry();
        bb.beginPositionEntry().appendBoolean(false).appendBoolean(false).endPositionEntry();
        Block filter = bb.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[true, true], [false, false]]"));
        assertThat(filter.toString(), containsString("positions=2"));

        var ib = IntBlock.newBlockBuilder(6);
        ib.beginPositionEntry().appendInt(0).appendInt(10).endPositionEntry();
        ib.beginPositionEntry().appendInt(20).appendInt(50).endPositionEntry();
        ib.beginPositionEntry().appendInt(90).appendInt(1000).endPositionEntry();
        filter = ib.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[0, 10], [20, 50]]"));
        assertThat(filter.toString(), containsString("positions=2"));

        var lb = LongBlock.newBlockBuilder(6);
        lb.beginPositionEntry().appendLong(0).appendLong(10).endPositionEntry();
        lb.beginPositionEntry().appendLong(20).appendLong(50).endPositionEntry();
        lb.beginPositionEntry().appendLong(90).appendLong(1000).endPositionEntry();
        filter = lb.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[0, 10], [20, 50]]"));
        assertThat(filter.toString(), containsString("positions=2"));

        var db = DoubleBlock.newBlockBuilder(6);
        db.beginPositionEntry().appendDouble(0).appendDouble(10).endPositionEntry();
        db.beginPositionEntry().appendDouble(0.002).appendDouble(10e8).endPositionEntry();
        db.beginPositionEntry().appendDouble(90).appendDouble(1000).endPositionEntry();
        filter = db.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[0.0, 10.0], [0.002, 1.0E9]]"));
        assertThat(filter.toString(), containsString("positions=2"));

        assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("3c").toString().equals("[33 63]");
        assert new BytesRef("cat").toString().equals("[63 61 74]") && new BytesRef("dog").toString().equals("[64 6f 67]");
        var bytesBlock = BytesRefBlock.newBlockBuilder(6);
        bytesBlock.beginPositionEntry().appendBytesRef(new BytesRef("1a")).appendBytesRef(new BytesRef("3c")).endPositionEntry();
        bytesBlock.beginPositionEntry().appendBytesRef(new BytesRef("cat")).appendBytesRef(new BytesRef("dog")).endPositionEntry();
        bytesBlock.beginPositionEntry().appendBytesRef(new BytesRef("pig")).appendBytesRef(new BytesRef("chicken")).endPositionEntry();
        filter = bytesBlock.build().filter(0, 1);
        assertThat(filter.toString(), containsString("[[[31 61], [33 63]], [[63 61 74], [64 6f 67]]"));
        assertThat(filter.toString(), containsString("positions=2"));
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }

    BytesRefArray arrayOf(String... values) {
        var array = new BytesRefArray(values.length, bigArrays);
        Arrays.stream(values).map(BytesRef::new).forEach(array::append);
        return array;
    }

    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());
}
