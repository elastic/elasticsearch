/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.BitSet;
import java.util.stream.IntStream;

public class FilteredBlockTests extends ESTestCase {

    public void testFilterAllPositions() {
        var positionCount = 100;
        var vector = new IntVector(IntStream.range(0, positionCount).toArray(), positionCount);
        var filteredVector = vector.filter();

        assertEquals(0, filteredVector.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredVector.getInt(0));

        var filteredBlock = vector.asBlock().filter();
        assertEquals(0, filteredBlock.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filteredBlock.getInt(0));
    }

    public void testKeepAllPositions() {
        var positionCount = 100;
        var vector = new IntVector(IntStream.range(0, positionCount).toArray(), positionCount);
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
        var vector = new IntVector(IntStream.range(0, positionCount).toArray(), positionCount);
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
        var vector = new IntVector(IntStream.range(0, positionCount).toArray(), positionCount);

        var filteredVector = vector.filter(IntStream.range(0, positionCount).filter(i1 -> i1 % 2 == 0).toArray());
        var filteredTwice = filteredVector.filter(IntStream.range(0, positionCount / 2).filter(i -> i % 2 == 0).toArray());

        assertEquals(positionCount / 4, filteredTwice.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount / 4 - 1);
        assertEquals(anyPosition * 4, filteredTwice.getInt(anyPosition));
    }

    public void testFilterOnNull() {
        Block block;
        if (randomBoolean()) {
            var nulls = new BitSet();
            nulls.set(1);
            block = new IntBlock(new int[] { 10, 0, 30, 40 }, 4, null, nulls);
        } else {
            BlockBuilder blockBuilder = BlockBuilder.newIntBlockBuilder(4);
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
        assertEquals(2, filtered.validPositionCount());
        assertFalse(filtered.isNull(1));
        assertEquals(30, filtered.getInt(1));
    }

    public void testFilterOnAllNullsBlock() {
        Block block;
        if (randomBoolean()) {
            var nulls = new BitSet();
            nulls.set(0, 4);
            block = new IntBlock(new int[] { 0, 0, 0, 0 }, 4, null, nulls);
        } else {
            BlockBuilder blockBuilder = BlockBuilder.newIntBlockBuilder(4);
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
        assertEquals(0, filtered.validPositionCount());
    }

    public void testFilterOnNoNullsBlock() {
        Block block;
        if (randomBoolean()) {
            block = new IntVector(new int[] { 10, 20, 30, 40 }, 4).asBlock();
        } else {
            BlockBuilder blockBuilder = BlockBuilder.newIntBlockBuilder(4);
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
        assertEquals(3, filtered.validPositionCount());

        assertEquals(20, filtered.asVector().get().getInt(0));
        assertEquals(30, filtered.asVector().get().getInt(1));
        assertEquals(40, filtered.asVector().get().getInt(2));

    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }
}
