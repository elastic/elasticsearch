/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.BitSet;
import java.util.stream.IntStream;

public class FilteredBlockTests extends ESTestCase {
    public void testFilterAllPositions() {
        var positionCount = 100;
        var block = new IntArrayBlock(IntStream.range(0, positionCount).toArray(), positionCount);
        var filtered = new FilteredBlock(block, new int[] {});

        assertEquals(0, filtered.getPositionCount());
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> filtered.getInt(0));
    }

    public void testKeepAllPositions() {
        var positionCount = 100;
        var block = new IntArrayBlock(IntStream.range(0, positionCount).toArray(), positionCount);

        var positions = IntStream.range(0, positionCount).toArray();
        var filtered = new FilteredBlock(block, positions);

        assertEquals(positionCount, filtered.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount);
        assertEquals(anyPosition, filtered.getInt(anyPosition));
    }

    public void testKeepSomePositions() {
        var positionCount = 100;
        var block = new IntArrayBlock(IntStream.range(0, positionCount).toArray(), positionCount);

        var positions = IntStream.range(0, positionCount).filter(i -> i % 2 == 0).toArray();
        var filtered = new FilteredBlock(block, positions);

        assertEquals(positionCount / 2, filtered.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount / 2);
        assertEquals(anyPosition * 2, filtered.getInt(anyPosition));
    }

    public void testFilterOnFilter() {
        var positionCount = 100;
        var block = new IntArrayBlock(IntStream.range(0, positionCount).toArray(), positionCount);

        var filtered = new FilteredBlock(block, IntStream.range(0, positionCount).filter(i1 -> i1 % 2 == 0).toArray());
        var filteredTwice = filtered.filter(IntStream.range(0, positionCount / 2).filter(i -> i % 2 == 0).toArray());

        assertEquals(positionCount / 4, filteredTwice.getPositionCount());
        var anyPosition = randomIntBetween(0, positionCount / 4 - 1);
        assertEquals(anyPosition * 4, filteredTwice.getInt(anyPosition));
    }

    public void testFilterOnNull() {
        var nulls = new BitSet();
        nulls.set(1);
        var block = new IntArrayBlock(new int[] { 10, 0, 30, 40 }, 4, nulls);

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
        var nulls = new BitSet();
        nulls.set(0, 4);
        var block = new IntArrayBlock(new int[] { 0, 0, 0, 0 }, 4, nulls);

        var filtered = block.filter(1, 2, 3);

        assertTrue(filtered.isNull(0));
        assertTrue(filtered.mayHaveNulls());
        assertTrue(filtered.areAllValuesNull());
        assertEquals(3, filtered.nullValuesCount());
        assertEquals(0, filtered.validPositionCount());
    }

    public void testFilterOnNoNullsBlock() {
        var nulls = new BitSet();
        var block = new IntArrayBlock(new int[] { 10, 20, 30, 40 }, 4, nulls);

        var filtered = block.filter(1, 2, 3);

        assertFalse(filtered.isNull(0));
        assertFalse(filtered.mayHaveNulls());
        assertFalse(filtered.areAllValuesNull());
        assertEquals(0, filtered.nullValuesCount());
        assertEquals(3, filtered.validPositionCount());
    }

}
