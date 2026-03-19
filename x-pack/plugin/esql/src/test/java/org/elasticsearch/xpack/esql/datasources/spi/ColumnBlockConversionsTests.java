/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;

public class ColumnBlockConversionsTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    // --- longColumn ---

    public void testLongColumnNoNulls() {
        long[] values = { 10L, 20L, 30L, 40L };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 4, true, false, null);
        try {
            assertEquals(4, block.getPositionCount());
            LongBlock lb = (LongBlock) block;
            assertEquals(10L, lb.getLong(0));
            assertEquals(20L, lb.getLong(1));
            assertEquals(30L, lb.getLong(2));
            assertEquals(40L, lb.getLong(3));
            for (int i = 0; i < 4; i++) {
                assertFalse(block.isNull(i));
            }
        } finally {
            block.close();
        }
    }

    public void testLongColumnWithNulls() {
        long[] values = { 10L, 0L, 30L, 0L };
        boolean[] isNull = { false, true, false, true };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 4, false, false, isNull);
        try {
            assertEquals(4, block.getPositionCount());
            LongBlock lb = (LongBlock) block;
            assertFalse(block.isNull(0));
            assertEquals(10L, lb.getLong(lb.getFirstValueIndex(0)));
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertEquals(30L, lb.getLong(lb.getFirstValueIndex(2)));
            assertTrue(block.isNull(3));
        } finally {
            block.close();
        }
    }

    public void testLongColumnRepeating() {
        long[] values = { 42L };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 5, true, true, null);
        try {
            assertEquals(5, block.getPositionCount());
            LongBlock lb = (LongBlock) block;
            for (int i = 0; i < 5; i++) {
                assertEquals(42L, lb.getLong(lb.getFirstValueIndex(i)));
            }
        } finally {
            block.close();
        }
    }

    public void testLongColumnRepeatingNull() {
        long[] values = { 0L };
        boolean[] isNull = { true };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 3, false, true, isNull);
        try {
            assertEquals(3, block.getPositionCount());
            for (int i = 0; i < 3; i++) {
                assertTrue(block.isNull(i));
            }
        } finally {
            block.close();
        }
    }

    public void testLongColumnCopiesArray() {
        long[] values = { 1L, 2L, 3L };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 3, true, false, null);
        try {
            values[0] = 999L;
            LongBlock lb = (LongBlock) block;
            assertEquals(1L, lb.getLong(0));
        } finally {
            block.close();
        }
    }

    // --- intColumnFromLongs ---

    public void testIntColumnNoNulls() {
        long[] values = { 1L, 2L, 3L };
        Block block = ColumnBlockConversions.intColumnFromLongs(blockFactory, values, 3, true, false, null);
        try {
            assertEquals(3, block.getPositionCount());
            IntBlock ib = (IntBlock) block;
            assertEquals(1, ib.getInt(0));
            assertEquals(2, ib.getInt(1));
            assertEquals(3, ib.getInt(2));
        } finally {
            block.close();
        }
    }

    public void testIntColumnWithNulls() {
        long[] values = { 100L, 0L, 300L };
        boolean[] isNull = { false, true, false };
        Block block = ColumnBlockConversions.intColumnFromLongs(blockFactory, values, 3, false, false, isNull);
        try {
            assertEquals(3, block.getPositionCount());
            IntBlock ib = (IntBlock) block;
            assertFalse(block.isNull(0));
            assertEquals(100, ib.getInt(ib.getFirstValueIndex(0)));
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertEquals(300, ib.getInt(ib.getFirstValueIndex(2)));
        } finally {
            block.close();
        }
    }

    public void testIntColumnRepeating() {
        long[] values = { 7L };
        Block block = ColumnBlockConversions.intColumnFromLongs(blockFactory, values, 4, true, true, null);
        try {
            assertEquals(4, block.getPositionCount());
            IntBlock ib = (IntBlock) block;
            for (int i = 0; i < 4; i++) {
                assertEquals(7, ib.getInt(ib.getFirstValueIndex(i)));
            }
        } finally {
            block.close();
        }
    }

    public void testIntColumnRepeatingNull() {
        long[] values = { 0L };
        boolean[] isNull = { true };
        Block block = ColumnBlockConversions.intColumnFromLongs(blockFactory, values, 3, false, true, isNull);
        try {
            assertEquals(3, block.getPositionCount());
            for (int i = 0; i < 3; i++) {
                assertTrue(block.isNull(i));
            }
        } finally {
            block.close();
        }
    }

    public void testIntColumnCopiesArray() {
        long[] values = { 10L, 20L, 30L };
        Block block = ColumnBlockConversions.intColumnFromLongs(blockFactory, values, 3, true, false, null);
        try {
            values[0] = 999L;
            IntBlock ib = (IntBlock) block;
            assertEquals(10, ib.getInt(0));
        } finally {
            block.close();
        }
    }

    public void testIntColumnLargerArrayThanRowCount() {
        long[] values = new long[100];
        for (int i = 0; i < 100; i++) {
            values[i] = i;
        }
        Block block = ColumnBlockConversions.intColumnFromLongs(blockFactory, values, 10, true, false, null);
        try {
            assertEquals(10, block.getPositionCount());
            IntBlock ib = (IntBlock) block;
            for (int i = 0; i < 10; i++) {
                assertEquals(i, ib.getInt(i));
            }
        } finally {
            block.close();
        }
    }

    // --- doubleColumn ---

    public void testDoubleColumnNoNulls() {
        double[] values = { 1.5, 2.5, 3.5 };
        Block block = ColumnBlockConversions.doubleColumn(blockFactory, values, 3, true, false, null);
        try {
            assertEquals(3, block.getPositionCount());
            DoubleBlock db = (DoubleBlock) block;
            assertEquals(1.5, db.getDouble(0), 0.0);
            assertEquals(2.5, db.getDouble(1), 0.0);
            assertEquals(3.5, db.getDouble(2), 0.0);
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnWithNulls() {
        double[] values = { 1.0, 0.0, 3.0 };
        boolean[] isNull = { false, true, false };
        Block block = ColumnBlockConversions.doubleColumn(blockFactory, values, 3, false, false, isNull);
        try {
            DoubleBlock db = (DoubleBlock) block;
            assertFalse(block.isNull(0));
            assertEquals(1.0, db.getDouble(db.getFirstValueIndex(0)), 0.0);
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertEquals(3.0, db.getDouble(db.getFirstValueIndex(2)), 0.0);
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnRepeating() {
        double[] values = { 3.14 };
        Block block = ColumnBlockConversions.doubleColumn(blockFactory, values, 3, true, true, null);
        try {
            DoubleBlock db = (DoubleBlock) block;
            for (int i = 0; i < 3; i++) {
                assertEquals(3.14, db.getDouble(db.getFirstValueIndex(i)), 0.0);
            }
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnCopiesArray() {
        double[] values = { 1.0, 2.0 };
        Block block = ColumnBlockConversions.doubleColumn(blockFactory, values, 2, true, false, null);
        try {
            values[0] = 999.0;
            DoubleBlock db = (DoubleBlock) block;
            assertEquals(1.0, db.getDouble(0), 0.0);
        } finally {
            block.close();
        }
    }

    // --- booleanColumnFromLongs ---

    public void testBooleanColumnNoNulls() {
        long[] values = { 1L, 0L, 1L, 1L };
        Block block = ColumnBlockConversions.booleanColumnFromLongs(blockFactory, values, 4, true, false, null);
        try {
            assertEquals(4, block.getPositionCount());
            BooleanBlock bb = (BooleanBlock) block;
            assertTrue(bb.getBoolean(0));
            assertFalse(bb.getBoolean(1));
            assertTrue(bb.getBoolean(2));
            assertTrue(bb.getBoolean(3));
        } finally {
            block.close();
        }
    }

    public void testBooleanColumnWithNulls() {
        long[] values = { 1L, 0L, 0L };
        boolean[] isNull = { false, true, false };
        Block block = ColumnBlockConversions.booleanColumnFromLongs(blockFactory, values, 3, false, false, isNull);
        try {
            BooleanBlock bb = (BooleanBlock) block;
            assertFalse(block.isNull(0));
            assertTrue(bb.getBoolean(bb.getFirstValueIndex(0)));
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertFalse(bb.getBoolean(bb.getFirstValueIndex(2)));
        } finally {
            block.close();
        }
    }

    public void testBooleanColumnRepeating() {
        long[] values = { 1L };
        Block block = ColumnBlockConversions.booleanColumnFromLongs(blockFactory, values, 5, true, true, null);
        try {
            BooleanBlock bb = (BooleanBlock) block;
            for (int i = 0; i < 5; i++) {
                assertTrue(bb.getBoolean(bb.getFirstValueIndex(i)));
            }
        } finally {
            block.close();
        }
    }

    public void testBooleanColumnRepeatingNull() {
        long[] values = { 0L };
        boolean[] isNull = { true };
        Block block = ColumnBlockConversions.booleanColumnFromLongs(blockFactory, values, 3, false, true, isNull);
        try {
            assertEquals(3, block.getPositionCount());
            for (int i = 0; i < 3; i++) {
                assertTrue(block.isNull(i));
            }
        } finally {
            block.close();
        }
    }

    public void testBooleanColumnCopiesArray() {
        long[] values = { 1L, 0L, 1L };
        Block block = ColumnBlockConversions.booleanColumnFromLongs(blockFactory, values, 3, true, false, null);
        try {
            values[0] = 0L;
            BooleanBlock bb = (BooleanBlock) block;
            assertTrue(bb.getBoolean(0));
        } finally {
            block.close();
        }
    }

    public void testBooleanColumnLargerArrayThanRowCount() {
        long[] values = new long[50];
        for (int i = 0; i < 50; i++) {
            values[i] = i % 2;
        }
        Block block = ColumnBlockConversions.booleanColumnFromLongs(blockFactory, values, 5, true, false, null);
        try {
            assertEquals(5, block.getPositionCount());
            BooleanBlock bb = (BooleanBlock) block;
            assertFalse(bb.getBoolean(0));
            assertTrue(bb.getBoolean(1));
            assertFalse(bb.getBoolean(2));
            assertTrue(bb.getBoolean(3));
            assertFalse(bb.getBoolean(4));
        } finally {
            block.close();
        }
    }

    // --- doubleColumnFromLongs ---

    public void testDoubleColumnFromLongsNoNulls() {
        long[] values = { 10L, 20L, 30L };
        Block block = ColumnBlockConversions.doubleColumnFromLongs(blockFactory, values, 3, true, false, null);
        try {
            DoubleBlock db = (DoubleBlock) block;
            assertEquals(10.0, db.getDouble(0), 0.0);
            assertEquals(20.0, db.getDouble(1), 0.0);
            assertEquals(30.0, db.getDouble(2), 0.0);
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnFromLongsWithNulls() {
        long[] values = { 10L, 0L, 30L };
        boolean[] isNull = { false, true, false };
        Block block = ColumnBlockConversions.doubleColumnFromLongs(blockFactory, values, 3, false, false, isNull);
        try {
            DoubleBlock db = (DoubleBlock) block;
            assertFalse(block.isNull(0));
            assertEquals(10.0, db.getDouble(db.getFirstValueIndex(0)), 0.0);
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertEquals(30.0, db.getDouble(db.getFirstValueIndex(2)), 0.0);
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnFromLongsRepeating() {
        long[] values = { 42L };
        Block block = ColumnBlockConversions.doubleColumnFromLongs(blockFactory, values, 3, true, true, null);
        try {
            DoubleBlock db = (DoubleBlock) block;
            for (int i = 0; i < 3; i++) {
                assertEquals(42.0, db.getDouble(db.getFirstValueIndex(i)), 0.0);
            }
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnFromLongsRepeatingNull() {
        long[] values = { 0L };
        boolean[] isNull = { true };
        Block block = ColumnBlockConversions.doubleColumnFromLongs(blockFactory, values, 3, false, true, isNull);
        try {
            assertEquals(3, block.getPositionCount());
            for (int i = 0; i < 3; i++) {
                assertTrue(block.isNull(i));
            }
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnFromLongsCopiesArray() {
        long[] values = { 10L, 20L, 30L };
        Block block = ColumnBlockConversions.doubleColumnFromLongs(blockFactory, values, 3, true, false, null);
        try {
            values[0] = 999L;
            DoubleBlock db = (DoubleBlock) block;
            assertEquals(10.0, db.getDouble(0), 0.0);
        } finally {
            block.close();
        }
    }

    public void testDoubleColumnFromLongsLargerArrayThanRowCount() {
        long[] values = new long[100];
        for (int i = 0; i < 100; i++) {
            values[i] = i * 10;
        }
        Block block = ColumnBlockConversions.doubleColumnFromLongs(blockFactory, values, 5, true, false, null);
        try {
            assertEquals(5, block.getPositionCount());
            DoubleBlock db = (DoubleBlock) block;
            for (int i = 0; i < 5; i++) {
                assertEquals((double) (i * 10), db.getDouble(i), 0.0);
            }
        } finally {
            block.close();
        }
    }

    // --- edge cases ---

    public void testSingleRow() {
        long[] values = { 99L };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 1, true, false, null);
        try {
            assertEquals(1, block.getPositionCount());
            LongBlock lb = (LongBlock) block;
            assertEquals(99L, lb.getLong(0));
        } finally {
            block.close();
        }
    }

    public void testAllNulls() {
        long[] values = { 0L, 0L, 0L };
        boolean[] isNull = { true, true, true };
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 3, false, false, isNull);
        try {
            assertEquals(3, block.getPositionCount());
            for (int i = 0; i < 3; i++) {
                assertTrue(block.isNull(i));
            }
        } finally {
            block.close();
        }
    }

    public void testLargerArrayThanRowCount() {
        long[] values = new long[100];
        for (int i = 0; i < 100; i++) {
            values[i] = i;
        }
        Block block = ColumnBlockConversions.longColumn(blockFactory, values, 10, true, false, null);
        try {
            assertEquals(10, block.getPositionCount());
            LongBlock lb = (LongBlock) block;
            for (int i = 0; i < 10; i++) {
                assertEquals(i, lb.getLong(i));
            }
        } finally {
            block.close();
        }
    }
}
