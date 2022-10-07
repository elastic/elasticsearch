/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.is;

public class BasicBlockTests extends ESTestCase {

    public void testEmpty() {
        Block intBlock = new IntArrayBlock(new int[] {}, 0);
        assertThat(0, is(intBlock.getPositionCount()));

        Block longBlock = new LongArrayBlock(new long[] {}, 0);
        assertThat(0, is(longBlock.getPositionCount()));

        Block doubleBlock = new DoubleArrayBlock(new double[] {}, 0);
        assertThat(0, is(doubleBlock.getPositionCount()));
    }

    public void testIntBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            int[] values = IntStream.range(0, positionCount).toArray();
            Block block = new IntArrayBlock(values, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0, is(block.getInt(0)));
            assertThat(positionCount - 1, is(block.getInt(positionCount - 1)));
            int pos = block.getInt(randomIntBetween(0, positionCount - 1));
            assertThat(pos, is(block.getInt(pos)));
            assertThat((long) pos, is(block.getLong(pos)));
            assertThat((double) pos, is(block.getDouble(pos)));
        }
    }

    public void testConstantIntBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(0, Integer.MAX_VALUE);
            int value = randomInt();
            Block block = new ConstantIntBlock(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getInt(0)));
            assertThat(value, is(block.getInt(positionCount - 1)));
            assertThat(value, is(block.getInt(randomIntBetween(1, positionCount - 1))));
        }
    }

    public void testLongBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            long[] values = LongStream.range(0, positionCount).toArray();
            Block block = new LongArrayBlock(values, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0L, is(block.getLong(0)));
            assertThat((long) positionCount - 1, is(block.getLong(positionCount - 1)));
            int pos = (int) block.getLong(randomIntBetween(0, positionCount - 1));
            assertThat((long) pos, is(block.getLong(pos)));
            assertThat((double) pos, is(block.getDouble(pos)));
        }
    }

    public void testConstantLongBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, Integer.MAX_VALUE);
            long value = randomLong();
            Block block = new ConstantLongBlock(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getLong(0)));
            assertThat(value, is(block.getLong(positionCount - 1)));
            assertThat(value, is(block.getLong(randomIntBetween(1, positionCount - 1))));
        }
    }

    public void testDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            double[] values = LongStream.range(0, positionCount).asDoubleStream().toArray();
            Block block = new DoubleArrayBlock(values, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0d, is(block.getDouble(0)));
            assertThat((double) positionCount - 1, is(block.getDouble(positionCount - 1)));
            int pos = (int) block.getDouble(randomIntBetween(0, positionCount - 1));
            assertThat((double) pos, is(block.getDouble(pos)));
            expectThrows(UOE, () -> block.getInt(pos));
            expectThrows(UOE, () -> block.getLong(pos));
        }
    }

    static final Class<UnsupportedOperationException> UOE = UnsupportedOperationException.class;

}
