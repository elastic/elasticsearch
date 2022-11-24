/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

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

    public void testConstantDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, Integer.MAX_VALUE);
            double value = randomDouble();
            Block block = new ConstantDoubleBlock(value, positionCount);
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getDouble(0)));
            assertThat(value, is(block.getDouble(positionCount - 1)));
            assertThat(value, is(block.getDouble(randomIntBetween(1, positionCount - 1))));
            assertThat(
                block.getObject(randomIntBetween(1, positionCount - 1)),
                is(block.getDouble(randomIntBetween(1, positionCount - 1)))
            );
        }
    }

    public void testBytesRefBlock() {
        int positionCount = randomIntBetween(0, 16 * 1024);
        BytesRefArrayBlock.Builder builder = BytesRefArrayBlock.builder(positionCount);
        BytesRef[] values = new BytesRef[positionCount];
        for (int i = 0; i < positionCount; i++) {
            BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            if (bytesRef.length > 0 && randomBoolean()) {
                bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
            }
            values[i] = bytesRef;
            if (randomBoolean()) {
                bytesRef = BytesRef.deepCopyOf(bytesRef);
            }
            builder.append(bytesRef);
        }
        BytesRefArrayBlock block = builder.build();
        assertThat(positionCount, is(block.getPositionCount()));
        BytesRef bytes = new BytesRef();
        for (int i = 0; i < positionCount; i++) {
            int pos = randomIntBetween(0, positionCount - 1);
            bytes = block.getBytesRef(pos, bytes);
            assertThat(bytes, equalTo(values[pos]));
            assertThat(block.getObject(pos), equalTo(values[pos]));
            expectThrows(UOE, () -> block.getInt(pos));
            expectThrows(UOE, () -> block.getLong(pos));
            expectThrows(UOE, () -> block.getDouble(pos));
        }
    }

    public void testBytesRefBlockBuilder() {
        int positionCount = randomIntBetween(1, 128);
        BytesRefArrayBlock.Builder builder = BytesRefArrayBlock.builder(positionCount);
        int firstBatch = randomIntBetween(0, positionCount - 1);
        for (int i = 0; i < firstBatch; i++) {
            builder.append(new BytesRef(randomByteArrayOfLength(between(1, 20))));
            IllegalStateException error = expectThrows(IllegalStateException.class, builder::build);
            assertThat(error.getMessage(), startsWith("Incomplete block; expected "));
        }
        int secondBatch = positionCount - firstBatch;
        for (int i = 0; i < secondBatch; i++) {
            IllegalStateException error = expectThrows(IllegalStateException.class, builder::build);
            assertThat(error.getMessage(), startsWith("Incomplete block; expected "));
            builder.append(new BytesRef(randomByteArrayOfLength(between(1, 20))));
        }
        int extra = between(1, 10);
        for (int i = 0; i < extra; i++) {
            BytesRef bytes = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> builder.append(bytes));
            assertThat(error.getMessage(), startsWith("Block is full; "));
        }
        BytesRefArrayBlock block = builder.build();
        assertThat(block.getPositionCount(), equalTo(positionCount));
    }

    public void testConstantBytesRefBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, Integer.MAX_VALUE);
            BytesRef value = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            Block block = new ConstantBytesRefBlock(value, positionCount);

            assertThat(block.getPositionCount(), is(positionCount));

            assertThat(block.getObject(0), is(value));
            assertThat(block.getObject(positionCount - 1), is(value));
            assertThat(block.getObject(randomIntBetween(1, positionCount - 1)), is(value));

            BytesRef bytes = new BytesRef();
            bytes = block.getBytesRef(0, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(positionCount - 1, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(randomIntBetween(1, positionCount - 1), bytes);
            assertThat(bytes, is(value));
        }
    }

    static final Class<UnsupportedOperationException> UOE = UnsupportedOperationException.class;

}
