/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BasicBlockTests extends ESTestCase {

    public void testEmpty() {
        assertThat(0, is(new IntBlock(new int[] {}, 0, new int[] {}, new BitSet()).getPositionCount()));
        assertThat(0, is(new IntVector(new int[] {}, 0).getPositionCount()));

        assertThat(0, is(new LongBlock(new long[] {}, 0, new int[] {}, new BitSet()).getPositionCount()));
        assertThat(0, is(new LongVector(new long[] {}, 0).getPositionCount()));

        assertThat(0, is(new DoubleBlock(new double[] {}, 0, new int[] {}, new BitSet()).getPositionCount()));
        assertThat(0, is(new DoubleVector(new double[] {}, 0).getPositionCount()));

        var emptyArray = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
        assertThat(0, is(new BytesRefBlock(emptyArray, 0, new int[] {}, new BitSet()).getPositionCount()));
        assertThat(0, is(new BytesRefVector(emptyArray, 0).getPositionCount()));
    }

    public void testSmallSingleValueDenseGrowthInt() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            BlockBuilder blockBuilder = BlockBuilder.newIntBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(blockBuilder::appendInt);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthLong() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            BlockBuilder blockBuilder = BlockBuilder.newLongBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(blockBuilder::appendLong);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthDouble() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            BlockBuilder blockBuilder = BlockBuilder.newDoubleBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(blockBuilder::appendDouble);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthBytesRef() {
        final BytesRef NULL_VALUE = new BytesRef();
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            BlockBuilder blockBuilder = BlockBuilder.newBytesRefBlockBuilder(initialSize);
            IntStream.range(0, 10).mapToObj(i -> NULL_VALUE).forEach(blockBuilder::appendBytesRef);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    private static void assertSingleValueDenseBlock(Block initialBlock) {
        final int positionCount = initialBlock.getPositionCount();
        int depth = randomIntBetween(1, 5);
        for (int d = 0; d < depth; d++) {
            Block block = initialBlock;
            assertThat(block.getTotalValueCount(), is(positionCount));
            assertThat(block.getPositionCount(), is(positionCount));
            for (int j = 0; j < 10; j++) {
                int pos = randomPosition(positionCount);
                assertThat(block.getFirstValueIndex(pos), is(pos));
                assertThat(block.getValueCount(pos), is(1));
                assertThat(block.isNull(pos), is(false));
            }
            assertThat(block.asVector().get().getPositionCount(), is(positionCount));
            assertThat(block.asVector().get().asBlock().getPositionCount(), is(positionCount));
            assertThat(block.nullValuesCount(), is(0));
            assertThat(block.mayHaveNulls(), is(false));
            assertThat(block.areAllValuesNull(), is(false));
            assertThat(block.validPositionCount(), is(block.getPositionCount()));

            initialBlock = block.asVector().get().asBlock();
        }
    }

    public void testIntBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            Block block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                BlockBuilder blockBuilder = BlockBuilder.newIntBlockBuilder(builderEstimateSize);
                IntStream.range(0, positionCount).forEach(blockBuilder::appendInt);
                block = blockBuilder.build();
            } else {
                block = new IntVector(IntStream.range(0, positionCount).toArray(), positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0, is(block.getInt(0)));
            assertThat(positionCount - 1, is(block.getInt(positionCount - 1)));
            int pos = block.getInt(randomPosition(positionCount));
            assertThat(pos, is(block.getInt(pos)));
            assertThat((long) pos, is(block.getLong(pos)));
            assertThat((double) pos, is(block.getDouble(pos)));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> BlockBuilder.newIntBlockBuilder(size),
                    (bb, value) -> bb.appendInt(value),
                    position -> position,
                    (randomNonNullPosition, b) -> {
                        assertThat((int) randomNonNullPosition, is(b.getInt(randomNonNullPosition.intValue())));
                    }
                );
            }
        }
    }

    public void testConstantIntBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            int value = randomInt();
            Block block;
            if (randomBoolean()) {
                block = BlockBuilder.newConstantIntBlockWith(value, positionCount);
            } else {
                block = new ConstantIntVector(value, positionCount).asBlock();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getInt(0)));
            assertThat(value, is(block.getInt(positionCount - 1)));
            assertThat(value, is(block.getInt(randomPosition(positionCount))));
            assertThat(block.isNull(randomPosition(positionCount)), is(false));
            assertSingleValueDenseBlock(block);
        }
    }

    public void testLongBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            Block block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                BlockBuilder blockBuilder = BlockBuilder.newLongBlockBuilder(builderEstimateSize);
                LongStream.range(0, positionCount).forEach(blockBuilder::appendLong);
                block = blockBuilder.build();
            } else {
                block = new LongVector(LongStream.range(0, positionCount).toArray(), positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0L, is(block.getLong(0)));
            assertThat((long) positionCount - 1, is(block.getLong(positionCount - 1)));
            int pos = (int) block.getLong(randomPosition(positionCount));
            assertThat((long) pos, is(block.getLong(pos)));
            assertThat((double) pos, is(block.getDouble(pos)));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> BlockBuilder.newLongBlockBuilder(size),
                    (bb, value) -> bb.appendLong(value),
                    position -> (long) position,
                    (randomNonNullPosition, b) -> {
                        assertThat((long) randomNonNullPosition, is(b.getLong(randomNonNullPosition.intValue())));
                    }
                );
            }
        }
    }

    public void testConstantLongBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            long value = randomLong();
            Block block;
            if (randomBoolean()) {
                block = BlockBuilder.newConstantLongBlockWith(value, positionCount);
            } else {
                block = new ConstantLongVector(value, positionCount).asBlock();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getLong(0)));
            assertThat(value, is(block.getLong(positionCount - 1)));
            assertThat(value, is(block.getLong(randomPosition(positionCount))));
            assertThat(block.isNull(randomPosition(positionCount)), is(false));
            assertSingleValueDenseBlock(block);
        }
    }

    public void testDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            Block block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                BlockBuilder blockBuilder = BlockBuilder.newDoubleBlockBuilder(builderEstimateSize);
                LongStream.range(0, positionCount).asDoubleStream().forEach(blockBuilder::appendDouble);
                block = blockBuilder.build();
            } else {
                block = new DoubleVector(LongStream.range(0, positionCount).asDoubleStream().toArray(), positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0d, is(block.getDouble(0)));
            assertThat((double) positionCount - 1, is(block.getDouble(positionCount - 1)));
            int pos = (int) block.getDouble(randomPosition(positionCount));
            assertThat((double) pos, is(block.getDouble(pos)));
            expectThrows(UOE, () -> block.getInt(pos));
            expectThrows(UOE, () -> block.getLong(pos));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> BlockBuilder.newDoubleBlockBuilder(size),
                    (bb, value) -> bb.appendDouble(value),
                    position -> (double) position,
                    (randomNonNullPosition, b) -> {
                        assertThat((double) randomNonNullPosition, is(b.getDouble(randomNonNullPosition.intValue())));
                    }
                );
            }
        }
    }

    public void testConstantDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            double value = randomDouble();
            Block block;
            if (randomBoolean()) {
                block = BlockBuilder.newConstantDoubleBlockWith(value, positionCount);
            } else {
                block = new ConstantDoubleVector(value, positionCount).asBlock();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getDouble(0)));
            assertThat(value, is(block.getDouble(positionCount - 1)));
            assertThat(value, is(block.getDouble(randomPosition(positionCount))));
            assertSingleValueDenseBlock(block);
            assertThat(block.getObject(randomPosition(positionCount)), is(block.getDouble(randomPosition(positionCount))));
        }
    }

    public void testBytesRefBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        BytesRef[] values = new BytesRef[positionCount];
        for (int i = 0; i < positionCount; i++) {
            BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            if (bytesRef.length > 0 && randomBoolean()) {
                bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
            }
            values[i] = bytesRef;
        }

        Block block;
        if (randomBoolean()) {
            final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
            BlockBuilder blockBuilder = BlockBuilder.newBytesRefBlockBuilder(builderEstimateSize);
            Arrays.stream(values).map(obj -> randomBoolean() ? obj : BytesRef.deepCopyOf(obj)).forEach(blockBuilder::appendBytesRef);
            block = blockBuilder.build();
        } else {
            BytesRefArray array = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
            Arrays.stream(values).forEach(array::append);
            block = new BytesRefVector(array, positionCount).asBlock();
        }

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
        assertSingleValueDenseBlock(block);

        if (positionCount > 1) {
            assertNullValues(
                positionCount,
                size -> BlockBuilder.newBytesRefBlockBuilder(size),
                (bb, value) -> bb.appendBytesRef(value),
                position -> values[position],
                (randomNonNullPosition, b) -> assertThat(
                    values[randomNonNullPosition],
                    is(b.getBytesRef(randomNonNullPosition, new BytesRef()))
                )
            );
        }
    }

    public void testBytesRefBlockBuilderWithNulls() {
        int positionCount = randomIntBetween(0, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        BlockBuilder blockBuilder = BlockBuilder.newBytesRefBlockBuilder(builderEstimateSize);
        BytesRef[] values = new BytesRef[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                // Add random sparseness
                blockBuilder.appendNull();
                values[i] = null;
            } else {
                BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
                if (bytesRef.length > 0 && randomBoolean()) {
                    bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                    bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
                }
                values[i] = bytesRef;
                if (randomBoolean()) {
                    bytesRef = BytesRef.deepCopyOf(bytesRef);
                }
                blockBuilder.appendBytesRef(bytesRef);
            }
        }
        Block block = blockBuilder.build();
        assertThat(positionCount, is(block.getPositionCount()));
        BytesRef bytes = new BytesRef();
        for (int i = 0; i < positionCount; i++) {
            int pos = randomIntBetween(0, positionCount - 1);
            bytes = block.getBytesRef(pos, bytes);
            if (values[pos] == null) {
                assertThat(block.isNull(pos), equalTo(true));
                assertThat(bytes, equalTo(new BytesRef()));
            } else {
                assertThat(bytes, equalTo(values[pos]));
                assertThat(block.getObject(pos), equalTo(values[pos]));
            }
            expectThrows(UOE, () -> block.getInt(pos));
            expectThrows(UOE, () -> block.getLong(pos));
            expectThrows(UOE, () -> block.getDouble(pos));
        }
    }

    public void testConstantBytesRefBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            BytesRef value = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            Block block;
            if (randomBoolean()) {
                block = BlockBuilder.newConstantBytesRefBlockWith(value, positionCount);
            } else {
                block = new ConstantBytesRefVector(value, positionCount).asBlock();
            }
            assertThat(block.getPositionCount(), is(positionCount));

            assertThat(block.getObject(0), is(value));
            assertThat(block.getObject(positionCount - 1), is(value));
            assertThat(block.getObject(randomPosition(positionCount)), is(value));
            assertSingleValueDenseBlock(block);

            BytesRef bytes = new BytesRef();
            bytes = block.getBytesRef(0, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(positionCount - 1, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(randomPosition(positionCount), bytes);
            assertThat(bytes, is(value));
        }
    }

    public void testSingleValueSparseInt() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        BlockBuilder blockBuilder = BlockBuilder.newIntBlockBuilder(builderEstimateSize);

        int[] values = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomInt();
                blockBuilder.appendInt(values[i]);
            } else {
                blockBuilder.appendNull();
            }
        }
        Block block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(positionCount));
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                nullCount++;
                // assertThat(block.getInt(i), is(0)); // Q: do we wanna allow access to the default value
            } else {
                assertThat(block.getInt(i), is(values[i]));
            }
        }
        assertThat(block.nullValuesCount(), is(nullCount));
        assertThat(block.asVector(), isEmpty());
    }

    public void testSingleValueSparseLong() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        BlockBuilder blockBuilder = BlockBuilder.newLongBlockBuilder(builderEstimateSize);

        long[] values = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomLong();
                blockBuilder.appendLong(values[i]);
            } else {
                blockBuilder.appendNull();
            }
        }
        Block block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(positionCount));
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                nullCount++;
                // assertThat(block.getInt(i), is(0)); // Q: do we wanna allow access to the default value
            } else {
                assertThat(block.getLong(i), is(values[i]));
            }
        }
        assertThat(block.nullValuesCount(), is(nullCount));
        assertThat(block.asVector(), isEmpty());
    }

    public void testSingleValueSparseDouble() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        BlockBuilder blockBuilder = BlockBuilder.newDoubleBlockBuilder(builderEstimateSize);

        double[] values = new double[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomDouble();
                blockBuilder.appendDouble(values[i]);
            } else {
                blockBuilder.appendNull();
            }
        }
        Block block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(positionCount));
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                nullCount++;
                // assertThat(block.getDouble(i), is(0)); // Q: do we wanna allow access to the default value
            } else {
                assertThat(block.getDouble(i), is(values[i]));
            }
        }
        assertThat(block.nullValuesCount(), is(nullCount));
        assertThat(block.asVector(), isEmpty());
    }

    interface BlockBuilderFactory {
        BlockBuilder create(int estimatedSize);
    }

    interface ValueAppender<T> {
        void appendValue(BlockBuilder blockBuilder, T value);
    }

    interface ValueSupplier<T> {
        T getValue(int position);
    }

    private static <T> void assertNullValues(
        int positionCount,
        BlockBuilderFactory blockBuilderFactory,
        ValueAppender<T> valueAppender,
        ValueSupplier<T> valueSupplier,
        BiConsumer<Integer, Block> asserter
    ) {
        assertThat("test needs at least two positions", positionCount, greaterThan(1));
        int randomNullPosition = randomIntBetween(0, positionCount - 1);
        int randomNonNullPosition = randomValueOtherThan(randomNullPosition, () -> randomIntBetween(0, positionCount - 1));
        BitSet nullsMask = new BitSet(positionCount);
        nullsMask.set(randomNullPosition);

        BlockBuilder blockBuilder = blockBuilderFactory.create(positionCount);
        IntStream.range(0, positionCount).forEach(position -> {
            if (nullsMask.get(position)) {
                blockBuilder.appendNull();
            } else {
                valueAppender.appendValue(blockBuilder, valueSupplier.getValue(position));
            }
        });
        Block block = blockBuilder.build();

        assertThat(positionCount, is(block.getPositionCount()));
        asserter.accept(randomNonNullPosition, block);
        assertTrue(block.isNull(randomNullPosition));
        assertFalse(block.isNull(randomNonNullPosition));
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }

    static final Class<UnsupportedOperationException> UOE = UnsupportedOperationException.class;

}
