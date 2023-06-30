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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BasicBlockTests extends ESTestCase {

    public void testEmpty() {
        assertThat(
            new IntArrayBlock(new int[] {}, 0, new int[] {}, new BitSet(), randomFrom(Block.MvOrdering.values())).getPositionCount(),
            is(0)
        );
        assertThat(IntBlock.newBlockBuilder(0).build().getPositionCount(), is(0));
        assertThat(new IntArrayVector(new int[] {}, 0).getPositionCount(), is(0));
        assertThat(IntVector.newVectorBuilder(0).build().getPositionCount(), is(0));

        assertThat(
            new LongArrayBlock(new long[] {}, 0, new int[] {}, new BitSet(), randomFrom(Block.MvOrdering.values())).getPositionCount(),
            is(0)
        );
        assertThat(LongBlock.newBlockBuilder(0).build().getPositionCount(), is(0));
        assertThat(new LongArrayVector(new long[] {}, 0).getPositionCount(), is(0));
        assertThat(LongVector.newVectorBuilder(0).build().getPositionCount(), is(0));

        assertThat(
            new DoubleArrayBlock(new double[] {}, 0, new int[] {}, new BitSet(), randomFrom(Block.MvOrdering.values())).getPositionCount(),
            is(0)
        );
        assertThat(DoubleBlock.newBlockBuilder(0).build().getPositionCount(), is(0));
        assertThat(new DoubleArrayVector(new double[] {}, 0).getPositionCount(), is(0));
        assertThat(DoubleVector.newVectorBuilder(0).build().getPositionCount(), is(0));

        var emptyArray = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
        assertThat(
            new BytesRefArrayBlock(emptyArray, 0, new int[] {}, new BitSet(), randomFrom(Block.MvOrdering.values())).getPositionCount(),
            is(0)
        );
        assertThat(BytesRefBlock.newBlockBuilder(0).build().getPositionCount(), is(0));
        assertThat(new BytesRefArrayVector(emptyArray, 0).getPositionCount(), is(0));
        assertThat(BytesRefVector.newVectorBuilder(0).build().getPositionCount(), is(0));

        assertThat(
            new BooleanArrayBlock(new boolean[] {}, 0, new int[] {}, new BitSet(), randomFrom(Block.MvOrdering.values()))
                .getPositionCount(),
            is(0)
        );
        assertThat(BooleanBlock.newBlockBuilder(0).build().getPositionCount(), is(0));
        assertThat(new BooleanArrayVector(new boolean[] {}, 0).getPositionCount(), is(0));
        assertThat(BooleanVector.newVectorBuilder(0).build().getPositionCount(), is(0));
    }

    public void testSmallSingleValueDenseGrowthInt() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            var blockBuilder = IntBlock.newBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(blockBuilder::appendInt);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthLong() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            var blockBuilder = LongBlock.newBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(blockBuilder::appendLong);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthDouble() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            var blockBuilder = DoubleBlock.newBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(blockBuilder::appendDouble);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthBytesRef() {
        final BytesRef NULL_VALUE = new BytesRef();
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            var blockBuilder = BytesRefBlock.newBlockBuilder(initialSize);
            IntStream.range(0, 10).mapToObj(i -> NULL_VALUE).forEach(blockBuilder::appendBytesRef);
            assertSingleValueDenseBlock(blockBuilder.build());
        }
    }

    public void testSmallSingleValueDenseGrowthBoolean() {
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            var blockBuilder = BooleanBlock.newBlockBuilder(initialSize);
            IntStream.range(0, 10).forEach(i -> blockBuilder.appendBoolean(i % 3 == 0));
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
            assertThat(block.asVector().getPositionCount(), is(positionCount));
            assertThat(block.asVector().asBlock().getTotalValueCount(), is(positionCount));
            assertThat(block.asVector().asBlock().getPositionCount(), is(positionCount));
            assertThat(block.nullValuesCount(), is(0));
            assertThat(block.mayHaveNulls(), is(false));
            assertThat(block.areAllValuesNull(), is(false));
            assertThat(block.mayHaveMultivaluedFields(), is(false));

            initialBlock = block.asVector().asBlock();
        }
    }

    public void testIntBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            IntBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                IntBlock.Builder blockBuilder = IntBlock.newBlockBuilder(builderEstimateSize);
                IntStream.range(0, positionCount).forEach(blockBuilder::appendInt);
                block = blockBuilder.build();
            } else {
                block = new IntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount).asBlock();
            }

            assertThat(block.getPositionCount(), equalTo(positionCount));
            assertThat(block.getInt(0), equalTo(0));
            assertThat(block.getInt(positionCount - 1), equalTo(positionCount - 1));
            int pos = block.getInt(randomPosition(positionCount));
            assertThat(pos, is(block.getInt(pos)));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> IntBlock.newBlockBuilder(size),
                    (bb, value) -> bb.appendInt(value),
                    position -> position,
                    IntBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat((int) randomNonNullPosition, is(b.getInt(randomNonNullPosition.intValue())));
                    }
                );
            }

            IntBlock.Builder blockBuilder = IntBlock.newBlockBuilder(1);
            IntBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
            assertThat(copy, equalTo(block));

            IntVector.Builder vectorBuilder = IntVector.newVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            );
            IntStream.range(0, positionCount).forEach(vectorBuilder::appendInt);
            IntVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
        }
    }

    public void testConstantIntBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            int value = randomInt();
            IntBlock block;
            if (randomBoolean()) {
                block = IntBlock.newConstantBlockWith(value, positionCount);
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
            LongBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                LongBlock.Builder blockBuilder = LongBlock.newBlockBuilder(builderEstimateSize);
                LongStream.range(0, positionCount).forEach(blockBuilder::appendLong);
                block = blockBuilder.build();
            } else {
                block = new LongArrayVector(LongStream.range(0, positionCount).toArray(), positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0L, is(block.getLong(0)));
            assertThat((long) positionCount - 1, is(block.getLong(positionCount - 1)));
            int pos = (int) block.getLong(randomPosition(positionCount));
            assertThat((long) pos, is(block.getLong(pos)));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> LongBlock.newBlockBuilder(size),
                    (bb, value) -> bb.appendLong(value),
                    position -> (long) position,
                    LongBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat((long) randomNonNullPosition, is(b.getLong(randomNonNullPosition.intValue())));
                    }
                );
            }

            LongBlock.Builder blockBuilder = LongBlock.newBlockBuilder(1);
            LongBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
            assertThat(copy, equalTo(block));

            LongVector.Builder vectorBuilder = LongVector.newVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            );
            LongStream.range(0, positionCount).forEach(vectorBuilder::appendLong);
            LongVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
        }
    }

    public void testConstantLongBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            long value = randomLong();
            LongBlock block;
            if (randomBoolean()) {
                block = LongBlock.newConstantBlockWith(value, positionCount);
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
            DoubleBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                var blockBuilder = DoubleBlock.newBlockBuilder(builderEstimateSize);
                LongStream.range(0, positionCount).asDoubleStream().forEach(blockBuilder::appendDouble);
                block = blockBuilder.build();
            } else {
                block = new DoubleArrayVector(LongStream.range(0, positionCount).asDoubleStream().toArray(), positionCount).asBlock();
            }

            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(0d, is(block.getDouble(0)));
            assertThat((double) positionCount - 1, is(block.getDouble(positionCount - 1)));
            int pos = (int) block.getDouble(randomPosition(positionCount));
            assertThat((double) pos, is(block.getDouble(pos)));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    size -> DoubleBlock.newBlockBuilder(size),
                    (bb, value) -> bb.appendDouble(value),
                    position -> (double) position,
                    DoubleBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat((double) randomNonNullPosition, is(b.getDouble(randomNonNullPosition.intValue())));
                    }
                );
            }

            DoubleBlock.Builder blockBuilder = DoubleBlock.newBlockBuilder(1);
            DoubleBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
            assertThat(copy, equalTo(block));

            DoubleVector.Builder vectorBuilder = DoubleVector.newVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            );
            IntStream.range(0, positionCount).mapToDouble(ii -> 1.0 / ii).forEach(vectorBuilder::appendDouble);
            DoubleVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
        }
    }

    public void testConstantDoubleBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            double value = randomDouble();
            DoubleBlock block;
            if (randomBoolean()) {
                block = DoubleBlock.newConstantBlockWith(value, positionCount);
            } else {
                block = new ConstantDoubleVector(value, positionCount).asBlock();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(value, is(block.getDouble(0)));
            assertThat(value, is(block.getDouble(positionCount - 1)));
            assertThat(value, is(block.getDouble(randomPosition(positionCount))));
            assertSingleValueDenseBlock(block);
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

        BytesRefBlock block;
        if (randomBoolean()) {
            final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
            var blockBuilder = BytesRefBlock.newBlockBuilder(builderEstimateSize);
            Arrays.stream(values).map(obj -> randomBoolean() ? obj : BytesRef.deepCopyOf(obj)).forEach(blockBuilder::appendBytesRef);
            block = blockBuilder.build();
        } else {
            BytesRefArray array = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
            Arrays.stream(values).forEach(array::append);
            block = new BytesRefArrayVector(array, positionCount).asBlock();
        }

        assertThat(positionCount, is(block.getPositionCount()));
        BytesRef bytes = new BytesRef();
        for (int i = 0; i < positionCount; i++) {
            int pos = randomIntBetween(0, positionCount - 1);
            bytes = block.getBytesRef(pos, bytes);
            assertThat(bytes, equalTo(values[pos]));
        }
        assertSingleValueDenseBlock(block);

        if (positionCount > 1) {
            assertNullValues(
                positionCount,
                size -> BytesRefBlock.newBlockBuilder(size),
                (bb, value) -> bb.appendBytesRef(value),
                position -> values[position],
                BytesRefBlock.Builder::build,
                (randomNonNullPosition, b) -> assertThat(
                    values[randomNonNullPosition],
                    is(b.getBytesRef(randomNonNullPosition, new BytesRef()))
                )
            );
        }

        BytesRefBlock.Builder blockBuilder = BytesRefBlock.newBlockBuilder(1);
        BytesRefBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
        assertThat(copy, equalTo(block));

        BytesRefVector.Builder vectorBuilder = BytesRefVector.newVectorBuilder(
            randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
        );
        IntStream.range(0, positionCount).mapToObj(ii -> new BytesRef(randomAlphaOfLength(5))).forEach(vectorBuilder::appendBytesRef);
        BytesRefVector vector = vectorBuilder.build();
        assertSingleValueDenseBlock(vector.asBlock());
    }

    public void testBytesRefBlockBuilderWithNulls() {
        int positionCount = randomIntBetween(0, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        var blockBuilder = BytesRefBlock.newBlockBuilder(builderEstimateSize);
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
        BytesRefBlock block = blockBuilder.build();
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
                assertThat(block.getBytesRef(pos, bytes), equalTo(values[pos]));
            }
        }
    }

    public void testConstantBytesRefBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            BytesRef value = new BytesRef(randomByteArrayOfLength(between(1, 20)));
            BytesRefBlock block;
            if (randomBoolean()) {
                block = BytesRefBlock.newConstantBlockWith(value, positionCount);
            } else {
                block = new ConstantBytesRefVector(value, positionCount).asBlock();
            }
            assertThat(block.getPositionCount(), is(positionCount));

            BytesRef bytes = new BytesRef();
            bytes = block.getBytesRef(0, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(positionCount - 1, bytes);
            assertThat(bytes, is(value));
            bytes = block.getBytesRef(randomPosition(positionCount), bytes);
            assertThat(bytes, is(value));
            assertSingleValueDenseBlock(block);
        }
    }

    public void testBooleanBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            BooleanBlock block;
            if (randomBoolean()) {
                final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
                var blockBuilder = BooleanBlock.newBlockBuilder(builderEstimateSize);
                IntStream.range(0, positionCount).forEach(p -> blockBuilder.appendBoolean(p % 10 == 0));
                block = blockBuilder.build();
            } else {
                boolean[] values = new boolean[positionCount];
                for (int p = 0; p < positionCount; p++) {
                    values[p] = p % 10 == 0;
                }
                block = new BooleanArrayVector(values, positionCount).asBlock();
            }

            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.getBoolean(0), is(true));
            assertThat(block.getBoolean(positionCount - 1), is((positionCount - 1) % 10 == 0));
            assertSingleValueDenseBlock(block);

            if (positionCount > 1) {
                assertNullValues(
                    positionCount,
                    BooleanBlock::newBlockBuilder,
                    (bb, value) -> bb.appendBoolean(value),
                    position -> position % 10 == 0,
                    BooleanBlock.Builder::build,
                    (randomNonNullPosition, b) -> {
                        assertThat(b.getBoolean(randomNonNullPosition.intValue()), is(randomNonNullPosition % 10 == 0));
                    }
                );
            }

            BooleanBlock.Builder blockBuilder = BooleanBlock.newBlockBuilder(1);
            BooleanBlock copy = blockBuilder.copyFrom(block, 0, block.getPositionCount()).build();
            assertThat(copy, equalTo(block));

            BooleanVector.Builder vectorBuilder = BooleanVector.newVectorBuilder(
                randomBoolean() ? randomIntBetween(1, positionCount) : positionCount
            );
            IntStream.range(0, positionCount).mapToObj(ii -> randomBoolean()).forEach(vectorBuilder::appendBoolean);
            BooleanVector vector = vectorBuilder.build();
            assertSingleValueDenseBlock(vector.asBlock());
        }
    }

    public void testConstantBooleanBlock() {
        for (int i = 0; i < 1000; i++) {
            int positionCount = randomIntBetween(1, 16 * 1024);
            boolean value = randomBoolean();
            BooleanBlock block;
            if (randomBoolean()) {
                block = BooleanBlock.newConstantBlockWith(value, positionCount);
            } else {
                block = new ConstantBooleanVector(value, positionCount).asBlock();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(block.getBoolean(0), is(value));
            assertThat(block.getBoolean(positionCount - 1), is(value));
            assertThat(block.getBoolean(randomPosition(positionCount)), is(value));
            assertSingleValueDenseBlock(block);
        }
    }

    public void testSingleValueSparseInt() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        var blockBuilder = IntBlock.newBlockBuilder(builderEstimateSize);

        int actualValueCount = 0;
        int[] values = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomInt();
                blockBuilder.appendInt(values[i]);
                actualValueCount++;
            } else {
                blockBuilder.appendNull();
            }
        }
        IntBlock block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(actualValueCount));
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
        assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
    }

    public void testSingleValueSparseLong() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        var blockBuilder = LongBlock.newBlockBuilder(builderEstimateSize);

        int actualValueCount = 0;
        long[] values = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomLong();
                blockBuilder.appendLong(values[i]);
                actualValueCount++;
            } else {
                blockBuilder.appendNull();
            }
        }
        LongBlock block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(actualValueCount));
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                nullCount++;
            } else {
                assertThat(block.getLong(i), is(values[i]));
            }
        }
        assertThat(block.nullValuesCount(), is(nullCount));
        assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
    }

    public void testSingleValueSparseDouble() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        var blockBuilder = DoubleBlock.newBlockBuilder(builderEstimateSize);

        int actualValueCount = 0;
        double[] values = new double[positionCount];
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomDouble();
                blockBuilder.appendDouble(values[i]);
                actualValueCount++;
            } else {
                blockBuilder.appendNull();
            }
        }
        DoubleBlock block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(actualValueCount));
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                nullCount++;
            } else {
                assertThat(block.getDouble(i), is(values[i]));
            }
        }
        assertThat(block.nullValuesCount(), is(nullCount));
        assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
    }

    public void testSingleValueSparseBoolean() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        final int builderEstimateSize = randomBoolean() ? randomIntBetween(1, positionCount) : positionCount;
        var blockBuilder = BooleanBlock.newBlockBuilder(builderEstimateSize);

        boolean[] values = new boolean[positionCount];
        int actualValueCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (randomBoolean()) {
                values[i] = randomBoolean();
                blockBuilder.appendBoolean(values[i]);
                actualValueCount++;
            } else {
                blockBuilder.appendNull();
            }
        }
        BooleanBlock block = blockBuilder.build();

        assertThat(block.getPositionCount(), is(positionCount));
        assertThat(block.getTotalValueCount(), is(actualValueCount));
        int nullCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                nullCount++;
            } else {
                assertThat(block.getBoolean(i), is(values[i]));
            }
        }
        assertThat(block.nullValuesCount(), is(nullCount));
        assertThat(block.asVector(), nullCount > 0 ? is(nullValue()) : is(notNullValue()));
    }

    public void testToStringSmall() {
        final int estimatedSize = randomIntBetween(1024, 4096);

        var boolBlock = BooleanBlock.newBlockBuilder(estimatedSize).appendBoolean(true).appendBoolean(false).build();
        var boolVector = BooleanVector.newVectorBuilder(estimatedSize).appendBoolean(true).appendBoolean(false).build();
        for (Object obj : List.of(boolVector, boolBlock, boolBlock.asVector())) {
            String s = obj.toString();
            assertThat(s, containsString("[true, false]"));
            assertThat(s, containsString("positions=2"));
        }

        var intBlock = IntBlock.newBlockBuilder(estimatedSize).appendInt(1).appendInt(2).build();
        var intVector = IntVector.newVectorBuilder(estimatedSize).appendInt(1).appendInt(2).build();
        for (Object obj : List.of(intVector, intBlock, intBlock.asVector())) {
            String s = obj.toString();
            assertThat(s, containsString("[1, 2]"));
            assertThat(s, containsString("positions=2"));
        }
        for (IntBlock block : List.of(intBlock, intVector.asBlock())) {
            assertThat(block.filter(0).toString(), containsString("FilterIntVector[positions=1, values=[1]]"));
            assertThat(block.filter(1).toString(), containsString("FilterIntVector[positions=1, values=[2]]"));
            assertThat(block.filter(0, 1).toString(), containsString("FilterIntVector[positions=2, values=[1, 2]]"));
            assertThat(block.filter().toString(), containsString("FilterIntVector[positions=0, values=[]]"));
        }
        for (IntVector vector : List.of(intVector, intBlock.asVector())) {
            assertThat(vector.filter(0).toString(), containsString("FilterIntVector[positions=1, values=[1]]"));
            assertThat(vector.filter(1).toString(), containsString("FilterIntVector[positions=1, values=[2]]"));
            assertThat(vector.filter(0, 1).toString(), containsString("FilterIntVector[positions=2, values=[1, 2]]"));
            assertThat(vector.filter().toString(), containsString("FilterIntVector[positions=0, values=[]]"));
        }

        var longBlock = LongBlock.newBlockBuilder(estimatedSize).appendLong(10L).appendLong(20L).build();
        var longVector = LongVector.newVectorBuilder(estimatedSize).appendLong(10L).appendLong(20L).build();
        for (Object obj : List.of(longVector, longBlock, longBlock.asVector())) {
            String s = obj.toString();
            assertThat(s, containsString("[10, 20]"));
            assertThat(s, containsString("positions=2"));
        }

        var doubleBlock = DoubleBlock.newBlockBuilder(estimatedSize).appendDouble(3.3).appendDouble(4.4).build();
        var doubleVector = DoubleVector.newVectorBuilder(estimatedSize).appendDouble(3.3).appendDouble(4.4).build();
        for (Object obj : List.of(doubleVector, doubleBlock, doubleBlock.asVector())) {
            String s = obj.toString();
            assertThat(s, containsString("[3.3, 4.4]"));
            assertThat(s, containsString("positions=2"));
        }

        assert new BytesRef("1a").toString().equals("[31 61]") && new BytesRef("2b").toString().equals("[32 62]");
        var bytesRefBlock = BytesRefBlock.newBlockBuilder(estimatedSize)
            .appendBytesRef(new BytesRef("1a"))
            .appendBytesRef(new BytesRef("2b"))
            .build();
        var bytesRefVector = BytesRefVector.newVectorBuilder(estimatedSize)
            .appendBytesRef(new BytesRef("1a"))
            .appendBytesRef(new BytesRef("2b"))
            .build();
        for (Object obj : List.of(bytesRefVector, bytesRefVector, bytesRefBlock.asVector())) {
            String s = obj.toString();
            assertThat(s, containsString("positions=2"));
        }
    }

    public static List<List<Object>> valuesAtPositions(Block block, int from, int to) {
        List<List<Object>> result = new ArrayList<>(to - from);
        for (int p = from; p < to; p++) {
            if (block.isNull(p)) {
                result.add(null);
                continue;
            }
            int count = block.getValueCount(p);
            List<Object> positionValues = new ArrayList<>(count);
            int i = block.getFirstValueIndex(p);
            for (int v = 0; v < count; v++) {
                positionValues.add(switch (block.elementType()) {
                    case INT -> ((IntBlock) block).getInt(i++);
                    case LONG -> ((LongBlock) block).getLong(i++);
                    case DOUBLE -> ((DoubleBlock) block).getDouble(i++);
                    case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(i++, new BytesRef());
                    case BOOLEAN -> ((BooleanBlock) block).getBoolean(i++);
                    default -> throw new IllegalArgumentException("unsupported element type [" + block.elementType() + "]");
                });
            }
            result.add(positionValues);
        }
        return result;
    }

    public record RandomBlock(List<List<Object>> values, Block block) {
        int valueCount() {
            return values.stream().mapToInt(l -> l == null ? 0 : l.size()).sum();
        }
    }

    public static RandomBlock randomBlock(
        ElementType elementType,
        int positionCount,
        boolean nullAllowed,
        int minValuesPerPosition,
        int maxValuesPerPosition,
        int minDupsPerPosition,
        int maxDupsPerPosition
    ) {
        List<List<Object>> values = new ArrayList<>();
        var builder = elementType.newBlockBuilder(positionCount);
        for (int p = 0; p < positionCount; p++) {
            int valueCount = between(minValuesPerPosition, maxValuesPerPosition);
            if (valueCount == 0 || nullAllowed && randomBoolean()) {
                values.add(null);
                builder.appendNull();
                continue;
            }
            int dupCount = between(minDupsPerPosition, maxDupsPerPosition);
            if (valueCount != 1 || dupCount != 0) {
                builder.beginPositionEntry();
            }
            List<Object> valuesAtPosition = new ArrayList<>();
            values.add(valuesAtPosition);
            for (int v = 0; v < valueCount; v++) {
                switch (elementType) {
                    case INT -> {
                        int i = randomInt();
                        valuesAtPosition.add(i);
                        ((IntBlock.Builder) builder).appendInt(i);
                    }
                    case LONG -> {
                        long l = randomLong();
                        valuesAtPosition.add(l);
                        ((LongBlock.Builder) builder).appendLong(l);
                    }
                    case DOUBLE -> {
                        double d = randomDouble();
                        valuesAtPosition.add(d);
                        ((DoubleBlock.Builder) builder).appendDouble(d);
                    }
                    case BYTES_REF -> {
                        BytesRef b = new BytesRef(randomRealisticUnicodeOfLength(4));
                        valuesAtPosition.add(b);
                        ((BytesRefBlock.Builder) builder).appendBytesRef(b);
                    }
                    case BOOLEAN -> {
                        boolean b = randomBoolean();
                        valuesAtPosition.add(b);
                        ((BooleanBlock.Builder) builder).appendBoolean(b);
                    }
                    default -> throw new IllegalArgumentException("unsupported element type [" + elementType + "]");
                }
            }
            for (int i = 0; i < dupCount; i++) {
                BlockTestUtils.append(builder, randomFrom(valuesAtPosition));
            }
            if (valueCount != 1 || dupCount != 0) {
                builder.endPositionEntry();
            }
        }
        return new RandomBlock(values, builder.build());
    }

    interface BlockBuilderFactory<B extends Block.Builder> {
        B create(int estimatedSize);
    }

    interface BlockProducer<B extends Block, BB extends Block.Builder> {
        B build(BB blockBuilder);
    }

    interface ValueAppender<BB extends Block.Builder, T> {
        void appendValue(BB blockBuilder, T value);
    }

    interface ValueSupplier<T> {
        T getValue(int position);
    }

    private static <B extends Block, BB extends Block.Builder, T> void assertNullValues(
        int positionCount,
        BlockBuilderFactory<BB> blockBuilderFactory,
        ValueAppender<BB, T> valueAppender,
        ValueSupplier<T> valueSupplier,
        BlockProducer<B, BB> blockProducer,
        BiConsumer<Integer, B> asserter
    ) {
        assertThat("test needs at least two positions", positionCount, greaterThan(1));
        int randomNullPosition = randomIntBetween(0, positionCount - 1);
        int randomNonNullPosition = randomValueOtherThan(randomNullPosition, () -> randomIntBetween(0, positionCount - 1));
        BitSet nullsMask = new BitSet(positionCount);
        nullsMask.set(randomNullPosition);

        var blockBuilder = blockBuilderFactory.create(positionCount);
        IntStream.range(0, positionCount).forEach(position -> {
            if (nullsMask.get(position)) {
                blockBuilder.appendNull();
            } else {
                valueAppender.appendValue(blockBuilder, valueSupplier.getValue(position));
            }
        });
        var block = blockProducer.build(blockBuilder);

        assertThat(block.getPositionCount(), equalTo(positionCount));
        assertThat(block.getTotalValueCount(), equalTo(positionCount - 1));
        asserter.accept(randomNonNullPosition, block);
        assertTrue(block.isNull(randomNullPosition));
        assertFalse(block.isNull(randomNonNullPosition));
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }
}
