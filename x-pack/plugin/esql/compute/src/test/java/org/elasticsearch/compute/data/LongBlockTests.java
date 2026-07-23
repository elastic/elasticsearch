/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LongBlockTests extends BlockTestCase<LongBlock, LongBlock.Builder, Long> {
    @Override
    protected LongBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newLongBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(LongBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(LongBlock.Builder builder, Long value) {
        builder.appendLong(value);
    }

    @Override
    protected void appendMultivalued(LongBlock.Builder builder, List<Long> values) {
        builder.beginPositionEntry();
        for (long value : values) {
            builder.appendLong(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected LongBlock build(LongBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<Long> valuesAt(LongBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<Long> values = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            values.add(block.getLong(i));
        }
        return values;
    }

    @Override
    protected Long randomValue() {
        return randomLong();
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.LONG;
    }

    @Override
    protected boolean supportsConstantBlockFactory() {
        return true;
    }

    @Override
    protected LongBlock createConstantBlock(BlockFactory blockFactory, Long value, int positions) {
        return blockFactory.newConstantLongBlockWith(value, positions);
    }

    @Override
    protected void assertSingleValueBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantLongVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongVectorBlock.class));
        assertThat(block.asVector(), instanceOf(LongArrayVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongVectorBlock.class));
        assertThat(block.asVector(), instanceOf(LongBigArrayVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongBigArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongVectorBlock.class));
        assertThat(block.asVector(), instanceOf(LongArrayVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongArrayBlock.class));
    }

    @Override
    protected void assertConstantBlockFactoryRepresentation(LongBlock block) {
        assertThat(block, instanceOf(LongVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantLongVector.class));
    }

    @Override
    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {
        assertThat(block.asVector(), instanceOf(ConstantLongVector.class));
    }

    @Override
    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {
        assertThat(block, instanceOf(ConstantNullBlock.class));
    }

    public void testEmptyArrayBlockFactory() {
        try (
            LongBlock block = blockFactory().newLongArrayBlock(new long[] {}, 0, new int[] { 0 }, new BitSet(), Block.MvOrdering.UNORDERED)
        ) {
            assertThat(block, instanceOf(LongArrayBlock.class));
            assertBlock(block, List.of());
        }
    }

    public void testEmptyArrayVectorFactory() {
        LongVector vector = blockFactory().newLongArrayVector(new long[] {}, 0);
        try (LongBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(LongVectorBlock.class));
            assertThat(block.asVector(), instanceOf(LongArrayVector.class));
            assertLongVector(block.asVector(), List.of());
            assertBlock(block, List.of());
        }
    }

    public void testArrayVectorFactory() {
        int positionCount = randomIntBetween(1, 1024);
        long[] values = new long[positionCount];
        List<Long> expectedVector = new ArrayList<>(positionCount);
        List<List<Long>> expectedBlock = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            values[p] = randomLong();
            expectedVector.add(values[p]);
            expectedBlock.add(List.of(values[p]));
        }
        LongVector vector = blockFactory().newLongArrayVector(values, positionCount);
        try (LongBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(LongVectorBlock.class));
            assertThat(block.asVector(), instanceOf(LongArrayVector.class));
            assertLongVector(vector, expectedVector);
            assertBlock(block, expectedBlock);
        }
    }

    public void testEmptyVectorBuilder() {
        try (LongVector.Builder builder = blockFactory().newLongVectorBuilder(0)) {
            LongVector vector = builder.build();
            try (LongBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(LongVectorBlock.class));
                assertThat(block.asVector(), instanceOf(LongArrayVector.class));
                assertLongVector(vector, List.of());
                assertBlock(block, List.of());
            }
        }
    }

    public void testVectorBuilder() {
        int positionCount = randomIntBetween(1, 1024);
        List<Long> expectedVector = new ArrayList<>(positionCount);
        List<List<Long>> expectedBlock = new ArrayList<>(positionCount);
        try (LongVector.Builder builder = blockFactory().newLongVectorBuilder(randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                long value = randomLong();
                builder.appendLong(value);
                expectedVector.add(value);
                expectedBlock.add(List.of(value));
            }
            LongVector vector = builder.build();
            try (LongBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(LongVectorBlock.class));
                assertLongVector(vector, expectedVector);
                assertBlock(block, expectedBlock);
            }
        }
    }

    public void testDenseSequentialLongBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        List<List<Long>> expected = new ArrayList<>(positionCount);
        for (long value = 0; value < positionCount; value++) {
            expected.add(List.of(value));
        }
        try (LongBlock block = randomBoolean() ? buildBlock(blockFactory(), expected) : newSequentialArrayVectorBlock(positionCount)) {
            assertThat(block.getPositionCount(), equalTo(positionCount));
            assertThat(block.getLong(0), equalTo(0L));
            assertThat(block.getLong(positionCount - 1), equalTo((long) positionCount - 1));
            int position = randomIntBetween(0, positionCount - 1);
            assertThat(block.getLong(position), equalTo((long) position));
            assertBlock(block, expected);
        }
    }

    public void testSingleNullLongBlock() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        int nullPosition = randomIntBetween(0, positionCount - 1);
        List<List<Long>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(p == nullPosition ? null : List.of((long) p));
        }
        try (LongBlock block = buildBlock(blockFactory(), expected)) {
            assertTrue(block.isNull(nullPosition));
            int nonNullPosition = randomValueOtherThan(nullPosition, () -> randomIntBetween(0, positionCount - 1));
            assertThat(block.getLong(nonNullPosition), equalTo((long) nonNullPosition));
            assertBlock(block, expected);
        }
    }

    public void testCopyTo() {
        int positionCount = randomIntBetween(1, 1000);
        try (LongVector.Builder builder = blockFactory().newLongVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendLong(randomLong());
            }
            try (LongVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                long sentinel = randomLong();
                long[] dst = new long[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getLong(srcPosition + i)));
                }
                for (int i = 0; i < dstPosition; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
                for (int i = dstPosition + length; i < dst.length; i++) {
                    assertThat(dst[i], equalTo(sentinel));
                }
            }
        }
    }

    @Override
    protected void assertAdditionalInvariants(LongBlock block, List<List<Long>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : Long.BYTES));
        for (int p = 0; p < block.getPositionCount(); p++) {
            List<Long> values = expected.get(p);
            if (values == null) {
                continue;
            }
            for (long value : values) {
                assertTrue(block.hasValue(p, value));
            }
            assertFalse(block.hasValue(p, randomValueOtherThanMany(v -> values.contains(v), this::randomValue)));
        }
    }

    private LongBlock newSequentialArrayVectorBlock(int positionCount) {
        long[] values = new long[positionCount];
        for (int p = 0; p < positionCount; p++) {
            values[p] = p;
        }
        return blockFactory().newLongArrayVector(values, positionCount).asBlock();
    }

    private static void assertLongVector(LongVector vector, List<Long> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        assertThat(vector.valueMaxByteSize(), equalTo(Long.BYTES));
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getLong(p), equalTo(expected.get(p)));
        }
    }
}
