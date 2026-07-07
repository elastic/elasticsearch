/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.ArrayList;
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
        try (LongBlock block = blockFactory().newLongArrayBlock(new long[] {}, 0, new int[] { 0 }, new BitSet(), Block.MvOrdering.UNORDERED)) {
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

    private static void assertLongVector(LongVector vector, List<Long> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        assertThat(vector.valueMaxByteSize(), equalTo(Long.BYTES));
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getLong(p), equalTo(expected.get(p)));
        }
    }
}
