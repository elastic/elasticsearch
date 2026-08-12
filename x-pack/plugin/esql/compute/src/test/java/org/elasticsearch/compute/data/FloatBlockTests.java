/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FloatBlockTests extends BlockTestCase<FloatBlock, FloatBlock.Builder, Float> {
    @Override
    protected FloatBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newFloatBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(FloatBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(FloatBlock.Builder builder, Float value) {
        builder.appendFloat(value);
    }

    @Override
    protected void appendMultivalued(FloatBlock.Builder builder, List<Float> values) {
        builder.beginPositionEntry();
        for (float value : values) {
            builder.appendFloat(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected FloatBlock build(FloatBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<Float> valuesAt(FloatBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<Float> values = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            values.add(block.getFloat(i));
        }
        return values;
    }

    @Override
    protected Float randomValue() {
        return randomFloat();
    }

    @Override
    protected boolean positionHasValue(FloatBlock block, int position, Float value) {
        return block.hasValue(position, value);
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.FLOAT;
    }

    @Override
    protected boolean supportsConstantBlockFactory() {
        return true;
    }

    @Override
    protected FloatBlock createConstantBlock(BlockFactory blockFactory, Float value, int positions) {
        return blockFactory.newConstantFloatBlockWith(value, positions);
    }

    @Override
    protected void assertSingleValueBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantFloatVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatVectorBlock.class));
        assertThat(block.asVector(), instanceOf(FloatArrayVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatVectorBlock.class));
        assertThat(block.asVector(), instanceOf(FloatBigArrayVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatBigArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatVectorBlock.class));
        assertThat(block.asVector(), instanceOf(FloatArrayVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatArrayBlock.class));
    }

    @Override
    protected void assertConstantBlockFactoryRepresentation(FloatBlock block) {
        assertThat(block, instanceOf(FloatVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantFloatVector.class));
    }

    @Override
    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {
        assertThat(block.asVector(), instanceOf(ConstantFloatVector.class));
    }

    @Override
    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {
        assertThat(block, instanceOf(ConstantNullBlock.class));
    }

    public void testEmptyArrayBlockFactory() {
        try (
            FloatBlock block = blockFactory().newFloatArrayBlock(
                new float[] {},
                0,
                new int[] { 0 },
                new BitSet(),
                Block.MvOrdering.UNORDERED
            )
        ) {
            assertThat(block, instanceOf(FloatArrayBlock.class));
            assertBlock(block, List.of());
        }
    }

    public void testEmptyArrayVectorFactory() {
        FloatVector vector = blockFactory().newFloatArrayVector(new float[] {}, 0);
        try (FloatBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(FloatVectorBlock.class));
            assertThat(block.asVector(), instanceOf(FloatArrayVector.class));
            assertFloatVector(block.asVector(), List.of());
            assertBlock(block, List.of());
        }
    }

    public void testArrayVectorFactory() {
        int positionCount = randomIntBetween(1, 1024);
        float[] values = new float[positionCount];
        List<Float> expectedVector = new ArrayList<>(positionCount);
        List<List<Float>> expectedBlock = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            values[p] = randomFloat();
            expectedVector.add(values[p]);
            expectedBlock.add(List.of(values[p]));
        }
        FloatVector vector = blockFactory().newFloatArrayVector(values, positionCount);
        try (FloatBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(FloatVectorBlock.class));
            assertThat(block.asVector(), instanceOf(FloatArrayVector.class));
            assertFloatVector(vector, expectedVector);
            assertBlock(block, expectedBlock);
        }
    }

    public void testEmptyVectorBuilder() {
        try (FloatVector.Builder builder = blockFactory().newFloatVectorBuilder(0)) {
            FloatVector vector = builder.build();
            try (FloatBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(FloatVectorBlock.class));
                assertThat(block.asVector(), instanceOf(FloatArrayVector.class));
                assertFloatVector(vector, List.of());
                assertBlock(block, List.of());
            }
        }
    }

    public void testVectorBuilder() {
        int positionCount = randomIntBetween(1, 1024);
        List<Float> expectedVector = new ArrayList<>(positionCount);
        List<List<Float>> expectedBlock = new ArrayList<>(positionCount);
        try (FloatVector.Builder builder = blockFactory().newFloatVectorBuilder(randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                float value = randomFloat();
                builder.appendFloat(value);
                expectedVector.add(value);
                expectedBlock.add(List.of(value));
            }
            FloatVector vector = builder.build();
            try (FloatBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(FloatVectorBlock.class));
                assertFloatVector(vector, expectedVector);
                assertBlock(block, expectedBlock);
            }
        }
    }

    public void testDenseSequentialFloatBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        List<List<Float>> expected = new ArrayList<>(positionCount);
        for (int value = 0; value < positionCount; value++) {
            expected.add(List.of((float) value));
        }
        try (FloatBlock block = randomBoolean() ? buildBlock(blockFactory(), expected) : newSequentialArrayVectorBlock(positionCount)) {
            assertThat(block.getPositionCount(), equalTo(positionCount));
            assertThat(block.getFloat(0), equalTo(0f));
            assertThat(block.getFloat(positionCount - 1), equalTo((float) positionCount - 1));
            int position = randomIntBetween(0, positionCount - 1);
            assertThat(block.getFloat(position), equalTo((float) position));
            assertBlock(block, expected);
        }
    }

    public void testSingleNullFloatBlock() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        int nullPosition = randomIntBetween(0, positionCount - 1);
        List<List<Float>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(p == nullPosition ? null : List.of((float) p));
        }
        try (FloatBlock block = buildBlock(blockFactory(), expected)) {
            assertTrue(block.isNull(nullPosition));
            int nonNullPosition = randomValueOtherThan(nullPosition, () -> randomIntBetween(0, positionCount - 1));
            assertThat(block.getFloat(nonNullPosition), equalTo((float) nonNullPosition));
            assertBlock(block, expected);
        }
    }

    public void testCopyTo() {
        int positionCount = randomIntBetween(1, 1000);
        try (FloatVector.Builder builder = blockFactory().newFloatVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendFloat(randomFloat());
            }
            try (FloatVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                float sentinel = randomFloat();
                float[] dst = new float[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getFloat(srcPosition + i)));
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

    public void testVectorFactorySerialization() throws IOException {
        // asBlock() takes ownership of the vector — close only the block.
        try (FloatBlock emptyBlock = blockFactory().newFloatVectorBuilder(0).build().asBlock()) {
            assertSerializationAtSupportedVersions(emptyBlock, List.of());
        }
        try (FloatVector toFilter = blockFactory().newFloatVectorBuilder(1).appendFloat(randomFloat()).build()) {
            // filter() returns a new vector; asBlock() owns that filtered vector, not toFilter.
            try (FloatBlock filtered = toFilter.filter(false).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of());
            }
        }
        try (FloatVector toFilter = blockFactory().newFloatVectorBuilder(1).appendFloat(randomFloat()).appendFloat(randomFloat()).build()) {
            float expected = toFilter.getFloat(0);
            try (FloatBlock filtered = toFilter.filter(false, 0).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of(List.of(expected)));
            }
        }
    }

    @Override
    protected void assertAdditionalInvariants(FloatBlock block, List<List<Float>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : Float.BYTES));
    }

    private FloatBlock newSequentialArrayVectorBlock(int positionCount) {
        float[] values = new float[positionCount];
        for (int p = 0; p < positionCount; p++) {
            values[p] = p;
        }
        return blockFactory().newFloatArrayVector(values, positionCount).asBlock();
    }

    private static void assertFloatVector(FloatVector vector, List<Float> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        assertThat(vector.valueMaxByteSize(), equalTo(Float.BYTES));
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getFloat(p), equalTo(expected.get(p)));
        }
    }
}
