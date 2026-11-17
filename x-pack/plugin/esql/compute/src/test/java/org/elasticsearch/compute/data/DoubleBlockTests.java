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

public class DoubleBlockTests extends BlockTestCase<DoubleBlock, DoubleBlock.Builder, Double> {
    @Override
    protected DoubleBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newDoubleBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(DoubleBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(DoubleBlock.Builder builder, Double value) {
        builder.appendDouble(value);
    }

    @Override
    protected void appendMultivalued(DoubleBlock.Builder builder, List<Double> values) {
        builder.beginPositionEntry();
        for (double value : values) {
            builder.appendDouble(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected DoubleBlock build(DoubleBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<Double> valuesAt(DoubleBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<Double> values = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            values.add(block.getDouble(i));
        }
        return values;
    }

    @Override
    protected Double randomValue() {
        return randomDouble();
    }

    @Override
    protected boolean positionHasValue(DoubleBlock block, int position, Double value) {
        return block.hasValue(position, value);
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.DOUBLE;
    }

    @Override
    protected boolean supportsConstantBlockFactory() {
        return true;
    }

    @Override
    protected DoubleBlock createConstantBlock(BlockFactory blockFactory, Double value, int positions) {
        return blockFactory.newConstantDoubleBlockWith(value, positions);
    }

    @Override
    protected void assertSingleValueBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantDoubleVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleVectorBlock.class));
        assertThat(block.asVector(), instanceOf(DoubleArrayVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleVectorBlock.class));
        assertThat(block.asVector(), instanceOf(DoubleBigArrayVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleBigArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleVectorBlock.class));
        assertThat(block.asVector(), instanceOf(DoubleArrayVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleArrayBlock.class));
    }

    @Override
    protected void assertConstantBlockFactoryRepresentation(DoubleBlock block) {
        assertThat(block, instanceOf(DoubleVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantDoubleVector.class));
    }

    @Override
    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {
        assertThat(block.asVector(), instanceOf(ConstantDoubleVector.class));
    }

    @Override
    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {
        assertThat(block, instanceOf(ConstantNullBlock.class));
    }

    public void testEmptyArrayBlockFactory() {
        try (
            DoubleBlock block = blockFactory().newDoubleArrayBlock(
                new double[] {},
                0,
                new int[] { 0 },
                new BitSet(),
                Block.MvOrdering.UNORDERED
            )
        ) {
            assertThat(block, instanceOf(DoubleArrayBlock.class));
            assertBlock(block, List.of());
        }
    }

    public void testEmptyArrayVectorFactory() {
        DoubleVector vector = blockFactory().newDoubleArrayVector(new double[] {}, 0);
        try (DoubleBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(DoubleVectorBlock.class));
            assertThat(block.asVector(), instanceOf(DoubleArrayVector.class));
            assertDoubleVector(block.asVector(), List.of());
            assertBlock(block, List.of());
        }
    }

    public void testArrayVectorFactory() {
        int positionCount = randomIntBetween(1, 1024);
        double[] values = new double[positionCount];
        List<Double> expectedVector = new ArrayList<>(positionCount);
        List<List<Double>> expectedBlock = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            values[p] = randomDouble();
            expectedVector.add(values[p]);
            expectedBlock.add(List.of(values[p]));
        }
        DoubleVector vector = blockFactory().newDoubleArrayVector(values, positionCount);
        try (DoubleBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(DoubleVectorBlock.class));
            assertThat(block.asVector(), instanceOf(DoubleArrayVector.class));
            assertDoubleVector(vector, expectedVector);
            assertBlock(block, expectedBlock);
        }
    }

    public void testEmptyVectorBuilder() {
        try (DoubleVector.Builder builder = blockFactory().newDoubleVectorBuilder(0)) {
            DoubleVector vector = builder.build();
            try (DoubleBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(DoubleVectorBlock.class));
                assertThat(block.asVector(), instanceOf(DoubleArrayVector.class));
                assertDoubleVector(vector, List.of());
                assertBlock(block, List.of());
            }
        }
    }

    public void testVectorBuilder() {
        int positionCount = randomIntBetween(1, 1024);
        List<Double> expectedVector = new ArrayList<>(positionCount);
        List<List<Double>> expectedBlock = new ArrayList<>(positionCount);
        try (DoubleVector.Builder builder = blockFactory().newDoubleVectorBuilder(randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                double value = randomDouble();
                builder.appendDouble(value);
                expectedVector.add(value);
                expectedBlock.add(List.of(value));
            }
            DoubleVector vector = builder.build();
            try (DoubleBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(DoubleVectorBlock.class));
                assertDoubleVector(vector, expectedVector);
                assertBlock(block, expectedBlock);
            }
        }
    }

    public void testDenseSequentialDoubleBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        List<List<Double>> expected = new ArrayList<>(positionCount);
        for (long value = 0; value < positionCount; value++) {
            expected.add(List.of((double) value));
        }
        try (DoubleBlock block = randomBoolean() ? buildBlock(blockFactory(), expected) : newSequentialArrayVectorBlock(positionCount)) {
            assertThat(block.getPositionCount(), equalTo(positionCount));
            assertThat(block.getDouble(0), equalTo(0d));
            assertThat(block.getDouble(positionCount - 1), equalTo((double) positionCount - 1));
            int position = randomIntBetween(0, positionCount - 1);
            assertThat(block.getDouble(position), equalTo((double) position));
            assertBlock(block, expected);
        }
    }

    public void testSingleNullDoubleBlock() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        int nullPosition = randomIntBetween(0, positionCount - 1);
        List<List<Double>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(p == nullPosition ? null : List.of((double) p));
        }
        try (DoubleBlock block = buildBlock(blockFactory(), expected)) {
            assertTrue(block.isNull(nullPosition));
            int nonNullPosition = randomValueOtherThan(nullPosition, () -> randomIntBetween(0, positionCount - 1));
            assertThat(block.getDouble(nonNullPosition), equalTo((double) nonNullPosition));
            assertBlock(block, expected);
        }
    }

    public void testCopyTo() {
        int positionCount = randomIntBetween(1, 1000);
        try (DoubleVector.Builder builder = blockFactory().newDoubleVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendDouble(randomDouble());
            }
            try (DoubleVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                double sentinel = randomDouble();
                double[] dst = new double[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getDouble(srcPosition + i)));
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
        try (DoubleBlock emptyBlock = blockFactory().newDoubleVectorBuilder(0).build().asBlock()) {
            assertSerializationAtSupportedVersions(emptyBlock, List.of());
        }
        try (DoubleVector toFilter = blockFactory().newDoubleVectorBuilder(1).appendDouble(randomDouble()).build()) {
            // filter() returns a new vector; asBlock() owns that filtered vector, not toFilter.
            try (DoubleBlock filtered = toFilter.filter(false).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of());
            }
        }
        try (
            DoubleVector toFilter = blockFactory().newDoubleVectorBuilder(1)
                .appendDouble(randomDouble())
                .appendDouble(randomDouble())
                .build()
        ) {
            double expected = toFilter.getDouble(0);
            try (DoubleBlock filtered = toFilter.filter(false, 0).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of(List.of(expected)));
            }
        }
    }

    @Override
    protected void assertAdditionalInvariants(DoubleBlock block, List<List<Double>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : Double.BYTES));
    }

    private DoubleBlock newSequentialArrayVectorBlock(int positionCount) {
        double[] values = new double[positionCount];
        for (int p = 0; p < positionCount; p++) {
            values[p] = p;
        }
        return blockFactory().newDoubleArrayVector(values, positionCount).asBlock();
    }

    private static void assertDoubleVector(DoubleVector vector, List<Double> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        assertThat(vector.valueMaxByteSize(), equalTo(Double.BYTES));
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getDouble(p), equalTo(expected.get(p)));
        }
    }
}
