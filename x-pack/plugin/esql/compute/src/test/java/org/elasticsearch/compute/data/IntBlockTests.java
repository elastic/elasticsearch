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

public class IntBlockTests extends BlockTestCase<IntBlock, IntBlock.Builder, Integer> {
    @Override
    protected IntBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newIntBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(IntBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(IntBlock.Builder builder, Integer value) {
        builder.appendInt(value);
    }

    @Override
    protected void appendMultivalued(IntBlock.Builder builder, List<Integer> values) {
        builder.beginPositionEntry();
        for (int value : values) {
            builder.appendInt(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected IntBlock build(IntBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<Integer> valuesAt(IntBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<Integer> values = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            values.add(block.getInt(i));
        }
        return values;
    }

    @Override
    protected Integer randomValue() {
        return randomInt();
    }

    @Override
    protected boolean positionHasValue(IntBlock block, int position, Integer value) {
        return block.hasValue(position, value);
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.INT;
    }

    @Override
    protected boolean supportsConstantBlockFactory() {
        return true;
    }

    @Override
    protected IntBlock createConstantBlock(BlockFactory blockFactory, Integer value, int positions) {
        return blockFactory.newConstantIntBlockWith(value, positions);
    }

    @Override
    protected void assertSingleValueBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantIntVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntVectorBlock.class));
        assertThat(block.asVector(), instanceOf(IntArrayVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntVectorBlock.class));
        assertThat(block.asVector(), instanceOf(IntBigArrayVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntBigArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntVectorBlock.class));
        assertThat(block.asVector(), instanceOf(IntArrayVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntArrayBlock.class));
    }

    @Override
    protected void assertConstantBlockFactoryRepresentation(IntBlock block) {
        assertThat(block, instanceOf(IntVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantIntVector.class));
    }

    @Override
    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {
        assertThat(block.asVector(), instanceOf(ConstantIntVector.class));
    }

    @Override
    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {
        assertThat(block, instanceOf(ConstantNullBlock.class));
    }

    public void testEmptyArrayBlockFactory() {
        try (IntBlock block = blockFactory().newIntArrayBlock(new int[] {}, 0, new int[] { 0 }, new BitSet(), Block.MvOrdering.UNORDERED)) {
            assertThat(block, instanceOf(IntArrayBlock.class));
            assertBlock(block, List.of());
        }
    }

    public void testEmptyArrayVectorFactory() {
        IntVector vector = blockFactory().newIntArrayVector(new int[] {}, 0);
        try (IntBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(IntVectorBlock.class));
            assertThat(block.asVector(), instanceOf(IntArrayVector.class));
            assertIntVector(block.asVector(), List.of());
            assertBlock(block, List.of());
        }
    }

    public void testArrayVectorFactory() {
        int positionCount = randomIntBetween(1, 1024);
        int[] values = new int[positionCount];
        List<Integer> expectedVector = new ArrayList<>(positionCount);
        List<List<Integer>> expectedBlock = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            values[p] = randomInt();
            expectedVector.add(values[p]);
            expectedBlock.add(List.of(values[p]));
        }
        IntVector vector = blockFactory().newIntArrayVector(values, positionCount);
        try (IntBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(IntVectorBlock.class));
            assertThat(block.asVector(), instanceOf(IntArrayVector.class));
            assertIntVector(vector, expectedVector);
            assertBlock(block, expectedBlock);
        }
    }

    public void testEmptyVectorBuilder() {
        try (IntVector.Builder builder = blockFactory().newIntVectorBuilder(0)) {
            IntVector vector = builder.build();
            try (IntBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(IntVectorBlock.class));
                assertThat(block.asVector(), instanceOf(IntArrayVector.class));
                assertIntVector(vector, List.of());
                assertBlock(block, List.of());
            }
        }
    }

    public void testVectorBuilder() {
        int positionCount = randomIntBetween(1, 1024);
        List<Integer> expectedVector = new ArrayList<>(positionCount);
        List<List<Integer>> expectedBlock = new ArrayList<>(positionCount);
        try (IntVector.Builder builder = blockFactory().newIntVectorBuilder(randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                int value = randomInt();
                builder.appendInt(value);
                expectedVector.add(value);
                expectedBlock.add(List.of(value));
            }
            IntVector vector = builder.build();
            try (IntBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(IntVectorBlock.class));
                assertIntVector(vector, expectedVector);
                assertBlock(block, expectedBlock);
            }
        }
    }

    public void testDenseSequentialIntBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        List<List<Integer>> expected = new ArrayList<>(positionCount);
        for (int value = 0; value < positionCount; value++) {
            expected.add(List.of(value));
        }
        try (IntBlock block = randomBoolean() ? buildBlock(blockFactory(), expected) : newSequentialArrayVectorBlock(positionCount)) {
            assertThat(block.getPositionCount(), equalTo(positionCount));
            assertThat(block.getInt(0), equalTo(0));
            assertThat(block.getInt(positionCount - 1), equalTo(positionCount - 1));
            int position = randomIntBetween(0, positionCount - 1);
            assertThat(block.getInt(position), equalTo(position));
            assertThat(block.asVector().min(), equalTo(0));
            assertThat(block.asVector().max(), equalTo(positionCount - 1));
            assertBlock(block, expected);
        }
    }

    public void testSingleNullIntBlock() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        int nullPosition = randomIntBetween(0, positionCount - 1);
        List<List<Integer>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(p == nullPosition ? null : List.of(p));
        }
        try (IntBlock block = buildBlock(blockFactory(), expected)) {
            assertTrue(block.isNull(nullPosition));
            int nonNullPosition = randomValueOtherThan(nullPosition, () -> randomIntBetween(0, positionCount - 1));
            assertThat(block.getInt(nonNullPosition), equalTo(nonNullPosition));
            assertBlock(block, expected);
        }
    }

    public void testEmptyIntRangeVector() {
        IntVector vector = blockFactory().newIntRangeVector(0, 0);
        try (IntBlock block = vector.asBlock()) {
            assertBlock(block, List.of());
        }
    }

    public void testIntRangeVector() {
        int positionCount = between(1, 16 * 1024);
        int start = randomBoolean() ? 0 : between(1, 16 * 1024);
        IntVector vector = blockFactory().newIntRangeVector(start, start + positionCount);
        List<List<Integer>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(List.of(start + p));
        }
        try (IntBlock block = vector.asBlock()) {
            assertThat(vector.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertThat(vector.getInt(p), equalTo(start + p));
            }
            assertThat(vector.min(), equalTo(start));
            assertThat(vector.max(), equalTo(start + positionCount - 1));
            assertBlock(block, expected);
        }
    }

    public void testEmptyMinMax() {
        // asBlock() takes ownership of the vector — close only the block.
        try (IntBlock block = blockFactory().newIntVectorBuilder(0).build().asBlock()) {
            assertThat(block.asVector().min(), equalTo(Integer.MAX_VALUE));
            assertThat(block.asVector().max(), equalTo(Integer.MIN_VALUE));
            assertBlock(block, List.of());
        }
    }

    public void testCopyTo() {
        int positionCount = randomIntBetween(1, 1000);
        try (IntVector.Builder builder = blockFactory().newIntVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendInt(randomInt());
            }
            try (IntVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                int sentinel = randomInt();
                int[] dst = new int[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getInt(srcPosition + i)));
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
        try (IntBlock emptyBlock = blockFactory().newIntVectorBuilder(0).build().asBlock()) {
            assertSerializationAtSupportedVersions(emptyBlock, List.of());
        }
        try (IntVector toFilter = blockFactory().newIntVectorBuilder(1).appendInt(randomInt()).build()) {
            // filter() returns a new vector; asBlock() owns that filtered vector, not toFilter.
            try (IntBlock filtered = toFilter.filter(false).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of());
            }
        }
        try (IntVector toFilter = blockFactory().newIntVectorBuilder(1).appendInt(randomInt()).appendInt(randomInt()).build()) {
            int expected = toFilter.getInt(0);
            try (IntBlock filtered = toFilter.filter(false, 0).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of(List.of(expected)));
            }
        }
    }

    @Override
    protected void assertAdditionalInvariants(IntBlock block, List<List<Integer>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : Integer.BYTES));
        if (block.asVector() != null && block.asVector().isConstant() && expected.isEmpty() == false) {
            int value = expected.get(0).get(0);
            assertThat(block.asVector().min(), equalTo(value));
            assertThat(block.asVector().max(), equalTo(value));
        }
    }

    private IntBlock newSequentialArrayVectorBlock(int positionCount) {
        int[] values = new int[positionCount];
        for (int p = 0; p < positionCount; p++) {
            values[p] = p;
        }
        return blockFactory().newIntArrayVector(values, positionCount).asBlock();
    }

    private static void assertIntVector(IntVector vector, List<Integer> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        assertThat(vector.valueMaxByteSize(), equalTo(Integer.BYTES));
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getInt(p), equalTo(expected.get(p)));
        }
    }
}
