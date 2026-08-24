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

public class BooleanBlockTests extends BlockTestCase<BooleanBlock, BooleanBlock.Builder, Boolean> {
    @Override
    protected BooleanBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newBooleanBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(BooleanBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(BooleanBlock.Builder builder, Boolean value) {
        builder.appendBoolean(value);
    }

    @Override
    protected void appendMultivalued(BooleanBlock.Builder builder, List<Boolean> values) {
        builder.beginPositionEntry();
        for (boolean value : values) {
            builder.appendBoolean(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected BooleanBlock build(BooleanBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<Boolean> valuesAt(BooleanBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<Boolean> values = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            values.add(block.getBoolean(i));
        }
        return values;
    }

    @Override
    protected Boolean randomValue() {
        return randomBoolean();
    }

    @Override
    protected boolean positionHasValue(BooleanBlock block, int position, Boolean value) {
        return block.hasValue(position, value);
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.BOOLEAN;
    }

    @Override
    protected boolean supportsConstantBlockFactory() {
        return true;
    }

    @Override
    protected BooleanBlock createConstantBlock(BlockFactory blockFactory, Boolean value, int positions) {
        return blockFactory.newConstantBooleanBlockWith(value, positions);
    }

    @Override
    protected void assertSingleValueBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantBooleanVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanVectorBlock.class));
        assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanVectorBlock.class));
        assertThat(block.asVector(), instanceOf(BooleanBigArrayVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanBigArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanVectorBlock.class));
        assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanArrayBlock.class));
    }

    @Override
    protected void assertConstantBlockFactoryRepresentation(BooleanBlock block) {
        assertThat(block, instanceOf(BooleanVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantBooleanVector.class));
    }

    @Override
    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {
        assertThat(block.asVector(), instanceOf(ConstantBooleanVector.class));
    }

    @Override
    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {
        assertThat(block, instanceOf(ConstantNullBlock.class));
    }

    public void testEmptyArrayBlockFactory() {
        try (
            BooleanBlock block = blockFactory().newBooleanArrayBlock(
                new boolean[] {},
                0,
                new int[] { 0 },
                new BitSet(),
                Block.MvOrdering.UNORDERED
            )
        ) {
            assertThat(block, instanceOf(BooleanArrayBlock.class));
            assertBlock(block, List.of());
        }
    }

    public void testEmptyArrayVectorFactory() {
        BooleanVector vector = blockFactory().newBooleanArrayVector(new boolean[] {}, 0);
        try (BooleanBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(BooleanVectorBlock.class));
            assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
            assertBooleanVector(block.asVector(), List.of());
            assertBlock(block, List.of());
        }
    }

    public void testArrayVectorFactory() {
        int positionCount = randomIntBetween(1, 1024);
        boolean[] values = new boolean[positionCount];
        List<Boolean> expectedVector = new ArrayList<>(positionCount);
        List<List<Boolean>> expectedBlock = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            values[p] = randomBoolean();
            expectedVector.add(values[p]);
            expectedBlock.add(List.of(values[p]));
        }
        BooleanVector vector = blockFactory().newBooleanArrayVector(values, positionCount);
        try (BooleanBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(BooleanVectorBlock.class));
            assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
            assertBooleanVector(vector, expectedVector);
            assertBlock(block, expectedBlock);
        }
    }

    public void testEmptyVectorBuilder() {
        try (BooleanVector.Builder builder = blockFactory().newBooleanVectorBuilder(0)) {
            BooleanVector vector = builder.build();
            try (BooleanBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(BooleanVectorBlock.class));
                assertThat(block.asVector(), instanceOf(BooleanArrayVector.class));
                assertBooleanVector(vector, List.of());
                assertBlock(block, List.of());
            }
        }
    }

    public void testVectorBuilder() {
        int positionCount = randomIntBetween(1, 1024);
        List<Boolean> expectedVector = new ArrayList<>(positionCount);
        List<List<Boolean>> expectedBlock = new ArrayList<>(positionCount);
        try (BooleanVector.Builder builder = blockFactory().newBooleanVectorBuilder(randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                boolean value = randomBoolean();
                builder.appendBoolean(value);
                expectedVector.add(value);
                expectedBlock.add(List.of(value));
            }
            BooleanVector vector = builder.build();
            try (BooleanBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(BooleanVectorBlock.class));
                assertBooleanVector(vector, expectedVector);
                assertBlock(block, expectedBlock);
            }
        }
    }

    public void testDenseModuloBooleanBlock() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        List<List<Boolean>> expected = new ArrayList<>(positionCount);
        boolean[] values = new boolean[positionCount];
        for (int p = 0; p < positionCount; p++) {
            values[p] = p % 10 == 0;
            expected.add(List.of(values[p]));
        }
        try (
            BooleanBlock block = randomBoolean()
                ? buildBlock(blockFactory(), expected)
                : blockFactory().newBooleanArrayVector(values, positionCount).asBlock()
        ) {
            assertThat(block.getBoolean(0), equalTo(true));
            assertThat(block.getBoolean(positionCount - 1), equalTo((positionCount - 1) % 10 == 0));
            try (ToMask mask = block.toMask()) {
                assertThat(mask.hadMultivaluedFields(), equalTo(false));
                for (int p = 0; p < positionCount; p++) {
                    assertThat(mask.mask().getBoolean(p), equalTo(p % 10 == 0));
                }
            }
            assertBlock(block, expected);
        }
    }

    public void testSingleNullBooleanBlock() {
        int positionCount = randomIntBetween(2, 16 * 1024);
        int nullPosition = randomIntBetween(0, positionCount - 1);
        List<List<Boolean>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            expected.add(p == nullPosition ? null : List.of(p % 10 == 0));
        }
        try (BooleanBlock block = buildBlock(blockFactory(), expected)) {
            assertTrue(block.isNull(nullPosition));
            int nonNullPosition = randomValueOtherThan(nullPosition, () -> randomIntBetween(0, positionCount - 1));
            assertThat(block.getBoolean(nonNullPosition), equalTo(nonNullPosition % 10 == 0));
            assertBlock(block, expected);
        }
    }

    public void testAllTrueAllFalse() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        Boolean value = randomFrom(random(), null, true, false);
        Boolean[] bools = new Boolean[positionCount];
        List<List<Boolean>> expected = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            bools[p] = value == null ? randomBoolean() : value;
            expected.add(List.of(bools[p]));
        }
        try (BooleanVector.Builder builder = blockFactory().newBooleanVectorBuilder(randomIntBetween(0, positionCount))) {
            Arrays.stream(bools).forEach(builder::appendBoolean);
            // asBlock() takes ownership of the vector — close only the block.
            try (BooleanBlock block = builder.build().asBlock()) {
                BooleanVector vector = block.asVector();
                BasicBlockTests.assertToMask(vector);
                if (value == null) {
                    assertThat(vector.allTrue(), equalTo(Arrays.stream(bools).allMatch(v -> v)));
                    assertThat(vector.allFalse(), equalTo(Arrays.stream(bools).allMatch(v -> v == false)));
                } else if (value) {
                    assertTrue(vector.allTrue());
                    assertFalse(vector.allFalse());
                } else {
                    assertFalse(vector.allTrue());
                    assertTrue(vector.allFalse());
                }
                assertBlock(block, expected);
            }
        }
    }

    public void testCopyTo() {
        int positionCount = randomIntBetween(1, 1000);
        try (BooleanVector.Builder builder = blockFactory().newBooleanVectorBuilder(positionCount)) {
            for (int i = 0; i < positionCount; i++) {
                builder.appendBoolean(randomBoolean());
            }
            try (BooleanVector vector = builder.build()) {
                int srcPosition = randomIntBetween(0, positionCount - 1);
                int length = randomIntBetween(0, positionCount - srcPosition);
                int dstPosition = randomIntBetween(0, 10);
                boolean sentinel = randomBoolean();
                boolean[] dst = new boolean[dstPosition + length + randomIntBetween(0, 10)];
                Arrays.fill(dst, sentinel);
                vector.copyTo(srcPosition, dst, dstPosition, length);
                for (int i = 0; i < length; i++) {
                    assertThat(dst[dstPosition + i], equalTo(vector.getBoolean(srcPosition + i)));
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
        try (BooleanBlock emptyBlock = blockFactory().newBooleanVectorBuilder(0).build().asBlock()) {
            assertSerializationAtSupportedVersions(emptyBlock, List.of());
        }
        try (BooleanVector toFilter = blockFactory().newBooleanVectorBuilder(1).appendBoolean(randomBoolean()).build()) {
            // filter() returns a new vector; asBlock() owns that filtered vector, not toFilter.
            try (BooleanBlock filtered = toFilter.filter(false).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of());
            }
        }
        try (
            BooleanVector toFilter = blockFactory().newBooleanVectorBuilder(1)
                .appendBoolean(randomBoolean())
                .appendBoolean(randomBoolean())
                .build()
        ) {
            boolean expected = toFilter.getBoolean(0);
            try (BooleanBlock filtered = toFilter.filter(false, 0).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of(List.of(expected)));
            }
        }
    }

    @Override
    protected void assertAdditionalInvariants(BooleanBlock block, List<List<Boolean>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : Byte.BYTES));
        if (block.asVector() != null && block.asVector().isConstant() && expected.isEmpty() == false) {
            boolean value = expected.get(0).get(0);
            if (value) {
                assertTrue(block.asVector().allTrue());
                assertFalse(block.asVector().allFalse());
            } else {
                assertFalse(block.asVector().allTrue());
                assertTrue(block.asVector().allFalse());
            }
        }
    }

    private static void assertBooleanVector(BooleanVector vector, List<Boolean> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        assertThat(vector.valueMaxByteSize(), equalTo(Byte.BYTES));
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getBoolean(p), equalTo(expected.get(p)));
        }
    }
}
