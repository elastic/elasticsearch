/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class BytesRefBlockTests extends BlockTestCase<BytesRefBlock, BytesRefBlock.Builder, BytesRef> {
    @Override
    protected BytesRefBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newBytesRefBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(BytesRefBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(BytesRefBlock.Builder builder, BytesRef value) {
        builder.appendBytesRef(value);
    }

    @Override
    protected void appendMultivalued(BytesRefBlock.Builder builder, List<BytesRef> values) {
        builder.beginPositionEntry();
        for (BytesRef value : values) {
            builder.appendBytesRef(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected BytesRefBlock build(BytesRefBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<BytesRef> valuesAt(BytesRefBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<BytesRef> values = new ArrayList<>(end - start);
        BytesRef scratch = new BytesRef();
        for (int i = start; i < end; i++) {
            values.add(BytesRef.deepCopyOf(block.getBytesRef(i, scratch)));
        }
        return values;
    }

    @Override
    protected BytesRef randomValue() {
        return new BytesRef(randomByteArrayOfLength(between(1, 20)));
    }

    @Override
    protected boolean positionHasValue(BytesRefBlock block, int position, BytesRef value) {
        return block.hasValue(position, value, new BytesRef());
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    protected boolean supportsConstantBlockFactory() {
        return true;
    }

    @Override
    protected BytesRefBlock createConstantBlock(BlockFactory blockFactory, BytesRef value, int positions) {
        return blockFactory.newConstantBytesRefBlockWith(value, positions);
    }

    @Override
    protected void assertSingleValueBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantBytesRefVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefVectorBlock.class));
        assertThat(block.asVector(), instanceOf(BytesRefArrayVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(BytesRefBlock block) {
        // BytesRef values are always stored in a BytesRefArray; there is no separate big-array vector type.
        assertThat(block, instanceOf(BytesRefVectorBlock.class));
        assertThat(block.asVector(), instanceOf(BytesRefArrayVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefVectorBlock.class));
        assertThat(block.asVector(), instanceOf(BytesRefArrayVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefArrayBlock.class));
    }

    @Override
    protected void assertConstantBlockFactoryRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(BytesRefVectorBlock.class));
        assertThat(block.asVector(), instanceOf(ConstantBytesRefVector.class));
    }

    @Override
    protected void assertConstantInRangeLookupBlockRepresentation(Block block) {
        assertThat(block.asVector(), instanceOf(ConstantBytesRefVector.class));
    }

    @Override
    protected void assertConstantOutOfRangeLookupBlockRepresentation(Block block) {
        assertThat(block, instanceOf(ConstantNullBlock.class));
    }

    public void testEmptyArrayBlockFactory() {
        try (
            BytesRefBlock block = blockFactory().newBytesRefArrayBlock(
                new BytesRefArray(0, blockFactory().bigArrays()),
                0,
                new int[] { 0 },
                new BitSet(),
                Block.MvOrdering.UNORDERED
            )
        ) {
            assertThat(block, instanceOf(BytesRefArrayBlock.class));
            assertBlock(block, List.of());
        }
    }

    public void testEmptyArrayVectorFactory() {
        BytesRefVector vector = blockFactory().newBytesRefArrayVector(new BytesRefArray(0, blockFactory().bigArrays()), 0);
        try (BytesRefBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(BytesRefVectorBlock.class));
            assertThat(block.asVector(), instanceOf(BytesRefArrayVector.class));
            assertBytesRefVector(block.asVector(), List.of());
            assertBlock(block, List.of());
        }
    }

    public void testArrayVectorFactory() {
        int positionCount = randomIntBetween(1, 1024);
        BytesRefArray array = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
        List<BytesRef> expectedVector = new ArrayList<>(positionCount);
        List<List<BytesRef>> expectedBlock = new ArrayList<>(positionCount);
        for (int p = 0; p < positionCount; p++) {
            BytesRef value = randomValue();
            array.append(value);
            expectedVector.add(BytesRef.deepCopyOf(value));
            expectedBlock.add(List.of(BytesRef.deepCopyOf(value)));
        }
        BytesRefVector vector = blockFactory().newBytesRefArrayVector(array, positionCount);
        try (BytesRefBlock block = vector.asBlock()) {
            assertThat(block, instanceOf(BytesRefVectorBlock.class));
            assertThat(block.asVector(), instanceOf(BytesRefArrayVector.class));
            assertBytesRefVector(vector, expectedVector);
            assertBlock(block, expectedBlock);
        }
    }

    public void testEmptyVectorBuilder() {
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(0)) {
            BytesRefVector vector = builder.build();
            try (BytesRefBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(BytesRefVectorBlock.class));
                assertThat(block.asVector(), instanceOf(BytesRefArrayVector.class));
                assertBytesRefVector(vector, List.of());
                assertBlock(block, List.of());
            }
        }
    }

    public void testVectorBuilder() {
        int positionCount = randomIntBetween(1, 1024);
        List<BytesRef> expectedVector = new ArrayList<>(positionCount);
        List<List<BytesRef>> expectedBlock = new ArrayList<>(positionCount);
        try (BytesRefVector.Builder builder = blockFactory().newBytesRefVectorBuilder(randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                BytesRef value = randomValue();
                builder.appendBytesRef(value);
                expectedVector.add(BytesRef.deepCopyOf(value));
                expectedBlock.add(List.of(BytesRef.deepCopyOf(value)));
            }
            BytesRefVector vector = builder.build();
            try (BytesRefBlock block = vector.asBlock()) {
                assertThat(block, instanceOf(BytesRefVectorBlock.class));
                assertBytesRefVector(vector, expectedVector);
                assertBlock(block, expectedBlock);
            }
        }
    }

    public void testDenseBytesRefBlockWithOffsetAndLength() {
        assertDenseBytesRefBlock(() -> new BytesRef(randomByteArrayOfLength(between(1, 20))), true, b -> {});
    }

    public void testBytesRefBlockOnGeoPoints() {
        assertDenseBytesRefBlock(
            () -> new BytesRef(WellKnownBinary.toWKB(GeometryTestUtils.randomPoint(), ByteOrder.LITTLE_ENDIAN)),
            false,
            wkb -> WellKnownText.fromWKB(wkb.bytes, wkb.offset, wkb.length)
        );
    }

    public void testBytesRefBlockOnCartesianPoints() {
        assertDenseBytesRefBlock(
            () -> new BytesRef(WellKnownBinary.toWKB(ShapeTestUtils.randomPoint(), ByteOrder.LITTLE_ENDIAN)),
            false,
            wkb -> WellKnownText.fromWKB(wkb.bytes, wkb.offset, wkb.length)
        );
    }

    public void testAppendPagedBytesCursorVector() {
        byte[] data = randomByteArrayOfLength(between(1, 100));
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(data, 0, data.length);
        try (
            BytesRefVector.Builder cursorBuilder = blockFactory().newBytesRefVectorBuilder(1);
            BytesRefVector.Builder bytesRefBuilder = blockFactory().newBytesRefVectorBuilder(1)
        ) {
            cursorBuilder.append(cursor);
            bytesRefBuilder.appendBytesRef(new BytesRef(data));
            try (BytesRefVector v1 = cursorBuilder.build(); BytesRefVector v2 = bytesRefBuilder.build()) {
                assertThat(BytesRefVector.equals(v1, v2), is(true));
            }
        }
    }

    public void testAppendPagedBytesCursorBlock() {
        byte[] data = randomByteArrayOfLength(between(1, 100));
        PagedBytesCursor cursor = new PagedBytesCursor();
        cursor.init(data, 0, data.length);
        try (
            BytesRefBlock.Builder cursorBuilder = blockFactory().newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder bytesRefBuilder = blockFactory().newBytesRefBlockBuilder(1)
        ) {
            cursorBuilder.append(cursor);
            bytesRefBuilder.appendBytesRef(new BytesRef(data));
            try (BytesRefBlock b1 = cursorBuilder.build(); BytesRefBlock b2 = bytesRefBuilder.build()) {
                assertThat(BytesRefBlock.equals(b1, b2), is(true));
            }
        }
    }

    public void testConstantBytesRefCopiesBytes() {
        int positionCount = randomIntBetween(1, 16 * 1024);
        BytesRef value = new BytesRef(randomByteArrayOfLength(between(1, 20)));
        BytesRef originalValue = BytesRef.deepCopyOf(value);
        try (BytesRefBlock block = createConstantBlock(blockFactory(), value, positionCount)) {
            for (int b = 0; b < value.length; b++) {
                if (randomBoolean()) {
                    value.bytes[b] = randomByte();
                }
            }
            assertBlock(block, repeat(originalValue, positionCount));

            var v0 = block.getBytesRef(randomInt(positionCount - 1), new BytesRef());
            var v1 = block.getBytesRef(randomInt(positionCount - 1), new BytesRef());
            v1.length = 0;
            var v2 = block.getBytesRef(randomInt(positionCount - 1), new BytesRef());
            assertThat(v2, equalTo(v0));
        }
    }

    public void testDirectBytesRefVector() {
        byte[] bytes = new byte[10 * 1024];
        int positionCount = between(100, 500);
        int[] offsets = new int[positionCount + 1];
        int offset = 0;
        try (var builder = blockFactory().newBytesRefVectorBuilder(positionCount)) {
            offsets[0] = offset;
            for (int p = 0; p < positionCount; p++) {
                byte[] values = randomByteArrayOfLength(between(5, 10));
                System.arraycopy(values, 0, bytes, offset, values.length);
                builder.appendBytesRef(new BytesRef(values));
                offset += values.length;
                offsets[p + 1] = offset;
            }
            try (var vector1 = builder.build(); var vector2 = blockFactory().newDirectBytesRefVector(bytes, offsets, positionCount)) {
                BytesRef scratch1 = new BytesRef();
                BytesRef scratch2 = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    assertThat(vector1.getBytesRef(p, scratch1), equalTo(vector2.getBytesRef(p, scratch2)));
                }
            }
        }
    }

    public void testSparseBytesRefWithOffsetAndLength() {
        int positionCount = randomIntBetween(2, 1024);
        List<List<BytesRef>> expected = new ArrayList<>(positionCount);
        try (BytesRefBlock.Builder builder = createBuilder(blockFactory(), randomIntBetween(0, positionCount))) {
            for (int p = 0; p < positionCount; p++) {
                if (randomBoolean()) {
                    appendNull(builder);
                    expected.add(null);
                } else {
                    BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
                    if (bytesRef.length > 0 && randomBoolean()) {
                        bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                        bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
                    }
                    appendSingle(builder, randomBoolean() ? bytesRef : BytesRef.deepCopyOf(bytesRef));
                    expected.add(List.of(BytesRef.deepCopyOf(bytesRef)));
                }
            }
            try (BytesRefBlock block = build(builder)) {
                assertBlock(block, expected);
            }
        }
    }

    public void testVectorFactorySerialization() throws IOException {
        // asBlock() takes ownership of the vector — close only the block.
        try (BytesRefBlock emptyBlock = blockFactory().newBytesRefVectorBuilder(0).build().asBlock()) {
            assertSerializationAtSupportedVersions(emptyBlock, List.of());
        }
        try (BytesRefVector toFilter = blockFactory().newBytesRefVectorBuilder(0).appendBytesRef(randomValue()).build()) {
            // filter() returns a new vector; asBlock() owns that filtered vector, not toFilter.
            try (BytesRefBlock filtered = toFilter.filter(false).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of());
            }
        }
        BytesRef first = randomValue();
        BytesRef second = randomValue();
        try (BytesRefVector toFilter = blockFactory().newBytesRefVectorBuilder(0).appendBytesRef(first).appendBytesRef(second).build()) {
            try (BytesRefBlock filtered = toFilter.filter(false, 0).asBlock()) {
                assertSerializationAtSupportedVersions(filtered, List.of(List.of(BytesRef.deepCopyOf(first))));
            }
        }
    }

    @Override
    protected void assertAdditionalInvariants(BytesRefBlock block, List<List<BytesRef>> expected) {
        int expectedMax = 0;
        for (List<BytesRef> values : expected) {
            if (values == null) {
                continue;
            }
            for (BytesRef value : values) {
                expectedMax = Math.max(expectedMax, value.length);
            }
        }
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : expectedMax));
    }

    @FunctionalInterface
    private interface BytesRefAssertions {
        void accept(BytesRef value) throws Exception;
    }

    private void assertDenseBytesRefBlock(Supplier<BytesRef> supplier, boolean chomp, BytesRefAssertions assertions) {
        int positionCount = randomIntBetween(1, 16 * 1024);
        List<List<BytesRef>> expected = new ArrayList<>(positionCount);
        BytesRef[] values = new BytesRef[positionCount];
        for (int i = 0; i < positionCount; i++) {
            BytesRef bytesRef = supplier.get();
            if (chomp && bytesRef.length > 0 && randomBoolean()) {
                bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
            }
            values[i] = bytesRef;
            expected.add(List.of(BytesRef.deepCopyOf(bytesRef)));
        }

        BytesRefBlock block;
        if (randomBoolean()) {
            try (BytesRefBlock.Builder builder = createBuilder(blockFactory(), randomIntBetween(0, positionCount))) {
                Arrays.stream(values).map(obj -> randomBoolean() ? obj : BytesRef.deepCopyOf(obj)).forEach(builder::appendBytesRef);
                block = builder.build();
            }
        } else {
            BytesRefArray array = new BytesRefArray(0, BigArrays.NON_RECYCLING_INSTANCE);
            Arrays.stream(values).forEach(array::append);
            block = blockFactory().newBytesRefArrayVector(array, positionCount).asBlock();
        }

        try (block) {
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < positionCount; i++) {
                int pos = randomIntBetween(0, positionCount - 1);
                scratch = block.getBytesRef(pos, scratch);
                assertThat(scratch, equalTo(values[pos]));
                try {
                    assertions.accept(scratch);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            assertBlock(block, expected);
        }
    }

    private static void assertBytesRefVector(BytesRefVector vector, List<BytesRef> expected) {
        assertThat(vector.getPositionCount(), equalTo(expected.size()));
        BytesRef scratch = new BytesRef();
        for (int p = 0; p < expected.size(); p++) {
            assertThat(vector.getBytesRef(p, scratch), equalTo(expected.get(p)));
        }
        assertThat(vector.valueMaxByteSize(), equalTo(expected.stream().mapToInt(v -> v.length).max().orElse(0)));
    }
}
