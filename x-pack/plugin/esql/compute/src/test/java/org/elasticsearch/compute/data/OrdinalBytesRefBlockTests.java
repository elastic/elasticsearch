/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * {@link BlockTestCase} coverage for {@link OrdinalBytesRefBlock} / {@link OrdinalBytesRefVector}.
 * <p>
 * There is no production ordinal block builder — values are appended as {@link BytesRef}s and
 * dictionary-encoded in a test-only {@link Builder} so the shared suite can exercise the encoding.
 * {@code B} is {@link BytesRefBlock} (not {@link OrdinalBytesRefBlock}) because filter / insertNulls
 * demote to a regular BytesRef encoding by design.
 */
public class OrdinalBytesRefBlockTests extends BlockTestCase<BytesRefBlock, OrdinalBytesRefBlockTests.Builder, BytesRef> {
    @Override
    protected Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return new Builder(blockFactory, estimatedSize);
    }

    @Override
    protected void appendNull(Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(Builder builder, BytesRef value) {
        builder.appendBytesRef(value);
    }

    @Override
    protected void appendMultivalued(Builder builder, List<BytesRef> values) {
        builder.beginPositionEntry();
        for (BytesRef value : values) {
            builder.appendBytesRef(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected BytesRefBlock build(Builder builder) {
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
        // Small vocabulary so the ordinal dictionary stays shared — the encoding under test.
        return new BytesRef("value-" + randomIntBetween(1, 20));
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
        return false;
    }

    @Override
    protected boolean supportsReusableVectorView() {
        // asVector() allocates a new OrdinalBytesRefVector wrapper each call that shares the dictionary.
        return false;
    }

    @Override
    protected void assertSingleValueBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
        assertThat(block.asVector(), instanceOf(OrdinalBytesRefVector.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
        assertThat(block.asVector(), instanceOf(OrdinalBytesRefVector.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
        assertThat(block.asVector(), instanceOf(OrdinalBytesRefVector.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
        assertThat(block.asVector(), instanceOf(OrdinalBytesRefVector.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(BytesRefBlock block) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
        assertThat(block.asVector(), equalTo(null));
    }

    @Override
    protected void assertAdditionalInvariants(BytesRefBlock block, List<List<BytesRef>> expected) {
        assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
        OrdinalBytesRefBlock ordinal = (OrdinalBytesRefBlock) block;
        assertThat(ordinal.asOrdinals(), sameInstance(ordinal));
        assertThat(
            block.valueMaxByteSize(),
            equalTo(block instanceof ConstantNullBlock ? 0 : ordinal.getDictionaryVector().valueMaxByteSize())
        );
    }

    public void testFilterDemotesOrdinalBlock() {
        int dictSize = between(1, 10);
        int positionCount = between(1, 100);
        List<List<BytesRef>> expected = new ArrayList<>(positionCount);
        for (int i = 0; i < positionCount; i++) {
            int valueCount = randomIntBetween(0, 2);
            if (valueCount == 0) {
                expected.add(null);
            } else if (valueCount == 1) {
                expected.add(List.of(new BytesRef("value" + randomIntBetween(0, dictSize - 1))));
            } else {
                List<BytesRef> values = new ArrayList<>(valueCount);
                for (int v = 0; v < valueCount; v++) {
                    values.add(new BytesRef("value" + randomIntBetween(0, dictSize - 1)));
                }
                expected.add(values);
            }
        }
        try (BytesRefBlock block = buildBlock(blockFactory(), expected)) {
            assertThat(block, instanceOf(OrdinalBytesRefBlock.class));
            int[] masks = new int[between(1, 100)];
            for (int i = 0; i < masks.length; i++) {
                masks[i] = randomIntBetween(0, positionCount - 1);
            }
            try (BytesRefBlock filtered = block.filter(true, masks)) {
                assertThat(filtered, not(instanceOf(OrdinalBytesRefBlock.class)));
            }
            assertSliceKeepsOrdinal((OrdinalBytesRefBlock) block, 0, 0);
            if (positionCount > 1) {
                assertSliceKeepsOrdinal((OrdinalBytesRefBlock) block, 0, 1);
                assertSliceKeepsOrdinal((OrdinalBytesRefBlock) block, 1, positionCount);
            }
            try (BytesRefBlock sliced = block.slice(0, positionCount)) {
                // Full-range slice returns the same block; asVector() allocates a fresh wrapper each call.
                assertThat(sliced, sameInstance(block));
            }
        }
    }

    public void testFilterDemotesOrdinalVector() {
        int positionCount = between(1, 100);
        List<List<BytesRef>> expected = denseExpectedValues(positionCount);
        try (BytesRefBlock block = buildBlock(blockFactory(), expected)) {
            OrdinalBytesRefVector vector = block.asVector().asOrdinals();
            assertThat(vector, instanceOf(OrdinalBytesRefVector.class));
            int[] masks = new int[between(1, 100)];
            for (int i = 0; i < masks.length; i++) {
                masks[i] = randomIntBetween(0, positionCount - 1);
            }
            try (BytesRefVector filtered = vector.filter(true, masks)) {
                assertThat(filtered, not(instanceOf(OrdinalBytesRefVector.class)));
            }
            assertSliceKeepsOrdinal(vector, 0, 0);
            if (positionCount > 1) {
                assertSliceKeepsOrdinal(vector, 0, 1);
                assertSliceKeepsOrdinal(vector, 1, positionCount);
            }
            try (BytesRefVector sliced = vector.slice(0, positionCount)) {
                assertThat(sliced, sameInstance(vector));
            }
        }
    }

    public void testEqualsRegularBytesRefVector() throws Exception {
        int positions = randomIntBetween(1, 100);
        List<List<BytesRef>> expected = denseExpectedValues(positions);
        try (BytesRefBlock ordinal = buildBlock(blockFactory(), expected); BytesRefBlock regular = buildRegularBlock(expected)) {
            assertThat(ordinal, instanceOf(OrdinalBytesRefBlock.class));
            assertTrue(BytesRefBlock.equals(ordinal, regular));
            assertTrue(BytesRefVector.equals(ordinal.asVector(), regular.asVector()));
            assertSerializationAtSupportedVersions(ordinal, expected);
            assertSerializationAtSupportedVersions(regular, expected);
            for (int p = 0; p < positions; p++) {
                try (BytesRefBlock f1 = regular.filter(false, p); BytesRefBlock f2 = ordinal.filter(false, p)) {
                    assertTrue(BytesRefBlock.equals(f1, f2));
                    assertSerializationAtSupportedVersions(f1, List.of(expected.get(p)));
                    assertSerializationAtSupportedVersions(f2, List.of(expected.get(p)));
                }
            }
        }
    }

    public void testEqualsRegularBytesRefBlock() throws Exception {
        List<List<BytesRef>> expected = mixedExpectedForEquals();
        try (BytesRefBlock ordinal = buildBlock(blockFactory(), expected); BytesRefBlock regular = buildRegularBlock(expected)) {
            assertThat(ordinal, instanceOf(OrdinalBytesRefBlock.class));
            assertTrue(BytesRefBlock.equals(ordinal, regular));
            assertSerializationAtSupportedVersions(ordinal, expected);
            assertSerializationAtSupportedVersions(regular, expected);
            for (int p = 0; p < expected.size(); p++) {
                try (BytesRefBlock f1 = regular.filter(false, p); BytesRefBlock f2 = ordinal.filter(false, p)) {
                    assertTrue(BytesRefBlock.equals(f1, f2));
                    List<List<BytesRef>> filteredExpected = new ArrayList<>(1);
                    filteredExpected.add(expected.get(p));
                    assertSerializationAtSupportedVersions(f1, filteredExpected);
                    assertSerializationAtSupportedVersions(f2, filteredExpected);
                }
            }
            try (BytesRefBlock e1 = regular.expand(); BytesRefBlock e2 = ordinal.expand()) {
                assertTrue(BytesRefBlock.equals(e1, e2));
                List<List<BytesRef>> expandedExpected = new ArrayList<>();
                for (List<BytesRef> positionValues : expected) {
                    if (positionValues == null) {
                        expandedExpected.add(null);
                    } else {
                        for (BytesRef value : positionValues) {
                            expandedExpected.add(List.of(value));
                        }
                    }
                }
                assertSerializationAtSupportedVersions(e1, expandedExpected);
                assertSerializationAtSupportedVersions(e2, expandedExpected);
            }
        }
    }

    public void testIsDense() {
        assertFalse(OrdinalBytesRefBlock.isDense(9, 1));
        assertFalse(OrdinalBytesRefBlock.isDense(10, 6));
        assertTrue(OrdinalBytesRefBlock.isDense(10, 5));
        assertTrue(OrdinalBytesRefBlock.isDense(100, 10));

        // Force a shared vocabulary so the built block is dense enough for isDense().
        List<List<BytesRef>> expected = new ArrayList<>(20);
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        for (int i = 0; i < 20; i++) {
            expected.add(List.of(i % 2 == 0 ? a : b));
        }
        try (BytesRefBlock block = buildBlock(blockFactory(), expected)) {
            OrdinalBytesRefBlock ordinal = (OrdinalBytesRefBlock) block;
            assertTrue(ordinal.isDense());
            assertTrue(ordinal.asVector().isDense());
        }
    }

    public void testAsOrdinalsIdentity() {
        try (BytesRefBlock block = buildBlock(blockFactory(), List.of(List.of(randomValue()), List.of(randomValue())))) {
            OrdinalBytesRefBlock ordinal = (OrdinalBytesRefBlock) block;
            assertThat(ordinal.asOrdinals(), sameInstance(ordinal));
            OrdinalBytesRefVector vector = ordinal.asVector();
            assertThat(vector.asOrdinals(), sameInstance(vector));
        }
    }

    private List<List<BytesRef>> mixedExpectedForEquals() {
        List<List<BytesRef>> expected = new ArrayList<>();
        expected.add(List.of(randomValue()));
        expected.add(null);
        expected.add(List.of(randomValue(), randomValue()));
        expected.add(List.of(randomValue()));
        return expected;
    }

    private BytesRefBlock buildRegularBlock(List<List<BytesRef>> expected) {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(expected.size())) {
            for (List<BytesRef> values : expected) {
                if (values == null) {
                    builder.appendNull();
                } else if (values.size() == 1) {
                    builder.appendBytesRef(values.get(0));
                } else {
                    builder.beginPositionEntry();
                    for (BytesRef value : values) {
                        builder.appendBytesRef(value);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static void assertSliceKeepsOrdinal(OrdinalBytesRefBlock block, int beginInclusive, int endExclusive) {
        try (BytesRefBlock sliced = block.slice(beginInclusive, endExclusive)) {
            assertThat(sliced, instanceOf(OrdinalBytesRefBlock.class));
            assertThat(sliced.getPositionCount(), equalTo(endExclusive - beginInclusive));
        }
    }

    private static void assertSliceKeepsOrdinal(OrdinalBytesRefVector vector, int beginInclusive, int endExclusive) {
        try (BytesRefVector sliced = vector.slice(beginInclusive, endExclusive)) {
            assertThat(sliced, instanceOf(OrdinalBytesRefVector.class));
            assertThat(sliced.getPositionCount(), equalTo(endExclusive - beginInclusive));
        }
    }

    /**
     * Test-only builder that dictionary-encodes appended {@link BytesRef} values into an
     * {@link OrdinalBytesRefBlock}.
     */
    static final class Builder implements Block.Builder {
        private final BlockFactory blockFactory;
        private final IntBlock.Builder ordinals;
        private final BytesRefHash hash;

        Builder(BlockFactory blockFactory, int estimatedSize) {
            this.blockFactory = blockFactory;
            boolean success = false;
            IntBlock.Builder ordinalsBuilder = null;
            BytesRefHash bytesRefHash = null;
            try {
                ordinalsBuilder = blockFactory.newIntBlockBuilder(estimatedSize);
                bytesRefHash = new BytesRefHash(Math.max(estimatedSize, 1), blockFactory.bigArrays());
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(ordinalsBuilder, bytesRefHash);
                }
            }
            this.ordinals = ordinalsBuilder;
            this.hash = bytesRefHash;
        }

        Builder appendBytesRef(BytesRef value) {
            int ord = Math.toIntExact(hash.add(value));
            ordinals.appendInt(ord < 0 ? -1 - ord : ord);
            return this;
        }

        @Override
        public Builder appendNull() {
            ordinals.appendNull();
            return this;
        }

        @Override
        public Builder beginPositionEntry() {
            ordinals.beginPositionEntry();
            return this;
        }

        @Override
        public Builder endPositionEntry() {
            ordinals.endPositionEntry();
            return this;
        }

        @Override
        public Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
            BytesRef scratch = new BytesRef();
            for (int p = beginInclusive; p < endExclusive; p++) {
                if (bytesRefBlock.isNull(p)) {
                    appendNull();
                    continue;
                }
                int valueCount = bytesRefBlock.getValueCount(p);
                int first = bytesRefBlock.getFirstValueIndex(p);
                if (valueCount == 1) {
                    appendBytesRef(bytesRefBlock.getBytesRef(first, scratch));
                } else {
                    beginPositionEntry();
                    for (int i = 0; i < valueCount; i++) {
                        appendBytesRef(bytesRefBlock.getBytesRef(first + i, scratch));
                    }
                    endPositionEntry();
                }
            }
            return this;
        }

        @Override
        public Builder mvOrdering(Block.MvOrdering mvOrdering) {
            ordinals.mvOrdering(mvOrdering);
            return this;
        }

        @Override
        public long estimatedBytes() {
            return ordinals.estimatedBytes() + hash.ramBytesUsed();
        }

        @Override
        public BytesRefBlock build() {
            BytesRef scratch = new BytesRef();
            BytesRefVector.Builder dictionaryBuilder = null;
            IntBlock ordinalsBlock = null;
            BytesRefVector dictionary = null;
            try {
                dictionaryBuilder = blockFactory.newBytesRefVectorBuilder(Math.toIntExact(hash.size()));
                for (long i = 0; i < hash.size(); i++) {
                    dictionaryBuilder.appendBytesRef(hash.get(i, scratch));
                }
                dictionary = dictionaryBuilder.build();
                dictionaryBuilder = null;
                ordinalsBlock = ordinals.build();
                OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalsBlock, dictionary);
                ordinalsBlock = null;
                dictionary = null;
                return result;
            } finally {
                Releasables.close(dictionaryBuilder, ordinalsBlock, dictionary);
            }
        }

        @Override
        public void close() {
            Releasables.close(ordinals, hash);
        }
    }
}
