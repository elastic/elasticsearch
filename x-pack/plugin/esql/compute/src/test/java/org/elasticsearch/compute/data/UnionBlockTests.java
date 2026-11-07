/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.EnumSerializationTestUtils.assertEnumSerialization;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * The union block is single-valued in this increment. {@link AbstractDelegatingCompoundBlock} refuses
 * filter, slice, keep-mask and expand on a multi-valued compound block, as it does for tdigest and
 * exponential histogram, so {@link #testMultivaluedFilterIsRefused} pins that boundary rather than
 * leaving it undiscovered.
 */
public class UnionBlockTests extends ComputeTestCase {

    private static final List<ElementType> LEGS = List.of(ElementType.LONG, ElementType.BYTES_REF, ElementType.BOOLEAN);
    private static final int LONG_LEG = 0;
    private static final int BYTES_LEG = 1;
    private static final int BOOL_LEG = 2;

    public void testTagsAndLegValuesReadBack() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory)) {
            assertThat(block.getPositionCount(), equalTo(4));
            assertThat(block.elementType(), equalTo(ElementType.UNION));
            assertThat(block.legCount(), equalTo(3));
            assertThat(block.legElementTypes(), equalTo(LEGS));

            assertThat(block.tag(0), equalTo(LONG_LEG));
            assertThat(((LongBlock) block.legBlock(LONG_LEG)).getLong(0), equalTo(7L));

            assertThat(block.tag(1), equalTo(BYTES_LEG));
            assertThat(bytesAt(block, BYTES_LEG, 1), equalTo("second"));

            assertTrue(block.isNull(2));
            assertThat(block.tag(2), equalTo(UnionBlock.NO_TAG));

            assertThat(block.tag(3), equalTo(BOOL_LEG));
            assertThat(((BooleanBlock) block.legBlock(BOOL_LEG)).getBoolean(3), equalTo(true));
        }
    }

    /**
     * The defining invariant: a present value lives in exactly the leg its tag names, and every other leg is
     * null at that position.
     */
    public void testOnlyTheTaggedLegHoldsAValue() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory)) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                int tag = block.tag(position);
                for (int leg = 0; leg < block.legCount(); leg++) {
                    assertThat(
                        "leg " + leg + " at position " + position + " tagged " + tag,
                        block.legBlock(leg).isNull(position),
                        equalTo(leg != tag)
                    );
                }
            }
        }
    }

    public void testNullEverywhere() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 3)) {
            builder.appendNull();
            builder.appendNull();
            builder.appendNull();
            try (UnionBlock block = builder.build()) {
                assertTrue(block.areAllValuesNull());
                assertTrue(block.mayHaveNulls());
                for (int position = 0; position < 3; position++) {
                    assertTrue(block.isNull(position));
                    assertThat(block.tag(position), equalTo(UnionBlock.NO_TAG));
                }
            }
        }
    }

    public void testSerializationRoundTrip() throws IOException {
        BlockFactory blockFactory = blockFactory();
        UnionBlock block = buildSample(blockFactory);
        Block deserialized = serializationRoundTrip(block);
        assertThat(deserialized, equalTo(block));
        assertThat(deserialized, instanceOf(UnionArrayBlock.class));
        assertThat(((UnionBlock) deserialized).legElementTypes(), equalTo(LEGS));
        Releasables.close(block, deserialized);
    }

    /**
     * A leg built through the builder keeps its own element type even when every one of its positions is null,
     * so the type is recoverable from the leg block on that path.
     */
    public void testALegHoldingNoValuesKeepsItsElementType() throws IOException {
        BlockFactory blockFactory = blockFactory();
        UnionBlock block;
        try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 2)) {
            appendLong(builder, 42L);
            builder.appendNull();
            block = builder.build();
        }
        assertThat(block.legBlock(BYTES_LEG).elementType(), equalTo(ElementType.BYTES_REF));
        assertTrue(block.legBlock(BYTES_LEG).areAllValuesNull());

        Block deserialized = serializationRoundTrip(block);
        assertThat(((UnionBlock) deserialized).legElementTypes(), equalTo(LEGS));
        assertThat(deserialized, equalTo(block));
        Releasables.close(block, deserialized);
    }

    /**
     * A leg can also arrive as a {@link ConstantNullBlock}, whose element type is {@link ElementType#NULL}
     * rather than the leg's, which is what happens when a union is assembled from per-leg blocks and one leg
     * matched no row at all. The declared type cannot be recovered from such a leg, so the block writes the
     * declared types itself and this asserts they survive the round trip.
     */
    public void testSerializationKeepsTheTypeOfAConstantNullLeg() throws IOException {
        BlockFactory blockFactory = blockFactory();
        UnionArrayBlock block;
        try (IntBlock.Builder tags = blockFactory.newIntBlockBuilder(2); LongBlock.Builder longLeg = blockFactory.newLongBlockBuilder(2)) {
            tags.appendInt(LONG_LEG);
            tags.appendNull();
            longLeg.appendLong(42L);
            longLeg.appendNull();
            block = new UnionArrayBlock(
                tags.build(),
                List.of(longLeg.build(), blockFactory.newConstantNullBlock(2), blockFactory.newConstantNullBlock(2)),
                LEGS,
                2,
                null
            );
        }
        assertThat(block.legBlock(BYTES_LEG), instanceOf(ConstantNullBlock.class));
        assertThat(block.legBlock(BYTES_LEG).elementType(), equalTo(ElementType.NULL));

        Block deserialized = serializationRoundTrip(block);
        UnionBlock asUnion = (UnionBlock) deserialized;
        assertThat(asUnion.legElementTypes(), equalTo(LEGS));
        assertThat(asUnion.legElementType(BYTES_LEG), equalTo(ElementType.BYTES_REF));
        assertThat(asUnion.tag(0), equalTo(LONG_LEG));
        assertThat(asUnion.tag(1), equalTo(UnionBlock.NO_TAG));
        assertThat(deserialized, equalTo(block));
        Releasables.close(block, deserialized);
    }

    public void testFilterKeepsTagsAlignedWithValues() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory)) {
            // Drop the long at position 0, keeping the bytes_ref, the null and the boolean.
            try (UnionBlock filtered = (UnionBlock) block.filter(false, new int[] { 1, 2, 3 }, 0, 3)) {
                assertThat(filtered.getPositionCount(), equalTo(3));
                assertThat(filtered.legElementTypes(), equalTo(LEGS));
                assertThat(filtered.tag(0), equalTo(BYTES_LEG));
                assertThat(bytesAt(filtered, BYTES_LEG, 0), equalTo("second"));
                assertThat(filtered.tag(1), equalTo(UnionBlock.NO_TAG));
                assertThat(filtered.tag(2), equalTo(BOOL_LEG));
                assertThat(((BooleanBlock) filtered.legBlock(BOOL_LEG)).getBoolean(2), equalTo(true));
                assertTrue("the long leg kept no values", filtered.legBlock(LONG_LEG).areAllValuesNull());
            }
        }
    }

    public void testSliceKeepsTagsAlignedWithValues() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory)) {
            try (UnionBlock sliced = (UnionBlock) block.slice(1, 3)) {
                assertThat(sliced.getPositionCount(), equalTo(2));
                assertThat(sliced.tag(0), equalTo(BYTES_LEG));
                assertThat(bytesAt(sliced, BYTES_LEG, 0), equalTo("second"));
                assertThat(sliced.tag(1), equalTo(UnionBlock.NO_TAG));
            }
        }
    }

    public void testKeepMaskKeepsTagsAlignedWithValues() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory); BooleanVector.Builder mask = blockFactory.newBooleanVectorBuilder(4)) {
            mask.appendBoolean(true);
            mask.appendBoolean(false);
            mask.appendBoolean(false);
            mask.appendBoolean(true);
            try (BooleanVector maskVector = mask.build(); UnionBlock masked = (UnionBlock) block.keepMask(maskVector)) {
                assertThat(masked.getPositionCount(), equalTo(4));
                assertThat(masked.tag(0), equalTo(LONG_LEG));
                assertThat(masked.tag(1), equalTo(UnionBlock.NO_TAG));
                assertThat(masked.tag(2), equalTo(UnionBlock.NO_TAG));
                assertThat(masked.tag(3), equalTo(BOOL_LEG));
            }
        }
    }

    public void testDeepCopyPreservesEverything() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory); UnionBlock copy = (UnionBlock) block.deepCopy(blockFactory)) {
            assertThat(copy, equalTo(block));
            assertThat(copy.legElementTypes(), equalTo(LEGS));
            assertNotSame(copy, block);
            assertNotSame(copy.legBlock(LONG_LEG), block.legBlock(LONG_LEG));
        }
    }

    public void testExpandIsTheSameBlockWhenSingleValued() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory)) {
            try (Block expanded = block.expand()) {
                assertSame(block, expanded);
            }
        }
    }

    public void testCopyFromAnotherUnionOfTheSameShape() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock source = buildSample(blockFactory); UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 4)) {
            builder.copyFrom(source, 1, 4);
            try (UnionBlock copied = builder.build()) {
                assertThat(copied.getPositionCount(), equalTo(3));
                assertThat(copied.tag(0), equalTo(BYTES_LEG));
                assertThat(bytesAt(copied, BYTES_LEG, 0), equalTo("second"));
                assertThat(copied.tag(1), equalTo(UnionBlock.NO_TAG));
                assertThat(copied.tag(2), equalTo(BOOL_LEG));
            }
        }
    }

    public void testEqualityDistinguishesTagsAndLegTypes() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock a = buildSample(blockFactory); UnionBlock b = buildSample(blockFactory)) {
            assertThat(a, equalTo(b));
            assertThat(a.hashCode(), equalTo(b.hashCode()));
        }
        // Same single value, but declared under a different set of legs.
        try (
            UnionBlock threeLegs = singleLongBlock(blockFactory, LEGS);
            UnionBlock oneLeg = singleLongBlock(blockFactory, List.of(ElementType.LONG))
        ) {
            assertThat(threeLegs, not(equalTo(oneLeg)));
        }
    }

    public void testRefCountingReleasesTheLegs() {
        BlockFactory blockFactory = blockFactory();
        UnionBlock block = buildSample(blockFactory);
        Block longLeg = block.legBlock(LONG_LEG);
        assertFalse(longLeg.isReleased());
        block.close();
        assertTrue("closing the union closes its legs", longLeg.isReleased());
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testBuilderRejectsAnEmptyLegList() {
        BlockFactory blockFactory = blockFactory();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> blockFactory.newUnionBlockBuilder(List.of(), 1));
        assertThat(e.getMessage(), equalTo("a union block needs at least one leg"));
    }

    /**
     * A leg's own block may be a {@link ConstantNullBlock} when the leg holds no values, but a leg DECLARED null could never hold
     * one: the builder refuses it where the mistake is cheap to see rather than at the first append.
     */
    public void testBuilderRejectsALegElementTypeNoLegCanHold() {
        BlockFactory blockFactory = blockFactory();
        for (ElementType elementType : ElementType.values()) {
            if (UnionBlockBuilder.canHoldLeg(elementType)) {
                continue;
            }
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> blockFactory.newUnionBlockBuilder(List.of(ElementType.LONG, elementType), 1)
            );
            assertThat(e.getMessage(), equalTo("a union leg cannot hold " + elementType));
        }
        assertThat("the refused legs allocated nothing", blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testAppendValueRejectsALegOutOfRange() {
        BlockFactory blockFactory = blockFactory();
        try (
            UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 1);
            Block source = blockFactory.newConstantLongBlockWith(1L, 1)
        ) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.appendValue(3, source, 0));
            assertThat(e.getMessage(), equalTo("leg 3 out of range for 3 legs"));
        }
    }

    public void testAppendValueRejectsTheWrongElementType() {
        BlockFactory blockFactory = blockFactory();
        try (
            UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 1);
            Block source = blockFactory.newConstantLongBlockWith(1L, 1)
        ) {
            // Leg 1 holds bytes, so a long value does not belong in it.
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.appendValue(BYTES_LEG, source, 0));
            assertThat(e.getMessage(), equalTo("leg 1 holds BYTES_REF but the value came from a LONG block"));
        }
    }

    /**
     * The source block's slot may be null, and the typed getters do not check: reading it raw would make a
     * missing value into a present, tagged zero, which no invariant on the block can catch.
     */
    public void testAppendValueRejectsANullSourcePosition() {
        BlockFactory blockFactory = blockFactory();
        try (
            UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 2);
            LongBlock.Builder source = blockFactory.newLongBlockBuilder(2)
        ) {
            source.appendLong(5L);
            source.appendNull();
            try (LongBlock sourceBlock = source.build()) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> builder.appendValue(LONG_LEG, sourceBlock, 1)
                );
                assertThat(e.getMessage(), equalTo("no value at index 1 of the source block; append a null instead"));
            }
        }
    }

    /**
     * An all-null union equals a {@link ConstantNullBlock} of the same length in both directions, so it has to
     * hash the same. The other compound blocks hash their first sub-block for exactly this reason.
     */
    public void testAnAllNullUnionHashesLikeAConstantNullBlock() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 3)) {
            builder.appendNull();
            builder.appendNull();
            builder.appendNull();
            try (UnionBlock allNull = builder.build(); Block constantNull = blockFactory.newConstantNullBlock(3)) {
                assertThat(allNull, equalTo(constantNull));
                assertThat(constantNull, equalTo((Block) allNull));
                assertThat(allNull.hashCode(), equalTo(constantNull.hashCode()));
            }
        }
    }

    public void testCopyFromRejectsADifferentLegShape() {
        BlockFactory blockFactory = blockFactory();
        try (
            UnionBlock.Builder target = blockFactory.newUnionBlockBuilder(List.of(ElementType.LONG), 1);
            UnionBlock source = buildSample(blockFactory)
        ) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> target.copyFrom(source, 0, 1));
            assertThat(e.getMessage(), equalTo("cannot copy a union of [LONG, BYTES_REF, BOOLEAN] into a union of [LONG]"));
        }
    }

    public void testConstructorRejectsMismatchedLegsAndTypes() {
        BlockFactory blockFactory = blockFactory();
        try (IntBlock tags = blockFactory.newConstantIntBlockWith(0, 1); Block leg = blockFactory.newConstantLongBlockWith(1L, 1)) {
            tags.incRef();
            leg.incRef();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new UnionArrayBlock(tags, List.of(leg), LEGS, 1, null)
            );
            assertThat(e.getMessage(), equalTo("got 1 legs but 3 leg element types"));
            tags.decRef();
            leg.decRef();
        }
    }

    public void testAConstantNullBlockReportsNoLegsAndNoTag() {
        BlockFactory blockFactory = blockFactory();
        try (Block block = blockFactory.newConstantNullBlock(3)) {
            UnionBlock asUnion = (UnionBlock) block;
            assertThat(asUnion.legCount(), equalTo(0));
            assertThat(asUnion.legElementTypes(), equalTo(List.of()));
            assertThat(asUnion.tag(0), equalTo(UnionBlock.NO_TAG));
            expectThrows(IndexOutOfBoundsException.class, () -> asUnion.legBlock(0));
            expectThrows(IndexOutOfBoundsException.class, () -> asUnion.legElementType(0));
        }
    }

    public void testLookupIsRefused() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock block = buildSample(blockFactory); IntBlock positions = blockFactory.newConstantIntBlockWith(0, 1)) {
            expectThrows(UnsupportedOperationException.class, () -> block.lookup(positions, ByteSizeValue.ofKb(1)));
        }
    }

    /**
     * Multi-valued compound blocks are refused by the shared base class, for a union exactly as for tdigest and
     * exponential histogram. This holds that line visible until multi-value lands in that base class.
     */
    public void testMultivaluedFilterIsRefused() {
        BlockFactory blockFactory = blockFactory();
        try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 1)) {
            builder.beginPositionEntry();
            appendLong(builder, 1L);
            appendLong(builder, 2L);
            builder.endPositionEntry();
            try (UnionBlock block = builder.build()) {
                assertTrue(block.mayHaveMultivaluedFields());
                assertThat(block.getValueCount(0), equalTo(2));
                assertThat(block.tag(0), equalTo(LONG_LEG));
                assertThat(block.tag(1), equalTo(LONG_LEG));
                UnsupportedOperationException e = expectThrows(
                    UnsupportedOperationException.class,
                    () -> block.filter(false, new int[] { 0 }, 0, 1)
                );
                assertThat(e.getMessage(), equalTo("Not yet implemented for multi-valued blocks"));
            }
        }
    }

    public void testBuildingUnderACrankyBreakerReleasesEverything() {
        testWithCrankyBlockFactory(blockFactory -> {
            try (UnionBlock block = buildSample(blockFactory)) {
                assertThat(block.getPositionCount(), equalTo(4));
            }
        });
    }

    public void testElementTypeSerializesByCode() {
        assertEnumSerialization(
            ElementType.class,
            ElementType.BOOLEAN,
            ElementType.INT,
            ElementType.LONG,
            ElementType.FLOAT,
            ElementType.DOUBLE,
            ElementType.NULL,
            ElementType.BYTES_REF,
            ElementType.DOC,
            ElementType.COMPOSITE,
            ElementType.UNKNOWN,
            ElementType.AGGREGATE_METRIC_DOUBLE,
            ElementType.EXPONENTIAL_HISTOGRAM,
            ElementType.TDIGEST,
            ElementType.LONG_RANGE,
            ElementType.DOUBLE_RANGE,
            ElementType.UNION
        );
    }

    /** A long at position 0, a keyword at 1, a null at 2 and a boolean at 3. */
    private UnionBlock buildSample(BlockFactory blockFactory) {
        try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(LEGS, 4)) {
            appendLong(builder, 7L);
            appendBytes(builder, "second");
            builder.appendNull();
            appendBoolean(builder, true);
            return builder.build();
        }
    }

    private UnionBlock singleLongBlock(BlockFactory blockFactory, List<ElementType> legs) {
        try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(legs, 1)) {
            appendLong(builder, 7L);
            return builder.build();
        }
    }

    private static void appendLong(UnionBlock.Builder builder, long value) {
        BlockFactory blockFactory = ((UnionBlockBuilder) builder).blockFactory;
        try (Block source = blockFactory.newConstantLongBlockWith(value, 1)) {
            builder.appendValue(LONG_LEG, source, 0);
        }
    }

    private static void appendBytes(UnionBlock.Builder builder, String value) {
        BlockFactory blockFactory = ((UnionBlockBuilder) builder).blockFactory;
        try (Block source = blockFactory.newConstantBytesRefBlockWith(new BytesRef(value), 1)) {
            builder.appendValue(BYTES_LEG, source, 0);
        }
    }

    private static void appendBoolean(UnionBlock.Builder builder, boolean value) {
        BlockFactory blockFactory = ((UnionBlockBuilder) builder).blockFactory;
        try (Block source = blockFactory.newConstantBooleanBlockWith(value, 1)) {
            builder.appendValue(BOOL_LEG, source, 0);
        }
    }

    private static String bytesAt(UnionBlock block, int leg, int valueIndex) {
        return ((BytesRefBlock) block.legBlock(leg)).getBytesRef(valueIndex, new BytesRef()).utf8ToString();
    }

    private Block serializationRoundTrip(Block block) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        Block.writeTypedBlock(block, out);
        try (BlockStreamInput input = new BlockStreamInput(out.bytes().streamInput(), blockFactory())) {
            return Block.readTypedBlock(input);
        }
    }
}
