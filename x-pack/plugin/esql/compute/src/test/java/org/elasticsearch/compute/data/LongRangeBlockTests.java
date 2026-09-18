/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.RangeFieldMapper;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class LongRangeBlockTests extends BlockTestCase<LongRangeBlock, LongRangeBlock.Builder, LongRangeBlockBuilder.LongRange> {
    @Override
    protected LongRangeBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newLongRangeBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(LongRangeBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(LongRangeBlock.Builder builder, LongRangeBlockBuilder.LongRange value) {
        builder.appendLongRange(value);
    }

    @Override
    protected void appendMultivalued(LongRangeBlock.Builder builder, List<LongRangeBlockBuilder.LongRange> values) {
        builder.beginPositionEntry();
        for (LongRangeBlockBuilder.LongRange value : values) {
            builder.appendLongRange(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected LongRangeBlock build(LongRangeBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<LongRangeBlockBuilder.LongRange> valuesAt(LongRangeBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<LongRangeBlockBuilder.LongRange> values = new ArrayList<>(end - start);
        LongRangeBlockBuilder.LongRange scratch = new LongRangeBlockBuilder.LongRange();
        for (int i = start; i < end; i++) {
            block.getLongRange(i, scratch);
            values.add(new LongRangeBlockBuilder.LongRange(scratch.from(), scratch.to()));
        }
        return values;
    }

    @Override
    protected LongRangeBlockBuilder.LongRange randomValue() {
        long from = randomLong();
        long to = randomValueOtherThan(from, () -> randomLong());
        return new LongRangeBlockBuilder.LongRange(Math.min(from, to), Math.max(from, to));
    }

    @Override
    protected boolean positionHasValue(LongRangeBlock block, int position, LongRangeBlockBuilder.LongRange value) {
        return block.hasValue(position, value, new LongRangeBlockBuilder.LongRange());
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.LONG_RANGE;
    }

    @Override
    protected TransportVersion minimumSerializationTransportVersion() {
        return RangeFieldMapper.ESQL_LONG_RANGES;
    }

    @Override
    protected boolean supportsLookup() {
        return false;
    }

    @Override
    protected boolean supportsDenseVector() {
        return false;
    }

    @Override
    protected boolean supportsConfigurableMvOrdering() {
        return false;
    }

    @Override
    protected void assertSingleValueBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(LongRangeBlock block) {
        assertThat(block, instanceOf(LongRangeArrayBlock.class));
    }

    @Override
    protected void assertAdditionalInvariants(LongRangeBlock block, List<List<LongRangeBlockBuilder.LongRange>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : Long.BYTES * 2));
    }

    public void testGetLongRangeMutatesScratchAcrossValueIndices() {
        try (LongRangeBlockBuilder builder = blockFactory().newLongRangeBlockBuilder(3)) {
            // Position 0: single-valued [10, 20)
            builder.appendLongRange(10L, 20L);
            // Position 1: multi-valued [30, 40), [50, 60), [70, 80)
            builder.from().beginPositionEntry();
            builder.from().appendLong(30L);
            builder.from().appendLong(50L);
            builder.from().appendLong(70L);
            builder.from().endPositionEntry();
            builder.to().beginPositionEntry();
            builder.to().appendLong(40L);
            builder.to().appendLong(60L);
            builder.to().appendLong(80L);
            builder.to().endPositionEntry();
            // Position 2: single-valued [100, 200)
            builder.appendLongRange(100L, 200L);

            try (LongRangeBlock block = builder.build()) {
                LongRangeBlockBuilder.LongRange scratch = new LongRangeBlockBuilder.LongRange();

                LongRangeBlockBuilder.LongRange got = block.getLongRange(block.getFirstValueIndex(0), scratch);
                assertThat("accessor must reuse the supplied scratch", got, sameInstance(scratch));
                assertThat(got.from(), equalTo(10L));
                assertThat(got.to(), equalTo(20L));

                // Multi-valued position: read each value-index in turn and check the scratch is overwritten.
                int firstMv = block.getFirstValueIndex(1);
                got = block.getLongRange(firstMv, scratch);
                assertThat(got.from(), equalTo(30L));
                assertThat(got.to(), equalTo(40L));
                got = block.getLongRange(firstMv + 1, scratch);
                assertThat(got.from(), equalTo(50L));
                assertThat(got.to(), equalTo(60L));
                got = block.getLongRange(firstMv + 2, scratch);
                assertThat(got.from(), equalTo(70L));
                assertThat(got.to(), equalTo(80L));

                got = block.getLongRange(block.getFirstValueIndex(2), scratch);
                assertThat(got.from(), equalTo(100L));
                assertThat(got.to(), equalTo(200L));
            }
        }
    }

    public void testLongRangeValueSemantics() {
        var a = new LongRangeBlockBuilder.LongRange(1L, 2L);
        var b = new LongRangeBlockBuilder.LongRange(1L, 2L);
        var c = new LongRangeBlockBuilder.LongRange(1L, 3L);

        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
        assertThat(a, not(equalTo(c)));
        assertThat(a.toString(), equalTo("LongRange[from=1, to=2]"));

        var ret = a.reset(7L, 9L);
        assertThat(ret, sameInstance(a));
        assertThat(a.from(), equalTo(7L));
        assertThat(a.to(), equalTo(9L));
        assertThat(a, not(equalTo(b)));
    }

    public void testLookupUnsupported() {
        try (LongRangeBlock block = buildBlock(blockFactory(), List.of(List.of(randomValue()))); IntBlock positions = positions()) {
            UnsupportedOperationException e = expectThrows(
                UnsupportedOperationException.class,
                () -> block.lookup(positions, ByteSizeValue.ofKb(100))
            );
            assertThat(e.getMessage(), equalTo("can't lookup values from LongRangeBlock"));
        }
    }

    private IntBlock positions() {
        try (IntBlock.Builder builder = blockFactory().newIntBlockBuilder(1)) {
            builder.appendInt(0);
            return builder.build();
        }
    }
}
