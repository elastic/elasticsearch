/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.TransportVersion;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DoubleRangeBlockTests extends BlockTestCase<DoubleRangeBlock, DoubleRangeBlock.Builder, DoubleRangeBlockBuilder.DoubleRange> {

    @Override
    protected DoubleRangeBlock.Builder createBuilder(BlockFactory blockFactory, int estimatedSize) {
        return blockFactory.newDoubleRangeBlockBuilder(estimatedSize);
    }

    @Override
    protected void appendNull(DoubleRangeBlock.Builder builder) {
        builder.appendNull();
    }

    @Override
    protected void appendSingle(DoubleRangeBlock.Builder builder, DoubleRangeBlockBuilder.DoubleRange value) {
        builder.appendDoubleRange(value);
    }

    @Override
    protected void appendMultivalued(DoubleRangeBlock.Builder builder, List<DoubleRangeBlockBuilder.DoubleRange> values) {
        builder.beginPositionEntry();
        for (DoubleRangeBlockBuilder.DoubleRange value : values) {
            builder.appendDoubleRange(value);
        }
        builder.endPositionEntry();
    }

    @Override
    protected DoubleRangeBlock build(DoubleRangeBlock.Builder builder) {
        return builder.build();
    }

    @Override
    protected List<DoubleRangeBlockBuilder.DoubleRange> valuesAt(DoubleRangeBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int start = block.getFirstValueIndex(position);
        int end = start + block.getValueCount(position);
        List<DoubleRangeBlockBuilder.DoubleRange> values = new ArrayList<>(end - start);
        DoubleRangeBlockBuilder.DoubleRange scratch = new DoubleRangeBlockBuilder.DoubleRange();
        for (int i = start; i < end; i++) {
            block.getDoubleRange(i, scratch);
            values.add(new DoubleRangeBlockBuilder.DoubleRange(scratch.from(), scratch.to()));
        }
        return values;
    }

    @Override
    protected DoubleRangeBlockBuilder.DoubleRange randomValue() {
        return new DoubleRangeBlockBuilder.DoubleRange(randomDouble(), randomDouble());
    }

    @Override
    protected ElementType expectedElementType() {
        return ElementType.DOUBLE_RANGE;
    }

    @Override
    protected TransportVersion minimumSerializationTransportVersion() {
        return TransportVersion.fromName("esql_double_range_value_holder");
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
    protected boolean supportsConstantBlockFactory() {
        return false;
    }

    @Override
    protected void assertSingleValueBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertDenseVectorBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertArrayBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertBigArrayVectorBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertBigArrayBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertEmptyBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertAllNullBlockRepresentation(DoubleRangeBlock block) {
        assertThat(block, instanceOf(DoubleRangeArrayBlock.class));
    }

    @Override
    protected void assertAdditionalInvariants(DoubleRangeBlock block, List<List<DoubleRangeBlockBuilder.DoubleRange>> expected) {
        assertThat(block.valueMaxByteSize(), equalTo(block instanceof ConstantNullBlock ? 0 : 2 * Double.BYTES));
    }

    public void testGetDoubleRangeMutatesScratchAcrossValueIndices() {
        try (DoubleRangeBlockBuilder builder = blockFactory().newDoubleRangeBlockBuilder(3)) {
            builder.appendDoubleRange(10.0, 20.0);
            builder.beginPositionEntry();
            builder.appendDoubleRange(30.0, 40.0);
            builder.appendDoubleRange(50.0, 60.0);
            builder.appendDoubleRange(70.0, 80.0);
            builder.endPositionEntry();
            builder.appendDoubleRange(100.0, 200.0);

            try (DoubleRangeBlock block = builder.build()) {
                DoubleRangeBlockBuilder.DoubleRange scratch = new DoubleRangeBlockBuilder.DoubleRange();

                block.getDoubleRange(block.getFirstValueIndex(0), scratch);
                assertThat(scratch.from(), equalTo(10.0));
                assertThat(scratch.to(), equalTo(20.0));

                int firstMv = block.getFirstValueIndex(1);
                block.getDoubleRange(firstMv, scratch);
                assertThat(scratch.from(), equalTo(30.0));
                assertThat(scratch.to(), equalTo(40.0));
                block.getDoubleRange(firstMv + 1, scratch);
                assertThat(scratch.from(), equalTo(50.0));
                assertThat(scratch.to(), equalTo(60.0));
                block.getDoubleRange(firstMv + 2, scratch);
                assertThat(scratch.from(), equalTo(70.0));
                assertThat(scratch.to(), equalTo(80.0));

                block.getDoubleRange(block.getFirstValueIndex(2), scratch);
                assertThat(scratch.from(), equalTo(100.0));
                assertThat(scratch.to(), equalTo(200.0));
            }
        }
    }

    public void testDoubleRangeValueSemantics() {
        var a = new DoubleRangeBlockBuilder.DoubleRange(1.5, 2.5);
        var b = new DoubleRangeBlockBuilder.DoubleRange(1.5, 2.5);
        var c = new DoubleRangeBlockBuilder.DoubleRange(1.5, 3.5);

        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
        assertNotEquals(a, c);
        assertThat(a.toString(), equalTo("DoubleRange[from=1.5, to=2.5]"));

        var ret = a.reset(7.0, 9.0);
        assertSame(ret, a);
        assertThat(a.from(), equalTo(7.0));
        assertThat(a.to(), equalTo(9.0));
        assertNotEquals(a, b);
    }
}
