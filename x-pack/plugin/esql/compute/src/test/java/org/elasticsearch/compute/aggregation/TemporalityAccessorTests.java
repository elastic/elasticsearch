/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TemporalityAccessorTests extends ESTestCase {

    private static Function<BytesRef, Temporality> failingHandler() {
        return v -> {
            fail("should not be called");
            return Temporality.CUMULATIVE;
        };
    }

    private static BlockFactory blockFactory() {
        return TestBlockFactory.getNonBreakingInstance();
    }

    public void testConstant() {
        TemporalityAccessor accessor = TemporalityAccessor.constant(Temporality.CUMULATIVE);
        assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.CONSTANT));
        assertThat(accessor.block(), nullValue());
        assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
        assertThat(accessor.get(100), equalTo(Temporality.CUMULATIVE));
        assertThat(accessor.get(Integer.MAX_VALUE), equalTo(Temporality.CUMULATIVE));

        accessor = TemporalityAccessor.constant(Temporality.DELTA);
        assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.CONSTANT));
        assertThat(accessor.block(), nullValue());
        assertThat(accessor.get(0), equalTo(Temporality.DELTA));
        assertThat(accessor.get(100), equalTo(Temporality.DELTA));
    }

    public void testCreateWithAllNullsReturnsConstant() {
        try (
            BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock nullBlock = builder.appendNull().appendNull().appendNull().appendNull().build()
        ) {
            TemporalityAccessor accessor = TemporalityAccessor.create(nullBlock, Temporality.DELTA, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.CONSTANT));
            assertThat(accessor.block(), sameInstance(nullBlock));
            assertThat(accessor.get(0), equalTo(Temporality.DELTA));
        }
    }

    public void testCreateWithEmptyBlockReturnsConstant() {
        try (BytesRefBlock emptyBlock = blockFactory().newBytesRefBlockBuilder(0).build()) {
            TemporalityAccessor accessor = TemporalityAccessor.create(emptyBlock, Temporality.CUMULATIVE, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.CONSTANT));
            assertThat(accessor.block(), sameInstance(emptyBlock));
            assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
        }
    }

    public void testCreateWithConstantCumulativeReturnsConstant() {
        try (BytesRefVector vector = blockFactory().newConstantBytesRefVector(Temporality.CUMULATIVE.bytesRef(), 10)) {
            BytesRefBlock block = vector.asBlock();
            TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.CONSTANT));
            assertThat(accessor.block(), sameInstance(block));
            assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
            assertThat(accessor.get(5), equalTo(Temporality.CUMULATIVE));
        }
    }

    public void testCreateWithConstantDeltaReturnsConstant() {
        try (BytesRefVector vector = blockFactory().newConstantBytesRefVector(Temporality.DELTA.bytesRef(), 10)) {
            BytesRefBlock block = vector.asBlock();
            TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.CUMULATIVE, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.CONSTANT));
            assertThat(accessor.block(), sameInstance(block));
            assertThat(accessor.get(0), equalTo(Temporality.DELTA));
        }
    }

    public void testCreateWithConstantInvalidValueUsesDynamicMode() {
        try (BytesRefVector vector = blockFactory().newConstantBytesRefVector(new BytesRef("invalid"), 10)) {
            AtomicReference<BytesRef> captured = new AtomicReference<>();
            TemporalityAccessor accessor = TemporalityAccessor.create(vector.asBlock(), Temporality.DELTA, v -> {
                captured.set(BytesRef.deepCopyOf(v));
                return Temporality.CUMULATIVE;
            });
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.DYNAMIC));
            assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
            assertThat(captured.get().utf8ToString(), equalTo("invalid"));
        }
    }

    public void testDynamicModeWithMixedValues() {
        try (
            BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(4);
            BytesRefBlock block = builder.appendBytesRef(Temporality.CUMULATIVE.bytesRef())
                .appendBytesRef(Temporality.DELTA.bytesRef())
                .appendBytesRef(Temporality.CUMULATIVE.bytesRef())
                .appendBytesRef(Temporality.DELTA.bytesRef())
                .build()
        ) {
            TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.DYNAMIC));
            assertThat(accessor.block(), sameInstance(block));
            assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
            assertThat(accessor.get(1), equalTo(Temporality.DELTA));
            assertThat(accessor.get(2), equalTo(Temporality.CUMULATIVE));
            assertThat(accessor.get(3), equalTo(Temporality.DELTA));
        }
    }

    public void testDynamicModeWithNullValues() {
        try (
            BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(3);
            BytesRefBlock block = builder.appendBytesRef(Temporality.CUMULATIVE.bytesRef())
                .appendNull()
                .appendBytesRef(Temporality.DELTA.bytesRef())
                .build()
        ) {
            TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.DYNAMIC));
            assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
            assertThat(accessor.get(1), equalTo(Temporality.DELTA));
            assertThat(accessor.get(2), equalTo(Temporality.DELTA));
        }
    }

    public void testDynamicModeWithInvalidValue() {
        try (
            BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(2);
            BytesRefBlock block = builder.appendBytesRef(Temporality.CUMULATIVE.bytesRef()).appendBytesRef(new BytesRef("unknown")).build()
        ) {
            AtomicReference<BytesRef> captured = new AtomicReference<>();
            TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, v -> {
                captured.set(BytesRef.deepCopyOf(v));
                return Temporality.CUMULATIVE;
            });
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.DYNAMIC));
            assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
            assertThat(accessor.get(1), equalTo(Temporality.CUMULATIVE));
            assertThat(captured.get().utf8ToString(), equalTo("unknown"));
        }
    }

    public void testOrdinalModeWithValidValues() {
        try (
            IntBlock.Builder ordinalsBuilder = blockFactory().newIntBlockBuilder(4);
            BytesRefVector.Builder dictBuilder = blockFactory().newBytesRefVectorBuilder(2)
        ) {
            dictBuilder.appendBytesRef(Temporality.CUMULATIVE.bytesRef());
            dictBuilder.appendBytesRef(Temporality.DELTA.bytesRef());
            ordinalsBuilder.appendInt(0);
            ordinalsBuilder.appendInt(1);
            ordinalsBuilder.appendInt(0);
            ordinalsBuilder.appendInt(1);

            try (OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ordinalsBuilder.build(), dictBuilder.build())) {
                TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, failingHandler());
                assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.ORDINAL));
                assertThat(accessor.block(), sameInstance(block));
                assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
                assertThat(accessor.get(1), equalTo(Temporality.DELTA));
                assertThat(accessor.get(2), equalTo(Temporality.CUMULATIVE));
                assertThat(accessor.get(3), equalTo(Temporality.DELTA));
            }
        }
    }

    public void testOrdinalModeWithNullValues() {
        try (
            IntBlock.Builder ordinalsBuilder = blockFactory().newIntBlockBuilder(3);
            BytesRefVector.Builder dictBuilder = blockFactory().newBytesRefVectorBuilder(1)
        ) {
            dictBuilder.appendBytesRef(Temporality.CUMULATIVE.bytesRef());
            ordinalsBuilder.appendInt(0);
            ordinalsBuilder.appendNull();
            ordinalsBuilder.appendInt(0);

            try (OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ordinalsBuilder.build(), dictBuilder.build())) {
                TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, failingHandler());
                assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.ORDINAL));
                assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
                assertThat(accessor.get(1), equalTo(Temporality.DELTA));
                assertThat(accessor.get(2), equalTo(Temporality.CUMULATIVE));
            }
        }
    }

    public void testOrdinalModeWithInvalidValue() {
        try (
            IntBlock.Builder ordinalsBuilder = blockFactory().newIntBlockBuilder(2);
            BytesRefVector.Builder dictBuilder = blockFactory().newBytesRefVectorBuilder(2)
        ) {
            dictBuilder.appendBytesRef(Temporality.CUMULATIVE.bytesRef());
            dictBuilder.appendBytesRef(new BytesRef("invalid"));
            ordinalsBuilder.appendInt(0);
            ordinalsBuilder.appendInt(1);

            try (OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ordinalsBuilder.build(), dictBuilder.build())) {
                AtomicReference<BytesRef> captured = new AtomicReference<>();
                TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, v -> {
                    captured.set(BytesRef.deepCopyOf(v));
                    return Temporality.DELTA;
                });
                assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.ORDINAL));
                assertThat(accessor.get(0), equalTo(Temporality.CUMULATIVE));
                assertThat(accessor.get(1), equalTo(Temporality.DELTA));
                assertThat(captured.get().utf8ToString(), equalTo("invalid"));
            }
        }
    }

    public void testDefaultTemporalityIsUsedForNullInDynamicMode() {
        try (
            BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(2);
            BytesRefBlock block = builder.appendNull().appendBytesRef(Temporality.CUMULATIVE.bytesRef()).build()
        ) {
            TemporalityAccessor accessor = TemporalityAccessor.create(block, Temporality.DELTA, failingHandler());
            assertThat(accessor.mode, equalTo(TemporalityAccessor.Mode.DYNAMIC));
            assertThat(accessor.get(0), equalTo(Temporality.DELTA));
            assertThat(accessor.get(1), equalTo(Temporality.CUMULATIVE));
        }
    }
}
