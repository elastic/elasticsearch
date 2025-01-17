/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MergePositionsOperatorTests extends ESTestCase {

    public void testSimple() throws Exception {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = new BlockFactory(breaker, bigArrays);
        IntVector selected = IntVector.range(0, 7, blockFactory);
        MergePositionsOperator mergeOperator = new MergePositionsOperator(
            0,
            new int[] { 1, 2 },
            new ElementType[] { ElementType.BYTES_REF, ElementType.INT },
            selected.asBlock(),
            blockFactory
        );
        {
            final IntBlock b1 = blockFactory.newConstantIntBlockWith(1, 1);
            final BytesRefBlock b2;
            try (var builder = blockFactory.newBytesRefBlockBuilder(1)) {
                b2 = builder.appendBytesRef(new BytesRef("w0")).build();
            }
            final IntBlock b3;
            try (var builder = blockFactory.newIntBlockBuilder(1)) {
                b3 = builder.appendNull().build();
            }
            mergeOperator.addInput(new Page(b1, b2, b3));
        }
        {
            final IntBlock b1 = blockFactory.newConstantIntBlockWith(2, 1);
            final BytesRefBlock b2;
            try (var builder = blockFactory.newBytesRefBlockBuilder(1)) {
                b2 = builder.beginPositionEntry()
                    .appendBytesRef(new BytesRef("a1"))
                    .appendBytesRef(new BytesRef("c1"))
                    .endPositionEntry()
                    .build();
            }
            final IntBlock b3;
            try (var builder = blockFactory.newIntBlockBuilder(1)) {
                b3 = builder.appendNull().build();
            }
            mergeOperator.addInput(new Page(b1, b2, b3));
        }
        {
            final IntBlock b1 = blockFactory.newConstantIntBlockWith(3, 2);
            final BytesRefBlock b2;
            try (var builder = blockFactory.newBytesRefBlockBuilder(2)) {
                b2 = builder.appendBytesRef(new BytesRef("f5"))
                    .beginPositionEntry()
                    .appendBytesRef(new BytesRef("k1"))
                    .appendBytesRef(new BytesRef("k2"))
                    .endPositionEntry()
                    .build();
            }
            final IntBlock b3;
            try (var builder = blockFactory.newIntBlockBuilder(2)) {
                b3 = builder.appendInt(2020).appendInt(2021).build();
            }
            mergeOperator.addInput(new Page(b1, b2, b3));
        }
        {
            final IntBlock b1 = blockFactory.newConstantIntBlockWith(5, 1);
            final BytesRefBlock b2;
            try (var builder = blockFactory.newBytesRefBlockBuilder(1)) {
                b2 = builder.beginPositionEntry()
                    .appendBytesRef(new BytesRef("r2"))
                    .appendBytesRef(new BytesRef("k2"))
                    .endPositionEntry()
                    .build();
            }
            final IntBlock b3;
            try (var builder = blockFactory.newIntBlockBuilder(1)) {
                b3 = builder.appendInt(2023).build();
            }
            mergeOperator.addInput(new Page(b1, b2, b3));
        }
        mergeOperator.finish();
        Page out = mergeOperator.getOutput();
        assertTrue(mergeOperator.isFinished());
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(7));
        assertThat(out.getBlockCount(), equalTo(2));
        BytesRefBlock f1 = out.getBlock(0);
        IntBlock f2 = out.getBlock(1);

        assertTrue(f1.isNull(0));
        assertThat(BlockUtils.toJavaObject(f1, 1), equalTo(new BytesRef("w0")));
        assertThat(BlockUtils.toJavaObject(f1, 2), equalTo(List.of(new BytesRef("a1"), new BytesRef("c1"))));
        assertThat(BlockUtils.toJavaObject(f1, 3), equalTo(List.of(new BytesRef("f5"), new BytesRef("k1"), new BytesRef("k2"))));
        assertTrue(f1.isNull(4));
        assertThat(BlockUtils.toJavaObject(f1, 5), equalTo(List.of(new BytesRef("r2"), new BytesRef("k2"))));
        assertTrue(f1.isNull(6));

        assertTrue(f2.isNull(0));
        assertTrue(f2.isNull(1));
        assertTrue(f2.isNull(2));
        assertThat(BlockUtils.toJavaObject(f2, 3), equalTo(List.of(2020, 2021)));
        assertTrue(f2.isNull(4));
        assertThat(BlockUtils.toJavaObject(f2, 5), equalTo(2023));
        assertTrue(f2.isNull(6));
        Releasables.close(mergeOperator, selected, out::releaseBlocks);
        MockBigArrays.ensureAllArraysAreReleased();
    }
}
