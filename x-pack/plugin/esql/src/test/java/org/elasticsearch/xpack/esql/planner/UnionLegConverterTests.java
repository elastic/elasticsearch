/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.UnionBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The shard half of a union column. A shard reads its field with its own loader and this records which leg the values
 * landed in, converting nothing - so what these check is that every value survives unchanged and carries the right tag,
 * including the shapes a mapping can produce that a single-valued view would lose.
 */
public class UnionLegConverterTests extends ESTestCase {

    private static final List<ElementType> LEGS = List.of(ElementType.LONG, ElementType.BYTES_REF);
    private static final int LONG_LEG = 0;
    private static final int BYTES_LEG = 1;

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private ValuesSourceReaderOperator.ConverterEvaluator converter(int leg) {
        return new UnionLegConverter(LEGS, leg).build(new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null));
    }

    public void testEveryValueKeepsItsLegAndValue() {
        LongBlock loaded;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(10L);
            builder.appendLong(20L);
            builder.appendLong(30L);
            loaded = builder.build();
        }
        try (var evaluator = converter(LONG_LEG); UnionBlock block = (UnionBlock) evaluator.convert(loaded)) {
            assertThat(block.getPositionCount(), equalTo(3));
            assertThat(block.legElementTypes(), equalTo(LEGS));
            LongBlock legBlock = (LongBlock) block.legBlock(LONG_LEG);
            for (int p = 0; p < 3; p++) {
                assertThat(block.tag(p), equalTo(LONG_LEG));
                assertThat(legBlock.getLong(p), equalTo((p + 1) * 10L));
            }
            assertTrue("the other leg holds nothing", block.legBlock(BYTES_LEG).areAllValuesNull());
        }
    }

    /** A shard may map the field as an array of its own type, so a multi-valued source is ordinary rather than exotic. */
    public void testAMultivaluedFieldKeepsItsRuns() {
        BytesRefBlock loaded;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.endPositionEntry();
            builder.appendBytesRef(new BytesRef("c"));
            loaded = builder.build();
        }
        try (var evaluator = converter(BYTES_LEG); UnionBlock block = (UnionBlock) evaluator.convert(loaded)) {
            assertThat(block.getPositionCount(), equalTo(2));
            assertThat(block.getValueCount(0), equalTo(2));
            assertThat(block.getValueCount(1), equalTo(1));
            BytesRefBlock legBlock = (BytesRefBlock) block.legBlock(BYTES_LEG);
            assertThat(legBlock.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("a"));
            assertThat(legBlock.getBytesRef(1, new BytesRef()).utf8ToString(), equalTo("b"));
            assertThat(legBlock.getBytesRef(2, new BytesRef()).utf8ToString(), equalTo("c"));
            for (int v = 0; v < 3; v++) {
                assertThat(block.tag(v), equalTo(BYTES_LEG));
            }
        }
    }

    public void testADocumentWithoutTheFieldStaysNull() {
        LongBlock loaded;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(1L);
            builder.appendNull();
            builder.appendLong(3L);
            loaded = builder.build();
        }
        try (var evaluator = converter(LONG_LEG); UnionBlock block = (UnionBlock) evaluator.convert(loaded)) {
            assertThat(block.getPositionCount(), equalTo(3));
            assertThat(block.tag(0), equalTo(LONG_LEG));
            assertTrue(block.isNull(1));
            assertThat(block.tag(1), equalTo(UnionBlock.NO_TAG));
            assertThat(block.tag(2), equalTo(LONG_LEG));
            assertThat(((LongBlock) block.legBlock(LONG_LEG)).getLong(2), equalTo(3L));
        }
    }

    /**
     * A page where no document has the field. Building k null legs would cost more and say the same, and the loaded
     * block may be typed rather than a null block, so its element type would not be the union's.
     */
    public void testAPageWithNothingForTheFieldBecomesAConstantNull() {
        LongBlock loaded;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(2)) {
            builder.appendNull();
            builder.appendNull();
            loaded = builder.build();
        }
        try (var evaluator = converter(LONG_LEG); Block block = evaluator.convert(loaded)) {
            assertThat(block.getPositionCount(), equalTo(2));
            assertThat(block.elementType(), equalTo(ElementType.NULL));
            assertTrue(block.areAllValuesNull());
            assertThat("a union that is null throughout reports no legs", ((UnionBlock) block).legCount(), equalTo(0));
        }
    }

    /**
     * The converter owns the block it is handed - the read path assigns the result over it and never closes it - so a
     * converter that did not release it would leak every page.
     */
    public void testTheLoadedBlockIsReleased() {
        LongBlock loaded;
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
            builder.appendLong(1L);
            loaded = builder.build();
        }
        assertTrue(loaded.hasReferences());
        try (var evaluator = converter(LONG_LEG); Block block = evaluator.convert(loaded)) {
            assertThat(block, instanceOf(UnionBlock.class));
            assertFalse("the loaded block must be released by the converter", loaded.hasReferences());
        }
    }

    public void testALegOutOfRangeIsRefusedWhenTheConverterIsBuilt() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new UnionLegConverter(LEGS, 2));
        assertThat(e.getMessage(), equalTo("leg 2 out of range for 2 legs"));
    }
}
