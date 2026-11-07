/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.UnionBlock;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A page assembled from more than one segment needs a destination block, and {@link ValuesReader} builds it from the
 * field's own supplier. For most fields the element type supplies one; for a union it cannot, because an
 * {@link ElementType} is a code and carries no leg element types. These pin that the four-argument
 * {@link ValuesSourceReaderOperator.FieldInfo} still derives its supplier from the element type - so every existing
 * field is unaffected - and that a union must be handed one, since otherwise the failure appears only on multi-segment
 * pages, which is the shape least likely to be noticed.
 */
public class FieldInfoBuilderSupplierTests extends ComputeTestCase {

    private static final ValuesSourceReaderOperator.BuildLoader UNUSED_LOADER = (driverContext, shard) -> {
        throw new AssertionError("the loader is not built here");
    };

    public void testTheElementTypeSuppliesTheBuilderByDefault() {
        BlockFactory blockFactory = blockFactory();
        var info = new ValuesSourceReaderOperator.FieldInfo("f", ElementType.LONG, false, UNUSED_LOADER);
        try (Block.Builder builder = info.builderSupplier().newBlockBuilder(blockFactory, 1)) {
            assertThat(builder, instanceOf(LongBlock.Builder.class));
        }
    }

    public void testAUnionCannotTakeTheDefaultSupplier() {
        BlockFactory blockFactory = blockFactory();
        var info = new ValuesSourceReaderOperator.FieldInfo("f", ElementType.UNION, false, UNUSED_LOADER);
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> info.builderSupplier().newBlockBuilder(blockFactory, 1)
        );
        assertThat(e.getMessage(), equalTo("a union block builder needs the leg element types"));
    }

    public void testAUnionSuppliesItsOwnBuilderFromItsLegTypes() {
        BlockFactory blockFactory = blockFactory();
        List<ElementType> legs = List.of(ElementType.LONG, ElementType.BYTES_REF);
        var info = new ValuesSourceReaderOperator.FieldInfo(
            "f",
            ElementType.UNION,
            false,
            UNUSED_LOADER,
            (factory, estimatedSize) -> factory.newUnionBlockBuilder(legs, estimatedSize)
        );
        try (Block.Builder builder = info.builderSupplier().newBlockBuilder(blockFactory, 1)) {
            assertThat(builder, instanceOf(UnionBlock.Builder.class));
            builder.appendNull();
            try (Block block = builder.build()) {
                assertThat(block.elementType(), equalTo(ElementType.UNION));
                assertThat(((UnionBlock) block).legElementTypes(), equalTo(legs));
            }
        }
    }
}
