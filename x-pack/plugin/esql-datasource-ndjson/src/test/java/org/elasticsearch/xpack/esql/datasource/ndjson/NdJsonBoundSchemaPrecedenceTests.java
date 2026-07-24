/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * When the planner has bound a read schema for a file, that schema decides each column's type — not
 * whatever the reader happens to know locally.
 *
 * <p>The reader-level schema can be a per-file or per-chunk inference: what the values in view looked
 * like. The bound schema is the planner's contract, either declared or reconciled across every file of
 * the dataset. Inference never has more information, so letting it win retypes a column behind the
 * compute engine's back and the resulting block fails to cast. That is the defect behind the compressed
 * NDJSON read crashes; these tests pin the rule that prevents it recurring.
 */
public class NdJsonBoundSchemaPrecedenceTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUpBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * The crash shape: a column declared {@code long} whose values are all int-sized. Inference calls it
     * {@code integer}; the engine expects {@code long}.
     */
    public void testBoundTypeWinsOverInferredType() throws Exception {
        FormatReader reader = new NdJsonFormatReader(null, blockFactory).withSchema(List.of(ref("v", DataType.INTEGER)));

        try (CloseableIterator<Page> pages = read(reader, "{\"v\":1}\n{\"v\":2}\n", List.of(ref("v", DataType.LONG)))) {
            assertTrue("expected a page", pages.hasNext());
            Page page = pages.next();
            try {
                Block block = page.getBlock(0);
                assertThat(
                    "the planner declared [long]; the reader must not retype the column from its own values",
                    block,
                    org.hamcrest.Matchers.instanceOf(LongBlock.class)
                );
                LongBlock longs = (LongBlock) block;
                assertEquals(1L, longs.getLong(0));
                assertEquals(2L, longs.getLong(1));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** A column the bound schema does not mention is still contributed by the projection. */
    public void testProjectionOnlyColumnIsStillRead() throws Exception {
        FormatReader reader = new NdJsonFormatReader(null, blockFactory).withSchema(
            List.of(ref("v", DataType.LONG), ref("w", DataType.LONG))
        );

        try (CloseableIterator<Page> pages = read(reader, "{\"v\":1,\"w\":7}\n", List.of(ref("v", DataType.LONG)))) {
            assertTrue("expected a page", pages.hasNext());
            Page page = pages.next();
            try {
                assertEquals("bound column plus the projection-only column", 2, page.getBlockCount());
                assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
                assertEquals(7L, ((LongBlock) page.getBlock(1)).getLong(0));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** Bound column order survives the merge — the projection must not reorder the page's channels. */
    public void testBoundColumnOrderIsPreserved() throws Exception {
        FormatReader reader = new NdJsonFormatReader(null, blockFactory).withSchema(
            List.of(ref("b", DataType.LONG), ref("a", DataType.LONG))
        );

        try (
            CloseableIterator<Page> pages = read(reader, "{\"a\":10,\"b\":20}\n", List.of(ref("a", DataType.LONG), ref("b", DataType.LONG)))
        ) {
            assertTrue("expected a page", pages.hasNext());
            Page page = pages.next();
            try {
                assertEquals("channel 0 is the bound schema's first column [a]", 10L, ((LongBlock) page.getBlock(0)).getLong(0));
                assertEquals("channel 1 is the bound schema's second column [b]", 20L, ((LongBlock) page.getBlock(1)).getLong(0));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private CloseableIterator<Page> read(FormatReader reader, String ndjson, List<Attribute> boundSchema) throws Exception {
        StorageObject object = new BytesStorageObject("file:///test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        return reader.read(object, FormatReadContext.builder().batchSize(10).readSchema(boundSchema).build());
    }

    private static Attribute ref(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}
