/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

/**
 * Unit tests for {@link SynthesizeExternalSource}, the per-row composer of the {@code _source}
 * metadata column for external rows.
 */
public class SynthesizeExternalSourceTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    /**
     * Regression: user data columns whose names happen to start with an underscore
     * ({@code _corrupt_record}, {@code _status}, ...) must reach the rendered {@code _source}.
     * A prior implementation used a leading-underscore prefix filter to drop synthetic channels
     * which silently swallowed these legitimate column names.
     */
    public void testUserColumnsStartingWithUnderscoreAreIncluded() throws Exception {
        try (
            IntBlock idCol = intBlock(42);
            BytesRefBlock corrupt = bytesRefBlock("not valid json");
            BytesRefBlock status = bytesRefBlock("ok")
        ) {
            String[] names = { "_corrupt_record", "id", "_status" };
            Block[] blocks = { corrupt, idCol, status };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("user column _corrupt_record must appear in _source: " + json, json.contains("_corrupt_record"));
                assertTrue("user column _status must appear in _source: " + json, json.contains("_status"));
                assertTrue("data column id must appear in _source: " + json, json.contains("\"id\""));
            }
        }
    }

    /**
     * Framework-injected synthetic channel ({@link ColumnExtractor#ROW_POSITION_COLUMN}) must
     * NOT appear in the rendered {@code _source}.
     */
    public void testRowPositionColumnIsExcluded() throws Exception {
        try (IntBlock idCol = intBlock(7); LongBlock rowPos = longBlock(123L)) {
            String[] names = { "id", ColumnExtractor.ROW_POSITION_COLUMN };
            Block[] blocks = { idCol, rowPos };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("data column id must appear in _source: " + json, json.contains("\"id\""));
                assertFalse(
                    "synthetic _rowPosition must be excluded from _source: " + json,
                    json.contains(ColumnExtractor.ROW_POSITION_COLUMN)
                );
            }
        }
    }

    /**
     * Block subtypes not enumerated by the value-rendering switch (here: {@link CompositeBlock})
     * must fail loudly. Falling through to {@code toString()} would emit implementation-specific
     * debug text into user-visible JSON; preferring an explicit failure forces the new type's
     * handling to be added intentionally rather than discovered as corrupt {@code _source}.
     */
    public void testUnsupportedBlockTypeRejected() throws Exception {
        try (IntBlock inner = intBlock(1)) {
            inner.incRef();
            try (CompositeBlock composite = new CompositeBlock(new Block[] { inner })) {
                String[] names = { "weird" };
                Block[] blocks = { composite };
                UnsupportedOperationException thrown = expectThrows(
                    UnsupportedOperationException.class,
                    () -> SynthesizeExternalSource.composePage(names, blocks, 1, blockFactory)
                );
                // The error originates inside CompositeBlock.getValueCount, which BlockUtils
                // calls during the toJavaObject dispatch — "Composite block" is its message.
                assertTrue(
                    "exception message must identify the rejected block: " + thrown.getMessage(),
                    thrown.getMessage().contains("Composite block") || thrown.getMessage().contains("COMPOSITE")
                );
            }
        }
    }

    private IntBlock intBlock(int value) {
        try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(1)) {
            b.appendInt(value);
            return b.build();
        }
    }

    private LongBlock longBlock(long value) {
        try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(1)) {
            b.appendLong(value);
            return b.build();
        }
    }

    private BytesRefBlock bytesRefBlock(String value) {
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(new BytesRef(value));
            return b.build();
        }
    }

    private static String bytesAt(BytesRefBlock block, int row) {
        BytesRef scratch = new BytesRef();
        BytesRef ref = block.getBytesRef(block.getFirstValueIndex(row), scratch);
        return ref.utf8ToString();
    }
}
