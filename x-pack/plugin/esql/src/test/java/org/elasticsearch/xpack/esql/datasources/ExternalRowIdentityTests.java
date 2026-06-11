/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * Unit tests for {@link ExternalRowIdentity}, the per-page composer of the {@code _id}
 * metadata column for external rows.
 */
public class ExternalRowIdentityTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testComposeBasicRoundTrip() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"), 1700000000000L);

        try (LongBlock rowPositions = positions(0L, 1L, 42L, 999L)) {
            try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, rowPositions, blockFactory)) {
                assertEquals(4, ids.getPositionCount());
                assertEquals("s3://bucket/file.parquet@1700000000000:0", asString(ids, 0));
                assertEquals("s3://bucket/file.parquet@1700000000000:1", asString(ids, 1));
                assertEquals("s3://bucket/file.parquet@1700000000000:42", asString(ids, 2));
                assertEquals("s3://bucket/file.parquet@1700000000000:999", asString(ids, 3));
            }
        }
    }

    /**
     * A file replaced in place under the same name (new mtime) must produce different ids than
     * its predecessor — the mtime salt is what distinguishes the two file generations. A missing
     * mtime ({@code 0}, the {@link org.elasticsearch.xpack.esql.datasources.spi.FileList}
     * convention) keeps the same uniform shape.
     */
    public void testMtimeSaltDistinguishesFileGenerations() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        BytesRef generationOne = ExternalRowIdentity.prefix(path, 1700000000000L);
        BytesRef generationTwo = ExternalRowIdentity.prefix(path, 1700000099999L);
        BytesRef unknownMtime = ExternalRowIdentity.prefix(path, 0L);

        try (LongBlock rowPositions = positions(7L)) {
            try (
                BytesRefBlock idsOne = ExternalRowIdentity.composePage(generationOne, rowPositions, blockFactory);
                BytesRefBlock idsTwo = ExternalRowIdentity.composePage(generationTwo, rowPositions, blockFactory);
                BytesRefBlock idsUnknown = ExternalRowIdentity.composePage(unknownMtime, rowPositions, blockFactory)
            ) {
                assertNotEquals("same row in two file generations must have distinct ids", asString(idsOne, 0), asString(idsTwo, 0));
                assertEquals("s3://bucket/file.parquet@1700000099999:7", asString(idsTwo, 0));
                assertEquals("missing mtime renders the 0 sentinel", "s3://bucket/file.parquet@0:7", asString(idsUnknown, 0));
            }
        }
    }

    /**
     * Same physical row across two query runs with different extractor ids must produce the same
     * {@code _id} string. The extractor id occupies the high bits of the {@code _rowPosition}
     * encoded value; the {@code _id} composer masks it off and renders only the physical position.
     * This is the stability guarantee that makes {@code _id} usable for cross-source dedup.
     */
    public void testExtractorIdMaskedFromRenderedId() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"), 1700000000000L);

        long physical = 123L;
        // Two different extractor ids encoded into the high bits — distinct values that the
        // deferred-extraction path could legitimately emit on different runs / fragments.
        long encodedRunA = (5L << ColumnExtractor.LOCAL_POSITION_BITS) | physical;
        long encodedRunB = (42L << ColumnExtractor.LOCAL_POSITION_BITS) | physical;
        assertNotEquals("sanity: the two encodings must differ before masking", encodedRunA, encodedRunB);

        try (LongBlock posA = positions(encodedRunA); LongBlock posB = positions(encodedRunB)) {
            try (
                BytesRefBlock idA = ExternalRowIdentity.composePage(prefix, posA, blockFactory);
                BytesRefBlock idB = ExternalRowIdentity.composePage(prefix, posB, blockFactory)
            ) {
                String renderedA = asString(idA, 0);
                String renderedB = asString(idB, 0);
                assertEquals("extractor id must be masked off the rendered _id", renderedA, renderedB);
                assertEquals("s3://bucket/file.parquet@1700000000000:" + physical, renderedA);
            }
        }
    }

    public void testNullsArePreserved() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"), 1700000000000L);
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(7L);
            builder.appendNull();
            builder.appendLong(8L);
            try (LongBlock rowPositions = builder.build()) {
                try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, rowPositions, blockFactory)) {
                    assertEquals(3, ids.getPositionCount());
                    assertEquals("s3://bucket/file.parquet@1700000000000:7", asString(ids, 0));
                    assertTrue("null row position renders null _id", ids.isNull(1));
                    assertEquals("s3://bucket/file.parquet@1700000000000:8", asString(ids, 2));
                }
            }
        }
    }

    public void testEncodeDecimal() {
        byte[] buf = new byte[19];
        // Row 0 is a legitimate first row in a file and renders as '0'.
        assertEquals(1, ExternalRowIdentity.encodeDecimal(0L, buf));
        assertEquals('0', buf[18]);

        assertEquals(3, ExternalRowIdentity.encodeDecimal(123L, buf));
        assertEquals('1', buf[16]);
        assertEquals('2', buf[17]);
        assertEquals('3', buf[18]);
    }

    /**
     * Negative inputs can only come from corruption (e.g. an unmasked sentinel). The
     * {@code assert false} surfaces the bug class in CI; the production-only fallthrough still
     * renders {@code "0"} defensively. We exercise the assertion shape here so a future refactor
     * that silently drops the assert is caught.
     */
    public void testEncodeDecimalAssertsOnNegative() {
        byte[] buf = new byte[19];
        expectThrows(AssertionError.class, () -> ExternalRowIdentity.encodeDecimal(-7L, buf));
    }

    private LongBlock positions(long... values) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(values.length)) {
            for (long v : values) {
                builder.appendLong(v);
            }
            return builder.build();
        }
    }

    private String asString(BytesRefBlock block, int position) {
        BytesRef ref = new BytesRef();
        block.getBytesRef(block.getFirstValueIndex(position), ref);
        return ref.utf8ToString();
    }
}
