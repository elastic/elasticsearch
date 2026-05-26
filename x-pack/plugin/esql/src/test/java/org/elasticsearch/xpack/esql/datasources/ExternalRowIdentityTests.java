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
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"));

        try (LongBlock rowPositions = positions(0L, 1L, 42L, 999L)) {
            try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, rowPositions, blockFactory)) {
                assertEquals(4, ids.getPositionCount());
                assertEquals("s3://bucket/file.parquet:0", asString(ids, 0));
                assertEquals("s3://bucket/file.parquet:1", asString(ids, 1));
                assertEquals("s3://bucket/file.parquet:42", asString(ids, 2));
                assertEquals("s3://bucket/file.parquet:999", asString(ids, 3));
            }
        }
    }

    /**
     * Counter path for readers that do not emit {@code _rowPosition} (CSV / NDJSON / ORC / ...):
     * the iterator passes a file-local start offset + page position count, and composePage renders
     * {@code <prefix><startOffset + i>} for each row. No extractor-id packing, so values render
     * verbatim.
     */
    public void testComposeFromOffsetRange() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"));
        try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, 100L, 3, blockFactory)) {
            assertEquals(3, ids.getPositionCount());
            assertEquals("s3://bucket/data.csv:100", asString(ids, 0));
            assertEquals("s3://bucket/data.csv:101", asString(ids, 1));
            assertEquals("s3://bucket/data.csv:102", asString(ids, 2));
        }
    }

    public void testComposeFromOffsetZeroBased() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"));
        try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, 0L, 2, blockFactory)) {
            assertEquals("s3://bucket/data.csv:0", asString(ids, 0));
            assertEquals("s3://bucket/data.csv:1", asString(ids, 1));
        }
    }

    public void testComposeFromOffsetEmptyPage() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"));
        try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, 5L, 0, blockFactory)) {
            assertEquals(0, ids.getPositionCount());
        }
    }

    /**
     * Same physical row across two query runs with different extractor ids must produce the same
     * {@code _id} string. The extractor id occupies the high bits of the {@code _rowPosition}
     * encoded value; the {@code _id} composer masks it off and renders only the physical position.
     * This is the stability guarantee that makes {@code _id} usable for cross-source dedup.
     */
    public void testExtractorIdMaskedFromRenderedId() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"));

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
                assertEquals("s3://bucket/file.parquet:" + physical, renderedA);
            }
        }
    }

    public void testNullsArePreserved() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"));
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(3)) {
            builder.appendLong(7L);
            builder.appendNull();
            builder.appendLong(8L);
            try (LongBlock rowPositions = builder.build()) {
                try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, rowPositions, blockFactory)) {
                    assertEquals(3, ids.getPositionCount());
                    assertEquals("s3://bucket/file.parquet:7", asString(ids, 0));
                    assertTrue("null row position renders null _id", ids.isNull(1));
                    assertEquals("s3://bucket/file.parquet:8", asString(ids, 2));
                }
            }
        }
    }

    public void testEncodeDecimal() {
        byte[] buf = new byte[19];
        assertEquals(1, ExternalRowIdentity.encodeDecimal(0L, buf));
        assertEquals('0', buf[18]);

        assertEquals(3, ExternalRowIdentity.encodeDecimal(123L, buf));
        assertEquals('1', buf[16]);
        assertEquals('2', buf[17]);
        assertEquals('3', buf[18]);

        // Negative defensively clamps to 0 (physical row positions are non-negative; defends against
        // a silently-corrupted encoded value rather than emitting a stray minus sign).
        assertEquals(1, ExternalRowIdentity.encodeDecimal(-7L, buf));
        assertEquals('0', buf[18]);
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
