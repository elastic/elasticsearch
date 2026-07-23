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

import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link ExternalRowIdentity}, the per-page composer of the {@code _id}
 * metadata column for external rows. The contract under test: ids are opaque (no component —
 * least of all the storage path — is recoverable by inspection), fixed-length base64url,
 * deterministic for the same {@code (location, mtime, rowPosition)} triple, and distinct
 * whenever any component of the triple differs.
 */
public class ExternalRowIdentityTests extends ESTestCase {

    private static final String BASE64_URL_PATTERN = "[A-Za-z0-9_-]{" + ExternalRowIdentity.RENDERED_LENGTH + "}";

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    /**
     * Shape and distinctness: every rendered id is exactly {@code RENDERED_LENGTH} base64url
     * characters, never contains the storage path or any textual fragment of it, and distinct
     * row positions in one file yield distinct ids.
     */
    public void testComposeRendersOpaqueFixedLengthIds() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"), 1700000000000L);

        try (LongBlock rowPositions = positions(0L, 1L, 42L, 999L)) {
            try (BytesRefBlock ids = ExternalRowIdentity.composePage(prefix, rowPositions, blockFactory)) {
                assertEquals(4, ids.getPositionCount());
                Set<String> seen = new HashSet<>();
                for (int i = 0; i < 4; i++) {
                    String id = asString(ids, i);
                    assertTrue("id [" + id + "] must be fixed-length base64url", id.matches(BASE64_URL_PATTERN));
                    assertFalse("storage path must not leak into _id: " + id, id.contains("bucket"));
                    assertFalse("storage path must not leak into _id: " + id, id.contains("file.parquet"));
                    seen.add(id);
                }
                assertEquals("distinct row positions must yield distinct ids", 4, seen.size());
            }
        }
    }

    /** Same triple, two independent compositions: ids must be byte-for-byte identical. */
    public void testComposeIsDeterministic() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        try (LongBlock rowPositions = positions(0L, 7L, 123456789L)) {
            try (
                BytesRefBlock first = ExternalRowIdentity.composePage(ExternalRowIdentity.prefix(path, 42L), rowPositions, blockFactory);
                BytesRefBlock second = ExternalRowIdentity.composePage(ExternalRowIdentity.prefix(path, 42L), rowPositions, blockFactory)
            ) {
                for (int i = 0; i < 3; i++) {
                    assertEquals("same (location, mtime, rowPosition) must compose the same _id", asString(first, i), asString(second, i));
                }
            }
        }
    }

    /**
     * Each component of the identity triple is load-bearing: a file replaced in place under the
     * same name (new mtime) must produce different ids than its predecessor, a different location
     * with the same mtime must produce different ids, and a missing mtime ({@code 0}, the
     * {@link org.elasticsearch.xpack.esql.datasources.spi.FileList} convention) is just another
     * salt value — well-formed, distinct from real generations.
     */
    public void testEveryTripleComponentDistinguishesIds() {
        StoragePath path = StoragePath.of("s3://bucket/file.parquet");
        BytesRef generationOne = ExternalRowIdentity.prefix(path, 1700000000000L);
        BytesRef generationTwo = ExternalRowIdentity.prefix(path, 1700000099999L);
        BytesRef unknownMtime = ExternalRowIdentity.prefix(path, 0L);
        BytesRef sibling = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/other.parquet"), 1700000000000L);

        try (LongBlock rowPositions = positions(7L)) {
            try (
                BytesRefBlock idsOne = ExternalRowIdentity.composePage(generationOne, rowPositions, blockFactory);
                BytesRefBlock idsTwo = ExternalRowIdentity.composePage(generationTwo, rowPositions, blockFactory);
                BytesRefBlock idsUnknown = ExternalRowIdentity.composePage(unknownMtime, rowPositions, blockFactory);
                BytesRefBlock idsSibling = ExternalRowIdentity.composePage(sibling, rowPositions, blockFactory)
            ) {
                String one = asString(idsOne, 0);
                String two = asString(idsTwo, 0);
                String unknown = asString(idsUnknown, 0);
                String other = asString(idsSibling, 0);
                assertNotEquals("same row in two file generations must have distinct ids", one, two);
                assertNotEquals("unknown-mtime generation must not collide with a real one", one, unknown);
                assertNotEquals("same row in two different files must have distinct ids", one, other);
                assertTrue("unknown mtime still renders a well-formed id", unknown.matches(BASE64_URL_PATTERN));
            }
        }
    }

    /**
     * Same physical row across two query runs with different extractor ids must produce the same
     * {@code _id} string. The extractor id occupies the high bits of the {@code _rowPosition}
     * encoded value; the {@code _id} composer masks it off so only the physical position enters
     * the identity bytes. This is the stability guarantee that makes {@code _id} usable for
     * cross-source dedup.
     */
    public void testExtractorIdMaskedFromRenderedId() {
        BytesRef prefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/file.parquet"), 1700000000000L);

        long physical = 123L;
        // Two different extractor ids encoded into the high bits — distinct values that the
        // deferred-extraction path could legitimately emit on different runs / fragments.
        long encodedRunA = (5L << ColumnExtractor.LOCAL_POSITION_BITS) | physical;
        long encodedRunB = (42L << ColumnExtractor.LOCAL_POSITION_BITS) | physical;
        assertNotEquals("sanity: the two encodings must differ before masking", encodedRunA, encodedRunB);

        try (LongBlock posA = positions(encodedRunA); LongBlock posB = positions(encodedRunB); LongBlock posPlain = positions(physical)) {
            try (
                BytesRefBlock idA = ExternalRowIdentity.composePage(prefix, posA, blockFactory);
                BytesRefBlock idB = ExternalRowIdentity.composePage(prefix, posB, blockFactory);
                BytesRefBlock idPlain = ExternalRowIdentity.composePage(prefix, posPlain, blockFactory)
            ) {
                String renderedA = asString(idA, 0);
                assertEquals("extractor id must be masked off the rendered _id", renderedA, asString(idB, 0));
                assertEquals("encoded and unencoded forms of the same physical row must agree", renderedA, asString(idPlain, 0));
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
                    assertTrue("non-null row renders an id", asString(ids, 0).matches(BASE64_URL_PATTERN));
                    assertTrue("null row position renders null _id", ids.isNull(1));
                    assertTrue("non-null row renders an id", asString(ids, 2).matches(BASE64_URL_PATTERN));
                    assertNotEquals("distinct rows around the null must stay distinct", asString(ids, 0), asString(ids, 2));
                }
            }
        }
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
