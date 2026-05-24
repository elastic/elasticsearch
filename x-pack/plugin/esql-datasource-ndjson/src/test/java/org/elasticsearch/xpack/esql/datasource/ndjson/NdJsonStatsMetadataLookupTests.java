/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCache;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;

/** Lookup-side gate for NDJSON. Capture side is in {@code NdJsonStatsCaptureTests}. */
public class NdJsonStatsMetadataLookupTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        ExternalStatsCache.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        ExternalStatsCache.clearForTests();
        super.tearDown();
    }

    public void testCacheMissPublishesSizeButNoRowCount() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n");
        SourceMetadata md = new NdJsonFormatReader(null, blockFactory).metadata(o);
        assertTrue("statistics must be present (sizeInBytes is always known)", md.statistics().isPresent());
        assertFalse("rowCount must be absent on cache miss", md.statistics().get().rowCount().isPresent());
        assertTrue("sizeInBytes must be present on cache miss", md.statistics().get().sizeInBytes().isPresent());
    }

    public void testCacheHitPublishesRowCount() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        ExternalStatsCache.put(o, 3L);
        SourceMetadata md = new NdJsonFormatReader(null, blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        OptionalLong rc = md.statistics().get().rowCount();
        assertTrue(rc.isPresent());
        assertEquals(3L, rc.getAsLong());
    }

    public void testCacheHitAlsoPublishesSizeInBytes() throws Exception {
        String content = "{\"a\":1}\n{\"a\":2}\n";
        StorageObject o = obj(content);
        ExternalStatsCache.put(o, 2L);
        SourceMetadata md = new NdJsonFormatReader(null, blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        assertEquals(content.getBytes(StandardCharsets.UTF_8).length, md.statistics().get().sizeInBytes().getAsLong());
    }

    public void testCacheHitPublishesColumnStats() throws Exception {
        StorageObject o = obj("{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\"}\n");
        java.util.Map<String, ExternalStatsCache.ColumnStats> cols = java.util.Map.of(
            "id",
            new ExternalStatsCache.ColumnStats(0L, 1, 2),
            "name",
            new ExternalStatsCache.ColumnStats(
                0L,
                new org.apache.lucene.util.BytesRef("alpha"),
                new org.apache.lucene.util.BytesRef("beta")
            )
        );
        ExternalStatsCache.put(o, new ExternalStatsCache.Stats(2L, java.util.OptionalLong.empty(), cols));
        SourceMetadata md = new NdJsonFormatReader(null, blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        java.util.Optional<java.util.Map<String, org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics.ColumnStatistics>> colStats =
            md.statistics().get().columnStatistics();
        assertTrue("column stats must be published on cache hit", colStats.isPresent());
        var idStats = colStats.get().get("id");
        assertNotNull(idStats);
        assertEquals(java.util.OptionalLong.of(0L), idStats.nullCount());
        assertEquals(1, idStats.minValue().orElse(null));
        assertEquals(2, idStats.maxValue().orElse(null));
    }

    public void testCacheHitWithBytesReadPopulatesSizeInBytesWhenLengthUnknown() throws Exception {
        StorageObject streamOnly = streamOnlyObject("{\"a\":1}\n");
        ExternalStatsCache.put(streamOnly, new ExternalStatsCache.Stats(1L, java.util.OptionalLong.of(8L), java.util.Map.of()));
        SourceMetadata md = new NdJsonFormatReader(null, blockFactory).metadata(streamOnly);
        assertTrue(md.statistics().isPresent());
        assertTrue("stream-only sizeInBytes must be served from cache.bytesRead", md.statistics().get().sizeInBytes().isPresent());
        assertEquals(8L, md.statistics().get().sizeInBytes().getAsLong());
    }

    /** Stream-only sources throw from length() — metadata() still flows mtime so the cache participates. */
    public void testLengthUnsupportedStillProducesStats() throws Exception {
        StorageObject streamOnly = streamOnlyObject("{\"a\":1}\n{\"a\":2}\n");
        SourceMetadata md = new NdJsonFormatReader(null, blockFactory).metadata(streamOnly);
        assertTrue("stream-only compression must still produce stats keyed on mtime", md.statistics().isPresent());
        assertFalse("sizeInBytes must be absent when length() is unsupported", md.statistics().get().sizeInBytes().isPresent());
    }

    private StorageObject streamOnlyObject(String ndjson) {
        byte[] bytes = ndjson.getBytes(StandardCharsets.UTF_8);
        String uniquePath = "memory://" + UUID.randomUUID() + ".ndjson.bz2";
        Instant fixedMtime = Instant.now();
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException("Range reads not supported");
            }

            @Override
            public long length() {
                throw new UnsupportedOperationException("Decompressed length is unknown for stream-only compression");
            }

            @Override
            public Instant lastModified() {
                return fixedMtime;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(uniquePath);
            }
        };
    }

    private StorageObject obj(String ndjson) {
        byte[] bytes = ndjson.getBytes(StandardCharsets.UTF_8);
        String uniquePath = "memory://" + UUID.randomUUID() + ".ndjson";
        Instant fixedMtime = Instant.now();
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException("Range reads not needed");
            }

            @Override
            public long length() {
                return bytes.length;
            }

            @Override
            public Instant lastModified() {
                return fixedMtime;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(uniquePath);
            }
        };
    }
}
