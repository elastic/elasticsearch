/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

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

/** Lookup-side gate for CSV. Capture side is in {@code CsvStatsCaptureTests}. */
public class CsvStatsMetadataLookupTests extends ESTestCase {

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
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n");
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertTrue("statistics must be present (sizeInBytes is always known)", md.statistics().isPresent());
        assertFalse("rowCount must be absent on cache miss", md.statistics().get().rowCount().isPresent());
        assertTrue("sizeInBytes must be present on cache miss", md.statistics().get().sizeInBytes().isPresent());
    }

    public void testCacheHitPublishesRowCount() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        ExternalStatsCache.put(o, 3L);
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        OptionalLong rc = md.statistics().get().rowCount();
        assertTrue(rc.isPresent());
        assertEquals(3L, rc.getAsLong());
    }

    public void testCacheHitAlsoPublishesSizeInBytes() throws Exception {
        String content = "id:integer,n:integer\n1,10\n2,20\n";
        StorageObject o = obj(content);
        ExternalStatsCache.put(o, 2L);
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        assertEquals(content.getBytes(StandardCharsets.UTF_8).length, md.statistics().get().sizeInBytes().getAsLong());
    }

    public void testCacheHitPublishesColumnStats() throws Exception {
        StorageObject o = obj("id:integer,name:keyword\n1,alpha\n2,beta\n");
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
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        java.util.Optional<java.util.Map<String, org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics.ColumnStatistics>> colStats =
            md.statistics().get().columnStatistics();
        assertTrue("column stats must be published on cache hit", colStats.isPresent());
        var idStats = colStats.get().get("id");
        assertNotNull(idStats);
        assertEquals(java.util.OptionalLong.of(0L), idStats.nullCount());
        assertEquals(1, idStats.minValue().orElse(null));
        assertEquals(2, idStats.maxValue().orElse(null));
        var nameStats = colStats.get().get("name");
        assertNotNull(nameStats);
        assertEquals(new org.apache.lucene.util.BytesRef("alpha"), nameStats.minValue().orElse(null));
        assertEquals(new org.apache.lucene.util.BytesRef("beta"), nameStats.maxValue().orElse(null));
    }

    public void testCacheHitWithBytesReadPopulatesSizeInBytesWhenLengthUnknown() throws Exception {
        StorageObject streamOnly = streamOnlyObject("id:integer\n1\n2\n3\n");
        ExternalStatsCache.put(streamOnly, new ExternalStatsCache.Stats(3L, java.util.OptionalLong.of(17L), java.util.Map.of()));
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(streamOnly);
        assertTrue(md.statistics().isPresent());
        assertTrue("stream-only sizeInBytes must be served from cache.bytesRead", md.statistics().get().sizeInBytes().isPresent());
        assertEquals(17L, md.statistics().get().sizeInBytes().getAsLong());
    }

    /** Stream-only sources throw from length() — metadata() still flows mtime + cache-served rowCount. */
    public void testLengthUnsupportedStillProducesStats() throws Exception {
        StorageObject streamOnly = streamOnlyObject("id:integer,n:integer\n1,10\n2,20\n");
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(streamOnly);
        assertTrue("stream-only compression must still produce stats keyed on mtime", md.statistics().isPresent());
        assertFalse("sizeInBytes must be absent when length() is unsupported", md.statistics().get().sizeInBytes().isPresent());
    }

    /** StorageObject that mirrors a bzip2-wrapped source: stream-only, length() throws {@code UnsupportedOperationException}. */
    private StorageObject streamOnlyObject(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        String uniquePath = "memory://" + UUID.randomUUID() + ".csv.bz2";
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

    /** Unique path + stable mtime per object — matches the cache key shape (path, mtime). */
    private StorageObject obj(String csvContent) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        String uniquePath = "memory://" + UUID.randomUUID() + ".csv";
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
