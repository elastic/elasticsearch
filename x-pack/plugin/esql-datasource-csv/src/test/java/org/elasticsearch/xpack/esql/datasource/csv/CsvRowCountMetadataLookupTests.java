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
import org.elasticsearch.xpack.esql.datasources.cache.ExternalRowCountCache;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Verifies {@code CsvFormatReader.metadata()} publishes {@link SourceStatistics#rowCount()} when —
 * and only when — {@link ExternalRowCountCache} has an entry for the file. Lookup half of B'; the
 * write half (capture-on-close) is covered by {@code CsvRowCountCaptureTests}.
 */
public class CsvRowCountMetadataLookupTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        ExternalRowCountCache.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        ExternalRowCountCache.clearForTests();
        super.tearDown();
    }

    public void testCacheMissPublishesNoStatistics() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n");
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertFalse("cache miss must publish no statistics", md.statistics().isPresent());
    }

    public void testCacheHitPublishesRowCount() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        ExternalRowCountCache.put(o, 3L);
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        OptionalLong rc = md.statistics().get().rowCount();
        assertTrue(rc.isPresent());
        assertEquals(3L, rc.getAsLong());
    }

    public void testCacheHitAlsoPublishesSizeInBytes() throws Exception {
        String content = "id:integer,n:integer\n1,10\n2,20\n";
        StorageObject o = obj(content);
        ExternalRowCountCache.put(o, 2L);
        SourceMetadata md = new CsvFormatReader(blockFactory).metadata(o);
        assertTrue(md.statistics().isPresent());
        assertEquals(content.getBytes(StandardCharsets.UTF_8).length, md.statistics().get().sizeInBytes().getAsLong());
    }

    /** Unique path + stable mtime per object — the cache only keys on (path, length), but matching real-file shape. */
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
