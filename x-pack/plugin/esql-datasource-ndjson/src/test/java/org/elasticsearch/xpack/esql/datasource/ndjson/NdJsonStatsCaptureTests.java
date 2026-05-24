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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCache;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;

/** NDJSON counterpart of {@code CsvRowCountCaptureTests}: the cache-write side on close. */
public class NdJsonStatsCaptureTests extends ESTestCase {

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

    /** SKIP_ROW emits HeaderWarning; drop the context so ensureNoWarnings sees an empty list. */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    public void testWholeFileCleanDrainPopulatesCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            drain(it);
        }
        OptionalLong cached = ExternalStatsCache.lookupRowCount(o);
        assertTrue("clean whole-file drain must populate cache", cached.isPresent());
        assertEquals(3L, cached.getAsLong());
    }

    public void testCloseWithoutFullDrainDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            if (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        assertTrue("close-before-EOF must not populate cache", ExternalStatsCache.lookup(o).isEmpty());
    }

    public void testNonFirstSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).lastSplit(true).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-first split read must not populate cache", ExternalStatsCache.lookup(o).isEmpty());
    }

    public void testNonLastSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(true).lastSplit(false).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-last split read must not populate cache", ExternalStatsCache.lookup(o).isEmpty());
    }

    public void testRecordAlignedDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).recordAligned(true).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("record-aligned (parallel-sliced) read must not populate cache", ExternalStatsCache.lookup(o).isEmpty());
    }

    /** SKIP_ROW with a malformed line → errorCount > 0 → gate suppresses the cache write. */
    public void testSkipRowWithErrorsDoesNotPopulateCache() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("{\"a\":1}\nnot-a-json-object\n{\"a\":3}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("SKIP_ROW with errors must not populate cache (count is policy-dependent)", ExternalStatsCache.lookup(o).isEmpty());
    }

    /** rowLimit-cut iteration ends without natural EOF → cache write suppressed. */
    public void testRowLimitTruncatedReadDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(2).rowLimit(2).build();
        try (CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("rowLimit-truncated read must not populate cache", ExternalStatsCache.lookup(o).isEmpty());
    }

    public void testWholeFileCleanDrainPopulatesColumnStats() throws Exception {
        StorageObject o = obj("{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\"}\n{\"id\":3,\"name\":\"gamma\"}\n");
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            drain(it);
        }
        java.util.Optional<ExternalStatsCache.Stats> cached = ExternalStatsCache.lookup(o);
        assertTrue("clean whole-file drain must populate cache", cached.isPresent());
        assertEquals(3L, cached.get().rowCount());
        java.util.Map<String, ExternalStatsCache.ColumnStats> cols = cached.get().columns();
        assertFalse("at least one column must have captured stats", cols.isEmpty());
        ExternalStatsCache.ColumnStats id = cols.get("id");
        assertNotNull("id column stats expected", id);
        assertEquals(0L, id.nullCount());
        // NDJSON schema inference resolves integer literals as INTEGER, not LONG.
        assertEquals(1, id.min());
        assertEquals(3, id.max());
        ExternalStatsCache.ColumnStats name = cols.get("name");
        assertNotNull("name column stats expected", name);
        assertEquals(new org.apache.lucene.util.BytesRef("alpha"), name.min());
        assertEquals(new org.apache.lucene.util.BytesRef("gamma"), name.max());
    }

    public void testMissingJsonKeyIncrementsNullCount() throws Exception {
        StorageObject o = obj("{\"id\":1,\"name\":\"a\"}\n{\"id\":2}\n{\"id\":3,\"name\":\"c\"}\n");
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            drain(it);
        }
        java.util.Optional<ExternalStatsCache.Stats> cached = ExternalStatsCache.lookup(o);
        assertTrue(cached.isPresent());
        ExternalStatsCache.ColumnStats name = cached.get().columns().get("name");
        assertEquals("missing JSON key must increment nullCount", 1L, name.nullCount());
        assertEquals(new org.apache.lucene.util.BytesRef("a"), name.min());
        assertEquals(new org.apache.lucene.util.BytesRef("c"), name.max());
    }

    public void testStreamOnlyCaptureRecordsBytesRead() throws Exception {
        String body = "{\"id\":1}\n{\"id\":2}\n";
        StorageObject streamOnly = streamOnlyObj(body);
        try (
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                streamOnly,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            drain(it);
        }
        java.util.Optional<ExternalStatsCache.Stats> cached = ExternalStatsCache.lookup(streamOnly);
        assertTrue("stream-only whole-file drain must populate cache", cached.isPresent());
        assertTrue("bytesRead must be present for stream-only sources", cached.get().bytesRead().isPresent());
        assertEquals(body.getBytes(StandardCharsets.UTF_8).length, cached.get().bytesRead().getAsLong());
    }

    private StorageObject streamOnlyObj(String ndjson) {
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
                throw new UnsupportedOperationException("Decompressed length is unknown");
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

    private static void drain(CloseableIterator<Page> it) {
        while (it.hasNext()) {
            it.next().releaseBlocks();
        }
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
