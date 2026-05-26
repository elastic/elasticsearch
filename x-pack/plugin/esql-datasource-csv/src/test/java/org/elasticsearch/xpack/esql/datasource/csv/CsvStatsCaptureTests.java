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

/** Capture-on-close gate for CSV. Lookup side is in {@code CsvStatsMetadataLookupTests}. */
public class CsvStatsCaptureTests extends ESTestCase {

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
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n4,40\n");
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, FormatReadContext.builder().batchSize(10).build())) {
            drain(it);
        }
        OptionalLong cached = ExternalStatsCache.lookupAnyForTests(o)
            .map(s -> java.util.OptionalLong.of(s.rowCount()))
            .orElse(java.util.OptionalLong.empty());
        assertTrue("clean whole-file drain must populate cache", cached.isPresent());
        assertEquals(4L, cached.getAsLong());
    }

    public void testCloseWithoutFullDrainDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, FormatReadContext.builder().batchSize(10).build())) {
            if (it.hasNext()) {
                it.next().releaseBlocks();
            }
            // Close without reaching natural EOF — must not cache.
        }
        assertTrue("close-before-EOF must not populate cache", ExternalStatsCache.lookupAnyForTests(o).isEmpty());
    }

    public void testNonFirstSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(false).lastSplit(true).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-first split read must not populate cache", ExternalStatsCache.lookupAnyForTests(o).isEmpty());
    }

    public void testNonLastSplitDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).firstSplit(true).lastSplit(false).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("non-last split read must not populate cache", ExternalStatsCache.lookupAnyForTests(o).isEmpty());
    }

    public void testRecordAlignedDoesNotPopulateCache() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).recordAligned(true).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue("record-aligned (parallel-sliced) read must not populate cache", ExternalStatsCache.lookupAnyForTests(o).isEmpty());
    }

    /** SKIP_ROW with a malformed row → errorCount > 0 → gate suppresses the cache write. */
    public void testSkipRowWithErrorsDoesNotPopulateCache() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("id:integer,n:integer\n1,10\nnot-an-integer,20\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        assertTrue(
            "SKIP_ROW with errors must not populate cache (count is policy-dependent)",
            ExternalStatsCache.lookupAnyForTests(o).isEmpty()
        );
    }

    public void testWholeFileCleanDrainPopulatesColumnStats() throws Exception {
        StorageObject o = obj("id:integer,n:integer,name:keyword\n1,10,alpha\n2,20,beta\n3,30,gamma\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).projectedColumns(java.util.List.of("id", "n", "name")).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        java.util.Optional<ExternalStatsCache.Stats> cached = ExternalStatsCache.lookupAnyForTests(o);
        assertTrue("clean whole-file drain must populate cache", cached.isPresent());
        assertEquals(3L, cached.get().rowCount());
        java.util.Map<String, ExternalStatsCache.ColumnStats> cols = cached.get().columns();
        assertEquals(3, cols.size());
        ExternalStatsCache.ColumnStats id = cols.get("id");
        assertEquals(0L, id.nullCount());
        assertEquals(1, id.min());
        assertEquals(3, id.max());
        ExternalStatsCache.ColumnStats n = cols.get("n");
        assertEquals(10, n.min());
        assertEquals(30, n.max());
        ExternalStatsCache.ColumnStats name = cols.get("name");
        assertEquals(new org.apache.lucene.util.BytesRef("alpha"), name.min());
        assertEquals(new org.apache.lucene.util.BytesRef("gamma"), name.max());
    }

    public void testNullValuesCountTowardsColumnNullCount() throws Exception {
        // n column has one null encoded as empty field
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).projectedColumns(java.util.List.of("id", "n")).build();
        try (CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        java.util.Optional<ExternalStatsCache.Stats> cached = ExternalStatsCache.lookupAnyForTests(o);
        assertTrue(cached.isPresent());
        assertEquals(3L, cached.get().rowCount());
        ExternalStatsCache.ColumnStats n = cached.get().columns().get("n");
        assertEquals("null cell must increment nullCount", 1L, n.nullCount());
        assertEquals(10, n.min());
        assertEquals(30, n.max());
    }

    public void testStreamOnlyCaptureRecordsBytesRead() throws Exception {
        String body = "id:integer\n1\n2\n3\n";
        StorageObject streamOnly = streamOnlyObj(body);
        try (
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(
                streamOnly,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            drain(it);
        }
        java.util.Optional<ExternalStatsCache.Stats> cached = ExternalStatsCache.lookupAnyForTests(streamOnly);
        assertTrue("stream-only whole-file drain must populate cache", cached.isPresent());
        assertTrue("bytesRead must be present for stream-only sources", cached.get().bytesRead().isPresent());
        assertEquals(body.getBytes(StandardCharsets.UTF_8).length, cached.get().bytesRead().getAsLong());
    }

    /**
     * NULL_FIELD null-fills a malformed field but PRESERVES the row, so a parallel chunk's row count
     * stays accurate. The chunk must still publish its partial (so the file's COUNT(*) sum stays
     * complete) and must NOT poison the file — poison is reserved for SKIP_ROW, which drops rows.
     */
    public void testNullFieldChunkPublishesFullCountWithoutPoison() throws Exception {
        ErrorPolicy nullField = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 10, 1.0, false);
        // 3 data rows; row 2 has a non-integer in column n → null-filled, row preserved.
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,not-an-int\n3,30\n");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(10).recordAligned(true).errorPolicy(nullField).build();
        java.util.Map<String, java.util.List<java.util.Map<String, Object>>> sink =
            org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture.newSink();
        try (
            var handle = org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)
        ) {
            drain(it);
        }
        java.util.List<java.util.Map<String, Object>> contributions = sink.get(o.path().toString());
        assertNotNull("NULL_FIELD chunk must still publish its partial — its row count is accurate", contributions);
        boolean anyPoison = contributions.stream().anyMatch(c -> Boolean.TRUE.equals(c.get(ExternalStatsCache.CHUNK_HAD_ERRORS_KEY)));
        assertFalse("NULL_FIELD preserves rows, so the chunk must not poison the file", anyPoison);
        long published = contributions.stream()
            .filter(c -> c.containsKey(org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.STATS_ROW_COUNT))
            .mapToLong(
                c -> ((Number) c.get(org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()
            )
            .max()
            .orElse(-1L);
        assertEquals("the chunk's published row count must include the null-filled row", 3L, published);
    }

    private StorageObject streamOnlyObj(String csv) {
        byte[] bytes = csv.getBytes(StandardCharsets.UTF_8);
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
