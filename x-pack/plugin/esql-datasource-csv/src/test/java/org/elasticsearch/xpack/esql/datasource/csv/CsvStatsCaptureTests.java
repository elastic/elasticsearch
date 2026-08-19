/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Capture-on-close gate for CSV. The close hook publishes a flat {@code _stats.*} contribution to the
 * thread-bound {@link ExternalStatsCapture} sink (the only sink now — the legacy JVM-static cache was
 * removed in favour of the unified SchemaCacheEntry + coordinator reconcile). These tests bind a sink,
 * drain the reader, and assert the contribution via the production {@link SplitStats#of} read path.
 */
public class CsvStatsCaptureTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /** SKIP_ROW emits HeaderWarning; drop the context so ensureNoWarnings sees an empty list. */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    public void testWholeFileCleanDrainPublishesStats() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n4,40\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).build());
        assertNotNull("clean whole-file drain must publish a contribution", c);
        assertFalse("a whole-file read is not a partial chunk", c.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
        assertEquals(4L, SplitStats.of(c).rowCount());
    }

    public void testCloseWithoutFullDrainPublishesNothing() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, FormatReadContext.builder().batchSize(10).build())
        ) {
            if (it.hasNext()) {
                it.next().releaseBlocks();
            }
            // Close without reaching natural EOF — must not publish.
        }
        assertNull("close-before-EOF must not publish", sink.get(o.path().toString()));
    }

    public void testNonFirstSplitPublishesNothing() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        assertNull(
            "non-first split is neither a whole-file read nor a record-aligned chunk — no publish",
            capture(o, FormatReadContext.builder().batchSize(10).firstSplit(false).lastSplit(true).build())
        );
    }

    public void testNonLastSplitPublishesNothing() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        assertNull(
            "non-last split is neither a whole-file read nor a record-aligned chunk — no publish",
            capture(o, FormatReadContext.builder().batchSize(10).firstSplit(true).lastSplit(false).build())
        );
    }

    public void testRecordAlignedPublishesPartialChunk() throws Exception {
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,20\n3,30\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).recordAligned(true).build());
        assertNotNull("a clean record-aligned chunk must publish its partial", c);
        assertTrue("a record-aligned chunk publishes a partial-marked contribution", c.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
        assertEquals(3L, SplitStats.of(c).rowCount());
    }

    /** SKIP_ROW drops a malformed row, but the stats over the surviving rows are exact (fingerprint pins error_mode), so they commit. */
    public void testSkipRowWithDroppedRowsCommitsStatsOverSurvivors() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("id:integer,n:integer\n1,10\nnot-an-integer,20\n3,30\n");
        Map<String, Object> published = capture(o, FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build());
        assertNotNull("SKIP_ROW now commits the stats over surviving rows instead of publishing nothing", published);
        SplitStats stats = SplitStats.of(published);
        assertNotNull(stats);
        // The cache fingerprint pins error_mode, so a full scan drops the SAME row -- every statistic over the
        // survivors (id in {1,3}, 2 rows) is exact vs that scan, so all commit and serve.
        assertEquals("row count over survivors", 2L, stats.rowCount());
        assertEquals(1, ((Number) stats.columnMin("id")).intValue());
        assertEquals(3, ((Number) stats.columnMax("id")).intValue());
    }

    public void testWholeFileCleanDrainPublishesColumnStats() throws Exception {
        StorageObject o = obj("id:integer,n:integer,name:keyword\n1,10,alpha\n2,20,beta\n3,30,gamma\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).projectedColumns(List.of("id", "n", "name")).build());
        assertNotNull(c);
        SplitStats stats = SplitStats.of(c);
        assertEquals(3L, stats.rowCount());
        assertEquals(0L, stats.columnNullCount("id"));
        assertEquals(1L, ((Number) stats.columnMin("id")).longValue());
        assertEquals(3L, ((Number) stats.columnMax("id")).longValue());
        assertEquals(10L, ((Number) stats.columnMin("n")).longValue());
        assertEquals(30L, ((Number) stats.columnMax("n")).longValue());
        assertEquals(new BytesRef("alpha"), stats.columnMin("name"));
        assertEquals(new BytesRef("gamma"), stats.columnMax("name"));
    }

    /**
     * The direct-to-block path folds per-column null/min/max stats into the parse loop, while the
     * Jackson path re-walks the built blocks via {@code captureBlockStats}. For a cacheable read both
     * must publish identical column stats. A small batch size forces multi-page accumulation on both
     * arms, and the fixture mixes nulls, an empty keyword, signed integers, and negative doubles.
     */
    public void testDirectAndJacksonColumnStatsAgree() throws Exception {
        String csv = """
            id:long,n:integer,name:keyword,score:double
            5,10,alpha,1.5
            3,,bravo,
            9,30,,2.5
            1,20,charlie,-4.0
            """;
        List<String> cols = List.of("id", "n", "name", "score");
        FormatReadContext ctx = FormatReadContext.builder().batchSize(2).projectedColumns(cols).build();
        SplitStats direct = SplitStats.of(captureWith(new CsvFormatReader(blockFactory).withDirectBlockEnabled(true), csv, ctx));
        SplitStats jackson = SplitStats.of(captureWith(new CsvFormatReader(blockFactory).withDirectBlockEnabled(false), csv, ctx));
        assertEquals("row count", jackson.rowCount(), direct.rowCount());
        for (String col : cols) {
            assertEquals("nullCount[" + col + "]", jackson.columnNullCount(col), direct.columnNullCount(col));
            assertEquals("min[" + col + "]", jackson.columnMin(col), direct.columnMin(col));
            assertEquals("max[" + col + "]", jackson.columnMax(col), direct.columnMax(col));
        }
    }

    public void testNullValuesCountTowardsColumnNullCount() throws Exception {
        // n column has one null encoded as empty field
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,\n3,30\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).projectedColumns(List.of("id", "n")).build());
        assertNotNull(c);
        SplitStats stats = SplitStats.of(c);
        assertEquals(3L, stats.rowCount());
        assertEquals("null cell must increment nullCount", 1L, stats.columnNullCount("n"));
        assertEquals(10L, ((Number) stats.columnMin("n")).longValue());
        assertEquals(30L, ((Number) stats.columnMax("n")).longValue());
    }

    public void testStreamOnlyCaptureRecordsSizeInBytes() throws Exception {
        String body = "id:integer\n1\n2\n3\n";
        StorageObject streamOnly = streamOnlyObj(body);
        Map<String, Object> c = capture(streamOnly, FormatReadContext.builder().batchSize(10).build());
        assertNotNull("stream-only whole-file drain must publish a contribution", c);
        SplitStats stats = SplitStats.of(c);
        assertEquals(3L, stats.rowCount());
        assertEquals(
            "stream-only sources publish scan-counted bytes as sizeInBytes",
            body.getBytes(StandardCharsets.UTF_8).length,
            stats.sizeInBytes()
        );
    }

    /**
     * NULL_FIELD null-fills a malformed field but PRESERVES the row, so a parallel chunk's row count
     * stays accurate. The chunk must still publish its partial (so the file's COUNT(*) sum stays
     * complete). Poison is reserved for a scan cut short (LIMIT / error / truncation), not for a
     * row drop -- a dropped row still commits exact stats over the survivors.
     */
    public void testNullFieldChunkPublishesFullCountWithoutPoison() throws Exception {
        ErrorPolicy nullField = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 10, 1.0, false);
        // 3 data rows; row 2 has a non-integer in column n → null-filled, row preserved.
        StorageObject o = obj("id:integer,n:integer\n1,10\n2,not-an-int\n3,30\n");
        List<Map<String, Object>> contributions = captureAll(
            o,
            FormatReadContext.builder().batchSize(10).recordAligned(true).errorPolicy(nullField).build()
        );
        assertNotNull("NULL_FIELD chunk must still publish its partial — its row count is accurate", contributions);
        boolean anyPoison = contributions.stream().anyMatch(c -> Boolean.TRUE.equals(c.get(ExternalStats.CHUNK_HAD_ERRORS_KEY)));
        assertFalse("NULL_FIELD preserves rows, so the chunk must not poison the file", anyPoison);
        long published = contributions.stream().mapToLong(c -> SplitStats.of(c).rowCount()).max().orElse(-1L);
        assertEquals("the chunk's published row count must include the null-filled row", 3L, published);
    }

    public void testReproFfwTypeDriftValueCount() throws Exception {
        // File a: untyped header, 3 valid integer col values. Reconciled/anchor schema col:INTEGER pushed as readSchema.
        ErrorPolicy nullField = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 1.0, false);
        StorageObject fileA = obj("id,col,note\n1,123,alpha\n2,456,gamma\n3,789,delta\n");
        List<org.elasticsearch.xpack.esql.core.expression.Attribute> readSchema = List.of(
            attr("id", org.elasticsearch.xpack.esql.core.type.DataType.INTEGER),
            attr("col", org.elasticsearch.xpack.esql.core.type.DataType.INTEGER),
            attr("note", org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD)
        );
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(10)
            .projectedColumns(List.of("id", "col", "note"))
            .readSchema(readSchema)
            .errorPolicy(nullField)
            .build();
        Map<String, Object> c = capture(fileA, ctx);
        assertNotNull(c);
        SplitStats stats = SplitStats.of(c);
        assertEquals("rows", 3L, stats.rowCount());
        assertEquals("file a col value_count", 3L, stats.columnValueCount("col"));
    }

    public void testReproControlTypedHeaderNoReadSchema() throws Exception {
        // Control: typed header, NO readSchema (the path the passing tests use). Does value_count harvest at all?
        StorageObject o = obj("id:integer,col:integer,note:keyword\n1,123,alpha\n2,456,gamma\n3,789,delta\n");
        Map<String, Object> c = capture(
            o,
            FormatReadContext.builder().batchSize(10).projectedColumns(List.of("id", "col", "note")).build()
        );
        assertNotNull(c);
        SplitStats stats = SplitStats.of(c);
        assertEquals("rows", 3L, stats.rowCount());
        assertEquals("control col nullCount", 0L, stats.columnNullCount("col"));
        assertEquals("control col min", 123, ((Number) stats.columnMin("col")).intValue());
        assertEquals("control col value_count", 3L, stats.columnValueCount("col"));
    }

    private static org.elasticsearch.xpack.esql.core.expression.Attribute attr(
        String name,
        org.elasticsearch.xpack.esql.core.type.DataType type
    ) {
        return new org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute(
            org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
            name,
            type
        );
    }

    /** Binds a capture sink, drains the reader to EOF, returns the single contribution for the path (or null). */
    private Map<String, Object> capture(StorageObject o, FormatReadContext ctx) throws Exception {
        List<Map<String, Object>> all = captureAll(o, ctx);
        return all == null ? null : all.get(0);
    }

    /** Binds a capture sink, drains the given reader over a fresh cacheable object, returns its contribution. */
    private Map<String, Object> captureWith(CsvFormatReader reader, String csv, FormatReadContext ctx) throws Exception {
        StorageObject o = obj(csv);
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (var handle = ExternalStatsCapture.bind(sink); CloseableIterator<Page> it = reader.read(o, ctx)) {
            drain(it);
        }
        List<Map<String, Object>> c = sink.get(o.path().toString());
        assertNotNull("expected a published contribution", c);
        return c.get(0);
    }

    private List<Map<String, Object>> captureAll(StorageObject o, FormatReadContext ctx) throws Exception {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (var handle = ExternalStatsCapture.bind(sink); CloseableIterator<Page> it = new CsvFormatReader(blockFactory).read(o, ctx)) {
            drain(it);
        }
        List<Map<String, Object>> c = sink.get(o.path().toString());
        return c == null || c.isEmpty() ? null : c;
    }

    private static void drain(CloseableIterator<Page> it) {
        while (it.hasNext()) {
            it.next().releaseBlocks();
        }
    }

    private StorageObject streamOnlyObj(String csv) {
        return memoryObject(csv, "memory://" + UUID.randomUUID() + ".csv.bz2", false);
    }

    private StorageObject obj(String csvContent) {
        return memoryObject(csvContent, "memory://" + UUID.randomUUID() + ".csv", true);
    }

    private StorageObject memoryObject(String content, String uniquePath, boolean lengthKnown) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
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
                if (lengthKnown) {
                    return bytes.length;
                }
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
}
