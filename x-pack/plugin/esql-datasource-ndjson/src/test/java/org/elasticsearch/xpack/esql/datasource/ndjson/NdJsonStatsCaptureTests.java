/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

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
 * NDJSON capture-on-close gate. The close hook publishes a flat {@code _stats.*} contribution to the
 * thread-bound {@link ExternalStatsCapture} sink (the only sink — the legacy JVM-static cache was
 * removed in favour of the unified SchemaCacheEntry + coordinator reconcile). Asserts via the
 * production {@link SplitStats#of} read path.
 */
public class NdJsonStatsCaptureTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
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
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).build());
        assertNotNull("clean whole-file drain must publish a contribution", c);
        assertFalse("a whole-file read is not a partial chunk", c.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
        assertEquals(3L, SplitStats.of(c).rowCount());
    }

    public void testCloseWithoutFullDrainPublishesNothing() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(
                o,
                FormatReadContext.builder().batchSize(10).build()
            )
        ) {
            if (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        assertNull("close-before-EOF must not publish", sink.get(o.path().toString()));
    }

    public void testNonFirstSplitPublishesNothing() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        assertNull(capture(o, FormatReadContext.builder().batchSize(10).firstSplit(false).lastSplit(true).build()));
    }

    public void testNonLastSplitPublishesNothing() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        assertNull(capture(o, FormatReadContext.builder().batchSize(10).firstSplit(true).lastSplit(false).build()));
    }

    public void testRecordAlignedPublishesPartialChunk() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).recordAligned(true).build());
        assertNotNull("a clean record-aligned chunk must publish its partial", c);
        assertTrue(c.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
        assertEquals(3L, SplitStats.of(c).rowCount());
    }

    /** SKIP_ROW drops a malformed line, but the stats over the surviving lines are exact (fingerprint pins error_mode), so they commit. */
    public void testSkipRowWithDroppedRowsCommitsStatsOverSurvivors() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("{\"a\":1}\nnot-a-json-object\n{\"a\":3}\n");
        Map<String, Object> published = capture(o, FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build());
        assertNotNull("a dropped line now commits the stats over surviving lines instead of publishing nothing", published);
        SplitStats stats = SplitStats.of(published);
        assertNotNull(stats);
        // The cache fingerprint pins error_mode, so a full scan drops the SAME line -- every statistic over the
        // survivors (a in {1,3}, 2 lines) is exact vs that scan, so all commit and serve.
        assertEquals("row count over survivors", 2L, stats.rowCount());
        assertEquals(1, ((Number) stats.columnMin("a")).intValue());
        assertEquals(3, ((Number) stats.columnMax("a")).intValue());
    }

    /** rowLimit-cut iteration ends without natural EOF → write suppressed. */
    public void testRowLimitTruncatedReadPublishesNothing() throws Exception {
        StorageObject o = obj("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n");
        assertNull(capture(o, FormatReadContext.builder().batchSize(2).rowLimit(2).build()));
    }

    public void testWholeFileCleanDrainPublishesColumnStats() throws Exception {
        StorageObject o = obj("{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\"}\n{\"id\":3,\"name\":\"gamma\"}\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).build());
        assertNotNull(c);
        SplitStats stats = SplitStats.of(c);
        assertEquals(3L, stats.rowCount());
        assertEquals(0L, stats.columnNullCount("id"));
        assertEquals(1L, ((Number) stats.columnMin("id")).longValue());
        assertEquals(3L, ((Number) stats.columnMax("id")).longValue());
        assertEquals(new BytesRef("alpha"), stats.columnMin("name"));
        assertEquals(new BytesRef("gamma"), stats.columnMax("name"));
    }

    public void testMissingJsonKeyIncrementsNullCount() throws Exception {
        StorageObject o = obj("{\"id\":1,\"name\":\"a\"}\n{\"id\":2}\n{\"id\":3,\"name\":\"c\"}\n");
        Map<String, Object> c = capture(o, FormatReadContext.builder().batchSize(10).build());
        assertNotNull(c);
        SplitStats stats = SplitStats.of(c);
        assertEquals("missing JSON key must increment nullCount", 1L, stats.columnNullCount("name"));
        assertEquals(new BytesRef("a"), stats.columnMin("name"));
        assertEquals(new BytesRef("c"), stats.columnMax("name"));
    }

    public void testStreamOnlyCaptureRecordsSizeInBytes() throws Exception {
        String body = "{\"id\":1}\n{\"id\":2}\n";
        StorageObject streamOnly = streamOnlyObj(body);
        Map<String, Object> c = capture(streamOnly, FormatReadContext.builder().batchSize(10).build());
        assertNotNull("stream-only whole-file drain must publish a contribution", c);
        SplitStats stats = SplitStats.of(c);
        assertEquals(2L, stats.rowCount());
        assertEquals(
            "stream-only sources publish scan-counted bytes as sizeInBytes",
            body.getBytes(StandardCharsets.UTF_8).length,
            stats.sizeInBytes()
        );
    }

    /** Binds a capture sink, drains the reader to EOF, returns the single contribution for the path (or null). */
    private Map<String, Object> capture(StorageObject o, FormatReadContext ctx) throws Exception {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)
        ) {
            drain(it);
        }
        List<Map<String, Object>> c = sink.get(o.path().toString());
        return c == null || c.isEmpty() ? null : c.get(0);
    }

    private static void drain(CloseableIterator<Page> it) {
        while (it.hasNext()) {
            it.next().releaseBlocks();
        }
    }

    private StorageObject streamOnlyObj(String ndjson) {
        return memoryObject(ndjson, "memory://" + UUID.randomUUID() + ".ndjson.bz2", false);
    }

    private StorageObject obj(String ndjson) {
        return memoryObject(ndjson, "memory://" + UUID.randomUUID() + ".ndjson", true);
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
