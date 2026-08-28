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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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

    /**
     * SKIP_ROW plus a STRUCTURAL drop (a malformed line): decided while tokenising, before and independently of
     * the projection, so every scan shape drops the same line. The statistics over the survivors are exact for
     * every query carrying this identity and must commit -- the suppression below is scoped to projection-decided
     * drops, not to drops.
     */
    public void testSkipRowStructuralDropCommitsStatsOverSurvivors() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("{\"a\":1}\nnot-a-json-object\n{\"a\":3}\n");
        Map<String, Object> published = capture(o, FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build());
        assertNotNull("a structural drop is projection-independent; survivors must commit", published);
        SplitStats stats = SplitStats.of(published);
        assertNotNull(stats);
        assertEquals("row count over survivors", 2L, stats.rowCount());
        assertEquals(1, ((Number) stats.columnMin("a")).intValue());
        assertEquals(3, ((Number) stats.columnMax("a")).intValue());
    }

    /**
     * SKIP_ROW plus a coercion failure of a PROJECTED column: the survivor set is a function of the query's
     * projection, which the cache identity cannot carry -- a COUNT(*) scan of the same file decodes nothing,
     * drops nothing and answers 3 where this scan measured 2. The whole publish must be suppressed. The bound
     * readSchema types [a] as LONG deterministically, with no inference window in play, so the middle record's
     * value must fail coercion rather than widen the column.
     */
    public void testSkipRowCoercionDropSuppressesPublish() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("{\"a\":1}\n{\"a\":\"not-a-long\"}\n{\"a\":3}\n");
        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.LONG));
        long[] rows = new long[1];
        Map<String, Object> published = captureCounting(
            o,
            FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).readSchema(bound).build(),
            rows
        );
        assertEquals("the uncoercible record must actually drop", 2L, rows[0]);
        assertNull("a projection-dependent (coercion) drop must suppress the publish", published);
    }

    /**
     * A scalar then an object at the same name is not a value error. skip_row keeps every record and
     * the stats publish commits: nothing projection-dependent was dropped.
     */
    public void testSkipRowScalarThenObjectDoesNotSuppressPublish() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj("{\"a\":1}\n{\"a\":{\"b\":2}}\n{\"a\":3}\n");
        long[] rows = new long[1];
        Map<String, Object> published = captureCounting(
            o,
            FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).build(),
            rows
        );
        assertEquals("a scalar/object mix is not a skip_row drop", 3L, rows[0]);
        assertNotNull("no projection-dependent drop, so the publish commits", published);
    }

    /**
     * Jackson validates its string-length limit LAZILY: an over-limit string in a field the query does not
     * project is skipped undecoded and never trips it, so the line survives and the publish commits. This is the
     * measured fact that makes the projected twin below a projection-DEPENDENT drop. Should a Jackson upgrade
     * make the skip path enforce the limit, this test reddens and the constraints arm of projectionDependentDrop
     * becomes over-broad -- still safe, but worth rescoping then.
     */
    public void testOverLimitStringUnprojectedSurvivesAndCommits() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj(overLimitStringFixture());
        List<Attribute> bound = List.of(new ReferenceAttribute(Source.EMPTY, "a", DataType.LONG));
        long[] rows = new long[1];
        Map<String, Object> published = captureCounting(
            o,
            FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).readSchema(bound).build(),
            rows
        );
        assertEquals("the over-limit line survives when its string field is not projected", 3L, rows[0]);
        assertNotNull("nothing dropped, so the publish commits", published);
    }

    /**
     * The same file with [b] PROJECTED: the string-shaped decode arm reads the value, the lazy limit trips and
     * the line drops -- a drop the unprojected twin above proves a COUNT(*) scan does not take. Projection
     * decided it, so the publish must be suppressed.
     */
    public void testOverLimitStringProjectedDropsAndSuppressesPublish() throws Exception {
        ErrorPolicy skipRowQuiet = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 1.0, false);
        StorageObject o = obj(overLimitStringFixture());
        List<Attribute> bound = List.of(
            new ReferenceAttribute(Source.EMPTY, "a", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "b", DataType.KEYWORD)
        );
        long[] rows = new long[1];
        Map<String, Object> published = captureCounting(
            o,
            FormatReadContext.builder().batchSize(10).errorPolicy(skipRowQuiet).readSchema(bound).build(),
            rows
        );
        assertEquals("the over-limit line drops once its string field is projected", 2L, rows[0]);
        assertNull("a lazily-validated constraint drop is projection-dependent and must suppress", published);
    }

    /** Three records; the middle one carries a string past Jackson's default 20,000,000-char limit. */
    private static String overLimitStringFixture() {
        return "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"" + "y".repeat(20_000_001) + "\"}\n{\"a\":3,\"b\":\"z\"}\n";
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

    /** {@link #capture} plus the surviving row count, so a suppress assertion can never pass vacuously. */
    private Map<String, Object> captureCounting(StorageObject o, FormatReadContext ctx, long[] rowsOut) throws Exception {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)
        ) {
            while (it.hasNext()) {
                Page page = it.next();
                rowsOut[0] += page.getPositionCount();
                page.releaseBlocks();
            }
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
