/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheEntry;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Exact-stat validation for the orthogonal per-stripe stats path of the NDJSON reader. These are the
 * correctness gate the user mandated: a reader must never produce a silently-wrong cached aggregate, so
 * every misalignment between records, pages, stripes, and chunk boundaries is exercised against an exact
 * expected row count — both at the reader's fragment-emission layer and end-to-end through the production
 * coordinator reconciler ({@link ExternalSourceCacheService#reconcileSourceStatsFromContributions}).
 *
 * <p>Stripes are a pure addressing grid; the reader attributes each record to {@code floor(scanStart / B)}
 * (scanStart = the byte position from which the record is scanned, i.e. the end of the previous record or
 * the chunk's first byte) as it parses, and emits one fragment per {@code (chunk, stripe)}. The decoder
 * caps each page at the record where the offset crosses a stripe line, so a page never straddles a stripe.
 */
public class NdJsonStripeStatsCaptureTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    // One fixed-width NDJSON record: {"a":N}\n == 8 bytes for single-digit N. Fixed width keeps byte
    // offsets predictable so stripe-boundary placement is exact.
    private static final int RECORD_BYTES = 8;

    private static byte[] ndjson(int firstValue, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            int v = (firstValue + i) % 10;
            sb.append("{\"a\":").append(v).append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** A parsed view of one emitted stripe fragment. */
    private record Frag(long ordinal, long rows, long start, long end, boolean atStart, boolean atEnd, boolean eof) {}

    /**
     * Reads {@code bytes} as one record-aligned chunk with stripe addressing at grid {@code stripeSize},
     * returning every per-stripe fragment the reader emits, sorted by ordinal.
     */
    private List<Frag> captureStripes(byte[] bytes, long baseOffset, boolean firstSplit, boolean fileFinal, int batchSize, long stripeSize)
        throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .stats(baseOffset, stripeSize, fileFinal)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        List<Frag> frags = new ArrayList<>();
        if (raw != null) {
            for (Map<String, Object> m : raw) {
                assertTrue("a stripe fragment must carry the partial-chunk marker", m.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
                assertTrue("a stripe fragment must carry a stripe ordinal", m.containsKey(ExternalStats.STRIPE_ORDINAL_KEY));
                frags.add(
                    new Frag(
                        ((Number) m.get(ExternalStats.STRIPE_ORDINAL_KEY)).longValue(),
                        ((Number) m.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue(),
                        ((Number) m.get(ExternalStats.COVERAGE_START_KEY)).longValue(),
                        ((Number) m.get(ExternalStats.COVERAGE_END_KEY)).longValue(),
                        (Boolean) m.get(ExternalStats.STRIPE_AT_START_KEY),
                        (Boolean) m.get(ExternalStats.STRIPE_AT_END_KEY),
                        (Boolean) m.get(ExternalStats.COVERAGE_IS_LAST_KEY)
                    )
                );
            }
        }
        frags.sort((x, y) -> Long.compare(x.ordinal, y.ordinal));
        return frags;
    }

    /** As {@link #captureStripes} but returns the raw contribution maps for feeding the reconciler. */
    private List<Map<String, Object>> captureRaw(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .stats(baseOffset, stripeSize, fileFinal)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new NdJsonFormatReader(null, blockFactory).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /**
     * Asserts the fragments form a complete, non-double-counting cover of a dense file of {@code totalRows}
     * records over {@code totalBytes} bytes read by one file-final scan: ordinals are dense from 0, every
     * stripe is complete on both sides (atStart and atEnd) so the coordinator can fold it without a
     * continuation, only the terminal stripe is eof, the head covers byte 0, the last stripe closes to the
     * file's byte length, and the rows sum exactly. (Byte ranges are NOT asserted contiguous across stripe
     * boundaries — the reconciler folds stripes by ordinal, not by inter-stripe byte adjacency.)
     */
    private void assertDenseFileFinalCover(List<Frag> frags, long totalRows, long totalBytes) {
        assertFalse("a non-empty file must emit at least one stripe fragment", frags.isEmpty());
        long rowSum = 0;
        long expectedOrdinal = 0;
        for (int i = 0; i < frags.size(); i++) {
            Frag f = frags.get(i);
            assertEquals("ordinals must be dense from 0 (empties fill oversized-record gaps)", expectedOrdinal, f.ordinal);
            assertTrue("every stripe of a file-final scan must anchor its left edge", f.atStart);
            assertTrue("every stripe of a file-final scan must be complete on the right", f.atEnd);
            assertEquals("only the terminal stripe is eof", i == frags.size() - 1, f.eof);
            assertTrue("coverage end must not precede start", f.end >= f.start);
            rowSum += f.rows;
            expectedOrdinal++;
        }
        assertEquals("the head fragment must cover the file's first byte", 0L, frags.get(0).start);
        assertEquals("the last stripe must close to the file's byte length", totalBytes, frags.get(frags.size() - 1).end);
        assertEquals("per-stripe rows must sum to the file's true row count", totalRows, rowSum);
    }

    public void testStripeSmallerThanBatchTilesDenselyAndSumsExact() throws Exception {
        // B (16) is far below the page's batchSize budget, so the decoder's page-cap — not batchSize —
        // ends each page. Records 8 bytes each: every ~2 records cross a stripe line.
        byte[] data = ndjson(1, 10); // 10 records, 80 bytes
        List<Frag> frags = captureStripes(data, 0, true, true, 1000, 16);
        assertDenseFileFinalCover(frags, 10, 10L * RECORD_BYTES);
    }

    public void testPageWouldStraddleStripeButIsCapped() throws Exception {
        // batchSize=1000 would pull the whole file into one page if uncapped; the per-stripe cap forces a
        // page break at each stripe line, so no fragment spans more than one stripe.
        byte[] data = ndjson(0, 12);
        List<Frag> frags = captureStripes(data, 0, true, true, 1000, 24);
        assertDenseFileFinalCover(frags, 12, 12L * RECORD_BYTES);
        // With a 96-byte file on a 24-byte grid, an uncapped huge-batch read would emit one fragment;
        // capping must split it into several per-stripe fragments instead.
        assertTrue("the per-stripe cap must split a huge-batch read into multiple stripe fragments", frags.size() > 1);
    }

    public void testRecordLargerThanStripeEmitsEmptyStripes() throws Exception {
        // B=3 < record size (8): every record spans multiple stripe rows, so stripe lines fall between
        // records and the skipped ordinals must surface as explicit empty (zero-row) fragments to keep
        // the cover contiguous.
        byte[] data = ndjson(1, 5); // 40 bytes
        List<Frag> frags = captureStripes(data, 0, true, true, 1000, 3);
        assertDenseFileFinalCover(frags, 5, 40);
        long emptyCount = frags.stream().filter(f -> f.rows == 0).count();
        assertTrue("oversized records must create at least one empty stripe", emptyCount > 0);
        for (Frag f : frags) {
            if (f.rows == 0) {
                assertEquals("an empty stripe is zero-length", f.start, f.end);
            }
        }
    }

    public void testRecordStartOnStripeBoundary() throws Exception {
        // B=7: record 1's scan-start (offset 7, just past record 0's '}') lands exactly on stripe line 7,
        // so record 1 opens stripe 1 cleanly. Tiling must stay exact.
        byte[] data = ndjson(2, 6);
        List<Frag> frags = captureStripes(data, 0, true, true, 1000, 7);
        assertDenseFileFinalCover(frags, 6, 6L * RECORD_BYTES);
    }

    public void testTinyBatchSplitsStripeAcrossPagesThenFolds() throws Exception {
        // batchSize=1 forces one record per page; multiple pages land in the same stripe and must
        // aggregate into a single per-stripe fragment (not one fragment per page).
        byte[] data = ndjson(1, 9);
        List<Frag> frags = captureStripes(data, 0, true, true, 1, 32);
        assertDenseFileFinalCover(frags, 9, 9L * RECORD_BYTES);
    }

    public void testNonFinalChunkTrailingStripeIsNotMarkedComplete() throws Exception {
        // A non-file-final chunk ends mid-stripe at a chunk boundary; its trailing stripe must be a
        // partial right fragment (atEnd=false, eof=false) so the next chunk's continuation completes the
        // cover. Marking it complete would silently drop the next chunk's records for that stripe.
        byte[] data = ndjson(1, 8); // 64 bytes
        List<Frag> frags = captureStripes(data, 0, true, false, 1000, 1024); // one big stripe, NOT file-final
        assertEquals("the whole non-final chunk is one stripe here", 1, frags.size());
        Frag only = frags.get(0);
        assertTrue("a non-final chunk's first stripe still anchors atStart at offset 0", only.atStart);
        assertFalse("a non-final chunk's trailing stripe must NOT be complete-on-the-right", only.atEnd);
        assertFalse("a non-final chunk must NOT mark its trailing stripe terminal", only.eof);
    }

    public void testTwoChunkScanFoldsToExactCountThroughReconciler() throws Exception {
        // Split a 10-record file into two record-aligned chunks at a record boundary, parse each with the
        // base offset and file-final flag the coordinator would set, and fold all fragments through the
        // production reconciler. The whole-file count must be exact.
        int total = 10;
        byte[] full = ndjson(1, total);
        int cut = 4 * RECORD_BYTES; // after record 3
        byte[] chunkA = slice(full, 0, cut);
        byte[] chunkB = slice(full, cut, full.length);

        long stripe = 16;
        List<Map<String, Object>> frags = new ArrayList<>();
        frags.addAll(captureRaw(chunkA, 0, true, false, 1000, stripe));
        frags.addAll(captureRaw(chunkB, cut, false, true, 1000, stripe));

        assertFoldsTo(frags, total);
    }

    public void testMisalignedScansFoldOnceThroughReconciler() throws Exception {
        // THE central guarantee end-to-end from real reader output: the SAME file read two ways —
        // once whole (file-final), once split into two chunks at a different boundary — contributes
        // record-canonical fragments that the reconciler folds to ONE exact count, never doubled.
        int total = 12;
        byte[] full = ndjson(3, total);
        long stripe = 20;

        List<Map<String, Object>> frags = new ArrayList<>();
        // Scan 1: whole file, file-final.
        frags.addAll(captureRaw(full, 0, true, true, 1000, stripe));
        // Scan 2: same file, split at record 5, different batchSize — misaligned paging of the shared stripes.
        int cut = 5 * RECORD_BYTES;
        frags.addAll(captureRaw(slice(full, 0, cut), 0, true, false, 3, stripe));
        frags.addAll(captureRaw(slice(full, cut, full.length), cut, false, true, 7, stripe));

        assertFoldsTo(frags, total);
    }

    /** Seeds the schema cache with the fragments' own fingerprint, reconciles, and asserts the folded row count. */
    private void assertFoldsTo(List<Map<String, Object>> fragments, long expectedRows) throws Exception {
        assertFalse("expected real reader fragments", fragments.isEmpty());
        String fingerprint = (String) fragments.get(0).get(ExternalStats.CONFIG_FINGERPRINT_KEY);
        long mtime = ((Number) fragments.get(0).get(ExternalStats.MTIME_MILLIS_KEY)).longValue();
        String path = "memory://stripe-fold-" + UUID.randomUUID() + ".ndjson";
        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(settings)) {
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of());
            List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG, Nullability.TRUE, null, false));
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "ndjson", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint), Map.of())
            );

            service.reconcileSourceStatsFromContributions(Map.of(path, fragments));

            SchemaCacheEntry enriched = service.getOrComputeSchema(
                key,
                k -> { throw new AssertionError("schema entry must remain cached"); }
            );
            assertEquals(
                "real reader fragments must fold to the exact whole-file row count",
                expectedRows,
                ((Number) enriched.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()
            );
        }
    }

    private static byte[] slice(byte[] src, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(src, from, out, 0, to - from);
        return out;
    }

    private StorageObject memoryObject(byte[] bytes) {
        String uniquePath = "memory://" + UUID.randomUUID() + ".ndjson";
        Instant fixedMtime = Instant.ofEpochMilli(1000L);
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
