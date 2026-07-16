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
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
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
 * <p>Stripes are a pure addressing grid; the reader attributes each record to {@code floor(recordStart / B)}
 * (recordStart = the byte of the record's opening brace) as it parses, and emits one fragment per stripe the
 * chunk's byte range overlaps using the byte-range cover model shared with the CSV reader (see
 * {@code StripeStatsHarvester}). A page is NOT capped at stripe lines — it may span stripes; the iterator
 * splits the page's rows by their recorded offsets. Each fragment's byte sub-range is the chunk range clamped
 * to the stripe's grid cell, so sibling chunks' fragments for a split stripe tile contiguously.
 *
 * <p>The format-agnostic byte-range-cover GEOMETRY (dense ordinals, contiguous tiling, empty stripes for
 * oversized records, the partial trailing stripe of a non-final chunk, a record starting on a stripe boundary)
 * is tested directly at the shared component's own layer in {@code StripeStatsHarvesterTests} and is not
 * duplicated here. What stays in this file is reader-specific: end-to-end folds through the production
 * reconciler, the harvest-scope (COUNT/PROJECTED/ALL) tests, and the NDJSON iterator's page-cap / page-split
 * behavior ({@link #testPageWouldStraddleStripeButIsCapped}, {@link #testTinyBatchSplitsStripeAcrossPagesThenFolds}),
 * which exercise the decoder's own paging rather than the harvester's cover math.
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
    /**
     * Byte-array cap-drop safe-miss (mirrors CSV's {@code testMaxRecordSizeDropSafeMissesStripeCapture}).
     * On the recordAligned (byte-array) path an oversized record is dropped and decoding CONTINUES, so the
     * harvested row count is {@code max_record_size}-dependent. Because the cap is a query pragma and not in
     * the cache fingerprint, the whole publish must safe-miss — otherwise a warm aggregate under a different
     * cap would serve this scan's under-count. Proven to publish an under-count without the capDropped gate.
     */
    public void testMaxRecordSizeByteArrayDropSafeMissesCapture() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("{\"a\":").append(i).append("}\n");
        }
        sb.append("{\"a\":\"").append("x".repeat(200)).append("\"}\n"); // one record far over a 64-byte cap
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .stats(0, 1000, true)
            .maxRecordBytes(64)
            .errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false)) // non-strict: drop the oversized record
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
        assertNull("byte-array cap-drop must publish no stats (max_record_size is not fingerprinted)", sink.get(o.path().toString()));
    }

    /**
     * G2 (elastic/elasticsearch#150920): a {@code rowLimit} slice truncates a page after the decoder recorded its
     * full record count, which would desync the per-record offset array from the sliced page and trip
     * {@code forEachRun}'s {@code recordCount == positionCount} assert on a legitimate truncation. So stripe
     * tracking must be disabled under a rowLimit ({@code statsStripeSize -> -1}) — a recordAligned context WITH a
     * rowLimit must publish no stripe stats (safe-miss), never harvest and risk the assert. Proven to harvest
     * (non-null publish) without the {@code rowLimit == NO_LIMIT} conjunct.
     */
    public void testRowLimitDisablesStripeCapture() throws Exception {
        byte[] bytes = ndjson(0, 20);
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(3) // small batch so a rowLimit slice lands mid-page
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .stats(0, 8, true)
            .rowLimit(5)
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
        assertNull("a rowLimit read must not harvest stripe stats (safe-miss)", sink.get(o.path().toString()));
    }

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
     * records over {@code totalBytes} bytes read by one file-final scan under the byte-range cover model:
     * ordinals are dense from 0; every stripe is complete on both sides (atStart and atEnd) so the
     * coordinator can fold it without a continuation; only the terminal stripe is eof; the head covers byte 0;
     * the byte sub-ranges tile contiguously across stripe boundaries (each stripe's start == the previous
     * stripe's end, grid-clamped) and the last stripe closes to the file's byte length; and the rows sum
     * exactly. Empty (zero-row) stripes carry their grid-width byte sub-range (start &lt; end), not a
     * zero-length range — that contiguity is what makes a single whole-file scan's cover complete.
     */
    private void assertDenseFileFinalCover(List<Frag> frags, long totalRows, long totalBytes) {
        assertFalse("a non-empty file must emit at least one stripe fragment", frags.isEmpty());
        long rowSum = 0;
        long expectedOrdinal = 0;
        long expectedStart = 0;
        for (int i = 0; i < frags.size(); i++) {
            Frag f = frags.get(i);
            assertEquals("ordinals must be dense from 0 (empties fill oversized-record gaps)", expectedOrdinal, f.ordinal);
            assertTrue("every stripe of a file-final scan must anchor its left edge", f.atStart);
            assertTrue("every stripe of a file-final scan must be complete on the right", f.atEnd);
            assertEquals("only the terminal stripe is eof", i == frags.size() - 1, f.eof);
            assertTrue("coverage end must not precede start", f.end >= f.start);
            assertEquals("byte sub-ranges must tile contiguously across stripe boundaries", expectedStart, f.start);
            expectedStart = f.end;
            rowSum += f.rows;
            expectedOrdinal++;
        }
        assertEquals("the head fragment must cover the file's first byte", 0L, frags.get(0).start);
        assertEquals("the last stripe must close to the file's byte length", totalBytes, frags.get(frags.size() - 1).end);
        assertEquals("per-stripe rows must sum to the file's true row count", totalRows, rowSum);
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

    public void testTinyBatchSplitsStripeAcrossPagesThenFolds() throws Exception {
        // batchSize=1 forces one record per page; multiple pages land in the same stripe and must
        // aggregate into a single per-stripe fragment (not one fragment per page).
        byte[] data = ndjson(1, 9);
        List<Frag> frags = captureStripes(data, 0, true, true, 1, 32);
        assertDenseFileFinalCover(frags, 9, 9L * RECORD_BYTES);
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

    // ---- Harvest-scope tests (esql.source.cache.stripe.columns) -------------------------------------

    /**
     * COUNT(*) — zero projected columns — must still harvest each stripe's row count under count/projected/all
     * and fold to the exact total. The NDJSON mirror of the regression: {@code captureBlockStats} used to
     * return early on a zero-block page, harvesting nothing for COUNT(*).
     */
    public void testCountStarHarvestsRowCountUnderCountProjectedAll() throws Exception {
        for (StripeColumnScope scope : List.of(StripeColumnScope.COUNT, StripeColumnScope.PROJECTED, StripeColumnScope.ALL)) {
            int total = 10;
            byte[] full = ndjson(1, total);
            int cut = 4 * RECORD_BYTES;
            long stripe = 16;
            List<Map<String, Object>> frags = new ArrayList<>();
            frags.addAll(captureScoped(slice(full, 0, cut), 0, true, false, 1000, stripe, null, scope));
            frags.addAll(captureScoped(slice(full, cut, full.length), cut, false, true, 1000, stripe, null, scope));
            assertFoldsTo(frags, total, "scope=" + scope);
        }
    }

    /** NONE harvests nothing — no contributions at all. */
    public void testNoneHarvestsNothing() throws Exception {
        byte[] full = ndjson(1, 10);
        List<Map<String, Object>> frags = captureScoped(full, 0, true, true, 1000, 16, null, StripeColumnScope.NONE);
        assertTrue("NONE must emit no stripe contributions", frags.isEmpty());
    }

    /** COUNT harvests rows but no per-column min/max; PROJECTED harvests min/max for the projected column "a". */
    public void testCountVsProjectedColumnHarvest() throws Exception {
        int total = 10;
        byte[] full = ndjson(0, total);
        long stripe = 1024; // one stripe over the whole file

        List<Map<String, Object>> countFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("a"), StripeColumnScope.COUNT);
        assertEquals("count scope folds row count", total, foldedRowCount(countFrags));
        assertFalse("count scope must NOT carry per-column stats", hasAnyColumnStat(countFrags, "a"));

        List<Map<String, Object>> projFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("a"), StripeColumnScope.PROJECTED);
        assertEquals("projected scope folds row count", total, foldedRowCount(projFrags));
        assertTrue("projected scope must carry per-column stats for a", hasAnyColumnStat(projFrags, "a"));
    }

    /**
     * ALL scope, projecting only field "a", must harvest per-stripe stats for the NON-projected field "b" too
     * — absent under PROJECTED. NDJSON is self-describing: ALL widens the decode to the full file schema, so
     * "b" lands in the folded whole-file summary even though the query never read it.
     */
    public void testAllScopeHarvestsUnprojectedField() throws Exception {
        byte[] full = ndjsonTwoField(10); // a=0..9, b=100..109 (LONG)
        long stripe = 4096; // one stripe over the whole file
        List<Attribute> twoFieldSchema = List.of(longCol("a"), longCol("b"));

        List<Map<String, Object>> allFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("a"), StripeColumnScope.ALL);
        Map<String, Object> meta = reconcileToMetadata(allFrags, twoFieldSchema);
        assertEquals("ALL folds the exact row count", 10L, ((Number) meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
        assertEquals(
            "ALL harvests unprojected field b min",
            100L,
            ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "b")).longValue()
        );
        assertEquals(
            "ALL harvests unprojected field b max",
            109L,
            ((Number) SourceStatisticsSerializer.extractColumnMax(meta, "b")).longValue()
        );
        assertEquals(
            "ALL harvests unprojected field b null_count",
            0L,
            SourceStatisticsSerializer.extractColumnNullCount(meta, "b").longValue()
        );

        // Russian-doll superset: PROJECTED("a") commits "a" but NOT "b"; ALL commits both.
        List<Map<String, Object>> projFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("a"), StripeColumnScope.PROJECTED);
        assertTrue("PROJECTED commits the projected field a", hasAnyColumnStat(projFrags, "a"));
        assertFalse("PROJECTED must NOT commit the unprojected field b", hasAnyColumnStat(projFrags, "b"));
        assertTrue("ALL commits the projected field a", hasAnyColumnStat(allFrags, "a"));
        assertTrue("ALL commits the unprojected field b (superset of PROJECTED)", hasAnyColumnStat(allFrags, "b"));
    }

    /**
     * ALL under a COUNT(*) read (zero projected columns) still harvests every file field, while PROJECTED over
     * the same read commits no column stats — the Russian-doll superset (NONE ⊂ COUNT ⊂ PROJECTED ⊂ ALL).
     */
    public void testAllScopeUnderCountStarHarvestsEveryField() throws Exception {
        byte[] full = ndjsonTwoField(10);
        long stripe = 4096;
        List<Attribute> twoFieldSchema = List.of(longCol("a"), longCol("b"));

        List<Map<String, Object>> allFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of(), StripeColumnScope.ALL);
        Map<String, Object> meta = reconcileToMetadata(allFrags, twoFieldSchema);
        assertEquals(
            "ALL+COUNT(*) folds the exact row count",
            10L,
            ((Number) meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()
        );
        assertEquals("ALL+COUNT(*) harvests a", 0L, ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "a")).longValue());
        assertEquals("ALL+COUNT(*) harvests b", 100L, ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "b")).longValue());

        List<Map<String, Object>> projFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of(), StripeColumnScope.PROJECTED);
        assertEquals("COUNT(*) still folds rows under PROJECTED", 10L, foldedRowCount(projFrags));
        assertFalse("PROJECTED+COUNT(*) commits no field a", hasAnyColumnStat(projFrags, "a"));
        assertFalse("PROJECTED+COUNT(*) commits no field b", hasAnyColumnStat(projFrags, "b"));
    }

    private static Attribute longCol(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.LONG, Nullability.TRUE, null, false);
    }

    // {"a":N,"b":100+N}\n — two LONG fields, fixed structure.
    private static byte[] ndjsonTwoField(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append("{\"a\":").append(i).append(",\"b\":").append(100 + i).append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Reconciles fragments through the production cache service and returns the enriched safeMetadata. */
    private Map<String, Object> reconcileToMetadata(List<Map<String, Object>> fragments, List<Attribute> schema) throws Exception {
        assertFalse("expected real reader fragments", fragments.isEmpty());
        String fingerprint = (String) fragments.get(0).get(ExternalStats.CONFIG_FINGERPRINT_KEY);
        long mtime = ((Number) fragments.get(0).get(ExternalStats.MTIME_MILLIS_KEY)).longValue();
        String path = "memory://stripe-fold-" + UUID.randomUUID() + ".ndjson";
        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.listing.ttl", "30s")
            .build();
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(settings)) {
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".ndjson", Map.of());
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "ndjson", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint), Map.of())
            );
            service.reconcileSourceStatsFromContributions(Map.of(path, fragments));
            SchemaCacheEntry enriched = service.getOrComputeSchema(
                key,
                k -> { throw new AssertionError("schema entry must remain cached"); }
            );
            return enriched.safeMetadata();
        }
    }

    private List<Map<String, Object>> captureScoped(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<String> projectedColumns,
        StripeColumnScope scope
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .stats(baseOffset, stripeSize, fileFinal)
            .statsColumnScope(scope)
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

    /** True iff any stripe fragment carries a {@code _stats.columns.<name>.*} key. */
    private static boolean hasAnyColumnStat(List<Map<String, Object>> fragments, String column) {
        String prefix = SourceStatisticsSerializer.STATS_COL_PREFIX + column + ".";
        for (Map<String, Object> frag : fragments) {
            for (String key : frag.keySet()) {
                if (key.startsWith(prefix)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Sums every fragment's row count (each fragment is a per-stripe partial). */
    private static long foldedRowCount(List<Map<String, Object>> fragments) {
        long total = 0;
        for (Map<String, Object> frag : fragments) {
            Object rc = frag.get(SourceStatisticsSerializer.STATS_ROW_COUNT);
            if (rc instanceof Number n) {
                total += n.longValue();
            }
        }
        return total;
    }

    private void assertFoldsTo(List<Map<String, Object>> fragments, long expectedRows, String message) throws Exception {
        assertFalse(message + ": expected real reader fragments", fragments.isEmpty());
        assertFoldsTo(fragments, expectedRows);
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

    public void testStreamingRecoverySkewSafeMissesStripeCapture() throws Exception {
        // reader-B2 (elastic/elasticsearch#150920): on the STREAMING decoder path a lenient parse-error recovery
        // rebuilds the parser and resets the byte baseline (parserSliceStart stays 0), so every record after the
        // malformed line is attributed a stripe TOO EARLY. forEachRun's alignment is count-only and NDJSON has no
        // emit-time byte tripwire, so the mis-attributed stripes would commit. Must safe-miss. Proven to publish
        // skewed fragments without the offsetBaselineLost gate.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            sb.append(i == 6 ? "{\"a\":!}\n" : "{\"a\":" + (i % 10) + "}\n"); // record 6 malformed, same 8-byte width
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject o = streamingMemoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .stats(0, 16, true) // 16-byte grid = 2 records/stripe -> the collapse is real
            .errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false))
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
        assertNull("a streaming-recovery offset-baseline reset must safe-miss the whole stripe publish", sink.get(o.path().toString()));
    }

    public void testCleanStreamingScanStillHarvestsStripes() throws Exception {
        // Over-suppression guard: offsetBaselineLost fires ONLY on recovery, so a clean streaming scan (no
        // malformed line) still harvests + publishes stripe stats. Also the suite's first streaming-path coverage.
        byte[] bytes = ndjson(0, 12);
        StorageObject o = streamingMemoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .stats(0, 24, true)
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
        assertNotNull("a clean streaming scan must still harvest stripe stats", raw);
        assertFalse("clean streaming scan must publish stripe fragments", raw.isEmpty());
        long totalRows = raw.stream().mapToLong(m -> ((Number) m.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()).sum();
        assertEquals("harvested rows total the file", 12L, totalRows);
    }

    /**
     * As {@link #memoryObject}, but {@code length()} is unavailable so the read takes the STREAMING decoder path
     * (same as a >16 MiB parallel segment whose byte-array fast path is declined).
     */
    private StorageObject streamingMemoryObject(byte[] bytes) {
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
                throw new UnsupportedOperationException("streaming-only");
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
