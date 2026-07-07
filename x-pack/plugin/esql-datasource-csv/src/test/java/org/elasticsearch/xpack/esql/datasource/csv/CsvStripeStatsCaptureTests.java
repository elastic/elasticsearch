/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

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
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Exact-stat validation for the CSV reader's orthogonal per-stripe stats path, mirroring
 * {@code NdJsonStripeStatsCaptureTests}. The central guarantee: the same file read two ways — whole, and
 * split into record-aligned chunks at a different boundary — contributes record-canonical fragments that
 * the production coordinator reconciler folds to ONE exact row count, never doubled. CSV recovers each
 * record's file-global byte start on the fast Jackson path via {@link ByteOffsetTrackingReader}, so this
 * also exercises multibyte content where char offsets and byte offsets diverge.
 */
public class CsvStripeStatsCaptureTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    // The CSV reader emits response warning headers (escaped-mode config + null-marker) via
    // HeaderWarning.addWarning. Drop them at teardown so ESTestCase.ensureAllWarningsAsserted doesn't fail.
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    // Fixed-width single-column records keep byte offsets predictable. ASCII: "v\n" = 2 bytes.
    private static final int ASCII_RECORD_BYTES = 2;

    private static byte[] asciiCsv(int firstValue, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append((firstValue + i) % 10).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    // "é\n": 'é' is U+00E9 = 2 UTF-8 bytes, plus '\n' = 3 bytes per record. Char offset (2 chars/record)
    // and byte offset (3 bytes/record) diverge, so this exercises the char->byte cursor end to end.
    private static final int MULTIBYTE_RECORD_BYTES = 3;

    private static byte[] multibyteCsv(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append('é').append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public void testTwoChunkAsciiScanFoldsToExactCount() throws Exception {
        int total = 10;
        byte[] full = asciiCsv(1, total);
        int cut = 4 * ASCII_RECORD_BYTES; // after record 3, on a record boundary
        long stripe = 7;

        List<Map<String, Object>> frags = new ArrayList<>();
        frags.addAll(captureRaw(slice(full, 0, cut), 0, true, false, 1000, stripe));
        frags.addAll(captureRaw(slice(full, cut, full.length), cut, false, true, 1000, stripe));

        assertFoldsTo(frags, total);
    }

    public void testMisalignedAsciiScansFoldOnceThroughReconciler() throws Exception {
        int total = 12;
        byte[] full = asciiCsv(3, total);
        long stripe = 9;

        List<Map<String, Object>> frags = new ArrayList<>();
        // Whole-file, file-final.
        frags.addAll(captureRaw(full, 0, true, true, 1000, stripe));
        // Same file, split at a different boundary with a tiny batch — misaligned paging of shared stripes.
        int cut = 5 * ASCII_RECORD_BYTES;
        frags.addAll(captureRaw(slice(full, 0, cut), 0, true, false, 3, stripe));
        frags.addAll(captureRaw(slice(full, cut, full.length), cut, false, true, 2, stripe));

        assertFoldsTo(frags, total);
    }

    public void testMultibyteScansFoldToExactCount() throws Exception {
        int total = 9;
        byte[] full = multibyteCsv(total);
        long stripe = 8; // not a multiple of the 3-byte record width, so boundaries fall mid-record-grid
        int cut = 4 * MULTIBYTE_RECORD_BYTES;

        List<Map<String, Object>> frags = new ArrayList<>();
        frags.addAll(captureRaw(full, 0, true, true, 1000, stripe));
        frags.addAll(captureRaw(slice(full, 0, cut), 0, true, false, 1000, stripe));
        frags.addAll(captureRaw(slice(full, cut, full.length), cut, false, true, 1000, stripe));

        assertFoldsTo(frags, total);
    }

    public void testHeaderRowScansFoldOnceThroughReconciler() throws Exception {
        // A wide header (8 bytes) the first split must skip: the bulk byte tracker has to be based PAST the
        // header (splitStartByte + recordReader.bytesRead()), not at splitStartByte. A mis-based tracker
        // would shift the first split's record offsets by the header width while the non-first split (no
        // header) stays correct, so the two chunkings would attribute boundary records to different stripes
        // and the per-stripe fold would disagree.
        byte[] header = "colname\n".getBytes(StandardCharsets.UTF_8); // 8 bytes
        int total = 10;
        byte[] data = asciiCsv(1, total); // data records sit at file bytes 8,10,...,26
        byte[] full = new byte[header.length + data.length];
        System.arraycopy(header, 0, full, 0, header.length);
        System.arraycopy(data, 0, full, header.length, data.length);
        long stripe = 9;
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "colname", DataType.KEYWORD, Nullability.TRUE, null, false)
        );

        List<Map<String, Object>> frags = new ArrayList<>();
        // Whole file (skips the header), file-final.
        frags.addAll(captureRaw(full, 0, true, true, 1000, stripe, true, null));
        // Same file split at a record boundary: the first chunk skips the header; the non-first chunk has
        // no header and reads with the planner-resolved schema.
        int cut = header.length + 4 * ASCII_RECORD_BYTES;
        frags.addAll(captureRaw(slice(full, 0, cut), 0, true, false, 1000, stripe, true, null));
        frags.addAll(captureRaw(slice(full, cut, full.length), cut, false, true, 1000, stripe, true, schema));

        assertFoldsTo(frags, total);
    }

    // ---- Stripe-boundary page geometry (mirrors NdJsonStripeStatsCaptureTests) ----------------------

    /** A parsed view of one emitted stripe fragment. */
    private record Frag(long ordinal, long rows, long start, long end, boolean atStart, boolean atEnd, boolean eof) {}

    /**
     * Reads {@code bytes} as one record-aligned chunk with stripe addressing at grid {@code stripeSize},
     * returning every per-stripe fragment the CSV reader emits, sorted by ordinal.
     */
    private List<Frag> captureStripes(byte[] bytes, long baseOffset, boolean firstSplit, boolean fileFinal, int batchSize, long stripeSize)
        throws Exception {
        List<Map<String, Object>> raw = captureRaw(bytes, baseOffset, firstSplit, fileFinal, batchSize, stripeSize);
        List<Frag> frags = new ArrayList<>();
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
        frags.sort((x, y) -> Long.compare(x.ordinal, y.ordinal));
        return frags;
    }

    /**
     * Asserts the fragments form a complete, non-double-counting cover of a dense file of {@code totalRows}
     * records over {@code totalBytes} bytes read by one file-final scan under the byte-range cover model
     * (the shared assertion NDJSON's suite makes): ordinals are dense from 0; every stripe anchors both grid
     * lines ({@code atStart}/{@code atEnd}) so the coordinator folds it without a continuation; only the
     * terminal stripe is eof; byte sub-ranges tile contiguously (each stripe's start == the previous stripe's
     * grid-clamped end) and the last stripe closes to the file's byte length; and the rows sum exactly.
     */
    private void assertDenseFileFinalCover(List<Frag> frags, long totalRows, long totalBytes) {
        assertFalse("a non-empty file must emit at least one stripe fragment", frags.isEmpty());
        long rowSum = 0;
        long expectedOrdinal = 0;
        long expectedStart = 0;
        for (int i = 0; i < frags.size(); i++) {
            Frag f = frags.get(i);
            assertEquals("ordinals must be dense from 0 (empties fill oversized-record gaps)", expectedOrdinal, f.ordinal);
            assertTrue("every stripe of a file-final scan must anchor its left grid line", f.atStart);
            assertTrue("every stripe of a file-final scan must anchor its right grid line", f.atEnd);
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
        // page break at each stripe line, so no fragment spans more than one stripe. (CSV mirror of NDJSON's
        // testPageWouldStraddleStripeButIsCapped.)
        byte[] data = asciiCsv(0, 12); // 24 bytes on a 6-byte grid -> several stripes
        List<Frag> frags = captureStripes(data, 0, true, true, 1000, 6);
        assertDenseFileFinalCover(frags, 12, 12L * ASCII_RECORD_BYTES);
        // With a huge uncapped batch the reader would emit one fragment; capping at each stripe line must
        // split it into several per-stripe fragments instead.
        assertTrue("the per-stripe cap must split a huge-batch read into multiple stripe fragments", frags.size() > 1);
    }

    public void testTinyBatchSplitsStripeAcrossPagesThenFolds() throws Exception {
        // batchSize=1 forces one record per page; multiple pages land in the same stripe and must aggregate
        // into a single per-stripe fragment (not one fragment per page). CSV mirror of NDJSON's
        // testTinyBatchSplitsStripeAcrossPagesThenFolds. Grid 8 with 2-byte records = 4 records/stripe, so a
        // stripe genuinely spans several single-record pages.
        byte[] data = asciiCsv(1, 9);
        List<Frag> frags = captureStripes(data, 0, true, true, 1, 8);
        assertDenseFileFinalCover(frags, 9, 9L * ASCII_RECORD_BYTES);
    }

    // ---- Cross-path per-stripe fragment parity ------------------------------------------------------

    /**
     * The three CSV harvest paths that read a plain single-column file — bulk Jackson ({@code convertRowsToPage}
     * off the byte-tracking iterator), fused bracket ({@code convertLinesToPage} under
     * {@code multi_value_syntax:brackets}), and direct-to-block ({@code advanceDirectRecord}) — must emit
     * BYTE-FOR-BYTE identical per-stripe stat fragments for the same bytes. Fragments compose ACROSS queries
     * (the cache fingerprint excludes projection), so a start-definition shift that preserved per-scan totals
     * would still corrupt a later fold against a differently-pathed sibling — and the total-length tripwire,
     * which only checks the whole-chunk byte span, cannot catch a per-stripe redistribution. So this asserts
     * the per-stripe stat payload (ordinal -&gt; row_count + coverage geometry + every column's
     * min/max/null_count/value_count), not merely equal whole-file totals. Config-derived addressing keys
     * (fingerprint, mtime) legitimately differ by path and are excluded from the comparison.
     */
    public void testThreeHarvestPathsEmitIdenticalPerStripeFragments() throws Exception {
        byte[] header = "n\n".getBytes(StandardCharsets.UTF_8);
        int total = 12;
        byte[] full = concat(header, asciiCsv(0, total)); // single INTEGER column n, values 0..9,0,1
        long stripe = 8; // small grid -> several stripes across the file

        Map<Long, Map<String, Object>> jackson = perStripeStats(captureBulkJacksonPath(full, stripe));
        Map<Long, Map<String, Object>> bracket = perStripeStats(captureBracketScalarPath(full, stripe));
        Map<Long, Map<String, Object>> direct = perStripeStats(captureDirectBlockScalarPath(full, stripe));

        assertFalse("bulk-Jackson path must emit stripe fragments", jackson.isEmpty());
        assertEquals("bracket path must cover the same stripe ordinals as bulk-Jackson", jackson.keySet(), bracket.keySet());
        assertEquals("direct-block path must cover the same stripe ordinals as bulk-Jackson", jackson.keySet(), direct.keySet());
        // Byte-for-byte per stripe: a real start-definition shift on any one path surfaces here, not as a
        // (still-correct) whole-file total. If this ever fails it is a genuine cross-path attribution bug.
        assertEquals("bracket path must emit byte-identical per-stripe fragments to bulk-Jackson", jackson, bracket);
        assertEquals("direct-block path must emit byte-identical per-stripe fragments to bulk-Jackson", jackson, direct);
    }

    /**
     * Projects each fragment down to its stripe ordinal and the stat payload that must be path-invariant
     * (row count, coverage geometry, and every per-column stat), dropping the config-derived addressing keys
     * (fingerprint/mtime) that legitimately vary with the read config.
     */
    private static Map<Long, Map<String, Object>> perStripeStats(List<Map<String, Object>> fragments) {
        Map<Long, Map<String, Object>> byOrdinal = new HashMap<>();
        for (Map<String, Object> frag : fragments) {
            long ordinal = ((Number) frag.get(ExternalStats.STRIPE_ORDINAL_KEY)).longValue();
            Map<String, Object> payload = new HashMap<>();
            for (Map.Entry<String, Object> e : frag.entrySet()) {
                String key = e.getKey();
                if (key.equals(ExternalStats.CONFIG_FINGERPRINT_KEY) || key.equals(ExternalStats.MTIME_MILLIS_KEY)) {
                    continue; // config-derived, legitimately path-dependent
                }
                payload.put(key, e.getValue());
            }
            assertNull("one fragment per stripe ordinal per path", byOrdinal.put(ordinal, payload));
        }
        return byOrdinal;
    }

    /** Bulk Jackson path: plain single column, no _rowPosition, direct-to-block DISABLED so the read routes onto convertRowsToPage. */
    private List<Map<String, Object>> captureBulkJacksonPath(byte[] bytes, long stripeSize) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("n"))
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .splitStartByte(0)
            .stats(0, stripeSize, true)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withDirectBlockEnabled(false)
                .withConfig(Map.of(CsvFormatReader.CONFIG_HEADER_ROW, true))
                .read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /** Fused bracket path: multi_value_syntax=brackets reading the SAME scalar file (values without brackets parse as single-value). */
    private List<Map<String, Object>> captureBracketScalarPath(byte[] bytes, long stripeSize) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("n"))
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .splitStartByte(0)
            .stats(0, stripeSize, true)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, true, CsvFormatReader.CONFIG_MULTI_VALUE_SYNTAX, "brackets")
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /** Direct-to-block path: plain single projected column, no _rowPosition, direct-block enabled (default). */
    private List<Map<String, Object>> captureDirectBlockScalarPath(byte[] bytes, long stripeSize) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("n"))
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .splitStartByte(0)
            .stats(0, stripeSize, true)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, true)
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    // ---- Harvest-scope tests (esql.source.cache.stripe.columns) -------------------------------------

    /**
     * COUNT(*) — zero projected columns — must still harvest each stripe's row count under count/projected/all
     * so a warm COUNT(*) folds to the exact total. This is the direct regression test for the bug where
     * {@code captureBlockStats} returned early on {@code columnCount == 0}, harvesting nothing.
     */
    public void testCountStarHarvestsRowCountUnderCountProjectedAll() throws Exception {
        for (StripeColumnScope scope : List.of(StripeColumnScope.COUNT, StripeColumnScope.PROJECTED, StripeColumnScope.ALL)) {
            int total = 10;
            byte[] full = asciiCsv(1, total);
            int cut = 4 * ASCII_RECORD_BYTES;
            long stripe = 7;
            List<Map<String, Object>> frags = new ArrayList<>();
            frags.addAll(captureScoped(slice(full, 0, cut), 0, true, false, 1000, stripe, null, scope));
            frags.addAll(captureScoped(slice(full, cut, full.length), cut, false, true, 1000, stripe, null, scope));
            assertFoldsTo(frags, total, "scope=" + scope);
        }
    }

    /** NONE harvests nothing — no contributions emitted at all, so the warm path has nothing to serve. */
    public void testNoneHarvestsNothing() throws Exception {
        byte[] full = asciiCsv(1, 10);
        List<Map<String, Object>> frags = captureScoped(full, 0, true, true, 1000, 7, null, StripeColumnScope.NONE);
        assertTrue("NONE must emit no stripe contributions", frags.isEmpty());
    }

    /**
     * COUNT harvests rows but NO per-column min/max; PROJECTED harvests min/max for the projected column.
     * Single numeric column "n" with a header so the projected harvest produces a real min/max.
     */
    public void testCountVsProjectedColumnHarvest() throws Exception {
        byte[] header = "n\n".getBytes(StandardCharsets.UTF_8);
        int total = 10; // values 0..9
        byte[] data = asciiCsv(0, total);
        byte[] full = concat(header, data);
        long stripe = 64; // one stripe over the whole file
        List<Attribute> schema = List.of(intCol("n"));

        // COUNT: row count present, no _stats.columns.* keys.
        List<Map<String, Object>> countFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("n"), StripeColumnScope.COUNT);
        assertEquals("count scope folds row count", total, foldedRowCount(countFrags));
        assertFalse("count scope must NOT carry per-column stats", hasAnyColumnStat(countFrags, "n"));

        // PROJECTED: row count present AND min/max for "n".
        List<Map<String, Object>> projFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("n"), StripeColumnScope.PROJECTED);
        assertEquals("projected scope folds row count", total, foldedRowCount(projFrags));
        assertTrue("projected scope must carry per-column stats for n", hasAnyColumnStat(projFrags, "n"));
    }

    /**
     * ALL scope, projecting only column "a", must harvest per-stripe stats for the NON-projected column "b"
     * too: under PROJECTED, "b" would be absent from the folded whole-file summary. This is the headline ALL
     * capability — a cold scan that never read "b" into its output page still commits b's min/max/null.
     */
    public void testAllScopeHarvestsUnprojectedColumn() throws Exception {
        // header a,b ; rows a=0..9 (INTEGER), b=100..109 (INTEGER). Query projects only "a".
        byte[] full = twoColumnCsv(10);
        long stripe = 1024; // one stripe over the whole file
        List<Attribute> twoColSchema = List.of(intCol("a"), intCol("b"));

        List<Map<String, Object>> allFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("a"), StripeColumnScope.ALL);
        Map<String, Object> meta = reconcileToMetadata(allFrags, twoColSchema);
        assertEquals("ALL folds the exact row count", 10L, ((Number) meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
        // b was never projected, yet ALL committed its min/max/null.
        assertEquals(
            "ALL harvests unprojected column b min",
            100,
            ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "b")).intValue()
        );
        assertEquals(
            "ALL harvests unprojected column b max",
            109,
            ((Number) SourceStatisticsSerializer.extractColumnMax(meta, "b")).intValue()
        );
        assertEquals(
            "ALL harvests unprojected column b null_count",
            0L,
            SourceStatisticsSerializer.extractColumnNullCount(meta, "b").longValue()
        );

        // Russian-doll superset check: PROJECTED("a") commits "a" but NOT "b"; ALL commits both.
        List<Map<String, Object>> projFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of("a"), StripeColumnScope.PROJECTED);
        assertTrue("PROJECTED commits the projected column a", hasAnyColumnStat(projFrags, "a"));
        assertFalse("PROJECTED must NOT commit the unprojected column b", hasAnyColumnStat(projFrags, "b"));
        assertTrue("ALL commits the projected column a", hasAnyColumnStat(allFrags, "a"));
        assertTrue("ALL commits the unprojected column b (superset of PROJECTED)", hasAnyColumnStat(allFrags, "b"));
    }

    /**
     * ALL scope under a COUNT(*) read (zero projected columns) must STILL harvest every file column. PROJECTED
     * over the same zero-projection read commits no column stats at all, so ALL's column set is a strict
     * superset (Russian-doll: NONE ⊂ COUNT ⊂ PROJECTED ⊂ ALL).
     */
    public void testAllScopeUnderCountStarHarvestsEveryColumn() throws Exception {
        byte[] full = twoColumnCsv(10);
        long stripe = 1024;
        List<Attribute> twoColSchema = List.of(intCol("a"), intCol("b"));

        // COUNT(*) = null projection. ALL must commit BOTH columns even though the page carries none.
        List<Map<String, Object>> allFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of(), StripeColumnScope.ALL);
        Map<String, Object> meta = reconcileToMetadata(allFrags, twoColSchema);
        assertEquals(
            "ALL+COUNT(*) folds the exact row count",
            10L,
            ((Number) meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()
        );
        assertEquals("ALL+COUNT(*) harvests a", 0, ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "a")).intValue());
        assertEquals("ALL+COUNT(*) harvests b", 100, ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "b")).intValue());

        // PROJECTED over the same zero-projection read commits NO per-column stats (rows only).
        List<Map<String, Object>> projFrags = captureScoped(full, 0, true, true, 1000, stripe, List.of(), StripeColumnScope.PROJECTED);
        assertEquals("COUNT(*) still folds rows under PROJECTED", 10L, foldedRowCount(projFrags));
        assertFalse("PROJECTED+COUNT(*) commits no column a", hasAnyColumnStat(projFrags, "a"));
        assertFalse("PROJECTED+COUNT(*) commits no column b", hasAnyColumnStat(projFrags, "b"));
    }

    /**
     * B1 stats regression: under the no-trim default an ALL-scope read routes through the house per-record
     * tokenizer (Jackson's grammar does not apply), and stripe capture must still compose correctly over a
     * padded-quoted file. A padded quote ({@code  "Alice, PhD"}) is ONE field, so the following integer
     * column keeps its real values; the Jackson bug would mis-split it and corrupt {@code val}'s min/max.
     * The captured min/max/count are stripe-layout invariant, so they equal the hand-computed truth
     * regardless of the (deliberately small) stripe grid.
     */
    public void testAllScopeCaptureCorrectOnPaddedQuotedNoTrim() throws Exception {
        byte[] full = "name:keyword,val:integer\n \"Alice, PhD\", 10\n \"Bob, MD\", 20\n \"Carol, PhD\", 30\n".getBytes(
            StandardCharsets.UTF_8
        );
        long stripe = 20; // several stripes across the file, exercising per-stripe emit + reconcile fold
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "name", DataType.KEYWORD, Nullability.TRUE, null, false),
            intCol("val")
        );
        Map<String, Object> meta = reconcileToMetadata(
            captureScoped(full, 0, true, true, 1000, stripe, List.of("val"), StripeColumnScope.ALL),
            schema
        );
        assertEquals(
            "row count over the padded-quoted file",
            3L,
            ((Number) meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()
        );
        assertEquals(
            "val min (padding must not corrupt the integer column)",
            10,
            ((Number) SourceStatisticsSerializer.extractColumnMin(meta, "val")).intValue()
        );
        assertEquals("val max", 30, ((Number) SourceStatisticsSerializer.extractColumnMax(meta, "val")).intValue());
    }

    // header "a,b\n" + rows "i,100+i\n"; both columns inferred INTEGER.
    private static byte[] twoColumnCsv(int count) {
        StringBuilder sb = new StringBuilder("a,b\n");
        for (int i = 0; i < count; i++) {
            sb.append(i).append(',').append(100 + i).append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Reconciles fragments through the production cache service and returns the enriched safeMetadata. */
    private Map<String, Object> reconcileToMetadata(List<Map<String, Object>> fragments, List<Attribute> schema) throws Exception {
        assertFalse("expected real reader fragments", fragments.isEmpty());
        String fingerprint = (String) fragments.get(0).get(ExternalStats.CONFIG_FINGERPRINT_KEY);
        long mtime = ((Number) fragments.get(0).get(ExternalStats.MTIME_MILLIS_KEY)).longValue();
        String path = "memory://stripe-fold-" + UUID.randomUUID() + ".csv";
        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(settings)) {
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of());
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint), Map.of())
            );
            service.reconcileSourceStatsFromContributions(Map.of(path, fragments));
            SchemaCacheEntry enriched = service.getOrComputeSchema(
                key,
                k -> { throw new AssertionError("schema entry must remain cached"); }
            );
            return enriched.safeMetadata();
        }
    }

    public void testNullMtimeIsUncacheable() throws Exception {
        // A source with no reliable last-modified (lastModified() == null) must NOT cache its stats: the reader
        // keeps pinnedMtimeMillis at -1 and safe-misses. This is the invariant chunkStorageObject in
        // StreamingParallelParsingCoordinator relies on — it now passes a null mtime rather than a fabricated
        // Instant.EPOCH, which (being 0, i.e. >= 0) would pass the caching gate and cache the chunk under mtime 0.
        StorageObject o = memoryObject(asciiCsv(1, 10), null);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(1000)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .splitStartByte(0)
            .stats(0, 7, true)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false)
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        assertNull("a source with no last-modified must be uncacheable (safe-miss)", sink.get(o.path().toString()));
    }

    private List<Map<String, Object>> captureRaw(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize
    ) throws Exception {
        return captureRaw(bytes, baseOffset, firstSplit, fileFinal, batchSize, stripeSize, false, null);
    }

    private List<Map<String, Object>> captureRaw(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        boolean headerRow,
        List<Attribute> readSchema
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .readSchema(readSchema)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, headerRow)
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /**
     * Capture variant that drives an explicit projection (null = COUNT(*) zero projection) and harvest scope.
     * When a projection is given the CSV is read with a header row so the column resolves by name.
     */
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
        boolean headerRow = projectedColumns != null;
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .statsColumnScope(scope)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, headerRow)
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /**
     * Byte-exactness tripwire: a MALFORMED UTF-8 byte (bare Latin-1 {@code 0xE9}) is decoder-replaced with
     * U+FFFD, which the byte tracker counts at 3 bytes vs 1 actual -- every subsequent record offset is
     * skewed, so differently-chunked scans would attribute boundary records to different stripes and
     * interleave into a wrong warm count under a "complete" cover. The emit-time inferred-vs-actual byte
     * check must SAFE-MISS the whole chunk (commit no stripe fragment) rather than publish mis-attributed
     * offsets. The multibyte tests above pin that WELL-FORMED multibyte input does not trip it.
     */
    public void testMalformedUtf8SafeMissesStripeCapture() throws Exception {
        byte[] clean = asciiCsv(0, 10);
        // One more SINGLE-COLUMN row whose value is a bare Latin-1 'é' (0xE9): invalid UTF-8, the decoder
        // REPLACEs it with U+FFFD. The row parses cleanly (one column, matching the schema) -- this is a
        // byte-width skew, not a structural drop -- so the tracker over-counts its width (3 vs 1 actual).
        byte[] badRow = new byte[] { (byte) 0xE9, '\n' };
        byte[] full = new byte[clean.length + badRow.length];
        System.arraycopy(clean, 0, full, 0, clean.length);
        System.arraycopy(badRow, 0, full, clean.length, badRow.length);
        long stripe = 7; // small grid -> multiple stripes, so a skewed offset would mis-attribute

        List<Map<String, Object>> frags = captureRaw(full, 0, true, true, 1000, stripe);
        for (Map<String, Object> f : frags) {
            assertFalse(
                "malformed UTF-8 must safe-miss stripe capture, not commit skewed offsets: " + f,
                f.containsKey(ExternalStats.STRIPE_SIZE_KEY)
            );
        }
    }

    /**
     * F1 regression: the FUSED BRACKET path ({@code multi_value_syntax:brackets}) is a SECOND CSV page-builder
     * ({@code convertLinesToPage}) alongside {@code convertRowsToPage}. When {@code _rowPosition} is projected it
     * tracks per-record byte offsets, so on a row drop its emitted stripe stats must stay byte-aligned. Stripe
     * attribution reads {@code acceptedRowStartBytes}, which BOTH page builders must rebuild from the surviving
     * rows; if the fused path forgets to (the bug), it emits NO stripe fragment (capture disabled) or attributes
     * rows by stale offsets. Here a provided schema means no schema-inference prefetch, so the very first batch is
     * the fused path -- the strongest trigger. Reconciling must fold to the exact survivor count + correct min/max.
     */
    public void testFusedBracketPathWithRowDropStaysStripeAligned() throws Exception {
        int total = 30;
        int badRow = total / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < total; i++) {
            // brackets column `tags`; the mid-file row has a non-integer id -> dropped under skip_row.
            sb.append(i == badRow ? "notanint" : Integer.toString(i)).append(",[a,b]\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        long stripe = 8; // small grid -> many stripes across the file
        List<Attribute> schema = List.of(
            intCol("id"),
            new ReferenceAttribute(Source.EMPTY, null, "tags", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false);

        List<Map<String, Object>> frags = captureFusedBracket(data, 0, true, true, 3, stripe, schema, skipRow);
        assertFalse("fused bracket + _rowPosition stripe capture must emit fragments (not disable on a row drop)", frags.isEmpty());

        Map<String, Object> meta = reconcileToMetadata(frags, schema);
        assertEquals(
            "row count over survivors",
            (long) (total - 1),
            ((Number) meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()
        );
        assertEquals(0, ((Number) meta.get(SourceStatisticsSerializer.columnMinKey("id"))).intValue());
        assertEquals(total - 1, ((Number) meta.get(SourceStatisticsSerializer.columnMaxKey("id"))).intValue());
    }

    /**
     * A record over {@code max_record_size} is recovered by the error policy as a per-row DROP on the
     * record-reader path -- unlike a normal SKIP_ROW drop, that survivor loss is a function of the
     * max_record_size query PRAGMA, which is NOT in the cache fingerprint (only max_field_size is). So a
     * warm query under a larger cap would keep the row and count N, but would be served this scan's N-1.
     * The reader must safe-miss the whole publish (no stripe fragment, no whole-file stats) so the file
     * re-scans warm rather than caching a pragma-dependent count. (The bulk/Jackson path is immune: there
     * an over-cap record is stream-fatal via CsvRecordCappingInputStream, which the coordinator poisons.)
     */
    public void testMaxRecordSizeDropSafeMissesStripeCapture() throws Exception {
        int total = 20;
        int badRow = total / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < total; i++) {
            // Every row is a few bytes except the mid-file row, whose id field blows past the 24-byte cap.
            String id = i == badRow ? "9".repeat(40) : Integer.toString(i);
            sb.append(id).append(",[a,b]\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        long stripe = 8; // small grid -> many stripes, so a served fragment would be a real (wrong) commit
        List<Attribute> schema = List.of(
            intCol("id"),
            new ReferenceAttribute(Source.EMPTY, null, "tags", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false);

        List<Map<String, Object>> frags = captureFusedBracket(data, 0, true, true, 3, stripe, schema, skipRow, 24);
        assertTrue(
            "an over-max_record_size drop must safe-miss the whole publish (pragma not fingerprinted), got: " + frags,
            frags.isEmpty()
        );
    }

    public void testMalformedUtf8OnRecordReaderPathSafeMissesStripeCapture() throws Exception {
        // Fused-bracket / _rowPosition path: record offsets come from CsvLogicalRecordReader's char-inference,
        // NOT the bulk tracker. A bare Latin-1 'é' (0xE9) is invalid UTF-8; the decoder replaces it with U+FFFD,
        // counted 3 bytes vs 1 actual, so the record-reader's inferred end diverges from the CountingInputStream
        // and every later record offset is skewed. The emit-time tripwire (now covering this path too) must
        // safe-miss rather than commit mis-attributed stripes. Proven to emit fragments without the extension.
        StringBuilder head = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            head.append(i).append(",[a,b]\n");
        }
        byte[] cleanBytes = head.toString().getBytes(StandardCharsets.UTF_8);
        byte[] badRow = new byte[] { '1', '0', ',', '[', (byte) 0xE9, ',', 'b', ']', '\n' }; // malformed byte in a value
        byte[] data = new byte[cleanBytes.length + badRow.length];
        System.arraycopy(cleanBytes, 0, data, 0, cleanBytes.length);
        System.arraycopy(badRow, 0, data, cleanBytes.length, badRow.length);

        List<Attribute> schema = List.of(
            intCol("id"),
            new ReferenceAttribute(Source.EMPTY, null, "tags", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false);

        List<Map<String, Object>> frags = captureFusedBracket(data, 0, true, true, 3, 8, schema, skipRow);
        assertTrue("malformed UTF-8 on the record-reader path must safe-miss stripe capture, got: " + frags, frags.isEmpty());
    }

    /** Drives the fused bracket path with {@code _rowPosition} projected (byte tracking on) and a provided schema. */
    public void testBracketPathWithoutRowPositionHarvestsStripes() throws Exception {
        // S1 (elastic/elasticsearch#150920): the bracket-aware path (multi_value_syntax: brackets) with scope
        // PROJECTED and NO _rowPosition projected used to harvest NOTHING on a chunked read — a silent,
        // permanent warm miss for a whole configuration. It now tracks per-row offsets off the advanced
        // recordReader and emits stripe fragments, matching NDJSON and CSV's bulk path. Byte exactness is
        // guarded by the emit-time inferred-vs-actual tripwire, so a skew safe-misses rather than serving wrong.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 12; i++) {
            sb.append(i).append(",[a,b]\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        List<Attribute> schema = List.of(
            intCol("id"),
            new ReferenceAttribute(Source.EMPTY, null, "tags", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("id", "tags")) // NO _rowPosition -> the S1 case
            .batchSize(3)
            .recordAligned(true)
            .firstSplit(true)
            .lastSplit(true)
            .readSchema(schema)
            .splitStartByte(0)
            .stats(0, 8, true)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false, CsvFormatReader.CONFIG_MULTI_VALUE_SYNTAX, "brackets")
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        assertNotNull("bracket path without _rowPosition must now harvest stripe fragments", raw);
        assertFalse("expected non-empty stripe fragments", raw.isEmpty());
        long totalRows = raw.stream().mapToLong(m -> ((Number) m.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue()).sum();
        assertEquals("harvested per-stripe rows must total the file", 12L, totalRows);
    }

    private List<Map<String, Object>> captureFusedBracket(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<Attribute> readSchema,
        ErrorPolicy policy
    ) throws Exception {
        return captureFusedBracket(
            bytes,
            baseOffset,
            firstSplit,
            fileFinal,
            batchSize,
            stripeSize,
            readSchema,
            policy,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );
    }

    public void testMaxRecordSizeDropOnRecordReaderPathSafeMissesStripeCapture() throws Exception {
        // reader-B1 (elastic/elasticsearch#150920): the NON-bracket record-reader path (rowPositionSlot >= 0, no
        // multi_value_syntax) drops an over-max_record_size record via CsvLogicalRecordReader, but the typed
        // CsvRecordTooLargeException is laundered by ExternalFailures.surface into an unchecked wrapper, so the
        // batch loop's catch treated it as an ordinary skip and never set recordCapDropped -> N-1 published under a
        // fingerprint that ignores the cap. Must safe-miss. A provided schema means no sampling, so the drop lands
        // in the batch loop (pins the loop set-site). Proven to publish 19 rows without the fix.
        int total = 20;
        int badRow = total / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < total; i++) {
            String id = i == badRow ? "9".repeat(40) : Integer.toString(i);
            sb.append(id).append(",x\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        List<Attribute> schema = List.of(
            intCol("id"),
            new ReferenceAttribute(Source.EMPTY, null, "tags", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false);
        List<Map<String, Object>> frags = captureRecordReaderPath(data, 0, true, true, 3, 8, schema, skipRow, 24);
        assertTrue(
            "an over-max_record_size drop on the record-reader path must safe-miss the whole publish, got: " + frags,
            frags.isEmpty()
        );
    }

    public void testMaxRecordSizeDropOnDirectBlockPathSafeMissesStripeCapture() throws Exception {
        // The DEFAULT plain non-bracket read (no _rowPosition projected) takes the direct-to-block path
        // (advanceDirectRecord). An over-max_record_size drop there must set recordCapDropped and safe-miss the
        // whole publish, exactly like the bracket, Jackson-bulk, and sampling catch sites. max_record_size is a
        // query PRAGMA not in the cache fingerprint, so publishing N-1 as complete would serve a stale under-count
        // to a later query under a larger cap. Regression guard: the direct-block catch omitted the flag (would
        // publish 19 rows as complete without the fix).
        int total = 20;
        int badRow = total / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < total; i++) {
            String id = i == badRow ? "9".repeat(40) : Integer.toString(i);
            sb.append(id).append(",x\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        List<Attribute> schema = List.of(
            intCol("id"),
            new ReferenceAttribute(Source.EMPTY, null, "tags", DataType.KEYWORD, Nullability.TRUE, null, false)
        );
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false);
        List<Map<String, Object>> frags = captureDirectBlockPath(data, 0, true, true, 3, 8, schema, skipRow, 24);
        assertTrue(
            "an over-max_record_size drop on the direct-block path must safe-miss the whole publish, got: " + frags,
            frags.isEmpty()
        );
    }

    public void testMaxRecordSizeDropDuringSamplingSafeMissesPublish() throws Exception {
        // reader-B1 second set-site: with NO provided schema, headerless inference SAMPLES the file through the
        // same CsvRecordIterator; a cap-dropped row inside the sample window vanishes and the whole-file publish
        // would carry N-1 with recordCapDropped still false. Must safe-miss. Red if only the batch-loop set-site
        // (B1.1) is applied.
        int total = 20;
        int badRow = total / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < total; i++) {
            String id = i == badRow ? "9".repeat(40) : Integer.toString(i);
            sb.append(id).append(",x\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject o = memoryObject(data);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of(ColumnExtractor.ROW_POSITION_COLUMN, "col0")) // headerless synthetic name
            .batchSize(1000)
            .recordAligned(false) // whole-file read -> wholeFileRead publish path, headerless inference samples
            .firstSplit(true)
            .lastSplit(true)
            .stats(0, 1000, true)
            .errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 100, 1.0, false))
            .maxRecordBytes(24)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false)
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        assertNull("a cap drop during schema sampling must safe-miss the whole publish", sink.get(o.path().toString()));
    }

    /** Drives the NON-bracket record-reader path (_rowPosition projected, no multi_value_syntax) with a cap. */
    private List<Map<String, Object>> captureRecordReaderPath(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<Attribute> readSchema,
        ErrorPolicy policy,
        int maxRecordBytes
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of(ColumnExtractor.ROW_POSITION_COLUMN, "id")) // routes to CsvRecordIterator
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(fileFinal)
            .readSchema(readSchema)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .errorPolicy(policy)
            .maxRecordBytes(maxRecordBytes)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false) // NO multi_value_syntax -> non-bracket record-reader path
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /** Drives the direct-to-block path (no _rowPosition projected, plain non-bracket CSV) with a cap. */
    private List<Map<String, Object>> captureDirectBlockPath(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<Attribute> readSchema,
        ErrorPolicy policy,
        int maxRecordBytes
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("id")) // no _rowPosition -> direct-to-block path
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(fileFinal)
            .readSchema(readSchema)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .errorPolicy(policy)
            .maxRecordBytes(maxRecordBytes)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false) // plain, non-bracket -> direct-block path
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    private List<Map<String, Object>> captureFusedBracket(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<Attribute> readSchema,
        ErrorPolicy policy,
        int maxRecordBytes
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of(ColumnExtractor.ROW_POSITION_COLUMN, "id"))
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(fileFinal)
            .readSchema(readSchema)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .errorPolicy(policy)
            .maxRecordBytes(maxRecordBytes)
            .statsColumnScope(StripeColumnScope.PROJECTED)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false, CsvFormatReader.CONFIG_MULTI_VALUE_SYNTAX, "brackets")
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /**
     * BUG 2 regression: the plain Jackson bulk path (non-UTF-8 encoding routes off the byte-tracking
     * iterator, and no {@code _rowPosition} is projected) never advances {@code CsvLogicalRecordReader},
     * so any derived per-row offset is frozen at the header boundary. Under ALL scope the reader used to
     * force {@code trackOffsets=true} regardless, deriving collapsed offsets that piled every row onto one
     * stripe and emitted stripe-addressed fragments carrying bogus per-stripe stats (which the reconciler
     * would then commit). The fix disables stripe capture on this path, so it emits NO stripe-addressed
     * fragment — a safe miss; a warm aggregate re-scans rather than serving a fabricated single-stripe
     * count/min/max. Reconciling whatever is emitted must therefore commit no whole-file stripe fold.
     */
    public void testAllScopePlainJacksonBulkPathSafeMissesNoBogusStripes() throws Exception {
        // ASCII bytes are identical under ISO-8859-1, so the data parses the same; the non-UTF-8 encoding
        // only forces the read off the byte-tracking path onto the plain Jackson bulk iterator.
        byte[] header = "n\n".getBytes(StandardCharsets.US_ASCII);
        byte[] data = asciiCsv(0, 10);
        byte[] full = concat(header, data);
        long stripe = 4; // many stripes over the file, so a collapse-to-one-stripe bug would be visible

        List<Map<String, Object>> frags = captureScopedWithEncoding(
            full,
            0,
            true,
            true,
            1000,
            stripe,
            List.of("n"),
            StripeColumnScope.ALL,
            "ISO-8859-1"
        );
        // Stripe capture is disabled: no emitted contribution may carry the stripe-addressing keys. Pre-fix,
        // every row collapsed onto stripe 0 and a stripe-addressed fragment (carrying STRIPE_SIZE_KEY /
        // STRIPE_ORDINAL_KEY) was emitted; post-fix none is, so the coordinator's stripe fold has nothing.
        for (Map<String, Object> frag : frags) {
            assertFalse("no fragment may be stripe-addressed (capture must be disabled)", frag.containsKey(ExternalStats.STRIPE_SIZE_KEY));
            assertFalse("no fragment may carry a stripe ordinal", frag.containsKey(ExternalStats.STRIPE_ORDINAL_KEY));
        }
    }

    /**
     * Capture variant that also overrides the read encoding, to steer onto the plain Jackson bulk path.
     * {@code trim_spaces} is forced on so the read actually reaches that path: under the no-trim default,
     * Jackson's grammar does not apply (padded-quoted mis-split, col-0 whitespace eating), so a
     * non-direct read routes through the house per-record tokenizer on {@code CsvLogicalRecordReader}
     * instead — which supplies byte-exact per-record offsets for any encoding and therefore captures
     * stripes correctly rather than safe-missing. Trimming restores the Jackson bulk path this variant
     * is meant to probe.
     */
    private List<Map<String, Object>> captureScopedWithEncoding(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<String> projectedColumns,
        StripeColumnScope scope,
        String encoding
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        boolean headerRow = projectedColumns != null;
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .statsColumnScope(scope)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(
                    CsvFormatReader.CONFIG_HEADER_ROW,
                    headerRow,
                    CsvFormatReader.CONFIG_ENCODING,
                    encoding,
                    CsvFormatReader.CONFIG_TRIM_SPACES,
                    true
                )
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    /**
     * B1 counterpart to {@link #testAllScopePlainJacksonBulkPathSafeMissesNoBogusStripes}: under the no-trim
     * DEFAULT a non-UTF-8 read does NOT route onto the plain Jackson bulk path (Jackson's grammar does not
     * apply — see {@code jacksonGrammarApplies}), so it flows through the house per-record tokenizer on
     * {@link CsvLogicalRecordReader}, which supplies byte-exact per-record offsets for ANY encoding. Stripe
     * capture must therefore SUCCEED here (not safe-miss), and differently-chunked scans of the same non-UTF-8
     * file must fold to the exact row count. Exercised with a single-byte ISO-8859-1 file whose {@code é}
     * (0xE9) is one byte in the read encoding but two in UTF-8, so char and byte cursors genuinely diverge.
     */
    /**
     * B1 counterpart to {@link #testAllScopePlainJacksonBulkPathSafeMissesNoBogusStripes}: under the no-trim
     * DEFAULT a non-UTF-8 read does NOT route onto the plain Jackson bulk path (Jackson's grammar does not
     * apply — see {@code jacksonGrammarApplies}), so it flows through the house per-record tokenizer on
     * {@link CsvLogicalRecordReader}, which supplies byte-exact per-record offsets for ANY encoding. Stripe
     * capture must therefore SUCCEED here (not safe-miss), and differently-chunked scans of the same non-UTF-8
     * file must fold to the exact row count. Exercised headerless (so a non-first split re-infers rather than
     * eating a data row as a header) with single-byte encodings whose {@code é} (0xE9) is one byte in the read
     * encoding but two in UTF-8, so char and byte cursors genuinely diverge — the case the byte tracker must
     * get right. ALL scope, COUNT(*) projection: the fold checks the harvested per-stripe row counts.
     */
    public void testNonUtf8NoTrimHousePathCapturesStripesCorrectly() throws Exception {
        for (String encoding : List.of("ISO-8859-1", "windows-1252")) {
            byte[] full = latin1DataNoHeader(encoding, 10);
            long stripe = 4; // many stripes across the file, so a collapsed/skewed offset would misattribute
            int cut = latin1RecordBytes(encoding) * 4; // a record boundary partway through
            List<Map<String, Object>> whole = captureScopedWithEncodingNoTrim(
                full,
                0,
                true,
                true,
                1000,
                stripe,
                null,
                StripeColumnScope.ALL,
                encoding
            );
            assertFoldsTo(whole, 10, "encoding=" + encoding + " whole-file");
            // Differently-chunked scan of the same file must fold to the SAME exact count.
            List<Map<String, Object>> split = new ArrayList<>();
            split.addAll(
                captureScopedWithEncodingNoTrim(slice(full, 0, cut), 0, true, false, 1000, stripe, null, StripeColumnScope.ALL, encoding)
            );
            split.addAll(
                captureScopedWithEncodingNoTrim(
                    slice(full, cut, full.length),
                    cut,
                    false,
                    true,
                    1000,
                    stripe,
                    null,
                    StripeColumnScope.ALL,
                    encoding
                )
            );
            assertFoldsTo(split, 10, "encoding=" + encoding + " two-chunk fold");
        }
    }

    // Headerless single-column file: 'count' records, each a lone high-byte 'é' encoded in 'encoding'.
    private static byte[] latin1DataNoHeader(String encoding, int count) {
        java.nio.charset.Charset cs = java.nio.charset.Charset.forName(encoding);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append('é').append('\n');
        }
        return sb.toString().getBytes(cs);
    }

    // Bytes per data record ("é\n") in the given single-byte encoding: 'é' is one byte, '\n' one byte.
    private static int latin1RecordBytes(String encoding) {
        return "é\n".getBytes(java.nio.charset.Charset.forName(encoding)).length;
    }

    /**
     * Encoding-overriding capture that leaves {@code trim_spaces} at its no-trim default (house per-record path)
     * and reads headerless (synthetic column names) so a non-first split re-infers from its own rows rather than
     * consuming a data row as a header. A {@code null} projection is a COUNT(*) read.
     */
    private List<Map<String, Object>> captureScopedWithEncodingNoTrim(
        byte[] bytes,
        long baseOffset,
        boolean firstSplit,
        boolean fileFinal,
        int batchSize,
        long stripeSize,
        List<String> projectedColumns,
        StripeColumnScope scope,
        String encoding
    ) throws Exception {
        StorageObject o = memoryObject(bytes);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .recordAligned(true)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .splitStartByte(baseOffset)
            .stats(baseOffset, stripeSize, fileFinal)
            .statsColumnScope(scope)
            .build();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (
            var handle = ExternalStatsCapture.bind(sink);
            CloseableIterator<Page> it = new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withConfig(
                Map.of(CsvFormatReader.CONFIG_HEADER_ROW, false, CsvFormatReader.CONFIG_ENCODING, encoding)
            ).read(o, ctx)
        ) {
            while (it.hasNext()) {
                it.next().releaseBlocks();
            }
        }
        List<Map<String, Object>> raw = sink.get(o.path().toString());
        return raw == null ? List.of() : raw;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static Attribute intCol(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.INTEGER, Nullability.TRUE, null, false);
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
        String path = "memory://stripe-fold-" + UUID.randomUUID() + ".csv";
        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(settings)) {
            SchemaCacheKey key = SchemaCacheKey.build(path, mtime, ".csv", Map.of());
            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "col0", DataType.KEYWORD, Nullability.TRUE, null, false)
            );
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(schema, "csv", path, Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint), Map.of())
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
        return memoryObject(bytes, Instant.ofEpochMilli(1000L));
    }

    private StorageObject memoryObject(byte[] bytes, Instant fixedMtime) {
        String uniquePath = "memory://" + UUID.randomUUID() + ".csv";
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
