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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

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
        String uniquePath = "memory://" + UUID.randomUUID() + ".csv";
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
