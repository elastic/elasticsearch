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
