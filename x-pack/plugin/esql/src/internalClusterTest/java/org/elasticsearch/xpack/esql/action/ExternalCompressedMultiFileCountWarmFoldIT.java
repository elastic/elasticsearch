/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Isolation test for the SECOND, TTL-independent cause of the ndjson warm-{@code COUNT(*)} loss
 * (esql-planning#1099): a compressed (streaming-coordinator) multi-file source whose per-file schema-cache
 * entries carry MANY per-stripe sub-entries. Each committed file entry retains one nested map per stripe
 * ({@code _stats.stripe.<k>}) FOREVER — even after the whole-file {@code 0..K} fold has materialized the
 * authoritative {@code _stats.row_count} — so a many-stripe file's entry weight is O(stripe count), not O(1).
 * With enough files × stripes the schema cache's weight budget is exceeded and the LRU evicts already-committed
 * entries; the multi-file warm serve is all-or-nothing ({@code aggregateFileStatistics} returns null if ANY file
 * lacks stats), so warm {@code COUNT(*)} re-scans the whole source. NDJSON hits this first because its
 * decompressed footprint (repeated JSON keys) is 2–3× CSV/TSV at equal rows, so it packs 2–3× the stripes per
 * file; TSV over the same shape stays under budget and short-circuits — the bench discriminator.
 * <p>
 * This test forces the condition at IT scale by pinning a small schema cache and a small stripe grid so a modest
 * file already carries many stripe sub-entries. It runs count-only (MIN/MAX is the separate GA gap #1103). The
 * gzip-tsv control drives the SAME {@code StreamingParallelParsingCoordinator} through {@code CsvFormatReader}.
 */
public class ExternalCompressedMultiFileCountWarmFoldIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 4;
    private static final int ROWS_PER_FILE = 60_000;
    private static final long TOTAL = (long) FILE_COUNT * ROWS_PER_FILE;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, GzipDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Small stripe grid -> many per-stripe sub-entries per file. Combined with the small cache below,
            // this reproduces the bench "thousands of stripes per file" entry-weight pressure at IT scale.
            .put("esql.source.cache.stripe.size", "64kb")
            // Deliberately small schema cache: the retained per-stripe sub-entries push the multi-file working
            // set over budget, so committed entries are evicted before the warm serve can fold across all files.
            .put("esql.source.cache.size", "256kb")
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testGzipNdjsonManyStripeMultiFileCountShortCircuits() throws Exception {
        assertWarmCountShortCircuits(writeAndRegister("ndjson.gz"));
    }

    public void testGzipTsvManyStripeMultiFileCountShortCircuits() throws Exception {
        assertWarmCountShortCircuits(writeAndRegister("tsv.gz"));
    }

    private void assertWarmCountShortCircuits(String dataset) {
        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, TOTAL);
            assertThat("cold COUNT(*) reads every row", response.documentsFound(), equalTo(TOTAL));
        }
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, TOTAL);
            assertThat(
                "warm COUNT(*) must short-circuit across a many-stripe compressed multi-file source",
                response.documentsFound(),
                equalTo(0L)
            );
        }
    }

    private static void assertSingleLong(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private String writeAndRegister(String format) throws IOException {
        Path dir = createTempDir();
        long v = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            Path file = dir.resolve("part-" + f + "." + format);
            v = switch (format) {
                case "ndjson.gz" -> writeGzipNdjson(file, v);
                case "tsv.gz" -> writeGzipTsv(file, v);
                default -> throw new IllegalArgumentException("unknown format: " + format);
            };
        }
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return registerDataset("compressed_multifile_" + format.replace('.', '_'), dirUri + "*." + format, Map.of());
    }

    private static final String PAD = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    private static long writeGzipNdjson(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder();
        long v = base;
        for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
            sb.append("{\"id\":").append(v).append(",\"value\":").append(v).append(",\"pad\":\"").append(PAD).append("\"}\n");
        }
        writeGzip(file, sb.toString());
        return v;
    }

    private static long writeGzipTsv(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder("id\tvalue\tpad\n");
        long v = base;
        for (int i = 0; i < ROWS_PER_FILE; i++, v++) {
            sb.append(v).append('\t').append(v).append('\t').append(PAD).append('\n');
        }
        writeGzip(file, sb.toString());
        return v;
    }

    private static void writeGzip(Path file, String content) throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (OutputStream os = new GZIPOutputStream(Files.newOutputStream(file))) {
            os.write(bytes);
        }
    }
}
