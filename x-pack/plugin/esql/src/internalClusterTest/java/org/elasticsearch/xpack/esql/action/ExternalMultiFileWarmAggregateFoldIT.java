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
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Multi-FILE warm short-circuit regression test. The sibling fold ITs
 * ({@link ExternalCsvMultiStripeFoldIT} / {@link ExternalNdJsonMultiStripeFoldIT}) only ever read ONE file:
 * they exercise the within-file per-stripe fold but never the cross-FILE merge of per-file whole-file
 * column statistics that the dataset-wide warm aggregate short-circuit depends on. That gap let a
 * regression through where, over a multi-file glob, warm {@code COUNT(*)} short-circuited correctly but
 * warm {@code MIN}/{@code MAX} full-scanned because the dataset-wide column min/max was never assembled
 * from the N per-file column stats.
 * <p>
 * Here we write {@code FILE_COUNT} separate files into a directory, glob them, and assert that after a cold
 * scan both {@code COUNT(*)} AND {@code MIN(col)}/{@code MAX(col)} short-circuit on the warm pass
 * ({@code documentsFound == 0}). COUNT(*) is run first (cold + warm) so its row-count-only per-file cache
 * entries are in place before MIN/MAX, reproducing the production ordering where COUNT short-circuits and
 * MIN/MAX must still serve from the merged dataset-wide column min/max. A small stripe grid forces each
 * file through the per-stripe fold so the per-file whole-file map is produced by the same path as
 * production, then merged across files. Run for CSV and NDJSON.
 * <p>
 * The precise pre-fix-fail / post-fix-pass regression for the cross-file column-stat merge defect lives in
 * {@code MergedSplitStatsTests#testColumnMinMaxUsesChildValueWhenNullCountUnknownButMinMaxPresent}: a
 * per-file {@code SplitStats} carrying a folded min/max with an unknown null_count must still contribute its
 * extremum to the dataset-wide {@code MIN}/{@code MAX} instead of poisoning it. This end-to-end IT is the
 * multi-FILE coverage the single-file fold ITs lacked.
 */
public class ExternalMultiFileWarmAggregateFoldIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 25;
    private static final int ROWS_PER_FILE = 60_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Tiny stripe grid so each file spans several canonical stripes -> the per-stripe emit + the
            // coordinator's 0..K + EOF fold runs per file, producing each file's whole-file column stats.
            .put("esql.source.cache.stripe.size", "64kb")
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 selects the parallel-parse path so each file is read in multiple chunks,
        // emitting per-stripe fragments the coordinator must interval-cover and fold — the production shape
        // at the 1rg-per-file ClickBench layout, where the cross-file column-stat merge bug surfaces.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    /**
     * Pins every query to one coordinator: the reconciled schema cache is per-coordinator, so the cold
     * scan and the warm short-circuit must hit the same node (mirrors {@link ExternalNdJsonAggregatePushdownIT}).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCsvMultiFileWarmCountAndMinMaxShortCircuit() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeCsvFile(dir.resolve("part-" + f + ".csv"), total);
        }
        String dataset = registerDataset("multifile_csv", globUri(dir, "*.csv"), Map.of());
        assertWarmAggregatesShortCircuit(dataset, total);
    }

    public void testNdjsonMultiFileWarmCountAndMinMaxShortCircuit() throws Exception {
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < FILE_COUNT; f++) {
            total += writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), total);
        }
        String dataset = registerDataset("multifile_ndjson", globUri(dir, "*.ndjson"), Map.of());
        assertWarmAggregatesShortCircuit(dataset, total);
    }

    /**
     * {@code value} runs 0..total-1 globally across all files, so the dataset-wide MIN is 0 and MAX is
     * {@code total-1}. After a cold scan that reads every row, the warm COUNT(*) and the warm MIN/MAX must
     * both short-circuit to {@code LocalSourceExec} (0 documents scanned), proving the cross-file column
     * min/max merge served the answer.
     */
    private void assertWarmAggregatesShortCircuit(String dataset, long total) {
        // COUNT(*): cold scans, warm must short-circuit (this already worked; it is the control).
        String countQuery = "FROM " + dataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("cold COUNT(*) reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(countQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, total);
            assertThat("warm COUNT(*) must short-circuit across many files", response.documentsFound(), equalTo(0L));
        }

        // MIN/MAX: cold scans, warm must short-circuit using the merged dataset-wide column min/max.
        String minMaxQuery = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat("cold MIN/MAX reads every row", response.documentsFound(), equalTo(total));
        }
        try (var response = run(syncEsqlQueryRequest(minMaxQuery).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, 0L, total - 1);
            assertThat(
                "warm MIN/MAX must short-circuit across many files (dataset-wide column min/max merged from per-file stats)",
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

    private static void assertMinMax(EsqlQueryResponse response, long expectedMin, long expectedMax) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expectedMin));
        assertThat(((Number) rows.get(0).get(1)).longValue(), equalTo(expectedMax));
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    /** Writes a CSV file with {@code value} starting at {@code base}; returns rows written. */
    private static long writeCsvFile(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder("id,name,value\n");
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            long v = base + i;
            sb.append(v).append(",row_").append(v).append(',').append(v).append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return ROWS_PER_FILE;
    }

    /** Writes an NDJSON file with {@code value} starting at {@code base}; returns rows written. */
    private static long writeNdjsonFile(Path file, long base) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            long v = base + i;
            sb.append("{\"id\":").append(v).append(",\"name\":\"row_").append(v).append("\",\"value\":").append(v).append("}\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return ROWS_PER_FILE;
    }
}
