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
 * Cross-FILE warm {@code MIN}/{@code MAX} partial-harvest correctness regression. The sibling
 * {@link ExternalMultiFileWarmAggregateFoldIT} only ever warms the files SYMMETRICALLY (cold {@code MIN}/
 * {@code MAX} over the same glob harvests {@code value} on every file), so it can never reach the wrong-data
 * path this test targets: a glob where one file is warm-cached WITH a column's stats and another is
 * warm-cached WITHOUT them.
 * <p>
 * Text formats (CSV/NDJSON) harvest per-column stats partially (none / count / projected / all scopes), so a
 * file scanned with COUNT scope has complete whole-file stats but NO entry for a physically-present column.
 * This is the {@code MIN}/{@code MAX} analogue of the {@code COUNT(col)} wrong-data bug: {@code COUNT(col)}
 * now safe-misses for an unharvested text column via {@code SplitStats.hasColumn}, but {@code MIN}/{@code MAX}
 * read {@code columnMin}/{@code columnMax} verbatim, and the cross-file merge skips a file whose stats lack
 * the column (treating its absent key as "all-null") instead of safe-missing. The warm aggregate then serves
 * a subset extremum.
 * <p>
 * Repro shape (asymmetric warm-up the symmetric IT cannot produce):
 * <ol>
 *   <li>{@code b.csv} holds the smaller {@code value}s; warm it with {@code COUNT(*)} only, so it caches
 *       count-only with no {@code value} column stats.</li>
 *   <li>{@code a.csv} holds the larger {@code value}s; warm it with {@code MIN(value)}, so it caches
 *       {@code value}'s min/max.</li>
 *   <li>Run {@code MIN(value)} over the {@code *.csv} glob. Both files are warm, so the rule short-circuits;
 *       on the buggy path it merges a's min and drops b (unharvested), returning a's larger min rather than
 *       the true dataset-wide min held in b.</li>
 * </ol>
 * The test asserts the dataset-wide correct answer, so it FAILS on the buggy path (warm {@code MIN} returns
 * a.csv's min) and PASSES once {@code MIN}/{@code MAX} safe-miss the unharvested file like {@code COUNT(col)}.
 */
public class ExternalMultiFileWarmMinMaxPartialHarvestIT extends AbstractExternalDataSourceIT {

    private static final int ROWS_PER_FILE = 20_000;
    // a.csv values are [A_BASE, A_BASE + ROWS_PER_FILE); b.csv values are [0, ROWS_PER_FILE). The true
    // dataset-wide MIN is 0 (in b.csv), which the buggy warm path drops because b.csv never harvested value.
    private static final long A_BASE = 1_000_000L;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Tiny stripe grid so each file spans several canonical stripes -> the per-stripe emit + the
            // coordinator's 0..K + EOF fold runs per file, producing each file's whole-file column stats by
            // the same path as production.
            .put("esql.source.cache.stripe.size", "64kb")
            .build();
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 selects the parallel-parse path so each file is read in multiple chunks,
        // emitting per-stripe fragments the coordinator interval-covers and folds -- the production shape.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    /**
     * Pins every query to one coordinator: the reconciled schema cache is per-coordinator, so the cold
     * scans and the warm short-circuit must hit the same node (mirrors {@link ExternalMultiFileWarmAggregateFoldIT}).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCsvWarmMinDoesNotDropUnharvestedFile() throws Exception {
        Path dir = createTempDir();
        Path a = dir.resolve("a.csv");
        Path b = dir.resolve("b.csv");
        writeCsvFile(a, A_BASE);
        writeCsvFile(b, 0L);
        String aDataset = registerDataset("partial_a_csv", StoragePath.fileUri(a), Map.of());
        String bDataset = registerDataset("partial_b_csv", StoragePath.fileUri(b), Map.of());
        String globDataset = registerDataset("partial_glob_csv", globUri(dir, "*.csv"), Map.of());
        assertWarmMinKeepsUnharvestedFile(aDataset, bDataset, globDataset);
    }

    public void testNdjsonWarmMinDoesNotDropUnharvestedFile() throws Exception {
        Path dir = createTempDir();
        Path a = dir.resolve("a.ndjson");
        Path b = dir.resolve("b.ndjson");
        writeNdjsonFile(a, A_BASE);
        writeNdjsonFile(b, 0L);
        String aDataset = registerDataset("partial_a_ndjson", StoragePath.fileUri(a), Map.of());
        String bDataset = registerDataset("partial_b_ndjson", StoragePath.fileUri(b), Map.of());
        String globDataset = registerDataset("partial_glob_ndjson", globUri(dir, "*.ndjson"), Map.of());
        assertWarmMinKeepsUnharvestedFile(aDataset, bDataset, globDataset);
    }

    private void assertWarmMinKeepsUnharvestedFile(String aDataset, String bDataset, String globDataset) {
        long trueMin = 0L;                               // smallest value lives in b.csv
        long trueMax = A_BASE + ROWS_PER_FILE - 1;       // largest value lives in a.csv

        // 1) Warm b.csv with COUNT(*) only -> caches count-only, NO `value` column stats.
        String countB = "FROM " + bDataset + " | STATS c = COUNT(*)";
        try (var response = run(syncEsqlQueryRequest(countB).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, ROWS_PER_FILE);
        }

        // 2) Warm a.csv with MIN(value) -> caches `value`'s min/max for a.csv.
        String minA = "FROM " + aDataset + " | STATS m = MIN(value)";
        try (var response = run(syncEsqlQueryRequest(minA).profile(true), TimeValue.timeValueMinutes(5))) {
            assertSingleLong(response, A_BASE);
        }

        // 3) Warm glob MIN(value)/MAX(value): both files are warm, so the rule may short-circuit. It must NOT
        // drop b.csv (unharvested `value`) and serve a.csv's min -- the dataset-wide MIN is b.csv's 0.
        String minMaxGlob = "FROM " + globDataset + " | STATS lo = MIN(value), hi = MAX(value)";
        try (var response = run(syncEsqlQueryRequest(minMaxGlob).profile(true), TimeValue.timeValueMinutes(5))) {
            assertMinMax(response, trueMin, trueMax);
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
        assertThat("dataset-wide MIN must include the unharvested file", ((Number) rows.get(0).get(0)).longValue(), equalTo(expectedMin));
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
