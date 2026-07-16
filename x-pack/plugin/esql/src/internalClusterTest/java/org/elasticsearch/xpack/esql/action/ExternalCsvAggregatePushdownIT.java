/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cold-then-warm: first run populates the cache; second run is rewritten to LocalSourceExec.
 * Mirrors {@link ExternalParquetCountPushdownIT}.
 */
public class ExternalCsvAggregatePushdownIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism=1 keeps the file on the single-thread path; record-aligned chunks bypass the capture-hook gate.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    /**
     * Pins every query to one coordinator. The reconciled schema cache is per-coordinator, not
     * cluster-replicated, so the cold scan and the warm short-circuit must hit the same node; the
     * default {@code run()} routes to a random node per call, which would land the warm query on a
     * coordinator whose cache the cold scan never enriched (see {@code ExternalCsvMultiNodePushdownIT},
     * which pins to node 0 for the same reason).
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testCountStarColdThenWarmShortCircuits() throws Exception {
        int totalRows = 200;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";

            // Cold: scan, capture hook populates the cache.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            // Warm: cache hit → optimizer rewrites to LocalSourceExec → no data-node scan.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm execution must not scan any documents (LocalSourceExec)", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /**
     * The strict declared-schema twin of {@link #testCountStarColdThenWarmShortCircuits}. A strict
     * ({@code dynamic:false}) CSV dataset reads no file body at resolution, so it cannot harvest the row-count itself;
     * {@code ExternalSourceResolver.strictSingleFileMetadata} seeds a schema-cache entry that the cold scan's capture
     * hook enriches with the count, so the warm run folds {@code COUNT(*)} to {@code LocalSourceExec}. Before the seed
     * fix, strict full-scanned on every run (regression guard for the strict declared-schema warm-COUNT fix).
     */
    public void testStrictCountStarColdThenWarmShortCircuits() throws Exception {
        int totalRows = 200;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerStrictDataset("csv_strict_agg", StoragePath.fileUri(csvFile), declaredColumns(), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";

            // Cold: strict text has no row-count at resolve, so it full-scans; the capture hook enriches the seed.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold strict execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            // Warm: seeded + reconciled entry serves the count → LocalSourceExec → no data-node scan.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm strict execution must not scan (LocalSourceExec)", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /** TSV twin of {@link #testStrictCountStarColdThenWarmShortCircuits} — TSV shares the CSV reader and the same seam. */
    public void testStrictCountStarColdThenWarmShortCircuitsTsv() throws Exception {
        int totalRows = 150;
        Path tsvFile = writeTsvFile(totalRows);
        try {
            String dataset = registerStrictDataset("tsv_strict_agg", StoragePath.fileUri(tsvFile), declaredColumns(), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold strict execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm strict execution must not scan (LocalSourceExec)", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(tsvFile);
        }
    }

    /**
     * The gz × warm intersection the title advertises: a strict single-file {@code .csv.gz} {@code COUNT(*)} warms
     * exactly like its plain twin (the strict {@code sourceType} is {@code "csv"} for both, and the served count comes
     * from the real decompressed scan), folding to {@code LocalSourceExec} on the warm run with the same count.
     */
    public void testStrictCountStarColdThenWarmShortCircuitsGzip() throws Exception {
        int totalRows = 120;
        Path csvGz = writeGzippedCsvFile(totalRows);
        try {
            String dataset = registerStrictDataset("csv_gz_strict_agg", StoragePath.fileUri(csvGz), declaredColumns(), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold strict gz execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm strict gz execution must not scan (LocalSourceExec)", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvGz);
        }
    }

    /**
     * Two strict datasets over the SAME file+config declare column {@code v} differently (integer vs keyword) and so
     * share one file+config stats-cache entry. A's {@code MIN(v)} harvests the numeric min (9); B's {@code MIN(v)} must
     * return the keyword min ("10", lexicographic) by re-scanning, never fold A's numeric 9. This is the strict-vs-strict
     * cross-declaration hazard — the strict warm serve strips per-column stats so a foreign declaration's MIN/MAX is
     * never folded. Correctness guard (independent of warm/cold).
     */
    public void testStrictConflictingDeclaredTypesServeCorrectMin() throws Exception {
        Path csv = createTempDir().resolve("conflict.csv");
        Files.writeString(csv, "9\n10\n", StandardCharsets.UTF_8);
        String uri = StoragePath.fileUri(csv);
        // Headerless: the single declared column binds by name against the self-encoded col0, not by position.
        String asInt = registerStrictDataset("conflict_int", uri, declaredColumn("v", "integer", "col0"), Map.of("header_row", false));
        String asKeyword = registerStrictDataset("conflict_kw", uri, declaredColumn("v", "keyword", "col0"), Map.of("header_row", false));

        // A (integer): MIN = 9. Run twice so any per-column harvest is reconciled into the shared entry.
        for (int i = 0; i < 2; i++) {
            try (var r = run(syncEsqlQueryRequest("FROM " + asInt + " | STATS m = MIN(v)"))) {
                assertThat(((Number) getValuesList(r).get(0).get(0)).longValue(), equalTo(9L));
            }
        }
        // B (keyword): MIN = "10" (lexicographic). Must NOT serve A's numeric 9.
        try (var r = run(syncEsqlQueryRequest("FROM " + asKeyword + " | STATS m = MIN(v)"))) {
            assertThat(getValuesList(r).get(0).get(0).toString(), equalTo("10"));
        }
    }

    /**
     * Inferred-victim twin of {@link #testStrictAndInferredOverSameFileServeCorrectMin}: the INFERRED dataset reads a
     * cache entry a strict sibling over the same file+config harvested with a different declared type. A {@code boolean}
     * column must never serve a foreign keyword {@code BytesRef} min through {@code buildBlock}'s BOOLEAN arm
     * ({@code Booleans.parseBoolean(bytesRef.toString())}, whose {@code toString()} is the byte-hex form → an
     * {@code IllegalArgumentException} crash). {@code servableExtremum} must safe-miss the foreign value so the warm
     * {@code MIN(v)} re-scans and answers correctly.
     */
    public void testInferredBooleanNotCrashedByStrictKeywordHarvest() throws Exception {
        Path csv = createTempDir().resolve("mixed_bool.csv");
        Files.writeString(csv, "v:boolean\ntrue\nfalse\n", StandardCharsets.UTF_8);
        String uri = StoragePath.fileUri(csv);
        String inferred = registerDataset("bool_inferred", uri, Map.of());
        String strict = registerStrictDataset("bool_strict", uri, declaredColumn("v", "keyword"), Map.of());

        String q = "FROM " + inferred + " | STATS m = MIN(v)";
        for (int i = 0; i < 2; i++) {
            try (var r = run(syncEsqlQueryRequest(q))) {
                assertThat(getValuesList(r).get(0).get(0), equalTo(false)); // MIN(boolean) = false
            }
        }
        // Strict sibling scans + harvests a keyword BytesRef min, reconciled onto the shared file+config entry.
        try (var r = run(syncEsqlQueryRequest("FROM " + strict + " | STATS m = MIN(v)"))) {
            getValuesList(r);
        }
        // Warm inferred MIN must still answer false — never crash on the foreign BytesRef parked in the entry.
        try (var r = run(syncEsqlQueryRequest(q))) {
            assertThat(getValuesList(r).get(0).get(0), equalTo(false));
        }
    }

    /**
     * Inferred-victim wrong-answer twin: an inferred {@code keyword} column must never serve a strict sibling's foreign
     * {@code Integer} min through {@code buildBlock}'s BYTES_REF arm ({@code toBytesRef(number.toString())}). Lexicographic
     * {@code MIN("9","10")} is {@code "10"} ('1' &lt; '9'); serving the numeric {@code 9} would answer {@code "9"}.
     * {@code servableExtremum} must safe-miss the foreign Number so the warm {@code MIN(v)} re-scans to {@code "10"}.
     */
    public void testInferredKeywordNotPollutedByStrictNumericHarvest() throws Exception {
        Path csv = createTempDir().resolve("mixed_kw.csv");
        Files.writeString(csv, "v:keyword\n9\n10\n", StandardCharsets.UTF_8);
        String uri = StoragePath.fileUri(csv);
        String inferred = registerDataset("kw_inferred", uri, Map.of());
        String strict = registerStrictDataset("kw_strict", uri, declaredColumn("v", "integer"), Map.of());

        String q = "FROM " + inferred + " | STATS m = MIN(v)";
        for (int i = 0; i < 2; i++) {
            try (var r = run(syncEsqlQueryRequest(q))) {
                assertThat(getValuesList(r).get(0).get(0).toString(), equalTo("10"));
            }
        }
        // Strict sibling scans + harvests an Integer min, reconciled onto the shared entry.
        try (var r = run(syncEsqlQueryRequest("FROM " + strict + " | STATS m = MIN(v)"))) {
            getValuesList(r);
        }
        // Warm inferred MIN must still answer "10" — never serve the strict sibling's numeric 9.
        try (var r = run(syncEsqlQueryRequest(q))) {
            assertThat(getValuesList(r).get(0).get(0).toString(), equalTo("10"));
        }
    }

    /**
     * An inferred dataset and a strict dataset over the same file+config must not serve each other's per-column stats.
     * The reconcile is schema-agnostic (matches path+mtime+fingerprint, not the schema-cache marker), so the inferred
     * numeric min can reach the strict entry — but the strict serve strips per-column stats, so strict {@code MIN(v)}
     * re-scans and returns the correct keyword min.
     */
    public void testStrictAndInferredOverSameFileServeCorrectMin() throws Exception {
        // A typed header names the inferred column `v` (type integer) and is skipped by the strict read too, so both
        // datasets share the same file+config (no header_row override → identical fingerprint).
        Path csv = createTempDir().resolve("mixed.csv");
        Files.writeString(csv, "v:integer\n9\n10\n", StandardCharsets.UTF_8);
        String uri = StoragePath.fileUri(csv);
        String inferred = registerDataset("mixed_inferred", uri, Map.of());
        String strict = registerStrictDataset("mixed_strict", uri, declaredColumn("v", "keyword"), Map.of());

        // Inferred infers v numeric → MIN = 9; run twice to reconcile the numeric per-column min into the cache.
        for (int i = 0; i < 2; i++) {
            try (var r = run(syncEsqlQueryRequest("FROM " + inferred + " | STATS m = MIN(v)"))) {
                assertThat(((Number) getValuesList(r).get(0).get(0)).longValue(), equalTo(9L));
            }
        }
        // Strict declares v:keyword → MIN = "10"; must not fold the inferred numeric 9.
        try (var r = run(syncEsqlQueryRequest("FROM " + strict + " | STATS m = MIN(v)"))) {
            assertThat(getValuesList(r).get(0).get(0).toString(), equalTo("10"));
        }
    }

    /**
     * A non-{@code FAIL_FAST} error policy ({@code skip_row} AND {@code null_field}) drops structurally-malformed
     * (width-overflow) rows — the CSV reader drops such a row "even under NULL_FIELD" — and the drop count depends on the
     * declared column count, so the harvested row-count is not guaranteed independent of the declaration. The strict warm
     * seed is therefore skipped for every non-{@code FAIL_FAST} policy: {@code COUNT(*)} re-scans on every query
     * ({@code documentsFound == totalRows}) rather than fold a possibly cross-declaration row-count. Guards the
     * {@code warmsRowCountSafely} gate (contrast {@link #testStrictCountStarColdThenWarmShortCircuits}, which warms with
     * the default {@code FAIL_FAST} policy).
     */
    public void testStrictNonFailFastErrorModesAreNeverWarmed() throws Exception {
        for (String mode : new String[] { "skip_row", "null_field" }) {
            int totalRows = 100;
            Path csv = writeCsvFile(totalRows);
            try {
                String dataset = registerStrictDataset(
                    "nff_" + mode,
                    StoragePath.fileUri(csv),
                    declaredColumns(),
                    Map.of("error_mode", mode)
                );
                String query = "FROM " + dataset + " | STATS c = COUNT(*)";
                // Both runs must scan — a non-FAIL_FAST policy is excluded from the warm path, so COUNT never folds.
                for (int i = 0; i < 2; i++) {
                    try (var r = run(syncEsqlQueryRequest(query).profile(true))) {
                        assertCount(r, totalRows);
                        assertThat(mode + " must never warm", r.documentsFound(), equalTo((long) totalRows));
                    }
                }
            } finally {
                Files.deleteIfExists(csv);
            }
        }
    }

    private static LinkedHashMap<String, DatasetFieldMapping> declaredColumn(String name, String type) {
        return declaredColumn(name, type, null);
    }

    /**
     * A single declared column bound to an explicit physical {@code path}. A headerless file has no header names, so a
     * declared column binds by name against the self-encoded {@code col<N>} of each field ({@code path: "col0"} for the
     * first) — a bare name with no path is a declared column the file does not supply, which reads null.
     */
    private static LinkedHashMap<String, DatasetFieldMapping> declaredColumn(String name, String type, String path) {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put(name, new DatasetFieldMapping(type, path));
        return properties;
    }

    /**
     * {@code warmsRowCountSafely} must never turn an invalid {@code error_mode} into a plan-time failure: it is an
     * optimization decision, not a validation gate. Before the {@code try/catch} around
     * {@link org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy#fromConfig}, an unparseable policy (here
     * {@code error_mode: bogus}) would throw {@code IllegalArgumentException} straight out of resolution — reached by
     * every query against the dataset, including one like {@code LIMIT 0} that the optimizer prunes to an empty plan
     * before ever touching the data node. That regressed a query that succeeded before the strict warm-COUNT path
     * existed, since the malformed config was previously only validated inside {@code FileSourceFactory}'s data-node
     * operator factory, which {@code LIMIT 0} never reaches.
     */
    public void testStrictBogusErrorModeDoesNotFailLimitZero() throws Exception {
        Path csv = writeCsvFile(10);
        try {
            String dataset = registerStrictDataset(
                "bogus_error_mode",
                StoragePath.fileUri(csv),
                declaredColumns(),
                Map.of("error_mode", "bogus")
            );
            try (var r = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"))) {
                assertThat(getValuesList(r), equalTo(List.of()));
            }
        } finally {
            Files.deleteIfExists(csv);
        }
    }

    public void testCountStarPushdownSingleRowFile() throws Exception {
        int totalRows = 1;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testMinMaxColdThenWarmShortCircuits() throws Exception {
        int totalRows = 50;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS lo = MIN(value), hi = MAX(value)";

            // Cold: scan + capture per-column stats for value.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertMinMax(response, 0L, (long) (totalRows - 1) * 10);
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            // Warm: cache hit → optimizer rewrites to LocalSourceExec → no data-node scan.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertMinMax(response, 0L, (long) (totalRows - 1) * 10);
                assertNoPushdownBypass(response);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testCountColumnColdThenWarmShortCircuits() throws Exception {
        int totalRows = 30;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(value)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /**
     * Regression for the cross-query partial-harvest path. A cold {@code COUNT(*)} caches the file's row
     * count but harvests no per-column stats (it projects zero columns). A later {@code COUNT(value)} on the
     * same file is warm and finds {@code row_count} but no {@code value} column in the cache.
     * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushStatsToExternalSource} is
     * format-agnostic; without the text safe-miss it
     * would serve {@code rowCount - columnNullCount(value) = rowCount - rowCount = 0}
     * ({@code columnNullCount} returns {@code rowCount} for an absent column under the implicit-nulls contract,
     * which line-oriented text formats do not honour). The fix makes it safe-miss and re-scan, so the warm
     * {@code COUNT(value)} is the true count, not 0.
     */
    public void testCountColumnWarmAfterCountStarColdReScansNotZero() throws Exception {
        int totalRows = 40;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            // Cold COUNT(*): caches row_count only, no per-column stats harvested.
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").profile(true))) {
                assertCount(response, totalRows);
            }
            // Warm COUNT(value): value was never harvested, so the rule must safe-miss and re-scan rather than
            // serve 0. The true count is totalRows (value is non-null for every row), and documentsFound shows
            // the re-scan happened (it is not a zero-scan LocalSourceExec serve).
            try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(value)").profile(true))) {
                assertCount(response, totalRows);
                assertThat(
                    "warm COUNT(value) of an un-harvested text column must re-scan, not serve 0",
                    response.documentsFound(),
                    equalTo((long) totalRows)
                );
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testMinMaxKeywordColdThenWarmShortCircuits() throws Exception {
        int totalRows = 5;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS lo = MIN(name), hi = MAX(name)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertKeywordMinMax(response, "row_0", "row_4");
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertKeywordMinMax(response, "row_0", "row_4");
                assertNoPushdownBypass(response);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testMinMaxBooleanColdThenWarmShortCircuits() throws Exception {
        // Mixed flag column: even rows true, odd rows false → MIN(flag)=false, MAX(flag)=true. The
        // mixed values catch a default-valued or swapped serve (false/false, true/true, true/false
        // would all fail) while proving the warm path reads the captured boolean stat.
        int totalRows = 6;
        StringBuilder sb = new StringBuilder("id:integer,flag:boolean\n");
        for (int i = 0; i < totalRows; i++) {
            sb.append(i).append(',').append(i % 2 == 0).append('\n');
        }
        Path csvFile = createTempDir().resolve("bool_pushdown_test.csv");
        Files.writeString(csvFile, sb.toString(), StandardCharsets.UTF_8);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS lo = MIN(flag), hi = MAX(flag)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertBooleanMinMax(response, false, true);
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertBooleanMinMax(response, false, true);
                assertNoPushdownBypass(response);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testAllNullColumnMinMaxReturnsNull() throws Exception {
        // Column 'maybe' is numeric and all cells are empty, so every value reads as null (an empty
        // cell on a numeric column has no representation other than null). The optimizer cannot
        // short-circuit here (cached min/max are null, so the rule bails to a regular scan), but the
        // regular scan must still return null on both cold and warm runs.
        StringBuilder sb = new StringBuilder("id:integer,maybe:long\n");
        for (int i = 0; i < 4; i++) {
            sb.append(i).append(',').append('\n');
        }
        Path csvFile = createTempDir().resolve("allnull.csv");
        Files.writeString(csvFile, sb.toString(), StandardCharsets.UTF_8);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS lo = MIN(maybe), hi = MAX(maybe)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                assertNull("MIN(all-null) must be null on cold path", rows.get(0).get(0));
                assertNull("MAX(all-null) must be null on cold path", rows.get(0).get(1));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows.size(), equalTo(1));
                assertNull("MIN(all-null) must remain null on warm path", rows.get(0).get(0));
                assertNull("MAX(all-null) must remain null on warm path", rows.get(0).get(1));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testFilteredAggregateDoesNotServeCachedStats() throws Exception {
        // 50 rows, value column ranges 0..490. WHERE narrows to 100..200 → MIN must be 100, not 0.
        int totalRows = 50;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String dataset = registerDataset("csv_agg", StoragePath.fileUri(csvFile), Map.of());
            String prime = "FROM " + dataset + " | STATS lo = MIN(value)";
            // Prime the cache with whole-file stats (min=0).
            try (var response = run(syncEsqlQueryRequest(prime).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
            }
            // Filtered query: MIN over rows where value >= 100 → 100, not the cached 0.
            String filtered = "FROM " + dataset + " | WHERE value >= 100 | STATS lo = MIN(value)";
            try (var response = run(syncEsqlQueryRequest(filtered).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(
                    "filtered MIN must reflect the filter, not the cached whole-file stats",
                    ((Number) rows.get(0).get(0)).longValue(),
                    equalTo(100L)
                );
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    private static void assertKeywordMinMax(EsqlQueryResponse response, String expectedMin, String expectedMax) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(2));
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(rows.get(0).get(0).toString(), equalTo(expectedMin));
        assertThat(rows.get(0).get(1).toString(), equalTo(expectedMax));
    }

    private static void assertBooleanMinMax(EsqlQueryResponse response, boolean expectedMin, boolean expectedMax) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(2));
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(rows.get(0).get(0), equalTo(expectedMin));
        assertThat(rows.get(0).get(1), equalTo(expectedMax));
    }

    private static void assertMinMax(EsqlQueryResponse response, long expectedMin, long expectedMax) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(2));
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expectedMin));
        assertThat(((Number) rows.get(0).get(1)).longValue(), equalTo(expectedMax));
    }

    private static void assertCount(EsqlQueryResponse response, long expected) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(1));
        assertThat(columns.get(0).name(), equalTo("c"));
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    /** No Async* operators ⇒ PushStatsToExternalSource fired ⇒ LocalSourceExec. */
    private static void assertNoPushdownBypass(EsqlQueryResponse response) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);
        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                assertFalse(
                    "expected no Async* operators on warm execution but found: " + op.operator(),
                    op.operator().startsWith("Async")
                );
            }
        }
    }

    private Path writeCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("count_pushdown_test.csv");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }

    private Path writeGzippedCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("count_pushdown_test.csv.gz");
        try (var out = new java.util.zip.GZIPOutputStream(Files.newOutputStream(tempFile))) {
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }
        return tempFile;
    }

    private Path writeTsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer\tname:keyword\tvalue:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append("\trow_").append(i).append('\t').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("count_pushdown_test.tsv");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }

    /** The strict declaration matching {@link #writeCsvFile}/{@link #writeTsvFile}: the entire schema, no inference. */
    private static LinkedHashMap<String, DatasetFieldMapping> declaredColumns() {
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("id", new DatasetFieldMapping("integer", null));
        properties.put("name", new DatasetFieldMapping("keyword", null));
        properties.put("value", new DatasetFieldMapping("integer", null));
        return properties;
    }
}
