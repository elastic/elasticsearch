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
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cold-then-warm: first run populates the cache; second run is rewritten to LocalSourceExec.
 * Mirrors {@link ExternalParquetCountPushdownIT}.
 */
public class ExternalCsvAggregatePushdownIT extends AbstractEsqlIntegTestCase {

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        return plugins;
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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 200;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS c = COUNT(*)";

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

    public void testCountStarPushdownSingleRowFile() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 1;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS c = COUNT(*)";

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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 50;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS lo = MIN(value), hi = MAX(value)";

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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 30;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS c = COUNT(value)";

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

    public void testMinMaxKeywordColdThenWarmShortCircuits() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 5;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS lo = MIN(name), hi = MAX(name)";

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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

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
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS lo = MIN(flag), hi = MAX(flag)";

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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // Column 'maybe' is keyword and all cells are empty. The optimizer cannot short-circuit
        // here (cached min/max are null, so the rule bails to a regular scan), but the regular
        // scan must still return null on both cold and warm runs.
        StringBuilder sb = new StringBuilder("id:integer,maybe:keyword\n");
        for (int i = 0; i < 4; i++) {
            sb.append(i).append(',').append('\n');
        }
        Path csvFile = createTempDir().resolve("allnull.csv");
        Files.writeString(csvFile, sb.toString(), StandardCharsets.UTF_8);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS lo = MIN(maybe), hi = MAX(maybe)";

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
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        // 50 rows, value column ranges 0..490. WHERE narrows to 100..200 → MIN must be 100, not 0.
        int totalRows = 50;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String prime = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS lo = MIN(value)";
            // Prime the cache with whole-file stats (min=0).
            try (var response = run(syncEsqlQueryRequest(prime).profile(true))) {
                List<List<Object>> rows = getValuesList(response);
                assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(0L));
            }
            // Filtered query: MIN over rows where value >= 100 → 100, not the cached 0.
            String filtered = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | WHERE value >= 100 | STATS lo = MIN(value)";
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
}
