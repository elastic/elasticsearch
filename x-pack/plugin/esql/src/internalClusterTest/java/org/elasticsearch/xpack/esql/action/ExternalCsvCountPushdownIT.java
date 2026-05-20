/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalRowCountCache;
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
 * End-to-end test that {@code EXTERNAL "<csv>" | STATS COUNT(*)} short-circuits via
 * {@code PushStatsToExternalSource} on the second invocation. The first execution scans the file
 * and populates {@code ExternalRowCountCache} via the iterator's capture hook; the second sees
 * a cache hit at {@code metadata()} time, publishes {@code SourceStatistics.rowCount}, and the
 * optimizer rewrites the aggregate subtree to a {@code LocalSourceExec} — no {@code Async*}
 * operator appears in any driver profile.
 *
 * <p>Mirrors {@link ExternalParquetCountPushdownIT} for the text-format reader chain.
 */
public class ExternalCsvCountPushdownIT extends AbstractEsqlIntegTestCase {

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
        // parsing_parallelism=1 keeps the file on the single-thread fallback path so the iterator
        // receives the whole-file FormatReadContext (firstSplit=true, lastSplit=true,
        // recordAligned=false) the capture hook gates on. The default parallel-parsing path runs
        // per-chunk record-aligned reads that this PR's cache deliberately excludes — covering
        // that case requires per-chunk aggregation, called out as out-of-scope follow-up.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ExternalRowCountCache.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        ExternalRowCountCache.clearForTests();
        super.tearDown();
    }

    public void testCountStarColdThenWarmShortCircuits() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 200;
        Path csvFile = writeCsvFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(csvFile) + "\" | STATS c = COUNT(*)";

            // Cold execution: cache is empty, the iterator scans the file end-to-end and the
            // capture hook writes (path, length) → totalRows on close. Async* operators are
            // expected to appear in the profile and documentsFound reflects the scanned rows.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }

            // Warm execution: metadata() lookup hits, SourceStatistics.rowCount publishes,
            // PushStatsToExternalSource rewrites the aggregate subtree to a LocalSourceExec.
            // No Async* operator should appear and documentsFound is zero (no data-node scan).
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

    private static void assertCount(EsqlQueryResponse response, long expected) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(1));
        assertThat(columns.get(0).name(), equalTo("c"));
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    /**
     * Asserts that no Async* operator appears in any driver profile — i.e. PushStatsToExternalSource
     * fired and the plan is a LocalSourceExec, not an Async-source scan.
     */
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
