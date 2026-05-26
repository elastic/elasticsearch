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
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
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

/** NDJSON counterpart of {@link ExternalCsvAggregatePushdownIT}: same cold-then-warm short-circuit assertion shape. */
public class ExternalNdJsonAggregatePushdownIT extends AbstractEsqlIntegTestCase {

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
        plugins.add(NdJsonDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        // See parallel comment in ExternalCsvCountPushdownIT.getPragmas.
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
        Path ndjsonFile = writeNdJsonFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(ndjsonFile) + "\" | STATS c = COUNT(*)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm execution must not scan any documents (LocalSourceExec)", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(ndjsonFile);
        }
    }

    public void testCountStarPushdownSingleRowFile() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 1;
        Path ndjsonFile = writeNdJsonFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(ndjsonFile) + "\" | STATS c = COUNT(*)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
            }
        } finally {
            Files.deleteIfExists(ndjsonFile);
        }
    }

    public void testMinMaxColdThenWarmShortCircuits() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 50;
        Path ndjsonFile = writeNdJsonFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(ndjsonFile) + "\" | STATS lo = MIN(value), hi = MAX(value)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertMinMax(response, 0L, (long) (totalRows - 1) * 10);
                assertThat("cold execution must scan rows", response.documentsFound(), equalTo((long) totalRows));
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertMinMax(response, 0L, (long) (totalRows - 1) * 10);
                assertNoPushdownBypass(response);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(ndjsonFile);
        }
    }

    public void testCountColumnColdThenWarmShortCircuits() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        int totalRows = 30;
        Path ndjsonFile = writeNdJsonFile(totalRows);
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(ndjsonFile) + "\" | STATS c = COUNT(value)";

            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
            }
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat(response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(ndjsonFile);
        }
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

    private Path writeNdJsonFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowCount; i++) {
            sb.append("{\"id\":").append(i).append(",\"name\":\"row_").append(i).append("\",\"value\":").append(i * 10).append("}\n");
        }
        Path tempFile = createTempDir().resolve("count_pushdown_test.ndjson");
        Files.writeString(tempFile, sb.toString(), StandardCharsets.UTF_8);
        return tempFile;
    }
}
