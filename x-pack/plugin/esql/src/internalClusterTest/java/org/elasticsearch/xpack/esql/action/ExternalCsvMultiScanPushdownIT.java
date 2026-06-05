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
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FORK_V9;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * CSV counterpart of {@link ExternalNdJsonMultiScanPushdownIT} — the cross-query cache-poisoning
 * guard for the CSV (and, via the shared {@code CsvFormatReader}, TSV) read path. The bug and fix are
 * format-agnostic: CSV, TSV, and NDJSON all run through the same {@code StreamingParallelParsingCoordinator}
 * and the same coverage reconciler, so the double-scan→cache-poison vector exists identically here.
 * Only NDJSON had a deterministic guard before; this closes the format gap so a CSV/TSV regression
 * fails in CI deterministically rather than by accident of spec-suite ordering.
 * <p>
 * A two-branch {@code FORK} re-scans the {@code EXTERNAL} source twice in one query; before the fix the
 * reconciler summed both covers and the next pushdown-eligible {@code COUNT(*)} read double the rows.
 */
public class ExternalCsvMultiScanPushdownIT extends AbstractEsqlIntegTestCase {

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
        plugins.add(GzipDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 forces the parallel parser (per-chunk PARTIAL contributions); see
        // ExternalNdJsonMultiScanPushdownIT for why this is required to reach the bug's code path.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 4).build());
    }

    /** Pins every query to one coordinator so the cold (poisoning) scan and the warm query share the per-coordinator cache. */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        try {
            return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    public void testMultiScanColdDoesNotDoubleCountWarmCountStar() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("requires FORK", FORK_V9.isEnabled());

        int totalRows = 500;
        Path gzFile = writeGzippedCsvFile(totalRows);
        try {
            String uri = StoragePath.fileUri(gzFile);

            // COLD: a two-branch FORK scans the same compressed CSV twice in one query.
            String forkQuery = "EXTERNAL \"" + uri + "\" | FORK (WHERE id >= 0) (WHERE id >= 0) | STATS rows = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(forkQuery).profile(true))) {
                assertSingleLong(response, "rows", 2L * totalRows);
                assertThat(
                    "FORK must scan the file more than once (guards that this test exercises the double-scan path)",
                    response.documentsFound(),
                    greaterThan((long) totalRows)
                );
            }

            // WARM: pushdown-eligible COUNT(*) must read the deduped cached row count, not the doubled one.
            String countQuery = "EXTERNAL \"" + uri + "\" | STATS c = COUNT(*)";
            try (var response = run(syncEsqlQueryRequest(countQuery).profile(true))) {
                assertCount(response, totalRows);
                assertNoPushdownBypass(response);
                assertThat("warm execution must short-circuit to LocalSourceExec", response.documentsFound(), equalTo(0L));
            }
        } finally {
            Files.deleteIfExists(gzFile);
        }
    }

    private static void assertCount(EsqlQueryResponse response, long expected) {
        assertSingleLong(response, "c", expected);
    }

    private static void assertSingleLong(EsqlQueryResponse response, String columnName, long expected) {
        List<? extends ColumnInfo> columns = response.columns();
        assertThat(columns.size(), equalTo(1));
        assertThat(columns.get(0).name(), equalTo(columnName));
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

    private Path writeGzippedCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("multiscan_count_pushdown_test.csv.gz");
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(tempFile))) {
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        }
        return tempFile;
    }
}
