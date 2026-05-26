/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

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
 * End-to-end regression test for ES|QL EXTERNAL aggregations over an uncompressed
 * multi-file CSV/NDJSON glob. Such reads route through {@code SEGMENTABLE_UNCOMPRESSED} →
 * {@code ParallelParsingCoordinator} → {@code OrderedParallelIterator}, which the fix changed to dispatch
 * byte-range segments in a sliding window of {@code MAX_CONCURRENT_OPEN_SEGMENTS}.
 * <p>
 * Many files in one glob, a small {@code target_split_size}, and {@code parsing_parallelism > 1} force a
 * high per-file segment count; the test asserts the aggregation is complete and correct — the windowed
 * dispatch drops no segment, double-counts none, preserves row accounting across boundaries. The
 * deterministic peak-open-stream bound is the unit test
 * {@code ParallelParsingCoordinatorTests#testConcurrentOpenSegmentsAreCapped}; this is its end-to-end
 * correctness counterpart.
 */
public class ExternalUncompressedMultiFileSegmentCapIT extends AbstractEsqlIntegTestCase {

    // More files than MAX_CONCURRENT_OPEN_SEGMENTS (=4) so the glob fans out beyond a single window.
    private static final int FILE_COUNT = 8;
    private static final int ROWS_PER_FILE = 5_000;
    private static final int TOTAL_ROWS = FILE_COUNT * ROWS_PER_FILE;

    /**
     * Re-enables datasource extension loading that {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses.
     */
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
        plugins.add(NdJsonDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 selects the SEGMENTABLE_UNCOMPRESSED parallel-parse path (the one the cap
        // guards) rather than the single-threaded fallback.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 8).build());
    }

    public void testCsvMultiFileGlobAggregatesAllRows() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path dir = createTempDir();
        for (int f = 0; f < FILE_COUNT; f++) {
            writeCsvFile(dir.resolve("part-" + f + ".csv"), f * ROWS_PER_FILE);
        }
        assertGlobAggregates(globUri(dir, "*.csv"));
    }

    public void testNdjsonMultiFileGlobAggregatesAllRows() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path dir = createTempDir();
        for (int f = 0; f < FILE_COUNT; f++) {
            writeNdjsonFile(dir.resolve("part-" + f + ".ndjson"), f * ROWS_PER_FILE);
        }
        assertGlobAggregates(globUri(dir, "*.ndjson"));
    }

    /**
     * Runs {@code STATS COUNT/MIN/MAX} over the glob. Column {@code a} is globally unique per row
     * (file index × rows + row), so COUNT/MIN/MAX together prove no segment was dropped, duplicated, or
     * mis-ordered. A small {@code target_split_size} forces many byte-range segments per file.
     */
    private void assertGlobAggregates(String globUri) throws Exception {
        String query = "EXTERNAL \"" + globUri + "\" WITH {\"target_split_size\":\"4kb\"} | STATS c = COUNT(*), mn = MIN(a), mx = MAX(a)";

        var request = syncEsqlQueryRequest(query);
        request.profile(true);

        try (var response = run(request, TimeValue.timeValueMinutes(5))) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.size(), equalTo(3));
            assertThat(columns.get(0).name(), equalTo("c"));
            assertThat(columns.get(1).name(), equalTo("mn"));
            assertThat(columns.get(2).name(), equalTo("mx"));

            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), equalTo(1));
            List<Object> row = values.get(0);
            assertThat(
                "every row from every file must be counted exactly once",
                ((Number) row.get(0)).longValue(),
                equalTo((long) TOTAL_ROWS)
            );
            assertThat(((Number) row.get(1)).longValue(), equalTo(0L));
            assertThat(((Number) row.get(2)).longValue(), equalTo((long) TOTAL_ROWS - 1));

            // Confirm the read actually went through the async external source operator, and that the
            // operator emitted every row (no segment silently dropped by the windowed dispatch).
            long rowsEmitted = 0;
            boolean foundAsyncOperator = false;
            for (var driver : response.profile().drivers()) {
                for (var op : driver.operators()) {
                    if (op.status() instanceof AsyncExternalSourceOperator.Status status) {
                        foundAsyncOperator = true;
                        rowsEmitted += status.rowsEmitted();
                    }
                }
            }
            assertTrue("expected at least one AsyncExternalSourceOperator in the profile", foundAsyncOperator);
            assertThat("source operator must emit every row across all files", rowsEmitted, equalTo((long) TOTAL_ROWS));
        }
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    private static void writeCsvFile(Path file, int baseValue) throws Exception {
        StringBuilder sb = new StringBuilder(ROWS_PER_FILE * 16);
        sb.append("a,b\n");
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            int a = baseValue + i;
            sb.append(a).append(",").append(a * 10).append("\n");
        }
        Files.writeString(file, sb);
    }

    private static void writeNdjsonFile(Path file, int baseValue) throws Exception {
        StringBuilder sb = new StringBuilder(ROWS_PER_FILE * 24);
        for (int i = 0; i < ROWS_PER_FILE; i++) {
            int a = baseValue + i;
            sb.append("{\"a\":").append(a).append(",\"b\":").append(a * 10).append("}\n");
        }
        Files.writeString(file, sb);
    }
}
