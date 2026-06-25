/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * When the streaming-parallel segmentator hits the {@code max_record_size} cap on a stream-only
 * (gzip) input, the read must honor the {@code error_mode}: a strict policy still hard-fails, while a
 * non-strict policy truncates the read at the undelimitable record and returns the records parsed so
 * far (rather than failing the whole query). The truncation also emits a partial-results
 * {@code Warning} and a server-side log line, but that emission is asserted at the unit level (see
 * {@code StreamingParallelParsingCoordinatorTests}) — surfacing it to the client depends on
 * external-read response-header propagation that is orthogonal to this change. Follow-up to the
 * record-boundary livelock fix (capped grow loop). See elastic/esql-planning#835.
 */
public class ExternalMaxRecordSizeTruncationIT extends AbstractEsqlIntegTestCase {

    private static final int LEADING_ROWS = 5;
    /**
     * A single field whose size exceeds the {@code max_record_size} cap (pinned to {@code 1mb} in
     * {@link #pragmas}). It has no <em>internal</em> newline, so while the segmentator grows its buffer to
     * find the record boundary it trips the cap before reaching the field's terminator. The cap-hit — and
     * therefore the partial count under {@code skip_row} — is guaranteed by
     * {@code GIANT_RECORD_BYTES > max_record_size}, independent of chunk sizing.
     */
    private static final int GIANT_RECORD_BYTES = 4 * 1024 * 1024;

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

    /**
     * Default (strict) policy: a record exceeding {@code max_record_size} must fail the query fast with
     * a diagnosable error, as before this change.
     */
    public void testStrictPolicyHardFailsOnOversizedRecord() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        Path file = writeGzipWithOversizedRecord();
        try {
            String query = "EXTERNAL \"" + StoragePath.fileUri(file) + "\" WITH {\"header_row\": false} | STATS c = COUNT(*)";
            // Pin allow_partial_results=false (cluster default is true) so the hard failure is thrown
            // rather than swallowed into a partial response.
            EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(pragmas(4, "1mb")).allowPartialResults(false);
            Exception e = expectThrows(Exception.class, () -> run(request, TimeValue.timeValueMinutes(2)).close());
            String trace = ExceptionsHelper.stackTrace(e);
            assertTrue("strict policy must hard-fail on the cap-hit, got: " + trace, trace.contains("record exceeded max_record_size"));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Non-strict policy ({@code error_mode: skip_row}): the read truncates at the oversized record and
     * returns the {@link #LEADING_ROWS} rows parsed before it, instead of failing the whole query. The
     * truncation also emits a partial-results {@code Warning} (and a server-side log line) via the same
     * {@link org.elasticsearch.common.logging.HeaderWarning} channel the lenient policies use for skipped
     * rows; that emission is unit-tested in {@code StreamingParallelParsingCoordinatorTests}. It is not
     * asserted here because the segmentator runs on a forked external-read thread whose response headers
     * are not collected back into the client response — a pre-existing limitation shared by all
     * external-read warnings, orthogonal to this truncation change.
     */
    public void testSkipRowPolicyReturnsPartialResults() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("max_record_size / parsing_parallelism pragmas are snapshot-only", Build.current().isSnapshot());
        Path file = writeGzipWithOversizedRecord();
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(file)
                + "\" WITH {\"header_row\": false, \"error_mode\": \"skip_row\"} | STATS c = COUNT(*)";
            EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(pragmas(4, "1mb"));
            try (EsqlQueryResponse response = run(request, TimeValue.timeValueMinutes(2))) {
                List<List<Object>> values = getValuesList(response);
                long count = ((Number) values.get(0).get(0)).longValue();
                assertThat("non-strict read must return only the rows parsed before the cap-hit", count, equalTo((long) LEADING_ROWS));
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private QueryPragmas pragmas(int parsingParallelism, String maxRecordSize) {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.PARSING_PARALLELISM.getKey(), parsingParallelism)
                .put(QueryPragmas.MAX_RECORD_SIZE.getKey(), maxRecordSize)
                .build()
        );
    }

    /**
     * Writes a gzip TSV with {@link #LEADING_ROWS} clean rows, then a single {@link #GIANT_RECORD_BYTES}-byte
     * field terminated by a newline, then a couple of trailing rows. The giant field has no <em>internal</em>
     * newline, so as the segmentator grows its buffer to locate that record's boundary it exceeds
     * {@code max_record_size} and trips {@code RECORD_TOO_LARGE} before consuming the terminator — which is
     * what truncates the read. The trailing rows past the giant record are therefore never reached.
     */
    private Path writeGzipWithOversizedRecord() throws Exception {
        Path file = createTempDir().resolve("oversized.tsv.gz");
        try (
            OutputStream os = Files.newOutputStream(file);
            OutputStream gz = new GZIPOutputStream(os);
            Writer w = new OutputStreamWriter(gz, StandardCharsets.UTF_8)
        ) {
            for (int i = 0; i < LEADING_ROWS; i++) {
                w.write("c0_" + i + "\tc1_" + i + "\tc2_" + i + "\n");
            }
            w.write("x".repeat(GIANT_RECORD_BYTES));
            w.write("\n");
            w.write("after0_0\tafter1_0\tafter2_0\n");
            w.write("after0_1\tafter1_1\tafter2_1\n");
        }
        return file;
    }
}
