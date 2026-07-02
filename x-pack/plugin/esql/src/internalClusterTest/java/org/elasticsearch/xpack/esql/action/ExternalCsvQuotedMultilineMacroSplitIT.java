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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end reproduction of the silent wrong-count bug for CSV with quoted multi-line fields
 * (esql-planning#896, problem B, hypothesis (b): a quoted field with an embedded newline straddling
 * a split seam where the probing and reading layers disagree on quote state).
 * <p>
 * The file has a single column. Every row holds a quoted value with embedded newlines; because the
 * column is quoted, those newlines belong to one logical row. The record-alignment probe
 * ({@code FileSplitProvider#computeRecordAlignedMacroSplitStarts}) assumes {@code inQuotes=false} at
 * each {@code target_split_size} stride offset, so it lands many macro-split starts <em>inside</em>
 * quoted values. Each such split then parses the interior lines as standalone rows.
 * <p>
 * The same defect surfaces with two different symptoms depending on {@code error_mode}, and both are
 * covered here so the fix can be verified against both:
 * <ul>
 *   <li>{@code null_field} ({@link #testCountWithQuotedMultilineFieldStraddlingMacroSplitNullField}):
 *       the malformed mid-quote content is null-filled and the rows kept, so {@code COUNT(*)} comes
 *       back <em>silently</em> wrong at HTTP 200.</li>
 *   <li>default {@code strict} ({@link #testCountWithQuotedMultilineFieldStraddlingMacroSplitStrict}):
 *       the same valid file fails the query with a parse error ("missing closing quote" /
 *       "unexpected character"), even though no record is actually malformed.</li>
 * </ul>
 * Both methods assert the correct outcome (the true row count), so both are red on {@code main} until
 * macro-split boundary alignment becomes quote-aware: the {@code null_field} case currently returns a
 * wrong count, and the {@code strict} case currently throws.
 * <p>
 * The file must exceed twice the reader's {@code minimumSegmentSize()} (1 MB) for macro-splits to be
 * created at all, hence the multi-MB body.
 */
public class ExternalCsvQuotedMultilineMacroSplitIT extends AbstractEsqlIntegTestCase {

    private static final int ROWS = 18000;
    private static final int LINES_PER_ROW = 3;
    private static final int LINE_WIDTH = 60;
    private static final long TRUE_ROW_COUNT = ROWS;

    /**
     * Re-enables extension loading that {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses.
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
        return plugins;
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    /**
     * {@code error_mode=null_field}: the mis-split produces a silently wrong {@code COUNT(*)} at HTTP 200.
     * Red on {@code main} (returns an inflated count); green once boundary alignment is quote-aware.
     */
    public void testCountWithQuotedMultilineFieldStraddlingMacroSplitNullField() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path csvFile = writeCsvFile();
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(csvFile)
                + "\" WITH {\"header_row\":false,\"error_mode\":\"null_field\",\"target_split_size\":\"1kb\"} | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
                List<? extends ColumnInfo> columns = response.columns();
                assertThat(columns.size(), equalTo(1));
                assertThat(columns.get(0).name(), equalTo("c"));

                List<List<Object>> values = getValuesList(response);
                assertThat(values.size(), equalTo(1));
                assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(TRUE_ROW_COUNT));

                long asyncOps = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .filter(op -> op.operator().startsWith("ExternalDataSourceOperator"))
                    .count();
                assertThat(asyncOps, greaterThanOrEqualTo(1L));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /**
     * Default {@code strict} mode (no {@code error_mode} option): the very same valid file must parse and
     * count correctly. Red on {@code main} because the mid-quote macro-split makes the reader see a
     * "missing closing quote" / "unexpected character" and fail the query, even though no record is
     * malformed. Green once boundary alignment is quote-aware. This is the more serious face of the bug:
     * a perfectly valid file is rejected purely because of how it was split.
     */
    public void testCountWithQuotedMultilineFieldStraddlingMacroSplitStrict() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path csvFile = writeCsvFile();
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(csvFile)
                + "\" WITH {\"header_row\":false,\"target_split_size\":\"1kb\"} | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);

            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
                List<? extends ColumnInfo> columns = response.columns();
                assertThat(columns.size(), equalTo(1));
                assertThat(columns.get(0).name(), equalTo("c"));

                List<List<Object>> values = getValuesList(response);
                assertThat(values.size(), equalTo(1));
                assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(TRUE_ROW_COUNT));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /**
     * Single-column CSV of {@value #ROWS} rows (no header line: {@code header_row=false}), each a small
     * quoted value spanning {@value #LINES_PER_ROW} embedded lines of {@value #LINE_WIDTH} chars (~190 B
     * per row, ~3 MB total). The sizing balances two opposing constraints:
     * <ul>
     *   <li>The schema-inference sampler only reads ~1 KB, so a single value cannot be both larger than
     *       the 1 KB stride (to straddle it) and small enough to parse during inference. Keeping values
     *       small lets the first row close and inference succeed.</li>
     *   <li>Straddling is then achieved by density rather than size: with ~1 KB strides over a ~3 MB
     *       body, stride offsets repeatedly land inside the small quoted values, and any offset before a
     *       value's last embedded newline is mis-detected as a record boundary.</li>
     * </ul>
     * The total comfortably exceeds {@code 2 x} {@code minimumSegmentSize} (1 MB) so macro-splits form.
     * The interior lines are comma-free, so a misaligned split parses them as extra rows that inflate
     * the count.
     */
    private Path writeCsvFile() throws Exception {
        Path file = createTempDir().resolve("macro-splits-quoted.csv");
        StringBuilder sb = new StringBuilder(ROWS * LINES_PER_ROW * (LINE_WIDTH + 2));
        for (int i = 0; i < ROWS; i++) {
            sb.append("\"");
            for (int line = 0; line < LINES_PER_ROW; line++) {
                sb.append("x".repeat(LINE_WIDTH)).append("\n");
            }
            sb.append("\"\n");
        }
        Files.writeString(file, sb);
        return file;
    }
}
