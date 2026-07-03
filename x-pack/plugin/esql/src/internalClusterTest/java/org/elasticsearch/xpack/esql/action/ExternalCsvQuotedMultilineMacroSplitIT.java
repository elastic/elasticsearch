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
 * End-to-end regression coverage for CSV with quoted multi-line fields, where a quoted value's embedded
 * newlines belong to one logical row. A start-anywhere boundary probe assumes {@code inQuotes=false} at
 * its offset, so probing a quoted file at an arbitrary stride can land <em>inside</em> a quoted value and
 * misread an interior newline as a record terminator. That once produced two symptoms depending on
 * {@code error_mode}: a silently inflated {@code COUNT(*)} at HTTP 200 under {@code null_field}, and a
 * spurious parse error under the default {@code strict} mode, both on files that are perfectly valid.
 * <p>
 * The shipped fix does not make the probe quote-aware; instead it disables start-anywhere splitting for
 * quoted CSV/TSV. {@code CsvRecordSplitter#supportsStridedProbing()} returns {@code false}, so
 * {@code FileSplitProvider} emits a single whole-file split and the file is read as one sequential,
 * quote-aware stream. Consequently {@code target_split_size} is a no-op for quoted files here (they are
 * never macro-split), and both cases below now return the true row count.
 * <ul>
 *   <li>{@code null_field} ({@link #testCountWithQuotedMultilineFieldStraddlingMacroSplitNullField}):
 *       no longer over-counts.</li>
 *   <li>default {@code strict} ({@link #testCountWithQuotedMultilineFieldStraddlingMacroSplitStrict}):
 *       no longer throws.</li>
 * </ul>
 * The body is multi-MB so that, absent the fix, macro-splits would form (a file must exceed twice the
 * reader's {@code minimumSegmentSize()} of 1 MB to be split); it stays large so this pins the whole-file
 * gate rather than trivially avoiding splits by being small.
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
     * {@code error_mode=null_field}: without the fix a mid-quote split would null-fill the interior lines
     * and keep them, inflating {@code COUNT(*)} at HTTP 200. With quoted files gated to a whole-file
     * sequential read, the count is exact.
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
     * Default {@code strict} mode (no {@code error_mode} option): the same valid file must parse and count
     * correctly. Without the fix a mid-quote macro-split makes the reader see a "missing closing quote" /
     * "unexpected character" and fail the query even though no record is malformed, the more serious face
     * of the bug (a valid file rejected purely because of how it was split). The whole-file gate removes it.
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
     * Exercises the parallel streaming path end-to-end. The other two tests reach the correct count even
     * single-threaded (the whole-file gate alone is enough), so on their own they do not prove the
     * {@code SEGMENTABLE_UNCOMPRESSED_SEQUENTIAL} branch is quote-safe. Here {@code parsing_parallelism>1}
     * makes {@code AsyncExternalSourceOperatorFactory#openWithParallelism} pass its {@code <=1} short-circuit
     * and, for a quoted (non-strided) reader, route the single whole-file stream into
     * {@code StreamingParallelParsingCoordinator}, which segments it quote-aware and parses the chunks
     * concurrently. That routing is deterministic from the reader/split, so a correct count under
     * concurrent parsing is the regression signal: were segmentation not quote-aware, the concurrently
     * parsed chunks would miscount the multi-line rows.
     */
    public void testStreamingBranchCountsCorrectlyWithParsingParallelism() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path csvFile = writeCsvFile();
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(csvFile)
                + "\" WITH {\"header_row\":false,\"target_split_size\":\"1kb\"} | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            // Explicit pragma: run(request, ...) does not apply getPragmas(), and the default parallelism is
            // allocatedProcessors (machine-dependent). Pin it >1 so this deterministically takes the parallel
            // streaming branch regardless of host core count.
            request.pragmas(new QueryPragmas(Settings.builder().put("parsing_parallelism", between(2, 4)).build()));
            request.profile(true);

            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
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
