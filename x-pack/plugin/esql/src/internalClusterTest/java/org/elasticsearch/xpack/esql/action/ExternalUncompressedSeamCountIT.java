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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.BufferedWriter;
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
 * Reproduction harness for the byte-range split-seam case — a record dropped at a byte-range split seam
 * yields an EXACTLY-one-short {@code COUNT(*)} on uncompressed CSV, silently at HTTP 200.
 * <p>
 * The sibling {@code ExternalUncompressedMultiFileSegmentCapIT} exercises the {@code header_row=true},
 * one-macro-split-per-file path (large {@code target_split_size}) and passes. This harness deliberately
 * hits the paths that one does not:
 * <ul>
 *   <li><b>{@code header_row=false} + {@code error_mode=null_field}</b> — the exact ClickBench repro
 *       options; with no header, a dropped seam/first record is not masked by header accounting.</li>
 *   <li><b>multiple byte-range macro-splits per file</b> — a small {@code target_split_size} cuts each
 *       file into several {@link org.elasticsearch.xpack.esql.datasources.FileSplit}s, so the seams
 *       between adjacent macro-splits (the {@code FileSplitProvider} locus) are crossed.</li>
 *   <li><b>a record landing exactly on a stride boundary</b> — fixed-width rows + a stride that is an
 *       exact multiple of the row width, so a record starts precisely at a macro-split offset.</li>
 *   <li><b>no trailing newline on the file's last record</b> — the final record carries no terminator.</li>
 * </ul>
 * Column {@code col0} is globally unique per row (0..total-1), so {@code COUNT/MIN/MAX} together prove
 * every record was seen exactly once across every seam.
 */
public class ExternalUncompressedSeamCountIT extends AbstractEsqlIntegTestCase {

    /** Re-enables datasource extension loading that {@link EsqlPluginWithEnterpriseOrTrialLicense} suppresses. */
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
        // parsing_parallelism > 1 selects the SEGMENTABLE_UNCOMPRESSED parallel-parse path.
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 8).put("max_concurrent_open_segments", 2).build());
    }

    /**
     * Multiple byte-range macro-splits per file, headerless, with a record landing exactly on each stride
     * boundary (fixed-width rows; stride a multiple of the row width). This is the strongest candidate for
     * the seam off-by-one — a record whose first byte is exactly a macro-split offset.
     */
    public void testCsvHeaderlessMultiMacroSplitRecordOnSeam() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path dir = createTempDir();
        // Fixed 32-byte rows: "col0,<padded-b>\n". target_split_size 1mb is an exact multiple of 32,
        // so every stride boundary lands on a record start. CSV's minimum segment is 1 MiB, so a 1 MiB
        // stride yields macro-splits at the segmenting floor (one read each) and isolates the macro seam.
        long rows = writeFixedWidthCsv(dir.resolve("part-0.csv"), 0, 4 * 1024 * 1024, true);
        assertExactCount(globUri(dir, "*.csv"), "\"header_row\":false,\"error_mode\":\"null_field\",\"target_split_size\":\"1mb\"", rows);
    }

    /** Headerless multi-file glob, several macro-splits per file, trailing newline present. */
    public void testCsvHeaderlessMultiFileMultiMacroSplit() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < 3; f++) {
            total += writeVarWidthCsv(dir.resolve("part-" + f + ".csv"), total, 5 * 1024 * 1024, true);
        }
        assertExactCount(globUri(dir, "*.csv"), "\"header_row\":false,\"error_mode\":\"null_field\",\"target_split_size\":\"1mb\"", total);
    }

    /** The file's final record carries NO trailing newline, across multiple macro-splits. */
    public void testCsvHeaderlessMultiMacroSplitNoTrailingNewline() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path dir = createTempDir();
        long rows = writeVarWidthCsv(dir.resolve("part-0.csv"), 0, 5 * 1024 * 1024, false);
        assertExactCount(globUri(dir, "*.csv"), "\"header_row\":false,\"error_mode\":\"null_field\",\"target_split_size\":\"1mb\"", rows);
    }

    /** Headerless, single macro-split per file (large target_split_size) but intra-file segmented. */
    public void testCsvHeaderlessIntraFileSegmented() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        Path dir = createTempDir();
        long total = 0;
        for (int f = 0; f < 3; f++) {
            total += writeVarWidthCsv(dir.resolve("part-" + f + ".csv"), total, 3_500_000, true);
        }
        assertExactCount(
            globUri(dir, "*.csv"),
            "\"header_row\":false,\"error_mode\":\"null_field\",\"target_split_size\":\"256mb\"",
            total
        );
    }

    private void assertExactCount(String globUri, String withOptions, long total) throws Exception {
        String query = "EXTERNAL \""
            + globUri
            + "\" WITH {"
            + withOptions
            + "} | RENAME col0 AS a | STATS c = COUNT(*), mn = MIN(a), mx = MAX(a)";

        var request = syncEsqlQueryRequest(query);
        try (var response = run(request, TimeValue.timeValueMinutes(5))) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns.size(), equalTo(3));
            List<List<Object>> values = getValuesList(response);
            assertThat(values.size(), equalTo(1));
            List<Object> row = values.get(0);
            assertThat("COUNT(*) must equal the true row count across every seam", ((Number) row.get(0)).longValue(), equalTo(total));
            assertThat("MIN(col0) proves the first record was not dropped", ((Number) row.get(1)).longValue(), equalTo(0L));
            assertThat("MAX(col0) proves the last record was not dropped", ((Number) row.get(2)).longValue(), equalTo(total - 1));
        }
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    /**
     * Writes variable-width rows {@code col0,col1\n} where {@code col0} runs {@code base..base+rows-1}, until
     * at least {@code minBytes} have been written. Returns rows written. When {@code trailingNewline} is false
     * the final record is written without its terminating {@code \n}.
     */
    private static long writeVarWidthCsv(Path file, long base, int minBytes, boolean trailingNewline) throws Exception {
        StringBuilder sb = new StringBuilder();
        long rows = 0;
        int written = 0;
        while (written < minBytes) {
            long a = base + rows;
            sb.append(a).append(',').append(a * 10).append('\n');
            written = sb.length();
            rows++;
        }
        if (trailingNewline == false && sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') {
            sb.deleteCharAt(sb.length() - 1);
        }
        Files.write(file, sb.toString().getBytes(StandardCharsets.US_ASCII));
        return rows;
    }

    /**
     * Writes fixed-32-byte rows so record boundaries land at deterministic byte offsets: {@code col0} is the
     * row's global index, {@code col1} is padded so each line (including {@code \n}) is exactly 32 bytes.
     * Chooses a row count so {@code rows*32 >= minBytes}. With a stride that is a multiple of 32, a record
     * starts exactly on every macro-split offset.
     */
    private static long writeFixedWidthCsv(Path file, long base, int minBytes, boolean trailingNewline) throws Exception {
        final int rowWidth = 32;
        long rows = (minBytes + rowWidth - 1) / rowWidth;
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.US_ASCII)) {
            for (long i = 0; i < rows; i++) {
                long a = base + i;
                String head = a + ",";
                int padLen = rowWidth - head.length() - 1; // -1 for the trailing '\n'
                StringBuilder line = new StringBuilder(rowWidth);
                line.append(head);
                for (int p = 0; p < padLen; p++) {
                    line.append('x');
                }
                if (trailingNewline == false && i == rows - 1) {
                    // Drop the final newline: pad one extra so the row still occupies rowWidth bytes.
                    line.append('x');
                } else {
                    line.append('\n');
                }
                assert line.length() == rowWidth : "row width drift: " + line.length();
                w.write(line.toString());
            }
        }
        return rows;
    }
}
