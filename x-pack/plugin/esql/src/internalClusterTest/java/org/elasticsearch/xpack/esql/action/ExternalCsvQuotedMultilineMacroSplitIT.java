/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end regression coverage for CSV with quoted multi-line fields, where a quoted value's embedded
 * newlines belong to one logical row. A quote-blind stride probe assumes {@code inQuotes=false} at its
 * offset, so probing a quoted file at an arbitrary stride can land <em>inside</em> a quoted value and
 * misread an interior newline as a record terminator. That produces two symptoms depending on
 * {@code error_mode}: a silently inflated {@code COUNT(*)} at HTTP 200 under {@code null_field}, and a
 * spurious parse error under the default {@code strict} mode, both on files that are perfectly valid.
 * <p>
 * {@code CsvRecordSplitter} is not strided ({@code supportsStridedProbing()} is {@code false}) but is
 * proven-capable ({@code supportsProvenProbing()} is {@code true}): it macro-splits a quoted file by
 * proving each split boundary is a true record start (a bounded two-hypothesis probe with a monotonic
 * exact-walk fallback), so a boundary is never cut inside a quoted value. The file is therefore fanned
 * out into {@code target_split_size} macro-splits across nodes and each split reads correctly.
 * <ul>
 *   <li>{@code null_field} ({@link #testCountWithQuotedMultilineFieldStraddlingMacroSplitNullField}):
 *       does not over-count even though the file is macro-split.</li>
 *   <li>default {@code strict} ({@link #testCountWithQuotedMultilineFieldStraddlingMacroSplitStrict}):
 *       parses without a spurious error.</li>
 * </ul>
 * The body is multi-MB so macro-splits form (a file must exceed twice the reader's
 * {@code minimumSegmentSize()} of 1 MB to be split); the count stays exact because every split boundary
 * is a proven record start.
 */
public class ExternalCsvQuotedMultilineMacroSplitIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 18000;
    private static final int LINES_PER_ROW = 3;
    private static final int LINE_WIDTH = 60;
    private static final long TRUE_ROW_COUNT = ROWS;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 1).build());
    }

    /**
     * {@code error_mode=null_field}: a quote-blind mid-quote split would null-fill the interior lines and
     * keep them, inflating {@code COUNT(*)} at HTTP 200. Proven macro-splitting cuts only at true record
     * starts, so the count is exact even though the file is fanned out into multiple splits.
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
     * correctly. A quote-blind mid-quote macro-split makes the reader see a "missing closing quote" /
     * "unexpected character" and fail the query even though no record is malformed, the more serious failure
     * mode (a valid file rejected purely because of how it was split). Proven boundaries prevent it.
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
     * Exercises the quoted read path with in-node parse parallelism end-to-end. With
     * {@code parsing_parallelism>1} the ~3 MB quoted file is parsed concurrently; a correct count under
     * concurrent parsing is the regression signal: were segmentation not quote-aware, the concurrently
     * parsed chunks would miscount the multi-line rows. At least one external source operator must appear
     * in the profile.
     * <p>
     * This test does not assert a macro-split fan-out: the exact number of macro-splits depends on the file
     * size and segment sizing, which is environment-dependent here. That a quoted file macro-splits via the
     * proven-probe path is covered deterministically by
     * {@code FileSplitProviderTests#testDiscoverSplitsMacroSplitsQuotedCsv}, and proven boundary correctness
     * by {@code CsvProvenProbeTests}; the routing onto the record-aligned coordinator by
     * {@code AsyncExternalSourceOperatorFactoryTests}.
     */
    public void testStreamingBranchCountsCorrectlyWithParsingParallelism() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("parsing_parallelism pragma is snapshot-only", Build.current().isSnapshot());

        Path csvFile = writeCsvFile();
        try {
            String query = "EXTERNAL \""
                + StoragePath.fileUri(csvFile)
                + "\" WITH {\"header_row\":false,\"target_split_size\":\"1kb\"} | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            // Explicit pragma: run(request, ...) does not apply getPragmas(), and the default parallelism is
            // allocatedProcessors (machine-dependent). Set it >1 so this deterministically exercises in-node
            // parse parallelism regardless of host core count.
            request.pragmas(new QueryPragmas(Settings.builder().put("parsing_parallelism", between(2, 4)).build()));
            request.profile(true);

            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
                List<List<Object>> values = getValuesList(response);
                assertThat(values.size(), equalTo(1));
                assertThat(((Number) values.get(0).get(0)).longValue(), equalTo(TRUE_ROW_COUNT));

                List<AsyncExternalSourceOperator.Status> externalStatuses = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .filter(op -> op.operator().startsWith("ExternalDataSourceOperator"))
                    .map(OperatorStatus::status)
                    .filter(AsyncExternalSourceOperator.Status.class::isInstance)
                    .map(AsyncExternalSourceOperator.Status.class::cast)
                    .toList();
                assertThat("profile must report at least one external source operator", externalStatuses.size(), greaterThanOrEqualTo(1));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /**
     * Randomized end-to-end oracle: a fresh random quoted CSV per run, queried at a random
     * {@code target_split_size}, {@code parsing_parallelism} and {@code error_mode}, so the split fan-out and
     * parse concurrency vary from run to run. Column {@code a} is the row index {@code 0..rows-1}, so the trio
     * {@code COUNT(*) == rows}, {@code MIN(a) == 0}, {@code MAX(a) == rows-1} is an exact oracle: any boundary
     * cut inside a quoted field (which carries embedded newlines, commas, doubled {@code ""} quotes, and mixed
     * LF/CRLF) would either inflate the count with interior lines parsed as standalone rows, drop or duplicate a
     * row (shifting {@code MIN}/{@code MAX}), or throw under {@code strict}. A correct proven-boundary read must
     * reproduce the trio for every seed.
     * <p>
     * The seed is logged so a failure reproduces deterministically via the standard {@code -Dtests.seed}.
     */
    public void testRandomizedQuotedCsvCountsExactlyAcrossSplits() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());
        assumeTrue("parsing_parallelism pragma is snapshot-only", Build.current().isSnapshot());

        Path csvFile = createTempDir().resolve("random-quoted.csv");
        long rows = writeRandomQuotedCsv(csvFile);
        try {
            // Vary the split fan-out (target_split_size, some below and some above the 1 MiB segment floor), the
            // in-node parse concurrency, and the error mode. The oracle must hold across every combination. A
            // null error_mode omits the option so the reader keeps its default (strict / fail_fast), the mode
            // under which a mis-split throws; the explicit lenient modes are where a mis-split would miscount
            // silently instead. Every valid file must count exactly under all of them.
            String targetSplitSize = randomFrom("512kb", "1mb", "2mb", "4mb");
            int parallelism = between(1, 4);
            String errorMode = randomFrom((String) null, "fail_fast", "null_field", "skip_row");
            logger.info(
                "randomized quoted CSV: rows={} target_split_size={} parsing_parallelism={} error_mode={}",
                rows,
                targetSplitSize,
                parallelism,
                errorMode == null ? "<default strict>" : errorMode
            );

            String errorModeOption = errorMode == null ? "" : ",\"error_mode\":\"" + errorMode + "\"";
            String query = "EXTERNAL \""
                + StoragePath.fileUri(csvFile)
                + "\" WITH {\"target_split_size\":\""
                + targetSplitSize
                + "\""
                + errorModeOption
                + "} | STATS c = COUNT(*), mn = MIN(a), mx = MAX(a)";

            var request = syncEsqlQueryRequest(query);
            request.pragmas(new QueryPragmas(Settings.builder().put("parsing_parallelism", parallelism).build()));

            try (var response = run(request, TimeValue.timeValueMinutes(5))) {
                List<List<Object>> values = getValuesList(response);
                assertThat(values.size(), equalTo(1));
                assertThat("COUNT(*) must equal the true row count", ((Number) values.get(0).get(0)).longValue(), equalTo(rows));
                assertThat("MIN(a) must be the first row index", ((Number) values.get(0).get(1)).longValue(), equalTo(0L));
                assertThat("MAX(a) must be the last row index", ((Number) values.get(0).get(2)).longValue(), equalTo(rows - 1));
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    /**
     * Writes a random quoted CSV to {@code file} and returns its row count. A header {@code a,b} names the two
     * columns; column {@code a} is the numeric row index {@code 0..rows-1} (the oracle key) and column {@code b}
     * is a fully quoted value carrying a random mix of embedded newlines (LF and CRLF), commas, and doubled
     * {@code ""} quotes, exactly the constructs a quote-blind stride probe misreads. The file is grown past
     * {@code 2 x} {@code minimumSegmentSize} (1 MiB) to a random size so macro-splits reliably form, while each
     * value stays small enough that schema inference (which samples only ~1 KB) sees the first row close.
     */
    private long writeRandomQuotedCsv(Path file) throws Exception {
        int targetBytes = between(2_500_000, 5_000_000);
        StringBuilder sb = new StringBuilder(targetBytes + 1024);
        sb.append("a,b\n");
        long rows = 0;
        while (sb.length() < targetBytes) {
            sb.append(rows).append(",\"").append(randomQuotedFieldBody()).append('"');
            sb.append(randomBoolean() ? "\n" : "\r\n");
            rows++;
        }
        Files.writeString(file, sb);
        return rows;
    }

    /**
     * A random quoted-field body (the bytes between the surrounding {@code "..."}): a handful of tokens joined by
     * separators that are legal only inside a quoted field, so a correct parser keeps the whole thing as one
     * value while a mid-quote split would split it into bogus records. Never emits a lone {@code "} (only doubled
     * {@code ""}) so the field stays well-formed.
     */
    private String randomQuotedFieldBody() {
        int tokens = between(1, 6);
        StringBuilder body = new StringBuilder();
        for (int t = 0; t < tokens; t++) {
            if (t > 0) {
                // A separator that is in-field content only because we are inside quotes.
                body.append(randomFrom("\n", "\r\n", ",", " ", "\"\""));
            }
            body.append(randomAlphaOfLengthBetween(1, 20));
        }
        return body.toString();
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
