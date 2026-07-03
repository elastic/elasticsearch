/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetReaderStatus;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end coverage that the profile-observability fields added to {@code AsyncExternalSourceOperator.Status}
 * and {@code EsqlQueryProfile.dataset_resolution} are populated when {@code FROM <dataset>}
 * queries execute against a local Parquet fixture.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ExternalSourceProfileIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class, CsvDataSourcePlugin.class);
    }

    /** Pin the planner to deterministic shapes so the AsyncExternalSourceOperator is reliably present. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testExternalQueryProfileFieldsArePopulated() throws Exception {
        Path parquetFile = writeParquetFile(300, 100);
        try {
            String dataset = registerDataset("profile", StoragePath.fileUri(parquetFile), Map.of());
            String query = "FROM " + dataset + " | LIMIT 5";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("profile must be present (request had profile=true)", response.profile());
                AsyncExternalSourceOperator.Status status = findAsyncExternalSourceStatus(response);
                // splitsTotal/currentSplit/processNanos are set synchronously by the operator and producer
                // before the consumer can observe EOF, so they are reliable across single-file paths.
                assertThat("process_nanos should be populated by the read loop", status.processNanos(), greaterThan(0L));
                assertThat(status.splitsTotal(), greaterThanOrEqualTo(1));
                assertThat(status.currentSplit(), greaterThanOrEqualTo(1));
                // bytesRead/splitsProcessed/formatReader live behind a producer-completion async hop on the
                // single-file path; the consumer can observe EOF before they're written, so the only reliable
                // assertion here is that the fields are present (i.e. not negative for the numeric ones).
                assertThat(status.splitsProcessed(), greaterThanOrEqualTo(0));
                assertThat(status.bytesRead(), greaterThanOrEqualTo(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    public void testExternalScanCountersArePopulated() throws Exception {
        // Many rows with small row groups produce several row-group splits for a single file.
        Path parquetFile = writeParquetFile(300, 25);
        try {
            String dataset = registerDataset("profile", StoragePath.fileUri(parquetFile), Map.of());
            String query = "FROM " + dataset + " | LIMIT 5";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("execution info must be present", response.getExecutionInfo());
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                // A single Parquet file is scanned, split into one or more row-group ranges.
                assertEquals("exactly one file scanned", 1, profile.filesScanned());
                assertThat("at least one split scanned", profile.splitsScanned(), greaterThanOrEqualTo(1));
                assertThat("bytes scanned populated", profile.bytesScanned(), greaterThan(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * A {@code COUNT(*)} that can be answered from the source's row-count metadata must not scan any
     * files: split discovery is skipped (see {@code ComputeService.canSkipSplitDiscovery}), so the
     * scan counters stay at zero and are omitted from the profile JSON.
     */
    public void testMetadataOnlyCountStarScansNoFiles() throws Exception {
        Path parquetFile = writeParquetFile(300, 25);
        try {
            String dataset = registerDataset("profile", StoragePath.fileUri(parquetFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("execution info must be present", response.getExecutionInfo());
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                assertEquals("metadata-only COUNT(*) must scan no files", 0, profile.filesScanned());
                assertEquals("metadata-only COUNT(*) must scan no splits", 0, profile.splitsScanned());
                assertEquals("metadata-only COUNT(*) must scan no bytes", 0L, profile.bytesScanned());
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * CSV has no embedded row count, so the first {@code COUNT(*)} must scan the whole file (the scan
     * counters are populated). After execution the row count is reconciled into the coordinator's
     * source-stats cache, so a second identical {@code COUNT(*)} is answered from metadata and scans
     * nothing, so the counters return to zero. This is the "read it once, then never again" behavior.
     */
    public void testCsvCountStarScansColdThenSkipsWarm() throws Exception {
        int rowCount = 200;
        Path csvFile = writeCsvFile(rowCount);
        try {
            String dataset = registerDataset("profile_csv", StoragePath.fileUri(csvFile), Map.of());
            String query = "FROM " + dataset + " | STATS c = COUNT(*)";

            // COLD: no cached row count yet, so the file is scanned to answer COUNT(*).
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCountValue(response, rowCount);
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                assertEquals("cold COUNT(*) scans the one CSV file", 1, profile.filesScanned());
                // The CSV file is far below the 64MB target split size and CSV is not range-aware, so it
                // produces exactly one whole-file split whose estimated size is the file length. Asserting
                // the exact split count also proves the scan stats are recorded once: if the top-level and
                // fragment discovery paths ever both counted this source, splitsScanned would read 2.
                assertEquals("cold COUNT(*) scans exactly one split", 1, profile.splitsScanned());
                assertEquals("cold COUNT(*) reads the whole CSV file", Files.size(csvFile), profile.bytesScanned());
            }

            // WARM: the row count was reconciled into the coordinator cache, so COUNT(*) is served
            // from metadata and no file is scanned.
            try (var response = run(syncEsqlQueryRequest(query).profile(true))) {
                assertCountValue(response, rowCount);
                EsqlQueryProfile profile = response.getExecutionInfo().queryProfile();
                assertEquals("warm COUNT(*) scans no files", 0, profile.filesScanned());
                assertEquals("warm COUNT(*) scans no splits", 0, profile.splitsScanned());
                assertEquals("warm COUNT(*) scans no bytes", 0L, profile.bytesScanned());
            }
        } finally {
            Files.deleteIfExists(csvFile);
        }
    }

    public void testFromDatasetProfileHasDatasetResolutionSpan() throws Exception {
        Path parquetFile = writeParquetFile(300, 100);
        try {
            registerDataSource("profile_src", Map.of());
            registerDataset("profile_ds", "profile_src", StoragePath.fileUri(parquetFile), Map.of());

            var request = syncEsqlQueryRequest("FROM profile_ds | LIMIT 5");
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("profile must be present (request had profile=true)", response.profile());
                assertNotNull("execution info must be present", response.getExecutionInfo());

                TimeSpanMarker datasetMarker = response.getExecutionInfo().queryProfile().datasetResolution();
                assertThat("dataset_resolution marker must exist", datasetMarker, notNullValue());
                assertThat("dataset_resolution span must be recorded", datasetMarker.timeSpan(), notNullValue());

                AsyncExternalSourceOperator.Status status = findAsyncExternalSourceStatus(response);
                assertThat(status.processNanos(), greaterThan(0L));
                assertThat(status.splitsTotal(), greaterThanOrEqualTo(1));
                assertThat(status.currentSplit(), greaterThanOrEqualTo(1));
                assertThat(status.splitsProcessed(), greaterThanOrEqualTo(0));
                assertThat(status.bytesRead(), greaterThanOrEqualTo(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * End-to-end coverage for the {@code format_reader} sub-object on
     * {@link AsyncExternalSourceOperator.Status}. The query intentionally drains every row (no
     * {@code LIMIT}) so the producer reaches its terminal {@code DONE} / EOF callback before the
     * consumer reads the operator status — that callback is where the producer commits the latest
     * format-reader snapshot to the buffer. With a {@code LIMIT} short enough to short-circuit
     * before the producer drains, the consumer can observe {@code formatReader == Map.of()}
     * (the race the prior version of this test fell into and the reason it was deleted).
     * <p>
     * Drives a small fixture so the full drain is fast even without {@code LIMIT}.
     */
    public void testFormatReaderSnapshotPopulatedAfterFullDrain() throws Exception {
        Path parquetFile = writeParquetFile(50, 25);
        try {
            String dataset = registerDataset("profile", StoragePath.fileUri(parquetFile), Map.of());
            String query = "FROM " + dataset;

            var request = syncEsqlQueryRequest(query);
            request.profile(true);

            try (var response = run(request, TIMEOUT)) {
                assertNotNull("profile must be present (request had profile=true)", response.profile());
                AsyncExternalSourceOperator.Status status = findAsyncExternalSourceStatus(response);
                FormatReaderStatus formatReader = status.formatReader();
                assertThat(
                    "format_reader snapshot must be populated after the producer drains the file",
                    formatReader,
                    instanceOf(ParquetReaderStatus.class)
                );
                ParquetReaderStatus parquetStatus = (ParquetReaderStatus) formatReader;
                assertThat(
                    "multi-row-group fixture should report at least one row group",
                    parquetStatus.rowGroupsInFile(),
                    greaterThanOrEqualTo(1L)
                );
                // read_nanos is wall-time and can read as zero on fast / containerized CI runners
                // (sub-microsecond synchronous reads + low-resolution clocks). Assert non-negative
                // rather than a strict positive — the deterministic shape signal lives in row_groups_in_file.
                assertThat("read_nanos must be non-negative", parquetStatus.readNanos(), greaterThanOrEqualTo(0L));
            }
        } finally {
            Files.deleteIfExists(parquetFile);
        }
    }

    /**
     * End-to-end correctness for the deferred-footer-read optimization across a real multi-file glob.
     * Writes three Parquet files into one directory and exercises every {@code schema_resolution} path:
     * <ul>
     *   <li>{@code FIRST_FILE_WINS + LIMIT} — the <em>defer</em> path: no global stats are required, so
     *       at planning time only the anchor file's footer is read, yet the query must still return a
     *       correct page of rows.</li>
     *   <li>{@code FIRST_FILE_WINS + COUNT(*)} (ungrouped) — the <em>eager</em> path: the planner still
     *       aggregates every file's row count, so the result must span all files.</li>
     *   <li>{@code UNION_BY_NAME} / {@code STRICT + COUNT(*)} — the reconciliation paths, which always
     *       read every file's metadata; the count must stay correct (regression guard).</li>
     * </ul>
     * The per-file footer-read <em>count</em> reduction itself is asserted at the resolver layer in
     * {@code ExternalSourceResolverTests} (the layer that issues the reads); there is no query-level
     * metric exposing planning-time footer reads, so this IT proves correctness rather than GET counts.
     */
    public void testMultiFileFirstFileWinsDeferAndEagerProduceCorrectResults() throws Exception {
        Path dir = createTempDir();
        int files = 3;
        int rowsPerFile = 100;
        for (int i = 0; i < files; i++) {
            writeParquet(dir.resolve("part_" + i + ".parquet"), rowsPerFile, 50);
        }
        String glob = StoragePath.fileUri(dir) + "/*.parquet";
        long totalRows = (long) files * rowsPerFile;

        // FFW + LIMIT: defer path. Only the anchor footer is needed at planning time, but the page must be correct.
        try (var response = run(syncEsqlQueryRequest(datasetQuery(glob, "first_file_wins", "| LIMIT 10")), TIMEOUT)) {
            assertThat(getValuesList(response), hasSize(10));
        }

        // FFW + ungrouped COUNT(*): eager path — the global row count must span every file.
        assertSingleLong(datasetQuery(glob, "first_file_wins", "| STATS c = COUNT(*)"), totalRows);

        // UNION_BY_NAME / STRICT still read every file for schema reconciliation; the count stays correct.
        assertSingleLong(datasetQuery(glob, "union_by_name", "| STATS c = COUNT(*)"), totalRows);
        assertSingleLong(datasetQuery(glob, "strict", "| STATS c = COUNT(*)"), totalRows);
    }

    /**
     * Registers (or reuses) a dataset over {@code glob} with the given {@code schema_resolution} dataset
     * setting — the {@code FROM <dataset>} equivalent of the legacy {@code EXTERNAL ... WITH
     * {"schema_resolution": ...}} clause — and returns a {@code FROM <dataset> <tail>} query. Each
     * resolution mode gets its own dataset name so the three modes can coexist against the same glob.
     */
    private String datasetQuery(String glob, String schemaResolution, String tail) {
        String dataset = registerDataset("profile_ffw_" + schemaResolution, glob, Map.of("schema_resolution", schemaResolution));
        return "FROM " + dataset + " " + tail;
    }

    private void assertSingleLong(String query, long expected) {
        try (var response = run(syncEsqlQueryRequest(query), TIMEOUT)) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat((Long) rows.get(0).get(0), equalTo(expected));
        }
    }

    private static AsyncExternalSourceOperator.Status findAsyncExternalSourceStatus(EsqlQueryResponse response) {
        AsyncExternalSourceOperator.Status found = null;
        assertThat(response.profile(), notNullValue());
        for (var driver : response.profile().drivers()) {
            for (var op : driver.operators()) {
                if (op.status() instanceof AsyncExternalSourceOperator.Status s) {
                    found = s;
                }
            }
        }
        assertThat("expected at least one AsyncExternalSourceOperator.Status in the driver profiles", found, notNullValue());
        return found;
    }

    private static void assertCountValue(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    private Path writeCsvFile(int rowCount) throws IOException {
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:integer\n");
        for (int i = 0; i < rowCount; i++) {
            sb.append(i).append(",row_").append(i).append(',').append(i * 10).append('\n');
        }
        Path tempFile = createTempDir().resolve("count_star_scan_test.csv");
        Files.write(tempFile, sb.toString().getBytes(StandardCharsets.UTF_8));
        return tempFile;
    }

    private Path writeParquetFile(int rowCount, int rowGroupSize) throws IOException {
        return writeParquet(createTempDir().resolve("profile_test.parquet"), rowCount, rowGroupSize);
    }
}
