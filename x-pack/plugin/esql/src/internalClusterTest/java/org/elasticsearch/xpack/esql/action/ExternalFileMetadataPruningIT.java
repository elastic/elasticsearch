/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test verifying that {@code WHERE _file.modified > cutoff} prunes files at
 * listing time (via {@code GlobExpander.applyFileMetadataFilters}) so disqualified files
 * are never opened for reading by the async source operator.
 */
public class ExternalFileMetadataPruningIT extends AbstractExternalDataSourceIT {

    private static final int ROWS_PER_FILE = 10;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class, CsvDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * Writes 3 files with distinct lastModified timestamps; filters out the oldest;
     * asserts that only 2 files' rows were emitted by the source operator.
     */
    public void testFileModifiedFilterPrunesAtListingTime() throws Exception {
        Path dir = createTempDir();
        Path fileOld = writeParquetFile(dir, "old.parquet");
        Path fileMid = writeParquetFile(dir, "mid.parquet");
        Path fileNew = writeParquetFile(dir, "new.parquet");

        Instant oldTime = Instant.parse("2020-01-01T00:00:00Z");
        Instant midTime = Instant.parse("2023-06-15T00:00:00Z");
        Instant newTime = Instant.parse("2025-03-01T00:00:00Z");

        Files.setLastModifiedTime(fileOld, FileTime.from(oldTime));
        Files.setLastModifiedTime(fileMid, FileTime.from(midTime));
        Files.setLastModifiedTime(fileNew, FileTime.from(newTime));

        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        String dataset = registerDataset("file_meta", dirUri + "*.parquet", Map.of());
        // Filter: _file.modified > 2022-01-01 → should include mid + new, exclude old
        String query = "FROM "
            + dataset
            + " METADATA _file.modified"
            + " | WHERE `_file.modified` > \"2022-01-01T00:00:00.000Z\""
            + " | STATS c = COUNT(*)";

        var request = syncEsqlQueryRequest(query);
        request.profile(true);

        try (var response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            long count = ((Number) rows.get(0).get(0)).longValue();
            assertThat("Expected rows from mid + new files only", count, equalTo((long) ROWS_PER_FILE * 2));

            assertSourceOperatorRowsPruned(response, ROWS_PER_FILE * 2);
        }
    }

    /**
     * Asserts that the async external source operator emitted exactly {@code expectedRows},
     * proving that pruned files were never opened (not merely filtered post-read).
     */
    private static void assertSourceOperatorRowsPruned(EsqlQueryResponse response, long expectedRows) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);

        long totalRowsEmitted = 0;
        boolean foundAsyncOperator = false;
        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                if (op.status() instanceof AsyncExternalSourceOperator.Status status) {
                    foundAsyncOperator = true;
                    totalRowsEmitted += status.rowsEmitted();
                }
            }
        }
        assertTrue("expected at least one AsyncExternalSourceOperator in profile", foundAsyncOperator);
        assertThat("source operator should emit exactly the rows from non-pruned files", totalRowsEmitted, equalTo(expectedRows));
    }

    /**
     * Variant: filter excludes ALL files. The metadata hint prunes the listing to nothing, but a filter excluding
     * every file returns zero rows, not an error — the listing prune is only an optimization and the {@code WHERE}
     * still runs on the rows, which yields zero.
     */
    public void testFileModifiedFilterExcludesAllFilesReturnsZeroRows() throws Exception {
        Path dir = createTempDir();
        Path fileA = writeParquetFile(dir, "a.parquet");
        Path fileB = writeParquetFile(dir, "b.parquet");

        Instant past = Instant.parse("2020-06-01T00:00:00Z");
        Files.setLastModifiedTime(fileA, FileTime.from(past));
        Files.setLastModifiedTime(fileB, FileTime.from(past));

        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        String dataset = registerDataset("file_meta", dirUri + "*.parquet", Map.of());
        // All files are from 2020, filter requires > 2024 → nothing qualifies
        String query = "FROM "
            + dataset
            + " METADATA _file.modified"
            + " | WHERE `_file.modified` > \"2024-01-01T00:00:00.000Z\""
            + " | STATS c = COUNT(*)";

        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            long count = ((Number) rows.get(0).get(0)).longValue();
            assertThat("an all-excluding _file.modified filter returns zero rows, not an error", count, equalTo(0L));
        }
    }

    /**
     * Variant: filter by _file.size prunes small files.
     */
    public void testFileSizeFilterPrunesAtListingTime() throws Exception {
        Path dir = createTempDir();
        Path fileSmall = writeParquetFile(dir, "small.parquet");
        Path fileBig = writeParquetFileWithRows(dir, "big.parquet", ROWS_PER_FILE * 5);

        long smallSize = Files.size(fileSmall);
        long bigSize = Files.size(fileBig);
        assertThat("big file must be larger than small file for the test to be meaningful", bigSize, greaterThan(smallSize));

        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        String dataset = registerDataset("file_meta", dirUri + "*.parquet", Map.of());
        // Filter: only files larger than the small file's size
        String query = "FROM " + dataset + " METADATA _file.size | WHERE `_file.size` > " + smallSize + " | STATS c = COUNT(*)";

        var request = syncEsqlQueryRequest(query);
        request.profile(true);

        try (var response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            long count = ((Number) rows.get(0).get(0)).longValue();
            assertThat("Expected rows from big file only", count, equalTo((long) ROWS_PER_FILE * 5));

            assertSourceOperatorRowsPruned(response, ROWS_PER_FILE * 5);
        }
    }

    /**
     * A fractional bound on {@code _file.size} must not be truncated onto a file's own size: the file whose size is
     * the bound's integral part is below the bound and its rows match.
     */
    public void testFileSizeFractionalBoundKeepsMatchingFile() throws Exception {
        Path dir = createTempDir();
        Path a = writeCsv(dir, "a.csv", "1");
        writeCsv(dir, "b.csv", "22");
        writeCsv(dir, "c.csv", "333");
        double bound = Files.size(a) + 1.5;

        String dataset = registerDataset("file_meta", dirUri(dir) + "*.csv", Map.of());
        String query = "FROM " + dataset + " METADATA _file.size | WHERE `_file.size` < " + bound + " | KEEP id | SORT id ASC";

        assertThat(ids(query), equalTo(List.of(1L, 22L)));
    }

    /**
     * A bare date is a datetime literal ES|QL accepts, but the listing receives it unresolved and cannot parse it as
     * an instant, so it must keep the file rather than prune it. The files carry a real modification time because
     * the listing never prunes a file whose time is unknown or the epoch.
     */
    public void testFileModifiedInWithBareDatesKeepsMatchingFiles() throws Exception {
        Path dir = createTempDir();
        FileTime time = FileTime.from(Instant.parse("2024-01-01T00:00:00Z"));
        Files.setLastModifiedTime(writeCsv(dir, "t.csv", "7"), time);
        Files.setLastModifiedTime(writeCsv(dir, "u.csv", "8"), time);

        String dataset = registerDataset("file_meta", dirUri(dir) + "*.csv", Map.of());
        String query = "FROM "
            + dataset
            + " METADATA _file.modified"
            + " | WHERE `_file.modified` IN (\"2024-01-01\", \"2024-01-02\")"
            + " | KEEP id | SORT id ASC";

        assertThat(ids(query), equalTo(List.of(7L, 8L)));
    }

    /**
     * ES|QL orders keywords by UTF-8 bytes, where the supplementary-plane name sorts above {@code U+E000}; UTF-16
     * code-unit order puts its leading surrogate below it.
     */
    public void testFileNameRangeOrdersLikeTheEngine() throws Exception {
        Path dir = createTempDir();
        writeCsv(dir, "\uD83D\uDE00.csv", "9");
        writeCsv(dir, "zz.csv", "10");
        writeCsv(dir, "\uF000.csv", "11");

        String dataset = registerDataset("file_meta", dirUri(dir) + "*.csv", Map.of());
        String query = "FROM " + dataset + " METADATA _file.name | WHERE `_file.name` > \"\uE000\" | KEEP id | SORT id ASC";

        assertThat(ids(query), equalTo(List.of(9L, 11L)));
    }

    private List<Long> ids(String query) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            return getValuesList(response).stream().map(row -> ((Number) row.get(0)).longValue()).toList();
        }
    }

    private static Path writeCsv(Path dir, String filename, String id) throws IOException {
        return Files.writeString(dir.resolve(filename), "id\n" + id + "\n", StandardCharsets.UTF_8);
    }

    private static String dirUri(Path dir) {
        String uri = StoragePath.fileUri(dir).toString();
        return uri.endsWith("/") ? uri : uri + "/";
    }

    private Path writeParquetFile(Path dir, String filename) throws IOException {
        return writeParquetFileWithRows(dir, filename, ROWS_PER_FILE);
    }

    private Path writeParquetFileWithRows(Path dir, String filename, int rowCount) throws IOException {
        return writeParquet(dir.resolve(filename), rowCount, 1024 * 1024);
    }
}
