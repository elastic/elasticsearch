/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Pins {@code COUNT(*)} when splits already carry the coordinator {@code readSchema}.
 * Skipping the execution {@code metadata()} GET is covered by the unit tests.
 */
public class ExternalCountStarUsesShippedSchemaIT extends AbstractExternalDataSourceIT {

    /** CSV/TSV {@code minimumSegmentSize} is 1 MiB; a smaller file never forms a non-leading FileSplit. */
    private static final int CSV_MIN_BYTES = 2 * 1024 * 1024 + 64 * 1024;
    /** NDJSON {@code minimumSegmentSize} defaults to 4 MiB. */
    private static final int NDJSON_MIN_BYTES = 4 * 1024 * 1024 + 256 * 1024;
    private static final Map<String, Object> MACRO_SPLITS = Map.of("target_split_size", "1kb");
    private static final TimeValue COUNT_TIMEOUT = TimeValue.timeValueMinutes(2);

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    public void testCountStarOverMacroSplitsHeaderedHeaderlessTsvAndNdjson() throws Exception {
        Path dir = createTempDir();

        Path headered = dir.resolve("headered.csv");
        int headeredRows = writeHeaderedCsv(headered, CSV_MIN_BYTES);
        assertThat("headered csv must exceed 2×CSV minimumSegmentSize", Files.size(headered), greaterThan(2L * 1024 * 1024));
        assertCountStar("headered csv", registerDataset("count_headered_csv", StoragePath.fileUri(headered), MACRO_SPLITS), headeredRows);

        Path headerless = dir.resolve("headerless.csv");
        int headerlessRows = writeHeaderlessCsv(headerless, CSV_MIN_BYTES);
        assertThat("headerless csv must exceed 2×CSV minimumSegmentSize", Files.size(headerless), greaterThan(2L * 1024 * 1024));
        LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("col0", new DatasetFieldMapping("integer", null));
        properties.put("col1", new DatasetFieldMapping("integer", null));
        properties.put("col2", new DatasetFieldMapping("integer", null));
        Map<String, Object> headerlessSettings = Map.of("header_row", false, "target_split_size", "1kb");
        assertCountStar(
            "headerless csv",
            registerStrictDataset("count_headerless_csv", StoragePath.fileUri(headerless), properties, headerlessSettings),
            headerlessRows
        );

        Path tsv = dir.resolve("rows.tsv");
        int tsvRows = writeHeaderedTsv(tsv, CSV_MIN_BYTES);
        assertThat("tsv must exceed 2×CSV minimumSegmentSize", Files.size(tsv), greaterThan(2L * 1024 * 1024));
        assertCountStar(
            "tsv",
            registerDataset("count_tsv", StoragePath.fileUri(tsv), Map.of("format", "tsv", "target_split_size", "1kb")),
            tsvRows
        );

        Path ndjson = dir.resolve("rows.ndjson");
        int ndjsonRows = writeNdjson(ndjson, NDJSON_MIN_BYTES);
        assertThat("ndjson must exceed NDJSON minimumSegmentSize", Files.size(ndjson), greaterThan(4L * 1024 * 1024));
        assertCountStar("ndjson", registerDataset("count_ndjson", StoragePath.fileUri(ndjson), MACRO_SPLITS), ndjsonRows);
    }

    public void testCountStarErrorModesOnShortRows() throws Exception {
        int goodRows = 20;
        int extraColumnRows = 3;
        Path file = createTempDir().resolve("extra-column-rows.csv");
        // CSV COUNT(*) null-fills short rows; extra-column rows are the structural drop (row.length > width).
        writeHeaderedCsvWithExtraColumnRows(file, goodRows, extraColumnRows);

        String failFast = registerDataset("count_fail_fast", StoragePath.fileUri(file), Map.of("error_mode", "fail_fast"));
        expectThrows(Exception.class, () -> runCountStar(failFast));

        assertCountStar(
            "skip_row",
            registerDataset("count_skip_row", StoragePath.fileUri(file), Map.of("error_mode", "skip_row")),
            goodRows
        );
        assertCountStar(
            "null_field",
            registerDataset("count_null_field", StoragePath.fileUri(file), Map.of("error_mode", "null_field")),
            goodRows
        );
    }

    public void testCountStarEqualsFilteredCountOnFirstFileWinsGlob() throws Exception {
        Path dir = createTempDir();
        int firstRows = writeHeaderedCsv(dir.resolve("a.csv"), 256);
        // Wider header than the FFW pin; data rows stay 3 fields so they match a.csv's width and are counted.
        int secondRows = writeWiderHeaderSameWidthDataCsv(dir.resolve("b.csv"), 256);
        String dataset = registerDataset(
            "count_ffw",
            globUri(dir, "*.csv"),
            Map.of("schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        long expected = firstRows + secondRows;
        assertCountStar("ffw count(*)", dataset, expected);
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | WHERE a IS NOT NULL | STATS c = COUNT(*)"), COUNT_TIMEOUT)) {
            assertThat("ffw WHERE a IS NOT NULL", countValue(response), equalTo(expected));
        }
    }

    public void testCountStarAtParallelismOne() throws Exception {
        assumeTrue("requires the snapshot-only external_parsing_parallelism pragma", canUseQueryPragmas());
        Path file = createTempDir().resolve("parallelism-one.csv");
        int rows = writeHeaderedCsv(file, CSV_MIN_BYTES);
        assertThat(Files.size(file), greaterThan(2L * 1024 * 1024));
        String dataset = registerDataset("count_p1", StoragePath.fileUri(file), MACRO_SPLITS);
        var request = syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)").pragmas(
            new QueryPragmas(Settings.builder().put("external_parsing_parallelism", 1).build())
        );
        try (var response = run(request, COUNT_TIMEOUT)) {
            assertThat("parallelism=1", countValue(response), equalTo((long) rows));
        }
    }

    private void assertCountStar(String reason, String dataset, long expectedRows) {
        assertThat(reason, runCountStar(dataset), equalTo(expectedRows));
    }

    private long runCountStar(String dataset) {
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"), COUNT_TIMEOUT)) {
            return countValue(response);
        }
    }

    private static long countValue(EsqlQueryResponse response) {
        List<List<Object>> values = getValuesList(response);
        assertThat(values.size(), equalTo(1));
        return ((Number) values.get(0).get(0)).longValue();
    }

    private static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir);
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    private static int writeHeaderedCsv(Path file, int minBytes) throws Exception {
        StringBuilder sb = new StringBuilder(minBytes + 64);
        sb.append("a,b,c\n");
        int rows = 0;
        while (sb.length() < minBytes) {
            sb.append(rows).append(',').append(rows).append(',').append(rows).append('\n');
            rows++;
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
        return rows;
    }

    private static int writeHeaderlessCsv(Path file, int minBytes) throws Exception {
        StringBuilder sb = new StringBuilder(minBytes + 64);
        int rows = 0;
        while (sb.length() < minBytes) {
            sb.append(rows).append(',').append(rows).append(',').append(rows).append('\n');
            rows++;
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
        return rows;
    }

    private static int writeHeaderedTsv(Path file, int minBytes) throws Exception {
        StringBuilder sb = new StringBuilder(minBytes + 64);
        sb.append("a\tb\tc\n");
        int rows = 0;
        while (sb.length() < minBytes) {
            sb.append(rows).append('\t').append(rows).append('\t').append(rows).append('\n');
            rows++;
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
        return rows;
    }

    private static int writeNdjson(Path file, int minBytes) throws Exception {
        StringBuilder sb = new StringBuilder(minBytes + 64);
        int rows = 0;
        while (sb.length() < minBytes) {
            sb.append("{\"a\":").append(rows).append(",\"b\":").append(rows).append(",\"c\":").append(rows).append("}\n");
            rows++;
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
        return rows;
    }

    private static void writeHeaderedCsvWithExtraColumnRows(Path file, int goodRows, int extraColumnRows) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("a,b,c\n");
        for (int i = 0; i < goodRows; i++) {
            sb.append(i).append(',').append(i).append(',').append(i).append('\n');
        }
        for (int i = 0; i < extraColumnRows; i++) {
            sb.append(i).append(',').append(i).append(',').append(i).append(",extra").append('\n');
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
    }

    private static int writeWiderHeaderSameWidthDataCsv(Path file, int minBytes) throws Exception {
        StringBuilder sb = new StringBuilder(minBytes + 64);
        sb.append("a,b,c,d\n");
        int rows = 0;
        while (sb.length() < minBytes) {
            sb.append(rows).append(',').append(rows).append(',').append(rows).append('\n');
            rows++;
        }
        Files.writeString(file, sb, StandardCharsets.UTF_8);
        return rows;
    }
}
