/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A dataset whose schema was answered from part of it must still be read in full.
 * <p>
 * Under {@code first_file_wins} one file defines the columns, so resolution lists only as far as that needs —
 * at most {@code partition_sample_size} keys. That listing is the schema's answer, not the query's file set:
 * split discovery discovers the rest for itself. Getting it wrong is silent, which is why this is a cluster test
 * rather than a unit test — a query still returns the rows it asked for, and only an answer that depends on the
 * files past the prefix can tell the difference. On a 90,567-file dataset it read 1,000 of them and looked right.
 */
public class ExternalPrefixNeverScansPartOfADatasetIT extends AbstractExternalDataSourceIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // One file on discovery's first attempt, so the retry is reachable from a test whose datasets are small.
        // Read when the split provider is built, which is why it is set here rather than updated at runtime.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExternalSourceSettings.FIRST_ATTEMPT_LISTING_FILES.getKey(), 1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class, CsvDataSourcePlugin.class);
    }

    /** The same, in the shape a real partitioned dataset has: hive folders and a globstar pattern. */
    /**
     * End to end over a real node, with discovery's own listing bounded to one file so the retry actually fires.
     * <p>
     * Every other test here bounds the schema's listing; this one bounds the listing split discovery performs for
     * itself, which is the only reason a query is fast. That listing is a guess at how much of the dataset the demand
     * needs, and a wrong guess must cost a second listing rather than rows: 35 rows over twelve ten-row files cannot
     * come from one, so the whole stack has to notice and list again. If it does not, this returns 10.
     */
    public void testABoundedDiscoveryListingStillAnswersTheWholeLimit() throws Exception {
        Path dir = createTempDir();
        int files = 12;
        int rowsPerFile = 10;
        for (int h = 0; h < files; h++) {
            Path hour = dir.resolve("year=2026").resolve(String.format(Locale.ROOT, "hour=%02d", h));
            Files.createDirectories(hour);
            writeParquet(hour.resolve("part-000.parquet"), rowsPerFile, rowsPerFile);
        }

        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("partition_detection", "hive");
        settings.put("partition_sample_size", 2);
        String dataset = registerLocalFileDataset("bounded_discovery_ds", dir.toUri() + "**/*.parquet", settings);

        // Covered by one file, so the prefix stands and this is the fast path.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 5"))) {
            assertThat("a demand one file covers is answered", getValuesList(response).size(), equalTo(5));
        }
        // Not covered by one file, so the prefix is discarded and the dataset is listed again.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 35"))) {
            assertThat("a demand one file cannot cover is not answered short", getValuesList(response).size(), equalTo(35));
        }
        // More than the dataset holds: every row, and no more.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 500"))) {
            assertThat(getValuesList(response).size(), equalTo(files * rowsPerFile));
        }
        // And the partition column is right for every file, including the eleven the bounded attempt never listed.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*) BY hour | SORT hour"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("one group per folder, none null", rows.size(), equalTo(files));
            for (List<Object> row : rows) {
                assertThat(row.get(1), notNullValue());
                assertThat(((Number) row.get(0)).longValue(), equalTo((long) rowsPerFile));
            }
        }
    }

    public void testEveryFileIsReadWhenTheSchemaListingWasAPrefixOfAPartitionedDataset() throws Exception {
        Path dir = createTempDir();
        int hours = 12;
        int rowsPerFile = 10;
        for (int h = 0; h < hours; h++) {
            Path hour = dir.resolve("year=2026").resolve(String.format(Locale.ROOT, "hour=%02d", h));
            Files.createDirectories(hour);
            writeParquet(hour.resolve("part-000.parquet"), rowsPerFile, rowsPerFile);
        }

        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("partition_detection", "hive");
        settings.put("partition_sample_size", 2);
        String dataset = registerLocalFileDataset("prefix_hive_ds", dir.toUri() + "**/*.parquet", settings);

        // The same control as the flat case: an ungrouped aggregate declines the bound, so this holds either way.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
            assertThat(
                "a partitioned dataset is read in full too",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo((long) hours * rowsPerFile)
            );
        }
        // This one is bounded, so it is the assertion that bites: 35 rows needs four files, and a query answered
        // from the two the schema listed returns 20.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 35"))) {
            assertThat(getValuesList(response).size(), equalTo(35));
        }

        // Partition values are path-derived and were detected over the paths the schema's listing saw. Every file
        // the query reads needs its own, including the ten past that prefix: a file with no entry would hand back
        // null for a column the dataset says it has.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*) BY hour | SORT hour"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("one group per partition folder, none of them null", rows.size(), equalTo(hours));
            for (List<Object> row : rows) {
                assertThat("a file past the prefix still knows its partition value", row.get(1), notNullValue());
                assertThat(((Number) row.get(0)).longValue(), equalTo((long) rowsPerFile));
            }
        }
    }

    /**
     * A file past the prefix holds the same columns in a different order from the anchor's. Its values still come
     * back under their own names.
     * <p>
     * This does not distinguish name-matching from positional matching: {@code ParquetFormatReader} resolves a
     * column by name whether or not the file was pinned to the anchor's schema. Proving that distinction needs a
     * positional format, and that test is not written.
     */
    public void testAFilePastThePrefixWithAnotherColumnOrderReturnsItsOwnValues() throws Exception {
        Path dir = createTempDir();
        int files = 12;
        int rowsPerFile = 10;
        String anchorOrder = "message test { required int64 id; required binary name (UTF8); required int32 value; }";
        String otherOrder = "message test { required int32 value; required int64 id; required binary name (UTF8); }";
        for (int i = 0; i < files; i++) {
            writeParquet(
                dir.resolve(String.format(Locale.ROOT, "part-%03d.parquet", i)),
                i < 2 ? anchorOrder : otherOrder,
                rowsPerFile,
                rowsPerFile,
                (g, row) -> {
                    g.add("id", (long) row);
                    g.add("name", "row_" + row);
                    g.add("value", row * 10);
                }
            );
        }

        // No file-order settings: a non-default order declines the bound in listingExtentsFor, which would leave
        // the schema's listing covering the whole dataset and this test exercising nothing.
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("partition_sample_size", 2);
        String dataset = registerLocalFileDataset("prefix_order_ds", dir.toUri() + "*.parquet", settings);

        // A row-returning query, deliberately: a dataset-wide aggregate takes the eager-statistics path, which
        // declines the bound outright and would leave the schema's listing covering every file.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, value | LIMIT " + files * rowsPerFile))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("every file is read, including the ten past the prefix", rows.size(), equalTo(files * rowsPerFile));
            for (List<Object> row : rows) {
                long id = ((Number) row.get(0)).longValue();
                assertThat("each row's value belongs to its own id", ((Number) row.get(1)).longValue(), equalTo(id * 10));
            }
        }
    }

    /**
     * A text format, where a file's columns come from reading it rather than from a footer, and where the read is
     * positional. One file defines the dataset's columns; the eleven past it carry a fourth column it does not have,
     * and a row that does not fit the dataset's schema is a row error.
     * <p>
     * {@code partition_sample_size} is 1 so the prefix is a single file, which is what makes this deterministic
     * without naming a file order — naming one declines the bound. Whichever file the provider lists first, the
     * other width does not fit it, so the error is raised either way; and answered from that one file alone there is
     * nothing to disagree with it and no error at all, which is what fails when the seam is reverted.
     */
    public void testATextFilePastThePrefixIsReadUnderTheAnchorsColumns() throws Exception {
        Path dir = createTempDir();
        int files = 12;
        int rowsPerFile = 10;
        for (int i = 0; i < files; i++) {
            boolean narrow = i == 0;
            StringBuilder csv = new StringBuilder(narrow ? "id,name,value\n" : "id,name,value,extra\n");
            for (int r = 0; r < rowsPerFile; r++) {
                int id = i * rowsPerFile + r;
                csv.append(id).append(",row_").append(id).append(',').append(id * 10).append(narrow ? "\n" : ",spare\n");
            }
            Files.writeString(dir.resolve(String.format(Locale.ROOT, "part-%03d.csv", i)), csv.toString(), StandardCharsets.UTF_8);
        }

        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "csv");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("partition_sample_size", 1);
        String dataset = registerLocalFileDataset("prefix_csv_ds", dir.toUri() + "*.csv", settings);

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP id, value | LIMIT " + files * rowsPerFile)).close()
        );
        assertThat(
            "a file past the prefix is read under the dataset's columns, so a row of another width is an error",
            e.getMessage() + causeChain(e),
            containsString("columns, the schema has")
        );
    }

    /** Flattens an exception's causes so an assertion can match a message the transport wrapped. */
    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t.getCause(); c != null; c = c.getCause()) {
            sb.append(' ').append(c.getMessage());
        }
        return sb.toString();
    }

    /**
     * The declared rail, bounded. A declared mapping is the whole schema, so no file is read to know the columns
     * and one bounded listing answers what is left — the file count and the partition columns. The file set is
     * still the whole dataset.
     * <p>
     * {@code schema_resolution} is set explicitly because the bound turns on it: {@code FileOrderConfig#forListing}
     * answers name-ascending for every other mode, and {@code listingExtentsFor} declines a bound for any order but
     * the default. A declared dataset that leaves it unset is never bounded and this case would assert nothing.
     */
    public void testEveryFileIsReadUnderADeclaredMapping() throws Exception {
        Path dir = createTempDir();
        int files = 12;
        int rowsPerFile = 10;
        for (int i = 0; i < files; i++) {
            writeParquet(dir.resolve(String.format(Locale.ROOT, "part-%03d.parquet", i)), rowsPerFile, rowsPerFile);
        }

        LinkedHashMap<String, DatasetFieldMapping> declared = new LinkedHashMap<>();
        declared.put("ident", new DatasetFieldMapping("long", "id"));
        declared.put("name", new DatasetFieldMapping("keyword", null));
        declared.put("value", new DatasetFieldMapping("integer", null));
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("partition_sample_size", 2);
        String dataset = registerStrictDataset("prefix_declared_ds", dir.toUri() + "*.parquet", declared, settings);

        // Row-returning: a dataset-wide aggregate takes the eager-statistics path, which declines the bound.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | KEEP ident, value | LIMIT " + files * rowsPerFile))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("every file is read under the declared mapping", rows.size(), equalTo(files * rowsPerFile));
            for (List<Object> row : rows) {
                assertThat("the renamed declared column is read from every file", row.get(0), notNullValue());
            }
        }
    }

    public void testEveryFileIsReadWhenTheSchemaListingWasAPrefix() throws Exception {
        Path dir = createTempDir();
        int files = 12;
        int rowsPerFile = 10;
        for (int i = 0; i < files; i++) {
            writeParquet(dir.resolve(String.format(Locale.ROOT, "part-%03d.parquet", i)), rowsPerFile, rowsPerFile);
        }

        // Two keys answer the schema, so ten of the twelve files lie past what resolution listed. No file-order
        // settings: listingExtentsFor declines the bound for any order but the default, and a declined bound
        // would list the whole dataset for the schema and leave this test asserting nothing.
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("partition_sample_size", 2);
        String dataset = registerLocalFileDataset("prefix_ds", dir.toUri() + "*.parquet", settings);

        // A dataset-wide aggregate is never handed a prefix in the first place: it wants statistics over every
        // file, which declines the bound outright. Asserted as the control for the bounded cases below, not as one
        // of them - this number is the same whatever split discovery does with a prefix.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(
                "every file is read, not the prefix the schema needed",
                ((Number) rows.get(0).get(0)).longValue(),
                equalTo((long) files * rowsPerFile)
            );
        }

        // And a bare limit reads enough files for its rows while still seeing the whole dataset: 35 rows needs
        // four files, which is more than the prefix and fewer than all twelve.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 35"))) {
            assertThat(getValuesList(response).size(), equalTo(35));
        }
    }
}
