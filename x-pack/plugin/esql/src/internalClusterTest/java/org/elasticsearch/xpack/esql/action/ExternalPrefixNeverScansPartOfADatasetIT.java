/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
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
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    /** The same, in the shape a real partitioned dataset has: hive folders and a globstar pattern. */
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

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
            assertThat(
                "a partitioned dataset is read in full too",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo((long) hours * rowsPerFile)
            );
        }
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

        // COUNT(*) cannot be answered from a prefix: twelve files of ten rows is 120, and a query that read only
        // the two files the schema needed would answer 20.
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
