/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;

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
     * A file past the prefix holds the same columns in a different order. Under {@code first_file_wins} the anchor's
     * schema is what every file is read under, so the columns have to be matched by name; read positionally, this
     * file's {@code value} column would be handed back as its {@code id}. Nothing in the prefix can catch that,
     * because the anchor's own order is the one the prefix agrees with.
     */
    public void testAFilePastThePrefixWithAnotherColumnOrderStillReadsByName() throws Exception {
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

        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("file_sort_by", "name");
        settings.put("file_order", "asc");
        settings.put("partition_sample_size", 2);
        String dataset = registerLocalFileDataset("prefix_order_ds", dir.toUri() + "*.parquet", settings);

        // Each row holds id=i and value=i*10, so the two sums differ by a factor of ten. Read positionally, the ten
        // files past the prefix would contribute their value column as id and the sums would converge.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS i = SUM(id), v = SUM(value)"))) {
            List<Object> row = getValuesList(response).get(0);
            long expectedId = 0;
            long expectedValue = 0;
            for (int r = 0; r < rowsPerFile; r++) {
                expectedId += r;
                expectedValue += r * 10L;
            }
            assertThat("every file's id column is its own", ((Number) row.get(0)).longValue(), equalTo(files * expectedId));
            assertThat("and so is its value column", ((Number) row.get(1)).longValue(), equalTo(files * expectedValue));
        }
    }

    /**
     * The declared rail bounds its listing for the same reason: a declared mapping is the whole schema, so no file
     * has to be read to know the columns and one page answers it. The file set is still the whole dataset, and
     * every file in it is still read under the declared mapping rather than under its own columns.
     * <p>
     * One declared column renames a physical one, which is what makes that second half visible: a file read as
     * itself produces the physical name, so the logical column would be null for every row of every file the
     * schema's listing did not reach.
     */
    public void testEveryFileIsReadUnderADeclaredMappingToo() throws Exception {
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
        // No file_sort_by here: it is only valid under first_file_wins, and the count does not depend on order.
        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("partition_sample_size", 2);
        String dataset = registerStrictDataset("prefix_declared_ds", dir.toUri() + "*.parquet", declared, settings);

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS c = COUNT(*)"))) {
            assertThat(
                "a declared mapping bounds the listing, not the read",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo((long) files * rowsPerFile)
            );
        }

        // Every row carries ident=its row index, so the sum counts only the files whose read was pinned to the
        // declared mapping. A file read as itself contributes nothing: it has no column of that name.
        long perFile = 0;
        for (int r = 0; r < rowsPerFile; r++) {
            perFile += r;
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS s = SUM(ident)"))) {
            assertThat(
                "a renamed declared column is read from every file, not just the ones resolution listed",
                ((Number) getValuesList(response).get(0).get(0)).longValue(),
                equalTo(files * perFile)
            );
        }
    }

    public void testEveryFileIsReadWhenTheSchemaListingWasAPrefix() throws Exception {
        Path dir = createTempDir();
        int files = 12;
        int rowsPerFile = 10;
        for (int i = 0; i < files; i++) {
            writeParquet(dir.resolve(String.format(Locale.ROOT, "part-%03d.parquet", i)), rowsPerFile, rowsPerFile);
        }

        Map<String, Object> settings = new HashMap<>();
        settings.put("format", "parquet");
        settings.put("schema_resolution", "first_file_wins");
        settings.put("file_sort_by", "name");
        settings.put("file_order", "asc");
        // Two keys answer the schema, so ten of the twelve files lie past what resolution listed.
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
