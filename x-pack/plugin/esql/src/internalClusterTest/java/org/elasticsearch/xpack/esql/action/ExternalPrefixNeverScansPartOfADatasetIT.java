/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

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
            java.nio.file.Files.createDirectories(hour);
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
