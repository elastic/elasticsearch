/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * One format per dataset: mixed registered extensions fail closed; gzip and plain csv of the same
 * format both read. Does not pin mixed parquet+csv {@code COUNT=9} success — that is out of product.
 */
public class DatasetOneFormatIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, GzipDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    public void testMixedRegisteredFormatsUnderCsvFailNamingParquet() throws Exception {
        Path dir = createTempDir();
        Path csv = dir.resolve("a.csv");
        Path parquet = dir.resolve("b.parquet");
        Files.writeString(csv, "id,n\n1,a\n2,b\n3,c\n4,d\n", StandardCharsets.UTF_8);
        writeParquet(parquet, 5, 100);
        String resource = StoragePath.fileUri(csv) + "," + StoragePath.fileUri(parquet);

        registerDataset("mixed_as_csv", resource, Map.of("format", "csv"));

        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM mixed_as_csv | STATS n = COUNT(*)"), TIMEOUT).close()
        );
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString(parquet.getFileName().toString()));
        assertThat(e.getMessage(), containsString("parquet"));
        assertThat(e.getMessage(), containsString("csv"));
    }

    public void testMixedCommaListWithoutFormatFailsAtQuery() throws Exception {
        Path dir = createTempDir();
        Path csv = dir.resolve("a.csv");
        Path parquet = dir.resolve("b.parquet");
        Files.writeString(csv, "id,n\n1,a\n", StandardCharsets.UTF_8);
        writeParquet(parquet, 2, 100);
        String resource = StoragePath.fileUri(csv) + "," + StoragePath.fileUri(parquet);

        registerDataset("mixed_no_format", resource, Map.of());

        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM mixed_no_format | LIMIT 1"), TIMEOUT).close());
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("implied formats"));
        assertThat(e.getMessage(), containsString("csv"));
        assertThat(e.getMessage(), containsString("parquet"));
    }

    public void testGzipThenPlainCsvReturnsBothFiles() throws Exception {
        assertGzipAndPlainCsvBothRows(true);
    }

    public void testPlainThenGzipCsvReturnsBothFiles() throws Exception {
        assertGzipAndPlainCsvBothRows(false);
    }

    public void testHomogeneousParquetGlobStillReads() throws Exception {
        Path dir = createTempDir();
        writeParquet(dir.resolve("a.parquet"), 2, 100);
        writeParquet(dir.resolve("b.parquet"), 3, 100);
        String glob = StoragePath.fileUri(dir) + "/*.parquet";
        registerDataset("homog_parquet", glob, Map.of());

        try (var response = run(syncEsqlQueryRequest("FROM homog_parquet | STATS n = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(5L));
        }
    }

    private void assertGzipAndPlainCsvBothRows(boolean gzipFirst) throws Exception {
        Path dir = createTempDir();
        Path plain = dir.resolve("plain.csv");
        Path gzipped = dir.resolve("gzipped.csv.gz");
        Files.writeString(plain, "n\n1\n", StandardCharsets.UTF_8);
        writeGzipped(gzipped, "n\n2\n");
        String resource = gzipFirst
            ? StoragePath.fileUri(gzipped) + "," + StoragePath.fileUri(plain)
            : StoragePath.fileUri(plain) + "," + StoragePath.fileUri(gzipped);
        String name = gzipFirst ? "csv_gz_then_plain" : "csv_plain_then_gz";
        registerDataset(name, resource, Map.of("format", "csv"));

        try (var response = run(syncEsqlQueryRequest("FROM " + name + " | SORT n | KEEP n"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(1L));
            assertThat(((Number) rows.get(1).get(0)).longValue(), equalTo(2L));
        }
    }
}
