/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * A Parquet writer may leave a row group's min/max unset while that row group holds non-null values:
 * parquet-java and Arrow both drop the extrema of a column chunk whose values exceed their statistics
 * size limit. Such a row group can hold the column's smallest or largest value,
 * so an ungrouped MIN/MAX answered from the other row groups' extrema is wrong. These cases compare the
 * aggregate against the same aggregate forced onto the scan path.
 */
public class ExternalParquetRowGroupWithoutExtremaIT extends AbstractExternalDataSourceIT {

    private static final String LONG_VALUE = "a".repeat(5000);
    private static final int ROWS_PER_GROUP = 100;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * Row group one holds {@code m000}..{@code m099}. Row group two holds {@code n100}..{@code n199} with one
     * 5000-character value and one {@code zz}, so the writer leaves its extrema unset.
     */
    private Path writeFixture() throws IOException {
        Path file = createTempDir().resolve("rowgroup_without_extrema.parquet");
        writeParquet(file, "message test { required int64 id; required binary s (UTF8); }", 2 * ROWS_PER_GROUP, 1024, (g, i) -> {
            g.add("id", (long) i);
            String s;
            if (i == 150) {
                s = LONG_VALUE;
            } else if (i == 151) {
                s = "zz";
            } else {
                s = (i < ROWS_PER_GROUP ? "m" : "n") + String.format(Locale.ROOT, "%03d", i);
            }
            g.add("s", s);
        });
        return file;
    }

    /** Pins the fixture's shape, so a writer change that stops producing it fails here rather than passing vacuously. */
    private static void assertFixtureShape(Path file) throws IOException {
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(file),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build()
            )
        ) {
            List<BlockMetaData> groups = reader.getFooter().getBlocks();
            assertThat("row groups", groups.size(), equalTo(2));
            Statistics<?> first = groups.get(0).getColumns().get(1).getStatistics();
            Statistics<?> second = groups.get(1).getColumns().get(1).getStatistics();
            assertTrue("first row group carries min/max", first.hasNonNullValue());
            assertFalse("second row group carries no min/max", second.hasNonNullValue());
        }
    }

    /** A 5000-character value would bury the assertion message, so compare a short description of it. */
    private static String describe(Object value) {
        String s = String.valueOf(value);
        return s.length() <= 16 ? s : s.substring(0, 8) + "... (" + s.length() + " chars)";
    }

    public void testMinMaxOverRowGroupWithoutExtremaMatchesScan() throws Exception {
        Path file = writeFixture();
        assertFixtureShape(file);
        String dataset = registerDataset("rg_without_extrema", StoragePath.fileUri(file), Map.of());

        List<Object> scanned;
        try (
            var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS lo = MIN(s), hi = MAX(s) BY k = id >= 0 | KEEP lo, hi"))
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            scanned = rows.get(0);
        }
        assertThat("scan MIN", describe(scanned.get(0)), equalTo(describe(LONG_VALUE)));
        assertThat("scan MAX", scanned.get(1), equalTo("zz"));

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS lo = MIN(s), hi = MAX(s)"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            List<Object> row = rows.get(0);
            assertThat("MIN is the row group without extrema's value", describe(row.get(0)), equalTo(describe(LONG_VALUE)));
            assertThat("MAX is the row group without extrema's value", row.get(1), equalTo("zz"));
        }
    }

    public void testMinMaxAgreesWithSortOverRowGroupWithoutExtrema() throws Exception {
        Path file = writeFixture();
        assertFixtureShape(file);
        String dataset = registerDataset("rg_without_extrema_sort", StoragePath.fileUri(file), Map.of());
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS hi = MAX(s)"))) {
            assertThat(getValuesList(response).get(0).get(0), equalTo("zz"));
        }
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT s DESC | LIMIT 1 | KEEP s"))) {
            assertThat(getValuesList(response).get(0).get(0), equalTo("zz"));
        }
    }
}
