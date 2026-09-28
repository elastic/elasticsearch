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
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageTypeParser;
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
 * Regression test for esql-planning#2068: {@code MIN} and {@code MAX} over a Parquet column must
 * fall back to a scan when at least one row group holds values but records no min/max statistics.
 *
 * <p>A Parquet writer may omit a column chunk's statistics when a value is too large to store as a
 * statistic (e.g. parquet-java 1.18.1 drops the whole {@code Statistics} object — including the
 * null count — when any value in the chunk exceeds the 4096-byte threshold). Before the fix,
 * {@code extractStatistics} treated such a row group as contributing nothing to the file-level
 * bounds, publishing only the other row groups' bounds as the file's bounds. An ungrouped
 * {@code MIN} or {@code MAX} that is answered from statistics alone (no scan) then returned a value
 * that was not the file's true extreme.
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
     * Row group one holds {@code m000}..{@code m099}. Row group two holds {@code n100}..{@code n199}
     * with one 5000-character value at index 150 and {@code "zz"} at index 151, so parquet-java drops
     * all statistics for that chunk (isEmpty() == true on reading).
     *
     * <p>The actual minimum across both row groups is the 5000 {@code 'a'} characters (lexicographically
     * smaller than any {@code "m..."} value). The actual maximum is {@code "zz"} (lexicographically
     * larger than any {@code "n..."} value). Before the fix, the statistics path returned {@code "m000"}
     * and {@code "m099"} — the first row group's bounds — as the file's min and max.
     */
    private Path writeFixture() throws IOException {
        String schemaText = "message test { required int64 id; required binary s (UTF8); }";

        // Row group 1: rows 0-99, small strings only → parquet-java records statistics normally.
        Path rg1 = createTempDir().resolve("rg1.parquet");
        writeParquet(rg1, schemaText, ROWS_PER_GROUP, Integer.MAX_VALUE, (g, i) -> {
            g.add("id", (long) i);
            g.add("s", "m" + String.format(Locale.ROOT, "%03d", i));
        });

        // Row group 2: rows 100-199, one 5000-character value → parquet-java drops all statistics
        // for column s (Statistics.isEmpty() == true), so no min/max and no null_count are recorded.
        Path rg2 = createTempDir().resolve("rg2.parquet");
        writeParquet(rg2, schemaText, ROWS_PER_GROUP, Integer.MAX_VALUE, (g, i) -> {
            int rowIdx = i + ROWS_PER_GROUP;
            g.add("id", (long) rowIdx);
            String s;
            if (i == 50) {
                s = LONG_VALUE;
            } else if (i == 51) {
                s = "zz";
            } else {
                s = "n" + String.format(Locale.ROOT, "%03d", rowIdx);
            }
            g.add("s", s);
        });

        // Merge the two single-row-group files into one two-row-group file.
        Path file = createTempDir().resolve("rowgroup_without_extrema.parquet");
        var schema = MessageTypeParser.parseMessageType(schemaText);
        try (
            ParquetFileWriter writer = new ParquetFileWriter(
                new LocalOutputFile(file),
                schema,
                ParquetFileWriter.Mode.OVERWRITE,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.MAX_PADDING_SIZE_DEFAULT
            )
        ) {
            writer.start();
            for (Path source : List.of(rg1, rg2)) {
                LocalInputFile inputFile = new LocalInputFile(source);
                ParquetReadOptions options = ParquetReadOptions.builder(new PlainParquetConfiguration()).build();
                try (ParquetFileReader fileReader = new ParquetFileReader(inputFile, options)) {
                    List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
                    try (SeekableInputStream stream = inputFile.newStream()) {
                        writer.appendRowGroups(stream, blocks, false);
                    }
                }
            }
            writer.end(Map.of());
        }
        return file;
    }

    /**
     * Asserts that the fixture has exactly two row groups, where the first row group carries min/max
     * bounds for column {@code s} and the second does not. If a writer change stops producing a row
     * group without extrema, this assertion fails here rather than letting the test pass vacuously
     * (because both groups having bounds would make the statistics answer correct anyway).
     */
    private static void assertFixtureShape(Path file) throws IOException {
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(file),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build()
            )
        ) {
            List<BlockMetaData> groups = reader.getFooter().getBlocks();
            assertThat("row groups", groups.size(), equalTo(2));
            // Column index 1 is "s" (index 0 is "id").
            Statistics<?> first = groups.get(0).getColumns().get(1).getStatistics();
            Statistics<?> second = groups.get(1).getColumns().get(1).getStatistics();
            assertTrue("first row group carries min/max for s", first != null && first.hasNonNullValue());
            assertTrue(
                "second row group carries no min/max for s (isEmpty or hasNonNullValue==false)",
                second == null || second.isEmpty() || second.hasNonNullValue() == false
            );
        }
    }

    /** Describes a potentially very long string value so assertion messages stay readable. */
    private static String describe(Object value) {
        String s = String.valueOf(value);
        return s.length() <= 16 ? s : s.substring(0, 8) + "... (" + s.length() + " chars)";
    }

    /**
     * Core regression: an ungrouped {@code MIN}/{@code MAX} (which is answered from file-level
     * statistics when every function is pushable) must return the same values as a scan-forced
     * variant. Before the fix, the statistics path returned {@code "m000"} and {@code "m099"}
     * (the first row group's bounds), while the scan found the 5000-character {@code 'a'} value
     * and {@code "zz"} in the second row group.
     */
    public void testMinMaxOverRowGroupWithoutExtremaMatchesScan() throws Exception {
        Path file = writeFixture();
        assertFixtureShape(file);
        String dataset = registerDataset("rg_without_extrema", StoragePath.fileUri(file), Map.of());

        // Force a scan by adding a grouping key — the statistics path is not taken for grouped aggs.
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

        // Ungrouped STATS: before the fix this returned the first row group's bounds; after the fix
        // it falls back to a scan and returns the same values as the forced-scan variant above.
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS lo = MIN(s), hi = MAX(s)"))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            List<Object> row = rows.get(0);
            assertThat("MIN matches scan", describe(row.get(0)), equalTo(describe(LONG_VALUE)));
            assertThat("MAX matches scan", row.get(1), equalTo("zz"));
        }
    }

    /**
     * Additional shape: confirms that {@code MAX(s)} (a simpler query with one function) agrees with
     * {@code SORT s DESC | LIMIT 1}, which always scans. Before the fix, {@code MAX} returned
     * {@code "m099"} (the first row group's maximum for {@code s}) while the sort found {@code "zz"}.
     */
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

    /**
     * Control: when every row group in the file carries min/max bounds for a column, the statistics
     * path must still serve {@code MIN}/{@code MAX} without a scan ({@code documentsFound() == 0}).
     * Without this control, removing Parquet {@code MIN}/{@code MAX} from the statistics path
     * entirely would turn the regression tests above green and read as a fix.
     */
    public void testMinMaxIsServedFromStatisticsWhenEveryRowGroupCarriesExtrema() throws Exception {
        String schemaText = "message test { required int64 id; required binary s (UTF8); }";

        // Two row groups, all small values — parquet-java records statistics for every chunk.
        Path rg1 = createTempDir().resolve("rg1.parquet");
        writeParquet(rg1, schemaText, ROWS_PER_GROUP, Integer.MAX_VALUE, (g, i) -> {
            g.add("id", (long) i);
            g.add("s", "m" + String.format(Locale.ROOT, "%03d", i));
        });
        Path rg2 = createTempDir().resolve("rg2.parquet");
        writeParquet(rg2, schemaText, ROWS_PER_GROUP, Integer.MAX_VALUE, (g, i) -> {
            int rowIdx = i + ROWS_PER_GROUP;
            g.add("id", (long) rowIdx);
            g.add("s", "n" + String.format(Locale.ROOT, "%03d", rowIdx));
        });

        Path file = createTempDir().resolve("rowgroup_with_extrema.parquet");
        var schema = MessageTypeParser.parseMessageType(schemaText);
        try (
            ParquetFileWriter writer = new ParquetFileWriter(
                new LocalOutputFile(file),
                schema,
                ParquetFileWriter.Mode.OVERWRITE,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.MAX_PADDING_SIZE_DEFAULT
            )
        ) {
            writer.start();
            for (Path source : List.of(rg1, rg2)) {
                LocalInputFile inputFile = new LocalInputFile(source);
                ParquetReadOptions options = ParquetReadOptions.builder(new PlainParquetConfiguration()).build();
                try (ParquetFileReader fileReader = new ParquetFileReader(inputFile, options)) {
                    List<BlockMetaData> blocks = fileReader.getFooter().getBlocks();
                    try (SeekableInputStream stream = inputFile.newStream()) {
                        writer.appendRowGroups(stream, blocks, false);
                    }
                }
            }
            writer.end(Map.of());
        }

        // Verify every row group carries bounds.
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new LocalInputFile(file),
                ParquetReadOptions.builder(new PlainParquetConfiguration()).build()
            )
        ) {
            for (BlockMetaData group : reader.getFooter().getBlocks()) {
                assertTrue("row group carries min/max for s", group.getColumns().get(1).getStatistics().hasNonNullValue());
            }
        }

        String dataset = registerDataset("rg_with_extrema", StoragePath.fileUri(file), Map.of());
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS lo = MIN(s), hi = MAX(s)"))) {
            List<Object> row = getValuesList(response).get(0);
            assertThat("MIN", row.get(0), equalTo("m000"));
            assertThat("MAX", row.get(1), equalTo("n199"));
            assertThat("answered from the footer, no rows read", response.documentsFound(), equalTo(0L));
        }
    }
}
