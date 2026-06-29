/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end integration tests for the {@code SORT … | LIMIT N} deferred-extraction optimization
 * over external Parquet sources. These tests are the safety net for the entire deferred-extraction
 * pipeline: planning ({@code InsertExternalFieldExtraction}), runtime wiring
 * ({@code AsyncExternalSourceOperatorFactory}, {@code SchemaAdaptingIterator}), the
 * {@code _rowPosition}-emitting reader paths in
 * {@code ParquetFormatReader} ({@code read} and {@code readRange}), the runtime extractor handshake
 * ({@code ColumnExtractorProducer} → {@code ColumnExtractor}), and the on-line materialization
 * operator ({@code ExternalFieldExtractOperator}).
 * <p>
 * Each test writes a Parquet file, runs a {@code SORT id ASC | LIMIT N} query that projects four
 * columns (the two extra "wide" columns push the projection past
 * {@code InsertExternalFieldExtraction.DEFERRED_COLUMN_MIN}, making the rule eligible to fire),
 * then asserts the returned rows match the values we wrote — id, name, value, and payload — for
 * the lowest-id rows. Because the rule rewrites the source projection to read only {@code id}
 * up front and re-fetches name/value/payload in a second pass against the survivors, any bug
 * along that pipeline manifests as either a runtime exception or wrong cell values, both caught
 * here. The previous regressions
 * (<i>{@code _rowPosition channel must be a dense LongVector}</i>,
 * <i>{@code reader iterator does not implement ColumnExtractorProducer}</i>, and
 * <i>{@code Local breaker must be accessed by a single thread at a time}</i>)
 * all reproduce as exceptions or as cell-value mismatches on this path.
 * <p>
 * Coverage matrix:
 * <ul>
 *     <li>Single row group → exercises the {@code read()} path with a single full-file extractor.</li>
 *     <li>Multiple row groups → triggers byte-range splits, exercises {@code readRange()} and the
 *         per-split scoped extractors. This is the path that produced both production
 *         regressions.</li>
 *     <li>Multiple files (wildcard URI) → multiple extractors registered concurrently per driver,
 *         each with independent ids; verifies the registry's id encoding survives mixing.</li>
 *     <li>Pushed filter ({@code WHERE}) → exercises the rule's pushed-expressions branch and the
 *         narrowed projection's interaction with predicate columns.</li>
 *     <li>Tiny limits ({@code LIMIT 1}) → boundary case for the extractor permutation step.</li>
 *     <li>Descending sort → ensures the lowest-rows assumption isn't baked into the runtime.</li>
 *     <li>Sort by a non-numeric column → string-encoded sort key still resolves the right
 *         {@code _rowPosition} per surviving row.</li>
 *     <li>Stress (many row groups + many pages per group) → exercises the producer-thread
 *         allocation paths under buffer-fill cycles where worker threads rotate, the original
 *         trigger for the local-breaker concurrency assertion.</li>
 * </ul>
 * <p>
 * Pinned to a single data node because {@code FROM <dataset>} queries against {@code file://} URIs are read
 * from each data node's local filesystem; with multi-node clusters every node would re-read the
 * same file and the coordinator would get duplicate rows. Real deployments use shared storage
 * (S3/HTTP/etc.) where the coordinator's split-aware planner shards the work, but {@code file://}
 * in-process tests bypass that path.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ExternalParquetTopNExtractionIT extends AbstractExternalDataSourceIT {

    private static final TimeValue LONG_TIMEOUT = TimeValue.timeValueMinutes(2);

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * Single-row-group file. Smallest possible scenario: only the {@code read()} path runs and a
     * single full-file extractor is registered for the driver.
     */
    public void testSortLimitSingleRowGroup() throws Exception {
        int totalRows = 200;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ totalRows + 1, /* startId */ 0);
        try {
            assertWideTopNReturnsLowestRows(StoragePath.fileUri(file), /* limit */ 10, /* startId */ 0);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Multi-row-group file. The byte-range splitter discovers one range per row group (small files
     * fall below the macro-split coalesce target), so each split goes through {@code readRange()}.
     * This is the exact path that produced both production regressions.
     */
    public void testSortLimitMultipleRowGroups() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50, /* startId */ 0);
        try {
            assertWideTopNReturnsLowestRows(StoragePath.fileUri(file), /* limit */ 25, /* startId */ 0);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Multi-row-group file with a {@code WHERE} clause. The pushed predicate forces the optimizer
     * to keep the predicate's column eager and to defer only the unreferenced columns.
     */
    public void testSortLimitWithPushedFilter() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50, /* startId */ 0);
        try {
            String dataset = registerDataset("topn_extract", StoragePath.fileUri(file), Map.of());
            String query = "FROM " + dataset + " | WHERE id >= 200 | SORT id ASC | LIMIT 25 | KEEP id, name, value, payload";
            assertRowsMatchExpectedRange(query, /* startId */ 200, /* count */ 25);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Two files queried via wildcard URI. Each file produces its own per-split extractors
     * registered against the same driver's {@code SourceExtractors}; the row-position id encoding
     * (extractor id in the high bits, local position in the low bits) must keep them straight.
     */
    public void testSortLimitMultipleFiles() throws Exception {
        Path dir = createTempDir();
        Path file1 = writeParquetFileAt(dir.resolve("part-00.parquet"), /* startId */ 0, /* rows */ 200, /* rowGroupSize */ 60);
        Path file2 = writeParquetFileAt(dir.resolve("part-01.parquet"), /* startId */ 200, /* rows */ 200, /* rowGroupSize */ 60);
        try {
            String uri = StoragePath.fileUri(dir) + "/part-*.parquet";
            assertWideTopNReturnsLowestRows(uri, /* limit */ 30, /* startId */ 0);
        } finally {
            Files.deleteIfExists(file1);
            Files.deleteIfExists(file2);
        }
    }

    /**
     * {@code LIMIT 1}. Boundary case for the extractor's permutation step: the sort produces a
     * single surviving row, and the extractor must still resolve the correct {@code _rowPosition}
     * back to the right deferred values. A regression where the extractor mis-handles the
     * 1-element case (off-by-one, empty builder, NPE on the permutation array) is caught here.
     */
    public void testSortLimitOne() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50, /* startId */ 0);
        try {
            assertWideTopNReturnsLowestRows(StoragePath.fileUri(file), /* limit */ 1, /* startId */ 0);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * {@code SORT id DESC | LIMIT N}. The deferred-extraction rule is direction-agnostic — it
     * only inspects the sort key and limit — but the runtime must still hand the extractor the
     * highest-id surviving rows. Regressions where the extractor caches or replays an
     * implicit "row 0" mapping show up as an extracted name that doesn't match the sort key.
     */
    public void testSortDescending() throws Exception {
        int totalRows = 200;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50, /* startId */ 0);
        try {
            String dataset = registerDataset("topn_extract", StoragePath.fileUri(file), Map.of());
            String query = "FROM " + dataset + " | SORT id DESC | LIMIT 10 | KEEP id, name, value, payload";
            // Lowest-rows helper assumes ASC; build the descending expectation by hand.
            try (var response = run(syncEsqlQueryRequest(query), LONG_TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat("returned row count", rows.size(), equalTo(10));
                for (int i = 0; i < 10; i++) {
                    long id = totalRows - 1 - i;
                    List<Object> row = rows.get(i);
                    assertEquals("row " + i + " id", id, ((Number) row.get(0)).longValue());
                    assertEquals("row " + i + " name", expectedName(id), bytesRefToString(row.get(1)));
                    assertEquals("row " + i + " value", (int) (id * 10), ((Number) row.get(2)).intValue());
                    assertEquals("row " + i + " payload", expectedPayload(id), bytesRefToString(row.get(3)));
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Stress: many row groups + many pages per group, 8K rows. Forces the producer-thread to
     * cycle through the buffer-fill / drain-block / resume pattern multiple times, where every
     * resume can land on a different {@code GENERIC} pool thread. Caught the original
     * {@code Local breaker must be accessed by a single thread at a time} assertion. Uses a wide
     * {@code SORT id ASC | LIMIT N} so the same extractor and encoder paths run, just exercised
     * over enough pages to keep the worker rotation realistic.
     */
    public void testSortLimitStressManyRowGroups() throws Exception {
        int totalRows = 8_000;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 200, /* startId */ 0);
        try {
            assertWideTopNReturnsLowestRows(StoragePath.fileUri(file), /* limit */ 100, /* startId */ 0);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Multi-row-group file with a sparse, non-contiguous predicate (matches every fifth row) and
     * a {@code SORT … | LIMIT N} on top. This is the regression that broke against a real
     * S3-backed Parquet file: the deferred-extraction inner reader was constructed with
     * {@code FilterCompat.NOOP}, dropping the pushed filter, so the {@code _rowPosition} stream
     * stayed in sync with an unfiltered scan but every materialized row failed the predicate
     * downstream. The bug only shows up when the predicate is sparse enough that the filtered
     * surface differs from the unfiltered surface — a {@code WHERE id >= K} pure-prefix predicate
     * would still pass even with the filter dropped (the lowest surviving rows match), so we
     * deliberately use {@code id % 5 == 0} which interleaves matches and non-matches across the
     * file. Every returned row must satisfy the predicate.
     */
    public void testSortLimitWithSparsePushedFilter() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50, /* startId */ 0);
        try {
            String dataset = registerDataset("topn_extract", StoragePath.fileUri(file), Map.of());
            // value = id * 10, so value % 50 == 0 picks ids {0, 5, 10, …}. The matching rows are
            // interleaved with non-matching rows in every row group; if the filter is silently
            // dropped, the lowest 25 rows after sort would be ids 0..24 (only 5 of which match).
            // With the filter applied, we must see ids 0, 5, 10, … 120 — every cell satisfying
            // the predicate.
            String query = "FROM " + dataset + " | WHERE value % 50 == 0 | SORT id ASC | LIMIT 25 | KEEP id, name, value, payload";
            try (var response = run(syncEsqlQueryRequest(query), LONG_TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat("returned row count", rows.size(), equalTo(25));
                for (int i = 0; i < rows.size(); i++) {
                    List<Object> row = rows.get(i);
                    long id = ((Number) row.get(0)).longValue();
                    int value = ((Number) row.get(2)).intValue();
                    long expectedId = i * 5L;
                    // Every row must satisfy the predicate; if the filter was dropped, the lowest
                    // 25 ids (0..24) would appear, most of which fail value % 50 == 0.
                    assertEquals("row " + i + " predicate (value % 50 must be 0)", 0, value % 50);
                    assertEquals("row " + i + " id (sparse-match sequence)", expectedId, id);
                    assertEquals("row " + i + " name consistency", expectedName(id), bytesRefToString(row.get(1)));
                    assertEquals("row " + i + " value consistency", (int) (id * 10), value);
                    assertEquals("row " + i + " payload consistency", expectedPayload(id), bytesRefToString(row.get(3)));
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Multi-row-group file with a {@code WHERE} that references one of the wide columns. The rule
     * must keep that column eager (it is read by the pushed expression) while still deferring the
     * other wide columns. A regression where the rule defers a referenced column would fail
     * validation at planning time, or worse, materialize the wrong cells at runtime.
     */
    public void testSortLimitWhereOnWideColumn() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50, /* startId */ 0);
        try {
            String dataset = registerDataset("topn_extract", StoragePath.fileUri(file), Map.of());
            String query = "FROM " + dataset + " | WHERE value >= 1500 | SORT id ASC | LIMIT 25 | KEEP id, name, value, payload";
            // value = id * 10, so value >= 1500 means id >= 150
            assertRowsMatchExpectedRange(query, /* startId */ 150, /* count */ 25);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Runs a wide {@code SORT id ASC | LIMIT N | KEEP id, name, value, payload} query against
     * {@code uri} and asserts that the returned rows correspond to ids
     * {@code [startId, startId + limit)} with the values written by the test (deterministic
     * function of the id). This is the central correctness check: any bug in the deferred
     * extraction (wrong scope, mis-resolved positions, swapped values across rows) shows up as
     * mismatched cells.
     */
    private void assertWideTopNReturnsLowestRows(String uri, int limit, int startId) throws IOException {
        String dataset = registerDataset("topn_extract", uri, Map.of());
        String query = "FROM " + dataset + " | SORT id ASC | LIMIT " + limit + " | KEEP id, name, value, payload";
        assertRowsMatchExpectedRange(query, startId, limit);
    }

    private void assertRowsMatchExpectedRange(String query, int startId, int count) throws IOException {
        var request = syncEsqlQueryRequest(query);
        try (var response = run(request, LONG_TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat("expected four projected columns", columns.size(), equalTo(4));
            assertThat("first column must be id", columns.get(0).name(), equalTo("id"));
            assertThat("second column must be name", columns.get(1).name(), equalTo("name"));
            assertThat("third column must be value", columns.get(2).name(), equalTo("value"));
            assertThat("fourth column must be payload", columns.get(3).name(), equalTo("payload"));

            List<List<Object>> rows = getValuesList(response);
            try {
                assertThat("returned row count", rows.size(), equalTo(count));
                for (int i = 0; i < count; i++) {
                    long id = startId + i;
                    List<Object> row = rows.get(i);
                    assertEquals("row " + i + " id", id, ((Number) row.get(0)).longValue());
                    assertEquals("row " + i + " name", expectedName(id), bytesRefToString(row.get(1)));
                    assertEquals("row " + i + " value", (int) (id * 10), ((Number) row.get(2)).intValue());
                    assertEquals("row " + i + " payload", expectedPayload(id), bytesRefToString(row.get(3)));
                }
                assertThat("expected at least one row", rows.size(), greaterThan(0));
            } catch (AssertionError e) {
                StringBuilder dump = new StringBuilder("Query [").append(query)
                    .append("] returned ")
                    .append(rows.size())
                    .append(" rows:\n");
                for (int i = 0; i < rows.size(); i++) {
                    dump.append(" [").append(i).append("] ").append(rows.get(i)).append('\n');
                }
                throw new AssertionError(e.getMessage() + "\n" + dump, e);
            }
        }
    }

    private static String bytesRefToString(Object cell) {
        if (cell instanceof BytesRef br) {
            return br.utf8ToString();
        }
        return String.valueOf(cell);
    }

    private static String expectedName(long id) {
        return "row_" + id;
    }

    private static String expectedPayload(long id) {
        return "payload_" + id + "_" + (id * 7919);
    }

    private Path writeParquetFile(int rowCount, int rowGroupSize, long startId) throws IOException {
        Path tempFile = createTempDir().resolve("topn_extraction_test.parquet");
        return writeParquetFileAt(tempFile, startId, rowCount, rowGroupSize);
    }

    /**
     * Writes a four-column Parquet file: {@code id} (int64, sort key), {@code name} (string),
     * {@code value} (int32), {@code payload} (string, intentionally larger to make deferred
     * extraction attractive). The extra columns are what gives the optimizer enough deferrable
     * fields to clear {@code DEFERRED_COLUMN_MIN}.
     */
    private Path writeParquetFileAt(Path path, long startId, int rowCount, int rowGroupSize) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test {"
                + " required int64 id;"
                + " required binary name (UTF8);"
                + " required int32 value;"
                + " required binary payload (UTF8);"
                + " }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                long id = startId + i;
                Group g = factory.newGroup();
                g.add("id", id);
                g.add("name", expectedName(id));
                g.add("value", (int) (id * 10));
                g.add("payload", expectedPayload(id));
                writer.write(g);
            }
        }

        Files.write(path, baos.toByteArray());
        return path;
    }
}
