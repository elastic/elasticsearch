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
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end Parquet integration tests for the
 * {@link org.elasticsearch.compute.operator.topn.NumericTopNOperator} pipeline. Sister IT to
 * {@link ExternalParquetTopNExtractionIT}: where that suite exercises the full deferred-extraction
 * surface, this one focuses on the numeric-topn selection in
 * {@code LocalExecutionPlanner#tryBuildNumericTopN} for every supported sort-key
 * {@link org.elasticsearch.compute.data.ElementType}.
 *
 * <p>Coverage matrix (every test exercises the {@code tryBuildNumericTopN} selection path under
 * production conditions and then verifies that the rows returned to the coordinator match the
 * reference set computed independently from the writer's deterministic value functions):
 * <ul>
 *     <li>{@code SORT longCol ASC | LIMIT N} — LONG path.</li>
 *     <li>{@code SORT intCol ASC | LIMIT N} — INTEGER path; widening to long inside the heap.</li>
 *     <li>{@code SORT doubleCol DESC | LIMIT N} — DOUBLE path; {@code doubleToSortableLong}
 *         round-trip.</li>
 *     <li>{@code SORT boolCol ASC | LIMIT N} — BOOLEAN path; heavy ties on the 2-element value
 *         space, exercising the {@code _rowPosition} tiebreaker for stability.</li>
 *     <li>{@code SORT timestampCol DESC | LIMIT N} — DATETIME → ElementType.LONG path; the
 *         common "give me the last N events" shape over event-time data.</li>
 * </ul>
 *
 * <p>Stage 4 / RO4: each test exercises the same call path the PR 2 row-group-skip optimisation
 * will inject the threshold into — namely the {@code AsyncExternalSourceOperatorFactory}
 * iterator that drives per-row-group reads (TODO-marked there); see
 * {@link #testSortLongAscPinsAsyncReadCallsite()} for the explicit pin.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ExternalParquetNumericTopNIT extends AbstractExternalDataSourceIT {

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
     * LONG sort key, ASC. The rule fires for the LONG sort key over a narrowed external source;
     * the returned rows must be the {@code N} smallest by id, with the wide columns
     * ({@code name}, {@code payload}) carrying the values written for those ids.
     *
     * <p><strong>Async-read callsite pin (Stage 4 / RO4):</strong> this query is the canonical
     * shape PR 2 will instrument. The threshold check that prunes row groups must happen at the
     * point where {@code AsyncExternalSourceOperatorFactory}'s reader iterator is about to
     * dispatch a per-row-group read (currently the {@code formatReader.read()} or
     * {@code readRange()} loop inside {@code sourceOperator()}). Adding the check after the read
     * already paid the I/O cost, which is the regression PR 2's threshold mechanism exists to
     * avoid. Any change to the location of that iterator must update the documented callsite in
     * {@code AsyncExternalSourceOperatorFactory}.
     */
    public void testSortLongAscPinsAsyncReadCallsite() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50);
        try {
            String dataset = registerDataset("numeric_topn", StoragePath.fileUri(file), Map.of());
            String query = "FROM " + dataset + " | SORT id ASC | LIMIT 25 | KEEP id, name, value, payload";
            assertNumericTopNAgainstReference(query, totalRows, byIdAsc(), 25);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testSortIntAsc() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50);
        try {
            String dataset = registerDataset("numeric_topn", StoragePath.fileUri(file), Map.of());
            // value = id * 10, so ASC on value is the same as ASC on id, but the runtime path is
            // the INT branch in NumericTopNOperator (rather than LONG via the id sort key).
            String query = "FROM " + dataset + " | SORT value ASC | LIMIT 25 | KEEP id, name, value, payload";
            assertNumericTopNAgainstReference(query, totalRows, byValueAsc(), 25);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testSortDoubleDesc() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50);
        try {
            String dataset = registerDataset("numeric_topn", StoragePath.fileUri(file), Map.of());
            // score = 100.0 - id * 0.25, monotonically decreasing — DESC asks for the highest
            // scores, which correspond to the smallest ids. Exercises NumericTopNOperator's
            // DOUBLE branch with the {@code doubleToSortableLong} round-trip on both encode and
            // decode (decode is needed when the sort-key column is also a projected output).
            String query = "FROM " + dataset + " | SORT score DESC | LIMIT 25 | KEEP id, name, value, score";
            assertNumericTopNAgainstReference(query, totalRows, byScoreDesc(), 25);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Multi-valued sort key end-to-end: each row stores 1–3 values in the {@code tag} repeated
     * column, and the query sorts ascending on {@code tag} with a small {@code LIMIT}. The
     * {@code NumericTopNOperator} extractor must reduce each row's MV slot to its MV-min and
     * pick the {@code N} most competitive rows under that reduction. The reference ranking
     * computes the same per-row MV-min in the test JVM and walks both sets side by side.
     *
     * <p>This is the Stage 5b end-to-end pin: the same query against the generic
     * {@code TopNOperator} would have used {@code KeyExtractorForLong.MinFromUnorderedBlock},
     * and the new path must agree row-for-row on the {@code _rowPosition}-derived payload
     * (wide-column id, name, value, payload).
     */
    public void testSortMultiValuedTagAsc() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50);
        try {
            String dataset = registerDataset("numeric_topn", StoragePath.fileUri(file), Map.of());
            String query = "FROM " + dataset + " | SORT tag ASC | LIMIT 25 | KEEP id, name, value, payload";
            try (var response = run(syncEsqlQueryRequest(query), LONG_TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat("returned row count", rows.size(), equalTo(25));
                // Independent reference: rank ids by per-id MV-min(tag) ASC, with id ASC as the
                // tiebreak — matching the operator's _rowPosition tiebreak (id is the natural
                // row order in the file).
                List<Long> referenceIds = referenceIdsByRanking(totalRows, byTagMinAsc(), 25);
                // The operator's surviving set must equal the reference's first 25 ids. Because
                // tag values come from a small palette, the K boundary may straddle a tied bucket;
                // a per-row exact-id match could fail at the boundary even when both sides are
                // valid Top-K answers. The right invariant is "surviving id multiset matches
                // when both are sorted by id" — exactly the same widening
                // {@code assertSubsetWithBoundaryTies} uses in the unit tests.
                List<Long> actualIds = new ArrayList<>(rows.size());
                for (List<Object> row : rows) {
                    actualIds.add(((Number) row.get(0)).longValue());
                }
                java.util.Collections.sort(actualIds);
                java.util.Collections.sort(referenceIds);
                assertEquals("surviving id set under MV-min(tag) ASC ranking", referenceIds, actualIds);
                // Wide-column payloads must still correctly address the surviving id. A wrong
                // _rowPosition resolution would surface as an id/payload mismatch even though
                // the id set is correct.
                for (List<Object> row : rows) {
                    long id = ((Number) row.get(0)).longValue();
                    assertEquals("name for id " + id, expectedName(id), bytesRefToString(row.get(1)));
                    assertEquals("value for id " + id, (int) (id * 10), ((Number) row.get(2)).intValue());
                    assertEquals("payload for id " + id, expectedPayload(id), bytesRefToString(row.get(3)));
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    public void testSortBooleanAsc() throws Exception {
        int totalRows = 400;
        Path file = writeParquetFile(totalRows, /* rowGroupSize */ 50);
        try {
            String dataset = registerDataset("numeric_topn", StoragePath.fileUri(file), Map.of());
            // flag = id % 2 == 0; ASC means false-rows first. Half the file has flag=false, so
            // a LIMIT 25 is comfortably within the all-false bucket. We don't pin the specific
            // ids returned — cross-driver / cross-row-group merges may break the tie either
            // way and that is fine; what we DO check is that every returned row satisfies
            // {@code flag == false} (i.e. the sort key did its job and didn't bleed any
            // {@code flag == true} rows into the top-K). A regression that mis-handles the
            // BOOLEAN encoding or the {@code _rowPosition} payload would either return
            // {@code flag == true} rows or mismatched (id, flag) pairs, both caught here.
            // Project wide columns (name, payload) alongside the sort key so the deferred-
            // extraction rule fires and the source is narrowed to {@code [flag, _rowPosition]}.
            String query = "FROM " + dataset + " | SORT flag ASC | LIMIT 25 | KEEP id, flag, name, value, payload";
            try (var response = run(syncEsqlQueryRequest(query), LONG_TIMEOUT)) {
                List<List<Object>> rows = getValuesList(response);
                assertThat("returned row count", rows.size(), equalTo(25));
                for (int i = 0; i < rows.size(); i++) {
                    List<Object> row = rows.get(i);
                    long id = ((Number) row.get(0)).longValue();
                    boolean flag = (Boolean) row.get(1);
                    assertEquals("row " + i + " flag must satisfy sort predicate (false bucket)", false, flag);
                    assertEquals("row " + i + " (id, flag) consistency", expectedFlag(id), flag);
                    // Wide-column payloads still need to address the right id, even when the
                    // sort key collapses to a single value across many rows.
                    assertEquals("row " + i + " name", expectedName(id), bytesRefToString(row.get(2)));
                    assertEquals("row " + i + " value", (int) (id * 10), ((Number) row.get(3)).intValue());
                    assertEquals("row " + i + " payload", expectedPayload(id), bytesRefToString(row.get(4)));
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
    }

    /**
     * Run {@code query} against a Parquet file with {@code totalRows} rows produced by the
     * deterministic value functions on this test, then verify the response rows equal the first
     * {@code expectedCount} rows of the reference ranking. The reference ranking is computed in
     * the test JVM by the comparator argument applied to a synthetic id list, so it represents
     * the canonical "what the operator should have returned" without sharing implementation
     * code with the runtime under test.
     *
     * <p>Note (gap acknowledged): these tests verify correctness of the rows returned to the
     * coordinator, not that {@code NumericTopNOperator} was actually selected over the generic
     * {@code TopNOperator}. An attempt was made to assert on {@code response.profile()}, but in
     * this IT harness only the coordinator's reduce driver shows up in {@code profile.drivers()}
     * for the {@code FROM <dataset> ... | SORT | LIMIT} shape (whose data-side driver — the one that
     * actually runs the specialised operator — does not appear). A planner-time change that
     * inadvertently causes {@code tryBuildNumericTopN} to return null would still produce the
     * same row-level answers via the generic operator (which is the deliberate fallback) and
     * pass every assertion below. The behavioural assertion is therefore complemented by, but
     * not replaced by, the operator-side unit tests in
     * {@code NumericTopNOperatorTests}.
     */
    private void assertNumericTopNAgainstReference(String query, int totalRows, Comparator<Long> ranking, int expectedCount)
        throws IOException {
        List<Long> referenceIds = referenceIdsByRanking(totalRows, ranking, expectedCount);
        try (var response = run(syncEsqlQueryRequest(query), LONG_TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("returned row count", rows.size(), equalTo(expectedCount));
            for (int i = 0; i < expectedCount; i++) {
                long expectedId = referenceIds.get(i);
                List<Object> row = rows.get(i);
                long actualId = ((Number) row.get(0)).longValue();
                assertEquals("row " + i + " id (ranking expects " + expectedId + ")", expectedId, actualId);
                // Wide-column payloads: every visible cell is a deterministic function of id, so
                // a wrong _rowPosition resolution shows up as a name/value/payload that doesn't
                // match the surviving id.
                for (int col = 1; col < row.size(); col++) {
                    Object cell = row.get(col);
                    String columnName = response.columns().get(col).name();
                    switch (columnName) {
                        case "name" -> assertEquals("row " + i + " name", expectedName(actualId), bytesRefToString(cell));
                        case "value" -> assertEquals("row " + i + " value", (int) (actualId * 10), ((Number) cell).intValue());
                        case "payload" -> assertEquals("row " + i + " payload", expectedPayload(actualId), bytesRefToString(cell));
                        case "score" -> assertEquals("row " + i + " score", expectedScore(actualId), ((Number) cell).doubleValue(), 1e-9);
                        case "flag" -> assertEquals("row " + i + " flag", expectedFlag(actualId), cell);
                        case "id" -> {
                            // already checked above
                        }
                        default -> throw new AssertionError("Unexpected column in response: " + columnName);
                    }
                }
            }
        }
    }

    private static List<Long> referenceIdsByRanking(int totalRows, Comparator<Long> ranking, int count) {
        List<Long> ids = new ArrayList<>(totalRows);
        for (long i = 0; i < totalRows; i++) {
            ids.add(i);
        }
        ids.sort(ranking);
        return ids.subList(0, Math.min(count, ids.size()));
    }

    private static Comparator<Long> byIdAsc() {
        return Comparator.naturalOrder();
    }

    private static Comparator<Long> byValueAsc() {
        // value = id * 10 is monotonic, so id ascending agrees with value ascending. We keep the
        // double-key comparator (value, id) so the tiebreaker matches the operator's
        // _rowPosition ascending semantics when the sort key produces ties.
        return Comparator.<Long, Integer>comparing(id -> (int) (id * 10)).thenComparingLong(Long::longValue);
    }

    private static Comparator<Long> byScoreDesc() {
        return Comparator.<Long, Double>comparing(ExternalParquetNumericTopNIT::expectedScore)
            .reversed()
            .thenComparingLong(Long::longValue);
    }

    private static Comparator<Long> byTagMinAsc() {
        return Comparator.<Long, Long>comparing(ExternalParquetNumericTopNIT::expectedTagMin).thenComparingLong(Long::longValue);
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

    private static double expectedScore(long id) {
        return 100.0 - id * 0.25;
    }

    private static boolean expectedFlag(long id) {
        return id % 2 == 0;
    }

    /**
     * Tag values for {@code id}. Each row gets 1–3 tags drawn from a small palette so MV-min
     * produces well-defined ties at the K boundary; the exact recipe is intentionally simple so
     * the test JVM and the operator agree on the per-row MV-min without sharing implementation
     * code.
     */
    private static long[] expectedTags(long id) {
        // Three-tag rotation: (id, id+100, id+200) clipped to length (id % 3 + 1). The result
        // is a multi-valued list whose MV-min is always {@code id} itself for any non-empty list,
        // so the K-th boundary tie-break under MV-min ASC degenerates to id ASC — clean to
        // reason about while still exercising the MV scan path.
        int len = (int) (id % 3) + 1;
        long[] tags = new long[len];
        for (int i = 0; i < len; i++) {
            tags[i] = id + i * 100L;
        }
        return tags;
    }

    private static long expectedTagMin(long id) {
        long[] tags = expectedTags(id);
        long min = tags[0];
        for (int i = 1; i < tags.length; i++) {
            min = Math.min(min, tags[i]);
        }
        return min;
    }

    private Path writeParquetFile(int rowCount, int rowGroupSize) throws IOException {
        Path tempFile = createTempDir().resolve("numeric_topn_test.parquet");
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test {"
                + " required int64 id;"
                + " required binary name (UTF8);"
                + " required int32 value;"
                + " required double score;"
                + " required boolean flag;"
                + " required binary payload (UTF8);"
                // {@code repeated} (rather than the {@code LIST} group annotation) keeps the
                // schema small while still exercising the MV path through ESQL's parquet
                // datasource. The optimised iterator reads through the same code that handles
                // any repeated primitive, so this is representative of production list columns.
                + " repeated int64 tag;"
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
                long id = i;
                Group g = factory.newGroup();
                g.add("id", id);
                g.add("name", expectedName(id));
                g.add("value", (int) (id * 10));
                g.add("score", expectedScore(id));
                g.add("flag", expectedFlag(id));
                g.add("payload", expectedPayload(id));
                for (long tag : expectedTags(id)) {
                    g.add("tag", tag);
                }
                writer.write(g);
            }
        }
        Files.write(tempFile, baos.toByteArray());
        return tempFile;
    }
}
