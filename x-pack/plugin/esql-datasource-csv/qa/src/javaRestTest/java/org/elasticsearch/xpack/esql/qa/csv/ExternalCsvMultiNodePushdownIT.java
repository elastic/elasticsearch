/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Multi-JVM coverage for external-text aggregate-metadata pushdown.
 * <p>
 * The single-JVM {@code internalClusterTest}s (e.g. {@code ExternalCsvAggregatePushdownIT}) cannot
 * exercise the cross-node stats return-flow: every node shares one JVM-static {@code ExternalStats},
 * so a data node's capture is visible to the coordinator through the static cache regardless of whether
 * {@code DriverCompletionInfo.capturedSourceMetadata} works. This test runs a real two-process cluster
 * with split roles (coordinator-only node 0, data-only node 1). Both queries target node 0; the cold
 * scan runs on node 1 (separate JVM, separate static cache), so the only way node 0's planning-time
 * cache can be populated for the warm query is the return-flow. A warm {@code documents_found == 0}
 * therefore proves the coordinator reconciled the data node's shipped-back stats.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ExternalCsvMultiNodePushdownIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(ExternalCsvMultiNodePushdownIT.class);

    /** standalone/employees.csv carries 100 data rows (101 lines incl. header). */
    private static final long EMPLOYEE_ROWS = 100L;

    private static final String LOCAL_DATA_SOURCE = "local_csv_ds";
    private static final String DATASET = "employees_local";
    private static final String CHUNKED_DATASET = "chunked_stripes_local";
    /** The default {@code esql.external.cache.stripe.size}; the fixture spans several of these. */
    private static final long STRIPE_BYTES = 8L * 1024 * 1024;
    /**
     * The generated chunked fixture, deliberately NOT ending in {@code .csv}. It must live in the
     * {@code iceberg-fixtures} resource directory, the only path the cluster will read ({@code Clusters} pins
     * {@code path.repo} and {@code esql.external.local_allowed_paths} to it), and two fixture loaders walk that
     * directory whole: {@code FixtureUtils.writeCompressedVariantsToFixturesPath} matches text extensions
     * anywhere beneath it and writes five compressed copies with no size limit, which this name defeats, while
     * {@code forEachFixtureEntryFromFilesystem} reads every uncompressed file into memory but skips anything
     * over 10 MB, which this fixture's size defeats. Deleting it after the test is what actually settles both;
     * these two are the belt for a run that dies first. The dataset declares its format explicitly, since the
     * name no longer says what the bytes are.
     */
    private static final String CHUNKED_FIXTURE = "standalone/chunked_stripes.data";
    /**
     * Parse threads the chunked read is pinned to, as a pragma fragment. {@code external_parsing_parallelism}
     * defaults to {@code EsExecutors.allocatedProcessors}, and at 1 the read takes the non-parallel path, which
     * publishes one whole-file summary and no stripe-addressed contribution -- so the stripe-count assertion
     * would be deciding on the runner's core count. 8 over the ~24mb fixture asks for ~3mb segments against the
     * 8mb stripe grid, so every stripe is stitched from more than one chunk, which is the geometry under test.
     */
    private static final String PARSING_PARALLELISM = ",\"external_parsing_parallelism\":\"8\"";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    public static ElasticsearchCluster cluster = Clusters.multiNodeTestCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    private static Path localFixturesPath;

    /** Datasets and {@code local} data sources are gated to snapshot builds today (same gate as {@code DataSourceCrudRestIT}). */
    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void resolveLocalFixtures() {
        localFixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, ExternalCsvMultiNodePushdownIT.class);
    }

    @AfterClass
    public static void cleanupDatasets() throws Exception {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        // Pin every request to node 0 (coordinator-only). Both the cold and warm query must hit the
        // same coordinator: the reconciled schema cache is per-coordinator, not cluster-replicated.
        return cluster.getHttpAddress(0);
    }

    /**
     * A CSV file carrying a UTF-8 BOM at its very start must read identically to the same file
     * without the mark, end-to-end through the REST API and a real two-process cluster.
     * <p>
     * Two fixture files exercise the two failure modes the unit tests also cover:
     * <ul>
     *   <li>{@code bom_headerless.csv}: BOM + headerless data; {@code col0} must infer
     *       {@code integer} and its first value must be {@code 1} (not a BOM-prefixed string).</li>
     *   <li>{@code bom_comment.csv}: BOM + comment line + typed header + data; the comment must
     *       still be recognized as a comment so {@code id} and {@code name} are the columns and
     *       all three data rows come back.</li>
     * </ul>
     * Both fixture files are generated by the {@code generateBomCsvFixtures} Gradle task because
     * the BOM bytes ({@code EF BB BF}) cannot be preserved through text-resource handling.
     */
    @SuppressWarnings("unchecked")
    public void testBomPrefixedFilesReadCorrectly() throws Exception {
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);

        DatasetRegistry.ensureDataSource(client(), LOCAL_DATA_SOURCE, "local", Map.of());

        // --- headerless file ---
        String headerlessUri = localFixturesPath.resolve("standalone/bom_headerless.csv").toUri().toString();
        DatasetRegistry.ensureDataset(client(), "bom_headerless_local", LOCAL_DATA_SOURCE, headerlessUri, "{ \"header_row\": false }");

        Map<String, Object> headerlessResult = runEsql("FROM bom_headerless_local | KEEP col0 | SORT col0 | LIMIT 1");
        List<Map<String, Object>> headerlessCols = (List<Map<String, Object>>) headerlessResult.get("columns");
        assertEquals("col0 must infer as integer, not keyword", "integer", headerlessCols.get(0).get("type"));
        List<List<Object>> headerlessValues = (List<List<Object>>) headerlessResult.get("values");
        assertEquals("first value must be 1, not the BOM character followed by 1", 1, ((Number) headerlessValues.get(0).get(0)).intValue());

        // --- comment-first file ---
        String commentUri = localFixturesPath.resolve("standalone/bom_comment.csv").toUri().toString();
        DatasetRegistry.ensureDataset(client(), "bom_comment_local", LOCAL_DATA_SOURCE, commentUri, "{}");

        // If the BOM masked the comment the schema would be wrong and KEEP id would fail.
        Map<String, Object> commentResult = runEsql("FROM bom_comment_local | KEEP id, name | SORT id | LIMIT 10");
        List<Map<String, Object>> commentCols = (List<Map<String, Object>>) commentResult.get("columns");
        assertThat("comment-first BOM file must resolve id and name as columns", commentCols, hasSize(2));
        assertEquals("id", commentCols.get(0).get("name"));
        assertEquals("name", commentCols.get(1).get("name"));
        List<List<Object>> commentValues = (List<List<Object>>) commentResult.get("values");
        assertEquals("comment-first BOM file must return 3 data rows", 3, commentValues.size());
    }

    public void testCountStarColdThenWarmShortCircuitsAcrossNodes() throws Exception {
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);
        String uri = localFixturesPath.resolve("standalone/employees.csv").toUri().toString();
        // Register a local-file data source + dataset over the fixture, then read it via FROM <dataset>. The
        // multi_value_syntax=brackets dataset setting mirrors the old inline WITH: employees.csv encodes
        // multi-value columns with bracket syntax (e.g. [val1,val2]); without it the commas inside brackets
        // are treated as column separators, producing a column-count mismatch at row 1. Format is inferred
        // from the .csv extension.
        DatasetRegistry.ensureDataSource(client(), LOCAL_DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), DATASET, LOCAL_DATA_SOURCE, uri, "{ \"multi_value_syntax\": \"brackets\" }");
        String query = "FROM " + DATASET + " | STATS c = COUNT(*)";

        // Cold: coordinator (no data role) dispatches the scan to the data node, which captures the
        // file's stats in its own JVM and ships them back via DriverCompletionInfo.
        Map<String, Object> cold = runEsql(query);
        assertEquals("cold COUNT(*) must be exact", EMPLOYEE_ROWS, count(cold));
        assertThat("cold run must scan rows", documentsFound(cold), greaterThan(0L));
        assertEquals("the external scan must run on the data node, not the coordinator", "data-node", scanNodeName(cold));

        // Warm: node 0's planning-time cache was enriched only by the return-flow (its own static
        // cache never saw the scan), so the optimizer short-circuits to a LocalSourceExec.
        Map<String, Object> warm = runEsql(query);
        assertEquals("warm COUNT(*) must stay exact", EMPLOYEE_ROWS, count(warm));
        assertEquals("warm run must short-circuit via the cross-node return-flow (zero docs scanned)", 0L, documentsFound(warm));
    }

    /**
     * The only test that puts a stripe record across a real transport.
     * <p>
     * {@link #testCountStarColdThenWarmShortCircuitsAcrossNodes} proves the return flow, but its hundred-row
     * fixture is a whole-file read: it publishes one authoritative summary and never a stripe-addressed
     * contribution, so the staging, the compact form and the coordinator's fold of a stripe record arriving as
     * a generic map are all on the other path -- and in a single JVM none of them crosses a wire.
     * <p>
     * This fixture is large enough to be cut into many chunks of roughly 3mb against the 8mb grid (see
     * {@link #PARSING_PARALLELISM}), so the data node publishes stripe records and nothing else, holds them
     * compact, serializes each as it writes the response, and the coordinator in the other JVM rebuilds and
     * folds them. A warm {@code documents_found == 0} is reachable only if every step of that worked.
     */
    public void testStripeRecordsCrossTheWireAndFoldOnTheCoordinator() throws Exception {
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);

        Path chunked = localFixturesPath.resolve(CHUNKED_FIXTURE);
        // Inside the try, not before it: the writer creates the file and then fills it over 600k rows, so a
        // throw part way through leaves ~24mb of padding behind on exactly the path the finally below is for.
        try {
            long rows = writeChunkedFixture(chunked);
            DatasetRegistry.ensureDataSource(client(), LOCAL_DATA_SOURCE, "local", Map.of());
            // The format is declared rather than inferred: the fixture's name carries no format extension, so that
            // the extension-keyed fixture walks over this shared directory do not pick a multi-megabyte generated
            // file up. An explicit format wins over the name on every path that resolves one.
            DatasetRegistry.ensureDataset(
                client(),
                CHUNKED_DATASET,
                LOCAL_DATA_SOURCE,
                chunked.toUri().toString(),
                "{ \"format\": \"csv\" }"
            );
            String query = "FROM " + CHUNKED_DATASET + " | STATS c = COUNT(*)";

            Map<String, Object> cold = runEsql(query, PARSING_PARALLELISM);
            assertEquals("cold COUNT(*) must be exact", rows, count(cold));
            assertThat("cold run must scan rows", documentsFound(cold), greaterThan(0L));
            assertEquals("the external scan must run on the data node, not the coordinator", "data-node", scanNodeName(cold));
            // Counted on the COORDINATOR, by walking each record after it was rebuilt from its generic form, so the
            // figure alone establishes both halves of the path: written through size()/entrySet() on the data node,
            // and read back here. Pinned to the stripe count rather than to positivity — one record would mean every
            // stripe collapsed into one, and one per chunk would mean the pieces never joined, and ">0" passes both.
            long expectedStripes = (Files.size(chunked) + STRIPE_BYTES - 1) / STRIPE_BYTES;
            assertThat(
                "the data node must stage one record per stripe of the file, not one per chunk and not one in total",
                stripesCommitted(cold),
                equalTo(expectedStripes)
            );

            Map<String, Object> warm = runEsql(query, PARSING_PARALLELISM);
            assertEquals("warm COUNT(*) must stay exact", rows, count(warm));
            assertEquals(
                "the warm serve proves the coordinator rebuilt and folded stripe records that reached it as generic maps",
                0L,
                documentsFound(warm)
            );
        } finally {
            // In a finally rather than an @AfterClass: the file is a regenerable ~24mb of padding, there is nothing
            // in it to inspect after a failure, and leaving it behind on the failing path is exactly how it would
            // accumulate in the shared fixtures directory.
            Files.deleteIfExists(chunked);
        }
    }

    /**
     * Writes a CSV well past the parallel-parse threshold, so the read is cut into many chunks and the default
     * 8mb stripe grid spans several of them. Generated here rather than checked in: the point is its size.
     *
     * @return the data row count the queries must agree with
     */
    private static long writeChunkedFixture(Path file) throws IOException {
        long rows = 0;
        try (BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            out.write("id,value,label\n");
            // ~40 bytes a row over 600k rows is ~24mb: several 8mb stripes, each stitched from several chunks.
            for (int i = 0; i < 600_000; i++) {
                out.write(Integer.toString(i));
                out.write(',');
                out.write(Integer.toString(i * 7));
                out.write(",row-padding-to-widen-the-line\n");
                rows++;
            }
        }
        return rows;
    }

    /** Stripe-addressed contributions the cold scan staged, summed over the external-source operators. */
    @SuppressWarnings("unchecked")
    private static long stripesCommitted(Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) profile.get("drivers");
        long committed = 0;
        for (Map<String, Object> driver : drivers) {
            for (Map<String, Object> operator : (List<Map<String, Object>>) driver.get("operators")) {
                Object status = operator.get("status");
                if (status instanceof Map<?, ?> fields && fields.get("stripes_committed") instanceof Number n) {
                    committed += n.longValue();
                }
            }
        }
        return committed;
    }

    private Map<String, Object> runEsql(String query) throws Exception {
        return runEsql(query, "");
    }

    /**
     * Runs {@code query} against the coordinator with profiling on.
     *
     * @param extraPragmas additional pragma entries, each a leading {@code ,"key":value} fragment, or empty
     */
    private Map<String, Object> runEsql(String query, String extraPragmas) throws Exception {
        Request request = new Request("POST", "/_query");
        String escaped = query.replace("\\", "\\\\").replace("\"", "\\\"");
        // external_distribution=round_robin forces the scan onto a data node (the adaptive default
        // would run a single small split locally on the coordinator, defeating the cross-JVM intent).
        request.setJsonEntity(
            "{\"query\":\"" + escaped + "\",\"profile\":true,\"pragma\":{\"external_distribution\":\"round_robin\"" + extraPragmas + "}}"
        );
        // ES|QL warns "No limit defined" for a bare aggregate; that's expected here, not a failure.
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /** Name of the node whose driver ran the external-source operator (the file scan). */
    @SuppressWarnings("unchecked")
    private static String scanNodeName(Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) profile.get("drivers");
        for (Map<String, Object> driver : drivers) {
            List<Map<String, Object>> operators = (List<Map<String, Object>>) driver.get("operators");
            for (Map<String, Object> operator : operators) {
                // The operator class is {@code AsyncExternalSourceOperator} but its profile-facing
                // toString() drops the {@code Async} prefix for user readability — match on the
                // public name to stay in sync with the profile output.
                if (String.valueOf(operator.get("operator")).startsWith("ExternalDataSourceOperator")) {
                    return String.valueOf(driver.get("node_name"));
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static long count(Map<String, Object> response) {
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        return ((Number) values.get(0).get(0)).longValue();
    }

    private static long documentsFound(Map<String, Object> response) {
        return ((Number) response.get("documents_found")).longValue();
    }
}
