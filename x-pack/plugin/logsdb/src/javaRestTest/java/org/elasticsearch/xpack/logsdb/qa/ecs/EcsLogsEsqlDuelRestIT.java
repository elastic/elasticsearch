/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.ecs;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Duel suite that verifies ES|QL query results are identical between {@code index.mode=logsdb}
 * (baseline) and {@code index.mode=logsdb_columnar} (contender) over an ECS-shaped log corpus.
 *
 * <p>One corpus of synthetic ECS log documents is indexed once into both data streams. The
 * corpus is sized between 20,000 and 100,000 documents so that multiple segments, merges, and
 * doc-value blocks are created — the regime where logsdb/columnar differences tend to surface.
 * Queries are then executed against both streams and results compared row-for-row.
 *
 * <p>The test extends {@link ESRestTestCase} directly rather than {@link
 * org.elasticsearch.xpack.logsdb.qa.AbstractChallengeRestTest} because:
 * <ul>
 *   <li>That class recreates templates and data streams in {@code @Before}/{@code @After} for
 *       every test method, which would require re-indexing 100k-300k documents per method.</li>
 *   <li>Its ES|QL helper string-concatenates queries into JSON, producing invalid JSON for any
 *       query containing a literal {@code "}, i.e. all {@code LIKE}, {@code RLIKE}, and
 *       {@code MATCH} expressions.</li>
 * </ul>
 *
 * <p>Instead, {@link #preserveClusterUponCompletion()} returns {@code true} to suppress the
 * per-method {@code wipeCluster()}, and setup runs once behind a static guard. An
 * {@link AfterClass} method deletes the data streams and templates when the class is done.
 */
@TimeoutSuite(millis = TimeUnits.HOUR)
public class EcsLogsEsqlDuelRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    private static final String BASELINE_DS = "logs-ecsduel-baseline";
    private static final String CONTENDER_DS = "logs-ecsduel-columnar";
    private static final String BASELINE_TMPL = "ecsduel-baseline-template";
    private static final String CONTENDER_TMPL = "ecsduel-columnar-template";

    /**
     * Template priority 500 outranks the built-in {@code logs} template (100) so our mapping and
     * settings are not overridden or merged with the built-in defaults.
     */
    private static final int TEMPLATE_PRIORITY = 500;

    /** Bulk batch size in documents. Kept small to limit per-request heap use on the single-node test cluster. */
    private static final int BATCH_SIZE = 1000;

    /** Number of random queries per test method. */
    private static final int QUERIES_PER_METHOD = 40;

    /**
     * Minimum fraction of queries per method that must return at least one row from the baseline.
     * A query returning no rows compares {@code []} to {@code []} and asserts nothing about either
     * index mode. The threshold is intentionally below 1.0 because individual predicates can be
     * legitimately selective; the WARN log names empty queries so a regression is diagnosable.
     *
     * <p>Note: ungrouped {@code STATS} always returns exactly one row even when no documents match
     * the filter, so {@code testRandomStatsQueries} satisfies this check trivially. The ratio
     * matters most for filter and eval queries where an empty result is a genuine signal.
     */
    private static final double MIN_NON_EMPTY_RATIO = 0.9;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        // x-pack-stack is intentionally omitted: it registers hundreds of APM/fleet templates
        // on startup, causing GC pressure under the 512MB default heap. Our test provides
        // its own templates at priority 500 so built-in stack templates are not needed.
        .user(USER, PASS)
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // Also disabling ML to avoid unneeded overhead:
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token)
            // Default socket timeout is 60s (ESRestTestCase.java:1839). Large bulk batches and
            // full-corpus ES|QL aggregations over 300k documents can exceed that.
            .put(CLIENT_SOCKET_TIMEOUT, "5m")
            .build();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Suppress the per-method wipeCluster() — data streams are cleaned up in @AfterClass.
        return true;
    }

    // ── one-time setup ────────────────────────────────────────────────────────────────────────

    private static boolean initialized = false;
    static int corpusSize;

    @Before
    public void setUpOnce() throws Exception {
        if (initialized) {
            return;
        }
        createTemplate(BASELINE_TMPL, BASELINE_DS + "*", "logsdb");
        createTemplate(CONTENDER_TMPL, CONTENDER_DS + "*", "logsdb_columnar");
        createDataStream(BASELINE_DS);
        createDataStream(CONTENDER_DS);

        corpusSize = randomIntBetween(20_000, 100_000);
        long startMs = System.currentTimeMillis();
        logger.info("Indexing corpus of {} documents into baseline and contender data streams", corpusSize);

        for (int offset = 0; offset < corpusSize; offset += BATCH_SIZE) {
            int count = Math.min(BATCH_SIZE, corpusSize - offset);
            // Generate once, POST to both data streams — guarantees byte-identical input.
            String batch = EcsLogsDataGenerator.bulkBatch(offset, count);
            postBulk(BASELINE_DS, batch);
            postBulk(CONTENDER_DS, batch);
        }

        // Refresh once per data stream — per-batch refresh would dominate runtime at 40-120 batches.
        doRefresh(BASELINE_DS);
        doRefresh(CONTENDER_DS);

        // Verify the corpus actually landed; fail-fast rather than producing a vacuous duel.
        assertCount(BASELINE_DS, corpusSize);
        assertCount(CONTENDER_DS, corpusSize);

        logger.info(
            "Corpus indexed in {} ms ({} docs, {} batches)",
            System.currentTimeMillis() - startMs,
            corpusSize,
            (corpusSize + BATCH_SIZE - 1) / BATCH_SIZE
        );
        initialized = true;
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        if (initialized) {
            try {
                deleteDataStream(BASELINE_DS);
                deleteDataStream(CONTENDER_DS);
                deleteTemplate(BASELINE_TMPL);
                deleteTemplate(CONTENDER_TMPL);
            } finally {
                // Reset so that -Dtests.iters=N re-indexes on the next iteration.
                initialized = false;
                corpusSize = 0;
            }
        }
    }

    // ── test methods ──────────────────────────────────────────────────────────────────────────

    public void testRandomFilterQueries() throws Exception {
        EcsEsqlQueryGenerator gen = new EcsEsqlQueryGenerator(random(), corpusSize);
        int nonEmpty = 0;
        for (int i = 0; i < QUERIES_PER_METHOD; i++) {
            if (runAndCompare(gen.randomFilterQuery())) {
                nonEmpty++;
            }
        }
        assertNonEmptyRatio(nonEmpty, QUERIES_PER_METHOD, "testRandomFilterQueries");
    }

    public void testRandomStatsQueries() throws Exception {
        EcsEsqlQueryGenerator gen = new EcsEsqlQueryGenerator(random(), corpusSize);
        int nonEmpty = 0;
        for (int i = 0; i < QUERIES_PER_METHOD; i++) {
            if (runAndCompare(gen.randomStatsQuery())) {
                nonEmpty++;
            }
        }
        assertNonEmptyRatio(nonEmpty, QUERIES_PER_METHOD, "testRandomStatsQueries");
    }

    public void testRandomEvalQueries() throws Exception {
        EcsEsqlQueryGenerator gen = new EcsEsqlQueryGenerator(random(), corpusSize);
        int nonEmpty = 0;
        for (int i = 0; i < QUERIES_PER_METHOD; i++) {
            if (runAndCompare(gen.randomEvalQuery())) {
                nonEmpty++;
            }
        }
        assertNonEmptyRatio(nonEmpty, QUERIES_PER_METHOD, "testRandomEvalQueries");
    }

    // ── query execution ───────────────────────────────────────────────────────────────────────

    /**
     * Runs {@code queryTemplate} against baseline and contender and asserts they agree.
     *
     * @return {@code true} if the baseline returned at least one row; {@code false} if the result
     *         was empty. An empty result compares {@code []} to {@code []} and therefore asserts
     *         nothing — callers should count empties and fail if the ratio is too high.
     */
    @SuppressWarnings("unchecked")
    private boolean runAndCompare(String queryTemplate) throws Exception {
        Map<String, Object> baseline = runEsql(queryTemplate, BASELINE_DS);
        Map<String, Object> contender = runEsql(queryTemplate, CONTENDER_DS);

        // is_partial must be absent or false on both sides
        Object basePartial = baseline.get("is_partial");
        Object contPartial = contender.get("is_partial");
        assertFalse("baseline result is partial for query: " + queryTemplate, Boolean.TRUE.equals(basePartial));
        assertFalse("contender result is partial for query: " + queryTemplate, Boolean.TRUE.equals(contPartial));

        List<Map<String, Object>> baseCols = (List<Map<String, Object>>) baseline.get("columns");
        List<Map<String, Object>> contCols = (List<Map<String, Object>>) contender.get("columns");
        assertEquals("Column list mismatch for query:\n" + queryTemplate, baseCols, contCols);

        List<List<Object>> baseRows = (List<List<Object>>) baseline.get("values");
        List<List<Object>> contRows = (List<List<Object>>) contender.get("values");
        if (baseRows == null) baseRows = List.of();
        if (contRows == null) contRows = List.of();

        assertEquals("Row count mismatch for query:\n" + queryTemplate, baseRows.size(), contRows.size());

        for (int row = 0; row < baseRows.size(); row++) {
            List<Object> baseRow = baseRows.get(row);
            List<Object> contRow = contRows.get(row);
            assertEquals("Row " + row + " length mismatch for query:\n" + queryTemplate, baseRow.size(), contRow.size());
            for (int col = 0; col < baseRow.size(); col++) {
                Object bv = baseRow.get(col);
                Object cv = contRow.get(col);
                // double-typed cells: use relative epsilon for AVG/SUM (floating-point summation
                // order genuinely differs between index modes' segment orderings)
                String colType = baseCols.get(col) != null ? String.valueOf(baseCols.get(col).get("type")) : "";
                if ("double".equals(colType) && bv instanceof Number bNum && cv instanceof Number cNum) {
                    double bd = bNum.doubleValue();
                    double cd = cNum.doubleValue();
                    if (Double.isNaN(bd) && Double.isNaN(cd)) {
                        continue; // both NaN: equal
                    }
                    double magnitude = Math.max(Math.abs(bd), Math.abs(cd));
                    double epsilon = magnitude > 0 ? magnitude * 1e-9 : 1e-9;
                    assertEquals("Row " + row + ", col " + col + " double mismatch for query:\n" + queryTemplate, bd, cd, epsilon);
                } else {
                    assertEquals("Row " + row + ", col " + col + " value mismatch for query:\n" + queryTemplate, bv, cv);
                }
            }
        }

        boolean hasRows = baseRows.isEmpty() == false;
        if (hasRows == false) {
            logger.warn("Empty baseline result for query:\n{}", queryTemplate.replace("$index", BASELINE_DS));
        }
        return hasRows;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> runEsql(String queryTemplate, String dataStream) throws IOException {
        String query = queryTemplate.replace("$index", dataStream);
        Request request = new Request("POST", "/_query");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        XContentBuilder body = XContentFactory.jsonBuilder();
        body.startObject();
        body.field("query", query);
        body.endObject();
        request.setJsonEntity(Strings.toString(body));
        Response response = client().performRequest(request);
        assertOK(response);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
    }

    // ── cluster operations ────────────────────────────────────────────────────────────────────

    private void createTemplate(String name, String pattern, String indexMode) throws IOException {
        String numShards = String.valueOf(randomIntBetween(1, 3));

        XContentBuilder mappings = XContentFactory.jsonBuilder();
        EcsLogsDataGenerator.writeMapping(mappings);

        Settings.Builder settings = Settings.builder();
        settings.put("index.number_of_shards", numShards);
        // No replicas: single-node test cluster cannot place replica shards, and replication
        // doubles indexing work / heap pressure with no benefit for correctness testing.
        settings.put("index.number_of_replicas", 0);
        if ("logsdb".equals(indexMode)) {
            settings.put("index.mode", "logsdb");
            // Match the columnar default so sequence-number differences don't cause divergence.
            // Same rationale as StaticLogsDbSubobjectsFalseVersusLogsDbColumnarRestIT:31-32.
            settings.put("index.disable_sequence_numbers", true);
        } else {
            settings.put("index.mode", "logsdb_columnar");
        }

        String body = Strings.format("""
            {
              "index_patterns": [ "%s" ],
              "template": {
                "settings": %s,
                "mappings": %s
              },
              "data_stream": {},
              "priority": %d
            }
            """, pattern, settings.build().toString(), Strings.toString(mappings), TEMPLATE_PRIORITY);
        Request req = new Request("PUT", "/_index_template/" + name);
        req.setJsonEntity(body);
        assertOK(client().performRequest(req));
    }

    private void createDataStream(String name) throws IOException {
        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + name)));
    }

    private static void deleteDataStream(String name) throws IOException {
        // Use adminClient() — the instance client may be closed by the time @AfterClass runs.
        // ESRestTestCase provides client() as an instance method, so use the class-level field.
        // We catch and ignore 404 in case setup never finished.
        try {
            Request req = new Request("DELETE", "/_data_stream/" + name);
            client().performRequest(req);
        } catch (Exception ignored) {}
    }

    private static void deleteTemplate(String name) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/_index_template/" + name));
        } catch (Exception ignored) {}
    }

    private void postBulk(String dataStream, String ndjson) throws IOException {
        Request req = new Request("POST", "/" + dataStream + "/_bulk");
        req.setEntity(streamingEntity(ndjson));
        Response response = client().performRequest(req);
        assertOK(response);
        Map<?, ?> body = entityAsMap(response);
        assertThat("bulk errors indexing into " + dataStream + ": " + body, body.get("errors"), equalTo(false));
    }

    private void doRefresh(String dataStream) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + dataStream + "/_refresh")));
    }

    @SuppressWarnings("unchecked")
    private void assertCount(String dataStream, int expected) throws IOException {
        Request req = new Request("POST", "/" + dataStream + "/_count");
        Response resp = client().performRequest(req);
        assertOK(resp);
        Map<String, Object> body = XContentHelper.convertToMap(XContentType.JSON.xContent(), resp.getEntity().getContent(), true);
        int actual = (int) body.get("count");
        assertThat("document count mismatch for " + dataStream + " — indexing may have been rejected", actual, equalTo(expected));
    }

    /**
     * Asserts that at least {@link #MIN_NON_EMPTY_RATIO} of the queries in a test method returned
     * at least one row. A run dominated by empty results would pass while asserting nothing, since
     * comparing {@code []} to {@code []} is vacuously true for both index modes. The WARN log in
     * {@link #runAndCompare} names the empty queries so a regression is diagnosable.
     */
    private void assertNonEmptyRatio(int nonEmpty, int total, String method) {
        assertThat(
            "Too many queries in "
                + method
                + " returned no rows ("
                + nonEmpty
                + "/"
                + total
                + " non-empty, need >= "
                + MIN_NON_EMPTY_RATIO
                + ") — the duel is comparing empty result sets and asserting nothing",
            (double) nonEmpty / total,
            greaterThanOrEqualTo(MIN_NON_EMPTY_RATIO)
        );
    }

    /**
     * Streams the NDJSON body rather than converting to a byte array, avoiding OOM for large
     * batches. See the same helper in {@code StandardVersusLogsIndexModeChallengeRestIT}.
     */
    private static HttpEntity streamingEntity(String ndjson) {
        Charset charset = ContentType.APPLICATION_JSON.getCharset();
        return new InputStreamEntity(new ReaderInputStream(new StringReader(ndjson), charset), -1, ContentType.APPLICATION_JSON);
    }
}
