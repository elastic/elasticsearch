/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.addBlobToFixture;

/**
 * Verifies that dataset queries targeting different S3 endpoints are not cross-contaminated
 * via the listing, schema, or file-metadata caches.
 *
 * <p>The bug: {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver} built
 * all three cache keys from the raw config map. For dataset queries, connection settings
 * (endpoint, region) and credentials live in a {@code _datasource} sub-map, not at the top
 * level. {@link org.elasticsearch.xpack.esql.datasources.cache.EndpointRegion} and
 * {@link org.elasticsearch.xpack.esql.datasources.cache.ListingCacheKey#computeCredentialHash}
 * both scan only top-level keys, so every dataset query produced {@code endpoint=""} and
 * {@code credentialHash=0} — all datasets shared one cache partition.
 *
 * <p>The fix: all cache-key build sites in {@code ExternalSourceResolver} now call
 * {@code storageConfig(config)} first, which merges the {@code _datasource} sub-map to the
 * top level before the key is computed.
 *
 * <p>The test sets up two in-process S3 fixtures at different ports. Both serve a file at
 * the same S3 path ({@code s3://test-bucket/warehouse/cache_isolation/test.ndjson}) but with
 * different row counts. A query against the first dataset warms the caches. The second dataset
 * is then queried: with the fix its listing/schema keys differ (different endpoints) and it
 * reads its own file; without the fix it would hit the first dataset's warm cache entry and
 * return the wrong count.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class DatasetCacheIsolationIT extends ESRestTestCase {

    // Test 1 — endpoint isolation: two fixtures, same path, different row counts
    private static final String FILE_KEY = WAREHOUSE + "/cache_isolation/test.ndjson";
    private static final String RESOURCE = "s3://" + BUCKET + "/" + FILE_KEY;
    private static final int ROWS_A = 3;
    private static final int ROWS_B = 7;
    private static final String DATA_SOURCE_A = "cache_iso_ds_a";
    private static final String DATA_SOURCE_B = "cache_iso_ds_b";
    private static final String DATASET_A = "cache_iso_dataset_a";
    private static final String DATASET_B = "cache_iso_dataset_b";

    // Test 2 — auth-mode isolation: anonymous then static credentials, same endpoint, distinct file
    // Uses a separate file path so the schema cache is independent of test 1.
    private static final String FILE_KEY_2 = WAREHOUSE + "/cache_isolation/anon_static.ndjson";
    private static final String RESOURCE_2 = "s3://" + BUCKET + "/" + FILE_KEY_2;
    private static final int ROWS_FILE_2 = 5;
    private static final String DATA_SOURCE_ANON = "cache_iso_ds_anon";
    private static final String DATA_SOURCE_STATIC2 = "cache_iso_ds_static2";
    private static final String DATASET_ANON = "cache_iso_dataset_anon";
    private static final String DATASET_STATIC2 = "cache_iso_dataset_static2";

    // Test 3 — wrong credentials: fixture returns 403; ES must propagate the error, not silently return 0 rows
    // Uses a distinct file path so the listing cache never carries a warm entry for these credentials.
    private static final String FILE_KEY_3 = WAREHOUSE + "/cache_isolation/wrong_creds.ndjson";
    private static final String RESOURCE_3 = "s3://" + BUCKET + "/" + FILE_KEY_3;
    private static final int ROWS_FILE_3 = 4;
    private static final String DATA_SOURCE_WRONG = "cache_iso_ds_wrong";
    private static final String DATASET_WRONG = "cache_iso_dataset_wrong";

    static DataSourcesS3HttpFixture fixtureA = new DataSourcesS3HttpFixture();
    static DataSourcesS3HttpFixture fixtureB = new DataSourcesS3HttpFixture();

    private static final ElasticsearchCluster cluster = ExternalDistributedClusters.testCluster(() -> fixtureA.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new org.junit.runners.model.Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
            assumeTrue("FROM <dataset> over external data sources required", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
            base.evaluate();
        }
    }).around(fixtureA).around(fixtureB).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void uploadFiles() {
        addBlobToFixture(fixtureA.getHandler(), FILE_KEY, ndjson(ROWS_A));
        addBlobToFixture(fixtureB.getHandler(), FILE_KEY, ndjson(ROWS_B));
        addBlobToFixture(fixtureA.getHandler(), FILE_KEY_2, ndjson(ROWS_FILE_2));
        addBlobToFixture(fixtureA.getHandler(), FILE_KEY_3, ndjson(ROWS_FILE_3));
    }

    // @BeforeClass would be cleaner, but ESRestTestCase.client() is null during @BeforeClass;
    // the REST client is only available after the cluster @ClassRule has started. @Before is safe
    // because DatasetRegistry.ensure* methods are idempotent and skip re-registration on repeat calls.
    @Before
    public void registerDatasetsAndDataSources() throws IOException {
        DatasetRegistry.ensureDataSource(
            client(),
            DATA_SOURCE_A,
            "s3",
            Map.of("endpoint", fixtureA.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        DatasetRegistry.ensureDataset(client(), DATASET_A, DATA_SOURCE_A, RESOURCE, null);

        DatasetRegistry.ensureDataSource(
            client(),
            DATA_SOURCE_B,
            "s3",
            Map.of("endpoint", fixtureB.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        DatasetRegistry.ensureDataset(client(), DATASET_B, DATA_SOURCE_B, RESOURCE, null);

        // Test 2 data sources/datasets — use RESOURCE_2 (distinct file path) to isolate
        // the schema cache from test 1's FILE_KEY entries.
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE_ANON, "s3", Map.of("endpoint", fixtureA.getAddress(), "auth", "anonymous"));
        DatasetRegistry.ensureDataset(client(), DATASET_ANON, DATA_SOURCE_ANON, RESOURCE_2, null);

        DatasetRegistry.ensureDataSource(
            client(),
            DATA_SOURCE_STATIC2,
            "s3",
            Map.of("endpoint", fixtureA.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        DatasetRegistry.ensureDataset(client(), DATASET_STATIC2, DATA_SOURCE_STATIC2, RESOURCE_2, null);

        // Test 3: credentials that the fixture rejects with 403. Uses a distinct path so the listing cache
        // for this endpoint+credentials pair is always cold, ensuring the S3 listing request is actually made.
        DatasetRegistry.ensureDataSource(
            client(),
            DATA_SOURCE_WRONG,
            "s3",
            Map.of("endpoint", fixtureA.getAddress(), "access_key", "wrong-access-key", "secret_key", "wrong-secret-key")
        );
        DatasetRegistry.ensureDataset(client(), DATASET_WRONG, DATA_SOURCE_WRONG, RESOURCE_3, null);
    }

    @AfterClass
    public static void cleanup() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    /**
     * Queries dataset A first (warming the listing and schema caches), then dataset B.
     * Dataset B must return its own row count from its own endpoint, not dataset A's cached value.
     *
     * <p>Before the fix, the listing and schema cache keys did not include the endpoint or
     * credential hash for dataset queries (those values were in the {@code _datasource} sub-map,
     * invisible to the key builders). Both datasets shared cache partition {@code (endpoint="",
     * credentialHash=0)}, so the second query would be served dataset A's warm schema entry
     * (with {@code STATS_ROW_COUNT=3}) via {@code canSkipSplitDiscovery}, returning 3 instead of 7.
     *
     * <p>The profile assertion after the first query confirms the coordinator actually went warm
     * (served from the schema-cache stats, no S3 scan) before dataset B is queried. Without it,
     * dataset B returning 7 could be a cold scan that happened to race ahead of A's cache harvest.
     */
    public void testSchemaAndListingCacheIsolatedByEndpoint() throws Exception {
        long countA = count("FROM " + DATASET_A + " | STATS count = COUNT(*)");
        assertEquals(ROWS_A, countA);

        // Confirm the coordinator is warm for dataset A before querying dataset B.
        // external_warm_aggregates > 0 means the COUNT(*) was served from the schema-cache stats
        // without re-scanning S3. Only after this signal can we be sure that dataset B returning 7
        // reflects cache isolation rather than a cold scan that produced the correct answer by chance.
        // Stripe reconciliation is async, so retry until warm or the 30 s deadline passes.
        String warmQuery = "FROM " + DATASET_A + " | STATS count = COUNT(*)";
        assertBusy(() -> assertCoordinatorWarm(warmQuery), 30, TimeUnit.SECONDS);

        long countB = count("FROM " + DATASET_B + " | STATS count = COUNT(*)");
        assertEquals(
            "Dataset B must read from its own endpoint (fixtureB, "
                + ROWS_B
                + " rows) — if this fails with "
                + ROWS_A
                + ", the listing/schema cache keys are not isolated by endpoint",
            ROWS_B,
            countB
        );
    }

    /**
     * Mirrors the scenario from public issue #158169: anonymous dataset first (warms the caches),
     * then a static-credentials dataset backed by the same endpoint and resource. Both must return
     * the actual file row count.
     *
     * <p>Note: cache contamination in this direction (anonymous first) would serve the anonymous
     * dataset's warm count to the static-credentials query — returning {@value ROWS_A}, not 0.
     * The 0-rows report therefore cannot be caused by cache contamination alone. If this test fails
     * with 0 for the static-credentials query there is a separate bug in how static credentials
     * handles the S3 response (distinct from the cache-isolation fix).
     */
    public void testAnonymousThenStaticCredentialsReturnsSameCount() throws IOException {
        long countAnon = count("FROM " + DATASET_ANON + " | STATS count = COUNT(*)");
        assertEquals(ROWS_FILE_2, countAnon);

        long countStatic = count("FROM " + DATASET_STATIC2 + " | STATS count = COUNT(*)");
        assertEquals(
            "Static-credentials dataset must return the file's actual row count ("
                + ROWS_FILE_2
                + ") — 0 would indicate a separate bug with static credentials (mirrors issue #158169)",
            ROWS_FILE_2,
            countStatic
        );
    }

    /**
     * Queries a dataset whose data source carries credentials that the S3 fixture rejects with HTTP 403
     * (the access key does not match). The query must fail with an error — <em>not</em> silently return
     * 0 rows. A 0-row result would indicate that the 403 is being swallowed somewhere in the resolution
     * or read path instead of propagated as a query error.
     *
     * <p>This mirrors the symptom in public issue #158169 ("static_credentials returns 0 rows") under
     * the hypothesis that the root cause is a 403 being swallowed rather than thrown. If the test fails
     * because {@code count} returns 0 instead of throwing, that is the bug.
     */
    public void testWrongCredentialsPropagateAsError() {
        // expectThrows asserts the lambda throws and returns the non-null exception. If count() returns
        // normally (0 rows), expectThrows fails — that is the bug: a swallowed 403.
        expectThrows(Exception.class, () -> count("FROM " + DATASET_WRONG + " | STATS count = COUNT(*)"));
    }

    @SuppressWarnings("unchecked")
    private long count(String query) throws IOException {
        // coordinator_only ensures both queries run entirely on the coordinator node, so the warm
        // schema entry written by dataset A's stats reconciliation is read from the same in-process
        // cache during dataset B's resolution. Without it, data nodes resolve files directly and
        // the coordinator cache path (canSkipSplitDiscovery) is not exercised.
        Settings pragmas = Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only").build();
        RestEsqlTestCase.RequestObjectBuilder req = new RestEsqlTestCase.RequestObjectBuilder().query(query).pragmasOk().pragmas(pragmas);
        Map<String, Object> result = RestEsqlTestCase.runEsqlSync(req, new AssertWarnings.NoWarnings(), null);
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertNotNull("Expected values in COUNT(*) result", values);
        assertEquals("Expected exactly one row from COUNT(*)", 1, values.size());
        return ((Number) values.get(0).get(0)).longValue();
    }

    /**
     * Runs {@code query} with profiling enabled and asserts that the coordinator served the result from
     * the schema-cache (warm path). {@code external_warm_aggregates > 0} in the profile means the
     * {@code COUNT(*)} was short-circuited at the coordinator without re-scanning S3.
     */
    @SuppressWarnings("unchecked")
    private void assertCoordinatorWarm(String query) throws IOException {
        Settings pragmas = Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only").build();
        RestEsqlTestCase.RequestObjectBuilder req = new RestEsqlTestCase.RequestObjectBuilder().query(query)
            .pragmasOk()
            .pragmas(pragmas)
            .profile(true);
        Map<String, Object> result = RestEsqlTestCase.runEsqlSync(req, new AssertWarnings.NoWarnings(), null);
        Map<String, Object> profile = (Map<String, Object>) result.get("profile");
        assertNotNull("Expected a profile in the response", profile);
        Number warmAggregates = (Number) profile.get("external_warm_aggregates");
        assertNotNull(
            "Expected external_warm_aggregates > 0 in profile — coordinator must be warm before querying the second dataset",
            warmAggregates
        );
        assertTrue("Expected external_warm_aggregates > 0 but got " + warmAggregates, warmAggregates.intValue() > 0);
    }

    private static byte[] ndjson(int rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= rows; i++) {
            sb.append("{\"val\":").append(i).append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
