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

    private static final String FILE_KEY = WAREHOUSE + "/cache_isolation/test.ndjson";
    private static final String RESOURCE = "s3://" + BUCKET + "/" + FILE_KEY;

    private static final int ROWS_A = 3;
    private static final int ROWS_B = 7;

    private static final String DATA_SOURCE_A = "cache_iso_ds_a";
    private static final String DATA_SOURCE_B = "cache_iso_ds_b";
    private static final String DATASET_A = "cache_iso_dataset_a";
    private static final String DATASET_B = "cache_iso_dataset_b";

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
     */
    public void testSchemaAndListingCacheIsolatedByEndpoint() throws IOException {
        long countA = count("FROM " + DATASET_A + " | STATS count = COUNT(*)");
        assertEquals(ROWS_A, countA);

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

    private static byte[] ndjson(int rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= rows; i++) {
            sb.append("{\"val\":").append(i).append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
