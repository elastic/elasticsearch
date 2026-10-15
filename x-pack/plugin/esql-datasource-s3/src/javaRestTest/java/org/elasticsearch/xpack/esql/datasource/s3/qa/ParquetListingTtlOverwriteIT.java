/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.addBlobToFixture;

/**
 * A Parquet object overwritten while its listing is still cached, inside a one-hour listing TTL. Two outcomes are possible, and the
 * cases below check both:
 * <ul>
 *   <li>A query that reads the object under the listed version fails with the retryable 503. The failure drops the listing, so the
 *   client's next attempt lists the object again and reads the new rows.</li>
 *   <li>An aggregate that the dataset aggregate cache answers sends no request, so it returns the listed file's result. That is a
 *   consistent snapshot of the listed version, not old metadata with new bytes.</li>
 * </ul>
 * The schema is strict, so the listing is unbounded and cached. Each case has its own object key and dataset, since the one-hour
 * cache outlives a test.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetListingTtlOverwriteIT extends ESRestTestCase {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String DATA_SOURCE = "same_length_ds";

    private static final String LARGE_V1 = "/same_length_overwrite/large_v1.parquet";
    private static final String LARGE_V2 = "/same_length_overwrite/large_v2.parquet";

    // The listing-TTL cases overwrite while the listing of the first query is cached. Each has its own key and dataset: the
    // one-hour cache outlives a test, so a shared dataset would answer the next test from the previous one's cache.
    private static final String GLOB_ROW = WAREHOUSE + "/same_length/ttl_rows/*.parquet";
    private static final String KEY_ROW = WAREHOUSE + "/same_length/ttl_rows/part-1.parquet";
    private static final String DATASET_ROW = "same_length_ttl_rows";
    private static final String GLOB_AGG = WAREHOUSE + "/same_length/ttl_agg/*.parquet";
    private static final String KEY_AGG = WAREHOUSE + "/same_length/ttl_agg/part-1.parquet";
    private static final String DATASET_AGG = "same_length_ttl_agg";

    /** ES|QL returns at most this many rows when the query has no LIMIT. */
    private static final int DEFAULT_ROW_LIMIT = 1_000;

    static DataSourcesS3HttpFixture fixture = new DataSourcesS3HttpFixture();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        // The listing and file-metadata caches keep the listed version for this long. Nothing here waits for it to expire:
        // the overwrite lands while the listing of the first query is still cached.
        .setting("esql.external.cache.listing.ttl", "1h")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "same-length-overwrite-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
            assumeTrue("FROM <dataset> over external data sources required", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
            base.evaluate();
        }
    }).around(fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
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
     * A row read inside the listing TTL. The listing is unbounded, so it is cached, and the read pins the listed version. The object
     * has been overwritten, so the read is refused with the retryable 503 and the failure drops the listing. The client's next
     * attempt lists the object again and returns the rows of the new file.
     */
    public void testRowReadInsideListingTtlFailsWith503ThenReadsNewRows() throws Exception {
        register(DATASET_ROW, GLOB_ROW);
        addBlobToFixture(fixture.getHandler(), KEY_ROW, resource(LARGE_V1));
        String rows = "FROM " + DATASET_ROW + " | KEEP v";
        assertEquals(DEFAULT_ROW_LIMIT, query(rows).size());

        addBlobToFixture(fixture.getHandler(), KEY_ROW, resource(LARGE_V2));
        ResponseException failed = expectThrows(ResponseException.class, () -> query(rows));
        assertEquals(
            "a read under the listed version must fail with the retryable object-changed status",
            RestStatus.SERVICE_UNAVAILABLE.getStatus(),
            failed.getResponse().getStatusLine().getStatusCode()
        );

        List<List<Long>> newRows = query(rows);
        assertEquals(DEFAULT_ROW_LIMIT, newRows.size());
        assertEquals(
            "the client's next attempt reads the rows of the new file",
            Long.valueOf(1_000_000L),
            newRows.stream().map(r -> r.get(0)).min(Long::compare).orElseThrow()
        );
    }

    /**
     * An aggregate inside the listing TTL is answered from the cache under the listed version, without reading the object: no GET is
     * sent, so no conditional request can fail. The answer is the listed file's, a consistent snapshot, not a mix of old metadata and
     * new bytes. The next read of the rows fails with the 503 (see the row test above).
     */
    public void testAggregateInsideListingTtlAnswersFromTheListedVersion() throws Exception {
        register(DATASET_AGG, GLOB_AGG);
        addBlobToFixture(fixture.getHandler(), KEY_AGG, resource(LARGE_V1));
        String max = "FROM " + DATASET_AGG + " | STATS m = MAX(v)";
        assertEquals(List.of(List.of(9_999L)), query(max));

        addBlobToFixture(fixture.getHandler(), KEY_AGG, resource(LARGE_V2));
        int requestsBefore = fixture.getHandler().requestLog().size();
        assertEquals(
            "the aggregate is answered from the listed version, which is the snapshot the listing names",
            List.of(List.of(9_999L)),
            query(max)
        );
        assertEquals("the aggregate must not reach the object store", requestsBefore, fixture.getHandler().requestLog().size());
    }

    /**
     * Registers the dataset with strict schema resolution. Its schema needs every file, so the listing is unbounded and cached. The
     * default first-file-wins listing is a sample, which the resolver never caches, so it re-lists on every query.
     */
    private static void register(String dataset, String resourcePath) throws IOException {
        DatasetRegistry.ensureDataSource(
            client(),
            DATA_SOURCE,
            "s3",
            Map.of("endpoint", fixture.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        DatasetRegistry.ensureDataset(
            client(),
            dataset,
            DATA_SOURCE,
            "s3://" + BUCKET + "/" + resourcePath,
            "{\"schema_resolution\": \"strict\"}"
        );
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = ParquetListingTtlOverwriteIT.class.getResourceAsStream(name)) {
            assertNotNull("missing test resource " + name, in);
            return in.readAllBytes();
        }
    }

    @SuppressWarnings("unchecked")
    private List<List<Long>> query(String esql) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder req = new RestEsqlTestCase.RequestObjectBuilder().query(esql).pragmasOk();
        Map<String, Object> result = RestEsqlTestCase.runEsqlSync(req, new AssertWarnings.NoWarnings(), null);
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertNotNull("Expected values in the ES|QL result", values);
        List<List<Long>> rows = new ArrayList<>();
        for (List<Object> row : values) {
            List<Long> longs = new ArrayList<>();
            for (Object cell : row) {
                longs.add(((Number) cell).longValue());
            }
            rows.add(longs);
        }
        return rows;
    }
}
