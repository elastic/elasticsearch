/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.azure;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.AzureBackendFixture;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
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

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * A Parquet blob in Azure overwritten in place with a file of the same byte length must be read as the new file. A
 * single-file dataset resolves its blob through a metadata probe, and the probe's ETag is the footer-cache version. This is
 * the path that a glob does not take: a glob's entries come from the listing. See elastic/esql-planning#2094.
 * <p>
 * The Azure fixture gives each write its own ETag, so the probe sees a new version after the overwrite. The test uses the
 * 10000-row fixtures, whose column data is above the 64 KiB footer prefetch.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class AzureSameLengthOverwriteIT extends ESRestTestCase {

    private static final String ENCRYPTION_PASSWORD_ID = "test";
    private static final String DATA_SOURCE = "same_length_azure_ds";
    private static final String DATASET = "same_length_azure_stats";
    private static final String KEY = WAREHOUSE + "/same_length/azure/stats.parquet";
    private static final String V1 = "/same_length_overwrite/large_v1.parquet";
    private static final String V2 = "/same_length_overwrite/large_v2.parquet";
    // The 100-row fixtures are under the 64 KiB footer prefetch, so their rows are served from the footer cache.
    private static final String KEY_ROWS = WAREHOUSE + "/same_length/azure/rows/part-1.parquet";
    private static final String DATASET_ROWS = "same_length_azure_rows";
    private static final String SMALL_V1 = "/same_length_overwrite/v1.parquet";
    private static final String SMALL_V2 = "/same_length_overwrite/v2.parquet";
    // The glob case takes its versions from List Blobs, which reports ETags unquoted, while downloads quote them.
    private static final String KEY_GLOB = WAREHOUSE + "/same_length/azure/glob/part-1.parquet";
    private static final String GLOB = WAREHOUSE + "/same_length/azure/glob/*.parquet";
    private static final String DATASET_GLOB = "same_length_azure_glob";

    static final DataSourcesAzureHttpFixture fixture = new DataSourcesAzureHttpFixture();
    private static final AzureBackendFixture backend = new AzureBackendFixture(fixture);

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
        // The listing and file-metadata caches hold the version for this long. The overwrite waits past it, so the
        // second query probes the blob again, as it would after the listing TTL in production.
        .setting("esql.external.cache.listing.ttl", "1s")
        .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "same-length-azure-overwrite-password")
        .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP Azure fixtures", inFipsJvm());
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

    /** {@code COUNT(*)}, {@code MIN} and {@code MAX} of a single-file dataset must come from the new blob after a same-length overwrite. */
    public void testStatisticsAfterSameLengthOverwriteComeFromNewFile() throws Exception {
        register(DATASET, KEY);
        backend.uploadBlob(KEY, resource(V1));
        String stats = "FROM " + DATASET + " | STATS c = COUNT(*), mn = MIN(v), mx = MAX(v)";
        assertEquals(List.of(List.of(10_000L, 0L, 9_999L)), query(stats));

        // Move past the listing TTL so the next query probes the blob again.
        safeSleep(1_100);
        backend.uploadBlob(KEY, resource(V2));
        assertEquals(
            "the blob was overwritten with a file of the same length; the statistics must come from the new file",
            List.of(List.of(10_000L, 1_000_000L, 1_009_999L)),
            query(stats)
        );
    }

    /**
     * The rows of a file small enough for the footer prefetch must come from the new blob after a same-length
     * overwrite. The split reads them through the footer cache under the probed version, so a probe that reported a
     * constant would serve the first rows again.
     */
    public void testRowsAfterSameLengthOverwriteComeFromNewFile() throws Exception {
        register(DATASET_ROWS, KEY_ROWS);
        backend.uploadBlob(KEY_ROWS, resource(SMALL_V1));
        String firstRow = "FROM " + DATASET_ROWS + " | SORT v | KEEP v | LIMIT 1";
        assertEquals(List.of(List.of(0L)), query(firstRow));

        safeSleep(1_100);
        backend.uploadBlob(KEY_ROWS, resource(SMALL_V2));
        assertEquals(
            "the blob was overwritten with a file of the same length; the rows must come from the new file",
            List.of(List.of(1_000_000L)),
            query(firstRow)
        );
    }

    /**
     * As the rows case, through a glob. The listing supplies each blob's ETag unquoted, and the download reports it quoted, so
     * the read succeeds only if the two forms are compared as the same version.
     */
    public void testGlobRowsAfterSameLengthOverwriteComeFromNewFile() throws Exception {
        register(DATASET_GLOB, GLOB);
        backend.uploadBlob(KEY_GLOB, resource(SMALL_V1));
        String firstRow = "FROM " + DATASET_GLOB + " | SORT v | KEEP v | LIMIT 1";
        assertEquals(List.of(List.of(0L)), query(firstRow));

        safeSleep(1_100);
        backend.uploadBlob(KEY_GLOB, resource(SMALL_V2));
        assertEquals(
            "the blob was overwritten with a file of the same length; the rows must come from the new file",
            List.of(List.of(1_000_000L)),
            query(firstRow)
        );
    }

    private static void register(String dataset, String key) throws IOException {
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE, backend.dataSourceType(), backend.dataSourceSettings());
        DatasetRegistry.ensureDataset(client(), dataset, DATA_SOURCE, backend.resourceUri(key), null);
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = AzureSameLengthOverwriteIT.class.getResourceAsStream(name)) {
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
