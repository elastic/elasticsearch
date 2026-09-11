/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.AwsCredentialsUtils;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end integration test for the HeadBucket-based region-discovery retry path.
 *
 * <p>When a data source has no explicit {@code region} and an endpoint override is set, the
 * provider seeds S3 clients with {@code us-east-1}. A custom-endpoint store that validates the
 * signing region rejects such requests with HTTP 400 AuthorizationHeaderMalformed. The provider
 * catches this, issues a HEAD bucket request to read the {@code x-amz-bucket-region} response
 * header, then retries the original operation signed for the discovered region.
 *
 * <p>The test fixture ({@link SeedingS3HttpFixture}) is put into region-validating mode via
 * {@link SeedingS3HttpFixture#setCorrectRegion}: wrong-region requests get 400 + the error code
 * or region header; correct-region requests pass through normally.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RegionDiscoveryIT extends ESRestTestCase {

    private static final String BUCKET = "region-discovery-bucket";
    private static final String ACCESS_KEY = "region_discovery_ak";
    private static final String SECRET_KEY = "region_discovery_sk";

    // Any region other than us-east-1 works: the provider seeds with us-east-1 when no region is
    // configured, so the first request will be signed incorrectly, triggering discovery.
    private static final String CORRECT_REGION = "eu-central-1";

    private static final String DATA_SOURCE = "region_discovery_ds";
    private static final String DATASET = "region_discovery_data";

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(
        BUCKET,
        AwsCredentialsUtils.fixedAccessKey(ACCESS_KEY, () -> CORRECT_REGION, "s3")
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        .keystore("cluster.state.encryption.password.test", "region-discovery-enc-password")
        .keystore("cluster.state.encryption.active_password_id", "test")
        // Do not set AWS_REGION: the provider explicitly seeds with us-east-1 when region is absent
        // and endpoint is set, so no environment variable should override that seeding.
        .environment("AWS_CONFIG_FILE", "/dev/null/aws/config")
        .environment("AWS_SHARED_CREDENTIALS_FILE", "/dev/null/aws/credentials")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void skipForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void setupFixture() {
        // Region-validating mode: wrong-region requests get 400 AuthorizationHeaderMalformed;
        // HEAD bucket additionally carries x-amz-bucket-region: eu-central-1.
        s3HttpFixture.setCorrectRegion(CORRECT_REGION);
        // A two-row CSV the query will read after region discovery.
        s3HttpFixture.seedBlob("data/test.csv", "id,city\n1,Vienna\n2,Berlin\n".getBytes(StandardCharsets.UTF_8));
    }

    @After
    public void cleanup() throws IOException {
        Request delDs = new Request("DELETE", "/_query/dataset/" + DATASET);
        delDs.setOptions(delDs.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        try {
            client().performRequest(delDs);
        } catch (Exception ignored) {}
        Request delDatasource = new Request("DELETE", "/_query/data_source/" + DATA_SOURCE);
        delDatasource.setOptions(delDatasource.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        try {
            client().performRequest(delDatasource);
        } catch (Exception ignored) {}
    }

    /**
     * Verifies the full 400-AuthorizationHeaderMalformed → HeadBucket → discover region →
     * retry path against a real fixture.
     *
     * <p>The data source has no {@code region}; the fixture rejects all wrong-region requests with
     * 400 AuthorizationHeaderMalformed. The provider must discover {@code eu-central-1} via
     * HeadBucket and successfully complete the query on the second attempt.
     */
    public void testQuerySucceedsAfterRegionDiscoveryRetry() throws IOException {
        // Data source: credentials + endpoint, deliberately no region.
        putDataSource();
        // Dataset: glob path (triggers listObjects, which is the entry point for the retry).
        putDataset();

        Request req = new Request("POST", "/_query");
        req.setJsonEntity("{\"query\":\"FROM " + DATASET + " | SORT id | LIMIT 10\"}");
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        Response response = client().performRequest(req);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString("Vienna"));
        assertThat(body, containsString("Berlin"));
    }

    private void putDataSource() throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + DATA_SOURCE);
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        req.setJsonEntity(
            Strings.format(
                """
                    {"type":"s3","settings":{"access_key":"%s","secret_key":"%s","endpoint":"%s"}}""",
                ACCESS_KEY,
                SECRET_KEY,
                s3HttpFixture.getAddress()
            )
        );
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }

    private void putDataset() throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + DATASET);
        req.setOptions(req.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        // Glob path: planning uses listObjects (not exists), which is the code path that has the
        // AuthorizationHeaderMalformed retry and HeadBucket discovery wired up.
        req.setJsonEntity(Strings.format("""
            {"data_source":"%s","resource":"s3://%s/data/*.csv"}""", DATA_SOURCE, BUCKET));
        assertThat(client().performRequest(req).getStatusLine().getStatusCode(), equalTo(200));
    }
}
