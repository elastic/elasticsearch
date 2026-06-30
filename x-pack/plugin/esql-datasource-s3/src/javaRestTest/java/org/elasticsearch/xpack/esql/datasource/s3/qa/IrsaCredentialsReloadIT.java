/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.DynamicAwsCredentials;
import fixture.aws.DynamicRegionSupplier;
import fixture.aws.sts.AwsStsHttpFixture;
import fixture.aws.sts.AwsStsHttpHandler;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Token-rotation regression guard for EKS IRSA on the {@code esql-datasource-s3} plugin. Sibling of
 * {@link IrsaWorkloadIdentityAuthIT}, which only proves the happy path with a single, never-changing
 * token. The custom {@code CustomWebIdentityTokenCredentialsProvider} exists precisely so rotated
 * service-account tokens are picked up before the cached STS credentials expire; this test exercises
 * that re-read path end-to-end.
 *
 * <p>Mirrors {@code RepositoryS3StsCredentialsReloadRestIT}: the STS fixture only issues credentials
 * when the token bytes on disk match {@code expectedWebIdentityTokenFileContents}, and credentials
 * are issued with a near-expiry TTL so the AWS SDK keeps refreshing them (re-reading the token file
 * each time).
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class IrsaCredentialsReloadIT extends ESRestTestCase {

    // Operator-managed symlink location relative to ${ES_PATH_CONF}; mirrors
    // CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION (not on the qa
    // compile classpath, so duplicated here).
    private static final String WEB_IDENTITY_TOKEN_FILE_LOCATION = "esql-datasource-s3/aws-web-identity-token-file";

    private static final String BUCKET = "irsa-reload-test-bucket";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "irsa_reload_ds";
    private static final String DATASET_NAME = "irsa_reload_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Vienna\"}\n{\"id\":2,\"city\":\"Berlin\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    // The token the STS fixture expects to see. The cluster's token file starts out mismatched
    // (see configFile below), so STS rejects credential requests until the test rewrites the file
    // to this value, proving the SDK re-reads the rotated token.
    private static volatile String expectedWebIdentityTokenFileContents = "valid-irsa-web-identity-token";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(BUCKET, dynamicCredentials::isAuthorized);

    private static final AwsStsHttpFixture stsHttpFixture = new AwsStsHttpFixture(
        dynamicCredentials::addValidCredentials,
        () -> expectedWebIdentityTokenFileContents,
        // Near-expiry TTL: the SDK refreshes credentials it considers within ~2 minutes of expiry,
        // so picking 115s forces a refresh (and a token-file re-read) on the next request.
        TimeValue.timeValueSeconds(115)
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.datasource.workload_identity.enabled", "true")
        // Start with a token that does NOT match what the STS fixture expects, so the initial query
        // fails; the test then rewrites this file to the valid value at runtime.
        .configFile(WEB_IDENTITY_TOKEN_FILE_LOCATION, Resource.fromString("not ready yet"))
        .systemProperty("org.elasticsearch.xpack.esql.datasource.s3.stsEndpointOverride", stsHttpFixture::getAddress)
        .environment("AWS_WEB_IDENTITY_TOKEN_FILE", () -> "/var/run/secrets/eks.amazonaws.com/serviceaccount/token")
        .environment("AWS_ROLE_ARN", AwsStsHttpHandler.ROLE_ARN)
        .environment("AWS_ROLE_SESSION_NAME", AwsStsHttpHandler.ROLE_NAME)
        .environment("AWS_REGION", regionSupplier)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(stsHttpFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void seedFixture() {
        s3HttpFixture.seedBlob(OBJECT_KEY, NDJSON_CONTENT);
    }

    public void testReloadCredentials() throws IOException {
        putWorkloadIdentityDataSource(DATASOURCE_NAME, s3HttpFixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "s3://" + BUCKET + "/" + OBJECT_KEY);

        // The token file starts mismatched, so STS returns 401 and the S3 read cannot obtain
        // credentials: the query must fail rather than silently return empty results.
        ResponseException ex = expectThrows(ResponseException.class, this::queryDataset);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), greaterThanOrEqualTo(400));

        // Rotate the token on disk to the value the STS fixture expects. The SDK re-reads the token
        // file on its next credential resolution, so the query now succeeds.
        Path tokenFilePath = cluster.getNodeConfigPath(0).resolve(WEB_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(tokenFilePath, expectedWebIdentityTokenFileContents);
        assertQueryCountsBothRows();

        // The STS fixture issues near-expiry credentials, so the SDK keeps refreshing them. Drop the
        // credentials registered so far: a successful query proves the SDK obtained fresh credentials
        // (re-reading the token file) rather than caching the now-invalidated ones.
        dynamicCredentials.clearValidCredentials();
        assertQueryCountsBothRows();
    }

    private void assertQueryCountsBothRows() throws IOException {
        Map<String, Object> result = queryDataset();
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat("IRSA query must return at least one stats row", values, hasSize(greaterThanOrEqualTo(1)));
        Number count = (Number) values.get(0).get(0);
        assertThat("IRSA query must count both seeded NDJSON rows", count.intValue(), equalTo(2));
    }

    private Map<String, Object> queryDataset() throws IOException {
        return runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1");
    }

    // -----------------------------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------------------------

    private static void putWorkloadIdentityDataSource(String name, String endpoint) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "s3")
                .startObject("settings")
                .field("auth", "workload_identity")
                .field("region", regionSupplier.get())
                .field("endpoint", endpoint)
                .endObject()
                .endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void putDataset(String name, String dataSource, String resource) throws IOException {
        Request req = new Request("PUT", "/_query/dataset/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("data_source", dataSource).field("resource", resource).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static Map<String, Object> runEsql(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", query).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        return entityAsMap(r);
    }
}
