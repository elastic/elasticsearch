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

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Negative regression guard for {@code auth=managed_identity} on the {@code esql-datasource-s3}
 * plugin: when IRSA is configured but credentials can never be obtained (the on-disk token never
 * matches what STS expects, so STS always returns 401), a query must fail with a clean error
 * response rather than hanging or surfacing an internal failure (e.g. NPE) to the client.
 *
 * <p>Complements {@link IrsaManagedIdentityAuthIT} (happy path) and {@link IrsaCredentialsReloadIT}
 * (token rotation). Here the token is deliberately and permanently wrong.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class S3ManagedIdentityBrokenAuthIT extends ESRestTestCase {

    // Operator-managed symlink location relative to ${ES_PATH_CONF}; mirrors
    // CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION (not on the qa
    // compile classpath, so duplicated here).
    private static final String WEB_IDENTITY_TOKEN_FILE_LOCATION = "esql-datasource-s3/aws-web-identity-token-file";

    private static final String BUCKET = "broken-auth-test-bucket";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "broken_auth_ds";
    private static final String DATASET_NAME = "broken_auth_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Vienna\"}\n{\"id\":2,\"city\":\"Berlin\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    // The token bytes the cluster writes to the entitled symlink location.
    private static final String ON_DISK_TOKEN = "on-disk-token";

    private static final DynamicRegionSupplier regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(BUCKET, dynamicCredentials::isAuthorized);

    // STS expects a token that is never equal to ON_DISK_TOKEN, so every AssumeRoleWithWebIdentity
    // call is rejected with 401 and no credentials are ever issued.
    private static final AwsStsHttpFixture stsHttpFixture = new AwsStsHttpFixture(
        dynamicCredentials::addValidCredentials,
        () -> "token-the-cluster-will-never-have",
        TimeValue.timeValueMinutes(15)
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.datasource.managed_identity.enabled", "true")
        .configFile(WEB_IDENTITY_TOKEN_FILE_LOCATION, Resource.fromString(ON_DISK_TOKEN))
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

    public void testQueryFailsCleanlyWhenCredentialsUnavailable() throws IOException {
        putManagedIdentityDataSource(DATASOURCE_NAME, s3HttpFixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "s3://" + BUCKET + "/" + OBJECT_KEY);

        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1")
        );
        // A clean error response (not a hang, not a 200 with empty results). The unresolvable
        // credentials surface as a normal query error.
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), greaterThanOrEqualTo(400));
    }

    // -----------------------------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------------------------

    private static void putManagedIdentityDataSource(String name, String endpoint) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "s3")
                .startObject("settings")
                .field("auth", "managed_identity")
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

    private static void runEsql(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", query).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        client().performRequest(req);
    }
}
