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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end regression guard for {@code auth=managed_identity} on the {@code esql-datasource-s3}
 * plugin via EKS IRSA (IAM Roles for Service Accounts).
 *
 * <p>Spawns a separate ES cluster JVM with:
 * <ul>
 *   <li>a fixture-supplied web-identity token symlinked at the entitled config path
 *       ({@code ${ES_PATH_CONF}/esql-datasource-s3/aws-web-identity-token-file}),</li>
 *   <li>{@code AWS_WEB_IDENTITY_TOKEN_FILE}, {@code AWS_ROLE_ARN}, and {@code AWS_ROLE_SESSION_NAME}
 *       env vars set so the plugin's
 *       {@code CustomWebIdentityTokenCredentialsProvider} activates,</li>
 *   <li>the test-only system property {@code
 *       org.elasticsearch.xpack.esql.datasource.s3.stsEndpointOverride} pointing at a local
 *       {@link AwsStsHttpFixture} so STS calls hit a fake endpoint instead of AWS,</li>
 *   <li>{@code esql.datasource.managed_identity.enabled=true} so the validator accepts the data
 *       source.</li>
 * </ul>
 *
 * <p>A successful query proves the full chain: PUT data_source(auth=managed_identity) →
 * cluster-setting gate → S3StorageProvider builds an S3 client → IRSA provider exchanges the
 * token at the entitled symlink for STS temporary credentials → those credentials sign an S3 GET
 * → NDJSON reader returns rows.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class IrsaManagedIdentityAuthIT extends ESRestTestCase {

    private static final String BUCKET = "irsa-test-bucket";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "irsa_managed_identity_ds";
    private static final String DATASET_NAME = "irsa_managed_identity_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Vienna\"}\n{\"id\":2,\"city\":\"Berlin\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    // Token contents are arbitrary opaque bytes from the SDK's perspective; the STS fixture echoes
    // them back as the WebIdentityToken parameter value, so the same string must appear in both
    // the cluster's config-file resource and the fixture's expected-token supplier.
    private static final String WEB_IDENTITY_TOKEN_FILE_CONTENTS = "test-irsa-web-identity-token";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    // STS issues credentials at runtime; we register them here so the S3 fixture can authorize
    // the resulting signed requests. Without this, signed S3 requests would all be rejected and
    // any test failure could be confused with a fixture misconfiguration.
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(BUCKET, dynamicCredentials::isAuthorized);

    private static final AwsStsHttpFixture stsHttpFixture = new AwsStsHttpFixture(
        dynamicCredentials::addValidCredentials,
        () -> WEB_IDENTITY_TOKEN_FILE_CONTENTS,
        TimeValue.timeValueMinutes(15)
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.datasource.managed_identity.enabled", "true")
        // The plugin requires the operator to symlink the EKS-injected web-identity token to a
        // fixed location under config; the cluster builder writes the token bytes there directly.
        .configFile("esql-datasource-s3/aws-web-identity-token-file", Resource.fromString(WEB_IDENTITY_TOKEN_FILE_CONTENTS))
        // Redirect STS calls to the loopback fixture instead of the real STS endpoint. Sysprop is
        // safe here (test JVM) and matches the seam declared in
        // CustomWebIdentityTokenCredentialsProvider.STS_ENDPOINT_OVERRIDE_PROPERTY.
        .systemProperty("org.elasticsearch.xpack.esql.datasource.s3.stsEndpointOverride", stsHttpFixture::getAddress)
        // The provider only checks AWS_WEB_IDENTITY_TOKEN_FILE for presence; the value is
        // ignored because the plugin always reads from the fixed entitled symlink.
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

    /**
     * Core regression guard: with IRSA configured end-to-end (token file at the entitled path,
     * STS fixture issuing credentials, cluster-setting gate open), an ESQL query against the
     * seeded NDJSON blob must return rows. A non-empty result proves every step of the chain.
     */
    public void testIrsaManagedIdentityAuthQueryReturnsRows() throws IOException {
        putManagedIdentityDataSource(DATASOURCE_NAME, s3HttpFixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "s3://" + BUCKET + "/" + OBJECT_KEY);

        // Trailing LIMIT 1 silences the ESRestTestCase strict-mode default-limit warning.
        Map<String, Object> result = runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat("auth=managed_identity (IRSA) query must return at least one stats row", values, hasSize(greaterThanOrEqualTo(1)));
        Number count = (Number) values.get(0).get(0);
        assertThat("auth=managed_identity (IRSA) query must count both seeded NDJSON rows", count.intValue(), equalTo(2));
    }

    /**
     * Negative companion mirroring the Azure IT: with the cluster setting flipped off, the data
     * source PUT must be rejected at the validator. Confirms the gate is wired through the cluster
     * setting and that the dynamic supplier in {@code EsqlPlugin} actually observes operator
     * changes.
     */
    public void testIrsaManagedIdentityAuthRejectedWhenClusterSettingDisabled() throws IOException {
        try {
            setManagedIdentityEnabled(false);
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> putManagedIdentityDataSource(DATASOURCE_NAME + "_disabled", s3HttpFixture.getAddress())
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(
                org.apache.http.util.EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("esql.datasource.managed_identity.enabled")
            );
        } finally {
            setManagedIdentityEnabled(true);
        }
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

    private static void setManagedIdentityEnabled(boolean enabled) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().startObject("persistent").field("esql.datasource.managed_identity.enabled", enabled).endObject().endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
