/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.DynamicAwsCredentials;
import fixture.aws.DynamicRegionSupplier;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
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
import java.util.UUID;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end regression guard for {@code auth=workload_identity} via EKS Pod Identity on the
 * {@code esql-datasource-s3} plugin.
 *
 * <p>Spawns a separate ES cluster JVM with:
 * <ul>
 *   <li>a fixture-supplied auth token symlinked at the entitled config path
 *       ({@code ${ES_PATH_CONF}/esql-datasource-s3/eks-pod-identity-token}),</li>
 *   <li>{@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE} env var set so the plugin's
 *       {@code S3DataSourcePlugin#storageProviders} sysprop redirect kicks in on first use,</li>
 *   <li>{@code AWS_CONTAINER_CREDENTIALS_FULL_URI} env var pointing at the local
 *       {@link PodIdentityCredentialsHttpFixture} so {@code ContainerCredentialsProvider}
 *       resolves credentials against a fake endpoint instead of the real EKS Pod Identity Agent,</li>
 *   <li>{@code esql.datasource.workload_identity.enabled=true} so the validator accepts the data
 *       source.</li>
 * </ul>
 *
 * <p>A successful query proves: PUT data_source(auth=workload_identity) → cluster-setting gate →
 * S3StorageProvider builds an S3 client → ContainerCredentialsProvider reads the auth token from
 * the entitled symlink (because the sysprop override redirected it there) → exchanges the token
 * at the credentials endpoint for AWS credentials → those credentials sign an S3 GET → NDJSON
 * reader returns rows.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PodIdentityWorkloadIdentityAuthIT extends ESRestTestCase {

    private static final String BUCKET = "pod-identity-test-bucket";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "pod_identity_workload_identity_ds";
    private static final String DATASET_NAME = "pod_identity_workload_identity_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Lisbon\"}\n{\"id\":2,\"city\":\"Madrid\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    private static final String AUTH_TOKEN_FILE_CONTENTS = "test-pod-identity-auth-token-" + UUID.randomUUID();

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final SeedingS3HttpFixture s3HttpFixture = new SeedingS3HttpFixture(BUCKET, dynamicCredentials::isAuthorized);

    private static final PodIdentityCredentialsHttpFixture credentialsFixture = new PodIdentityCredentialsHttpFixture(
        () -> AUTH_TOKEN_FILE_CONTENTS,
        dynamicCredentials::addValidCredentials,
        () -> "test-secret-key"
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.datasource.workload_identity.enabled", "true")
        // Operator-managed symlink the plugin redirects the AWS SDK at via JVM sysprop.
        .configFile("esql-datasource-s3/eks-pod-identity-token", Resource.fromString(AUTH_TOKEN_FILE_CONTENTS))
        // The plugin only checks the env var for presence to decide whether to set the sysprop;
        // any non-empty value works because the sysprop override pins the SDK to the entitled path.
        .environment("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", () -> "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/token")
        // ContainerCredentialsProvider reads the credentials endpoint URL from this env var.
        .environment("AWS_CONTAINER_CREDENTIALS_FULL_URI", credentialsFixture::getCredentialsUri)
        .environment("AWS_REGION", regionSupplier)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(credentialsFixture).around(cluster);

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
     * Core regression guard for the Pod Identity flow. Mirrors {@code IrsaWorkloadIdentityAuthIT}
     * but exercises the {@code ContainerCredentialsProvider} branch of the workload-identity
     * chain instead of the IRSA branch.
     */
    public void testPodIdentityWorkloadIdentityAuthQueryReturnsRows() throws IOException {
        putWorkloadIdentityDataSource(DATASOURCE_NAME, s3HttpFixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "s3://" + BUCKET + "/" + OBJECT_KEY);

        Map<String, Object> result = runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(
            "auth=workload_identity (Pod Identity) query must return at least one stats row",
            values,
            hasSize(greaterThanOrEqualTo(1))
        );
        Number count = (Number) values.get(0).get(0);
        assertThat("auth=workload_identity (Pod Identity) query must count both seeded NDJSON rows", count.intValue(), equalTo(2));
    }

    /**
     * Negative companion mirroring the IRSA IT: with the cluster setting flipped off, the data
     * source PUT must be rejected at the validator. Both ITs go through the same gate code path,
     * so this exists for symmetry — deleting one of the gates should fail the negative test in
     * both surfaces.
     */
    public void testPodIdentityWorkloadIdentityAuthRejectedWhenClusterSettingDisabled() throws IOException {
        try {
            setWorkloadIdentityEnabled(false);
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> putWorkloadIdentityDataSource(DATASOURCE_NAME + "_disabled", s3HttpFixture.getAddress())
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("esql.datasource.workload_identity.enabled"));
        } finally {
            setWorkloadIdentityEnabled(true);
        }
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

    private static void setWorkloadIdentityEnabled(boolean enabled) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().startObject("persistent").field("esql.datasource.workload_identity.enabled", enabled).endObject().endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
