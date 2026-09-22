/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import fixture.aws.DynamicAwsCredentials;
import fixture.aws.DynamicRegionSupplier;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Dual-plugin regression guard: an ES|QL S3 dataset query must not disturb {@code repository-s3}
 * Pod Identity credentials in the same JVM.
 *
 * <p>Assembles the pieces both existing single-plugin tests already use, with three intentional
 * differences from {@link PodIdentityManagedIdentityAuthIT}:
 * <ul>
 *   <li>the entitled token lives only at {@code repository-s3/eks-pod-identity-token} — the state
 *       published {@code repository-s3} instructions produce;</li>
 *   <li>the ES|QL data source uses {@code auth=anonymous}, so the query exercises storage-provider
 *       construction without depending on a token this cluster does not have for ES|QL;</li>
 *   <li>credentials expire in a few seconds so the second snapshot is forced to refresh rather than
 *       being served from the SDK cache.</li>
 * </ul>
 *
 * <p>With only the {@code repository-s3} symlink present, a snapshot succeeds before and after an
 * anonymous S3 dataset query — including after credential expiry forces a refresh.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PodIdentityTwoPluginIT extends ESRestTestCase {

    private static final String SNAPSHOT_BUCKET = "two-plugin-snapshot-bucket";
    private static final String DATASET_BUCKET = "two-plugin-dataset-bucket";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "two_plugin_anon_ds";
    private static final String DATASET_NAME = "two_plugin_anon_rows";
    private static final String REPOSITORY_NAME = "backups";
    private static final String REPOSITORY_S3_TOKEN_LOCATION = "repository-s3/eks-pod-identity-token";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Lisbon\"}\n{\"id\":2,\"city\":\"Madrid\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    /** Short enough that stale-time (expiry − 1 minute) is already past when credentials are issued. */
    private static final Duration CREDENTIAL_LIFETIME = Duration.ofSeconds(5);

    private static final String AUTH_TOKEN_FILE_CONTENTS = "test-pod-identity-auth-token-" + UUID.randomUUID();

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final S3HttpFixture snapshotS3Fixture = new S3HttpFixture(
        true,
        null,
        () -> SNAPSHOT_BUCKET,
        () -> "snap",
        S3ConsistencyModel::randomConsistencyModel,
        dynamicCredentials::isAuthorized
    );

    private static final SeedingS3HttpFixture datasetS3Fixture = new SeedingS3HttpFixture(
        DATASET_BUCKET,
        (accessKey, sessionToken) -> true
    );

    private static final PodIdentityCredentialsHttpFixture credentialsFixture = new PodIdentityCredentialsHttpFixture(
        () -> AUTH_TOKEN_FILE_CONTENTS,
        dynamicCredentials::addValidCredentials,
        () -> "test-secret-key",
        CREDENTIAL_LIFETIME
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
        // Snapshot repository reaches its bucket through the default S3 client.
        .setting("s3.client.default.endpoint", snapshotS3Fixture::getAddress)
        // Only the repository-s3 symlink — the state published instructions produce.
        .configFile(REPOSITORY_S3_TOKEN_LOCATION, Resource.fromString(AUTH_TOKEN_FILE_CONTENTS))
        .environment("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "${ES_PATH_CONF}/" + REPOSITORY_S3_TOKEN_LOCATION)
        .environment("AWS_CONTAINER_CREDENTIALS_FULL_URI", credentialsFixture::getCredentialsUri)
        .environment("AWS_REGION", regionSupplier)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(snapshotS3Fixture)
        .around(datasetS3Fixture)
        .around(credentialsFixture)
        .around(cluster);

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
        datasetS3Fixture.seedBlob(OBJECT_KEY, NDJSON_CONTENT);
    }

    public void testAnS3DatasetQueryDoesNotDisturbSnapshotCredentials() throws Exception {
        registerS3SnapshotRepository(REPOSITORY_NAME);
        assertSnapshotSucceeds(REPOSITORY_NAME, "before-query");

        putAnonymousDataSource(DATASOURCE_NAME, datasetS3Fixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "s3://" + DATASET_BUCKET + "/" + OBJECT_KEY);
        assertQueryReturnsRows("FROM " + DATASET_NAME + " | LIMIT 2");

        awaitCredentialExpiry();
        assertSnapshotSucceeds(REPOSITORY_NAME, "after-query");
    }

    private static void registerS3SnapshotRepository(String name) throws IOException {
        Request req = new Request("PUT", "/_snapshot/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "s3")
                .startObject("settings")
                .field("bucket", SNAPSHOT_BUCKET)
                .field("base_path", "snap")
                .field("client", "default")
                .endObject()
                .endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void assertSnapshotSucceeds(String repository, String snapshot) throws IOException {
        Request req = new Request("PUT", "/_snapshot/" + repository + "/" + snapshot);
        req.addParameter("wait_for_completion", "true");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("indices", "*").endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = entityAsMap(r);
        @SuppressWarnings("unchecked")
        Map<String, Object> snapshotInfo = (Map<String, Object>) body.get("snapshot");
        assertThat(snapshotInfo.get("state"), equalTo("SUCCESS"));
    }

    private static void putAnonymousDataSource(String name, String endpoint) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "s3")
                .startObject("settings")
                .field("auth", "anonymous")
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
            b.startObject()
                .field("data_source", dataSource)
                .field("resource", resource)
                .startObject("settings")
                .field("region", regionSupplier.get())
                .endObject()
                .endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void assertQueryReturnsRows(String query) throws IOException {
        Request req = new Request("POST", "/_query");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().field("query", query).endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> result = entityAsMap(r);
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(greaterThanOrEqualTo(1)));
    }

    private static void awaitCredentialExpiry() {
        // Wait past credential lifetime plus a small cushion so the repository's cached credentials
        // are stale and the next snapshot must re-resolve against the credentials endpoint.
        safeSleep(CREDENTIAL_LIFETIME.plusSeconds(2).toMillis());
    }
}
