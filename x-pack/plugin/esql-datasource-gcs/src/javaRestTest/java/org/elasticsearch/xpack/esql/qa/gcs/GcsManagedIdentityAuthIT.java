/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
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

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end regression guard for {@code auth=managed_identity} on GCS external data sources.
 *
 * <p>Spawns a separate ES cluster JVM with {@code GCE_METADATA_HOST} pointing at a local
 * {@link GoogleCloudStorageHttpFixture}, registers a GCS data source with {@code auth=managed_identity},
 * registers a dataset over a fixture-seeded NDJSON blob, and runs an ESQL query via REST. A
 * successful query proves the full chain end-to-end:
 * {@code PUT data_source(auth=managed_identity) → cluster setting gate → GCS client construction →
 * ComputeEngineCredentials hits the metadata fixture → bearer token used to read the bucket →
 * NDJSON reader returns rows}.
 *
 * <p>This is the GCS analog of {@code S3ManagedIdentityAuthIT} (S3) but as a REST IT rather
 * than {@code ESIntegTestCase}: the Google auth library reads {@code GCE_METADATA_HOST} from
 * environment variables only (see {@code GoogleCloudStorageService} in {@code repository-gcs}),
 * which Java cannot set on its own JVM at runtime — so the cluster must be a separate process.
 * Mirrors the pattern in {@code DefaultCredentialsRepositoryGcsClientYamlTestSuiteIT}.
 *
 * <p>Validator-level coverage of the {@code esql.datasource.managed_identity.enabled} gate
 * lives in {@code GcsDataSourceValidatorTests}; this IT focuses on the credential-resolution
 * happy path that unit tests cannot reach.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GcsManagedIdentityAuthIT extends ESRestTestCase {

    private static final String BUCKET = "test-workload-identity-bucket";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "managed_identity_gcs_ds";
    private static final String DATASET_NAME = "managed_identity_gcs_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Tokyo\"}\n{\"id\":2,\"city\":\"Seoul\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    // Path served by the fixture's FakeOAuth2HttpHandler; matches the path the Google auth
    // library calls on the metadata server when GCE_METADATA_HOST is set.
    private static final String METADATA_TOKEN_PATH = "computeMetadata/v1/instance/service-accounts/default/token";

    private static final GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(true, BUCKET, METADATA_TOKEN_PATH);

    // The DEFAULT distribution already bundles repository-gcs, workload-identity, and every
    // esql-datasource-* module this test needs, so no .module()/.plugin() calls are required.
    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // Open the workload identity gate so the validator accepts auth=managed_identity.
        .setting("esql.datasource.managed_identity.enabled", "true")
        // Redirect the GCE metadata server to our fixture so ComputeEngineCredentials.refresh()
        // resolves a bearer token against FakeOAuth2HttpHandler instead of metadata.google.internal.
        .environment("GCE_METADATA_HOST", () -> fixture.getAddress().replace("http://", ""))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

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
        // Seed the NDJSON blob the dataset will resolve to. The fixture handler is in-memory
        // and shared across tests; one seed per class is enough.
        fixture.getHandler().putBlob(OBJECT_KEY, new BytesArray(NDJSON_CONTENT));
    }

    @Before
    public void cleanupBefore() throws IOException {
        deleteDatasetIfExists(DATASET_NAME);
        deleteDataSourceIfExists(DATASOURCE_NAME);
    }

    @AfterClass
    public static void cleanupAfter() {
        // RuleChain stops cluster + fixture; nothing to do here beyond preventing a leak warning.
    }

    /**
     * Core regression guard: register an workload identity GCS data source, register a dataset, run an
     * ESQL query, and assert rows are returned. A non-empty result requires every step of the
     * workload identity credential chain to have worked: the validator gate accepted {@code auth=managed_identity},
     * the GCS storage client constructed successfully, {@link com.google.auth.oauth2.ComputeEngineCredentials}
     * resolved a bearer token from the fixture's metadata endpoint, and that token was used to
     * read the seeded NDJSON blob.
     */
    public void testManagedIdentityAuthQueryReturnsRows() throws IOException {
        putManagedIdentityDataSource(DATASOURCE_NAME, fixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "gs://" + BUCKET + "/" + OBJECT_KEY);

        // Trailing LIMIT 1 silences ESQL's "no limit defined, adding default limit of [1000]" warning,
        // which ESRestTestCase strict-mode would otherwise surface as WarningFailureException.
        Map<String, Object> result = runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat("auth=managed_identity query must return at least one stats row", values, hasSize(greaterThanOrEqualTo(1)));
        // STATS COUNT(*) over a 2-line NDJSON blob returns exactly one row whose first column is 2.
        Number count = (Number) values.get(0).get(0);
        assertThat("auth=managed_identity query must count both seeded NDJSON rows", count.intValue(), equalTo(2));
    }

    /**
     * Negative companion: with the cluster setting flipped off, the same PUT data-source
     * request must be rejected at the validator. Confirms the gate is wired through the
     * cluster setting (not just the validator unit-test default) and that the dynamic
     * supplier in {@code EsqlPlugin} actually observes operator changes.
     */
    public void testManagedIdentityAuthRejectedWhenClusterSettingDisabled() throws IOException {
        try {
            setManagedIdentityCredentialsEnabled(false);
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> putManagedIdentityDataSource(DATASOURCE_NAME, fixture.getAddress())
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(
                org.apache.http.util.EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("esql.datasource.managed_identity.enabled")
            );
        } finally {
            // Restore for the rest of the suite. @Before's cleanup runs against fresh state.
            setManagedIdentityCredentialsEnabled(true);
        }
    }

    // -----------------------------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------------------------

    private static void putManagedIdentityDataSource(String name, String endpoint) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "gcs")
                .startObject("settings")
                .field("auth", "managed_identity")
                .field("endpoint", endpoint)
                .field("project_id", "test-project")
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

    private static void deleteDataSourceIfExists(String name) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/_query/data_source/" + name));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    private static void deleteDatasetIfExists(String name) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/_query/dataset/" + name));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    private static void setManagedIdentityCredentialsEnabled(boolean enabled) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().startObject("persistent").field("esql.datasource.managed_identity.enabled", enabled).endObject().endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
