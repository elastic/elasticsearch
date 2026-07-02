/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.azure;

import fixture.azure.AzureHttpFixture;
import fixture.azure.MockAzureBlobStore;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.test.fixtures.tls.TestTrustStore;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end regression guard for {@code auth=managed_identity} on Azure Blob Storage external data sources.
 *
 * <p>Spawns a separate ES cluster JVM whose {@code AZURE_POD_IDENTITY_AUTHORITY_HOST} system property
 * points at a local {@link AzureHttpFixture} (HTTPS), registers an Azure data source with
 * {@code auth=managed_identity}, registers a dataset over a fixture-seeded NDJSON blob, and runs an ESQL query
 * via REST. A successful query proves the full chain end-to-end:
 * {@code PUT data_source(auth=managed_identity) → cluster setting gate → AzureStorageProvider builds an
 * Azure SDK client → ManagedIdentityCredential resolves a bearer token from the fixture's IMDS endpoint
 * → that bearer token authenticates the blob fetch → NDJSON reader returns rows}.
 *
 * <p>This is the Azure analog of {@code S3ManagedIdentityAuthIT} (S3) and {@code GcsManagedIdentityAuthIT}
 * (GCS), but as an HTTPS REST IT: Azure's IMDS / OAuth flows in the SDK are reached via system
 * properties that must be set
 * <em>before</em> the SDK initializes, which only works on a separate cluster JVM. Mirrors the
 * fixture-and-system-property pattern in {@code AzureRepositoryAnalysisRestIT}.
 *
 * <p>Validator-level coverage of the {@code esql.datasource.managed_identity.enabled} gate lives
 * in {@code AzureDataSourceValidatorTests}; this IT focuses on the credential-resolution happy path
 * that unit tests cannot reach.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class AzureManagedIdentityAuthIT extends ESRestTestCase {

    private static final String ACCOUNT = "testaccount";
    private static final String CONTAINER = "testcontainer";
    private static final String OBJECT_KEY = "data/rows.ndjson";
    private static final String DATASOURCE_NAME = "managed_identity_azure_ds";
    private static final String DATASET_NAME = "managed_identity_azure_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Vienna\"}\n{\"id\":2,\"city\":\"Berlin\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    private static final TestTlsCertificate TEST_TLS_CERTIFICATE = TestTlsCertificate.generate("localhost");
    private static final TestTrustStore TEST_TRUST_STORE = new TestTrustStore(TEST_TLS_CERTIFICATE::getPemCertificateStream);

    // MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE makes the fixture accept exactly the bearer token
    // emitted by its own metadata server, so a successful blob fetch proves the cluster used the
    // IMDS-discovered token rather than any other credential.
    private static final AzureHttpFixture fixture = new AzureHttpFixture(
        AzureHttpFixture.Protocol.HTTPS,
        TEST_TLS_CERTIFICATE,
        ACCOUNT,
        CONTAINER,
        null,
        null,
        AzureHttpFixture.MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE,
        MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE
    );

    // The DEFAULT distribution already bundles repository-azure and every esql-datasource-* module
    // this test needs, so no .module()/.plugin() calls are required.
    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        // Open the workload identity gate so the validator accepts auth=managed_identity.
        .setting("esql.datasource.managed_identity.enabled", "true")
        // Redirect the Azure IMDS endpoint to our fixture so ManagedIdentityCredential resolves a
        // bearer token against the metadata server instead of the default 169.254.169.254 (which
        // entitlements would block in the cluster JVM anyway).
        .systemProperty("AZURE_POD_IDENTITY_AUTHORITY_HOST", fixture::getMetadataAddress)
        .apply(builder -> TEST_TRUST_STORE.apply(builder, true))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(TEST_TRUST_STORE).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void seedFixture() throws Exception {
        // The fixture exposes no Java API for direct blob seeding; mimic an authenticated client:
        // ask the (plain-HTTP) IMDS metadata endpoint for the same bearer the cluster will use,
        // then PUT a BlockBlob over HTTPS using that token. Round-tripping through the fixture's
        // own auth path exercises the same predicate the cluster will be checked against.
        String token = fetchManagedIdentityToken();
        putBlockBlob(token, OBJECT_KEY, NDJSON_CONTENT);
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
     * Core regression guard: register an workload identity Azure data source, register a dataset, run an
     * ESQL query, and assert rows are returned. A non-empty result requires every step of the
     * workload identity credential chain to have worked: the validator gate accepted {@code auth=managed_identity},
     * the Azure storage client constructed successfully, {@code ManagedIdentityCredential}
     * resolved a bearer token from the fixture's metadata endpoint, and that token was accepted
     * by the fixture's strict bearer-token predicate while reading the seeded NDJSON blob.
     */
    public void testManagedIdentityAuthQueryReturnsRows() throws IOException {
        putManagedIdentityDataSource(DATASOURCE_NAME, fixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "wasbs://" + ACCOUNT + ".blob.core.windows.net/" + CONTAINER + "/" + OBJECT_KEY);

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
     * Negative companion: with the cluster setting flipped off, the same PUT data-source request
     * must be rejected at the validator. Confirms the gate is wired through the cluster setting
     * (not just the validator unit-test default) and that the dynamic supplier in {@code EsqlPlugin}
     * actually observes operator changes.
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
    // Blob seeding helpers
    // -----------------------------------------------------------------------------------------

    private static final Pattern ACCESS_TOKEN_PATTERN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    @SuppressForbidden(reason = "test seeds a blob through the Azure fixture's loopback HTTP/HTTPS endpoints")
    private static String fetchManagedIdentityToken() throws IOException {
        // Mirrors AzureMetadataServiceHttpHandler's accepted query string exactly.
        URI metadata = URI.create(
            fixture.getMetadataAddress() + "metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://storage.azure.com"
        );
        HttpURLConnection conn = (HttpURLConnection) metadata.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);
        try (InputStream in = conn.getInputStream()) {
            String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Matcher m = ACCESS_TOKEN_PATTERN.matcher(body);
            assertTrue("metadata response missing access_token: " + body, m.find());
            return m.group(1);
        } finally {
            conn.disconnect();
        }
    }

    @SuppressForbidden(reason = "test seeds a blob through the Azure fixture's loopback HTTP/HTTPS endpoints")
    private static void putBlockBlob(String bearerToken, String blobName, byte[] body) throws Exception {
        URI url = URI.create(fixture.getAddress() + "/" + CONTAINER + "/" + blobName);
        HttpsURLConnection conn = (HttpsURLConnection) url.toURL().openConnection();
        conn.setSSLSocketFactory(testSslContext().getSocketFactory());
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.setRequestProperty("Authorization", "Bearer " + bearerToken);
        conn.setRequestProperty("x-ms-blob-type", "BlockBlob");
        conn.setRequestProperty("x-ms-version", "2023-11-03");
        conn.setRequestProperty("Content-Length", Integer.toString(body.length));
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(10_000);
        try (OutputStream out = conn.getOutputStream()) {
            out.write(body);
        }
        int status = conn.getResponseCode();
        assertThat("PUT BlockBlob to fixture must succeed", status, equalTo(HttpURLConnection.HTTP_CREATED));
        conn.disconnect();
    }

    private static SSLContext testSslContext() throws Exception {
        // Trust the fixture's self-signed cert and only that cert. JKS is fine here: the test JVM
        // runs in non-FIPS mode for the snapshot-build IT, mirroring the rest of the qa suites.
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("fixture", TEST_TLS_CERTIFICATE.certificate());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    // -----------------------------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------------------------

    private static void putManagedIdentityDataSource(String name, String endpoint) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "azure")
                .startObject("settings")
                .field("auth", "managed_identity")
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
