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
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.tls.TestTlsCertificate;
import org.elasticsearch.test.fixtures.tls.TestTrustStore;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 * AKS Workload Identity end-to-end regression guard for {@code auth=workload_identity} on the
 * {@code esql-datasource-azure} plugin.
 *
 * <p>Sibling of {@link AzureWorkloadIdentityAuthIT}, which exercises the IMDS / Managed Identity
 * branch of the same chain. This class spawns a separate ES cluster JVM with the AKS env triple
 * set ({@code AZURE_FEDERATED_TOKEN_FILE}, {@code AZURE_CLIENT_ID}, {@code AZURE_TENANT_ID}), the
 * federated token symlinked at the entitled config path, and the Azure SDK's OAuth authority host
 * redirected to the local fixture's OAuth token endpoint. The {@code AzureStorageProvider}'s
 * workload-identity chain therefore picks the {@code WorkloadIdentityCredential} branch, exchanges
 * the federated token at the fixture's OAuth endpoint for an access token, and uses that token to
 * read the seeded blob over HTTPS.
 *
 * <p>A non-empty query result proves every step of the AKS path:
 * {@code PUT data_source(auth=workload_identity) → cluster setting gate → AzureStorageProvider
 * builds a chained credential → WorkloadIdentityCredential reads the entitled token symlink →
 * exchanges it at the OAuth fixture → that bearer reads the blob through the strict workload-
 * identity bearer predicate → NDJSON reader returns rows}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class AzureAksWorkloadIdentityAuthIT extends ESRestTestCase {

    private static final String ACCOUNT = "testaccount";
    private static final String CONTAINER = "testcontainer";
    private static final String OBJECT_KEY = "data/aks-rows.ndjson";
    private static final String DATASOURCE_NAME = "aks_workload_identity_azure_ds";
    private static final String DATASET_NAME = "aks_workload_identity_azure_rows";
    private static final byte[] NDJSON_CONTENT = "{\"id\":1,\"city\":\"Paris\"}\n{\"id\":2,\"city\":\"Rome\"}\n".getBytes(
        StandardCharsets.UTF_8
    );

    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String TENANT_ID = UUID.randomUUID().toString();

    private static final TestTlsCertificate TEST_TLS_CERTIFICATE = TestTlsCertificate.generate("localhost");
    private static final TestTrustStore TEST_TRUST_STORE = new TestTrustStore(TEST_TLS_CERTIFICATE::getPemCertificateStream);

    // WORK_IDENTITY_BEARER_TOKEN_PREDICATE makes the fixture accept exactly the bearer token
    // emitted by its own OAuth token service, so a successful blob fetch proves the cluster used
    // the WorkloadIdentityCredential path rather than a managed-identity bearer or any other source.
    private static final AzureHttpFixture fixture = new AzureHttpFixture(
        AzureHttpFixture.Protocol.HTTPS,
        TEST_TLS_CERTIFICATE,
        ACCOUNT,
        CONTAINER,
        TENANT_ID,
        CLIENT_ID,
        AzureHttpFixture.WORK_IDENTITY_BEARER_TOKEN_PREDICATE,
        MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("esql.datasource.workload_identity.enabled", "true")
        // Operator-managed symlink that the Azure SDK is pointed at via tokenFilePath().
        .configFile("esql-datasource-azure/azure-federated-token", Resource.fromString(fixture.getFederatedToken()))
        // Redirect the SDK's authority host to the fixture's OAuth token endpoint so federated
        // token exchanges hit the loopback fixture instead of the real Microsoft Entra service.
        .systemProperty("AZURE_AUTHORITY_HOST", fixture::getOAuthTokenServiceAddress)
        // Disable instance metadata discovery; without this, WorkloadIdentityCredential probes IMDS first.
        .systemProperty("tests.azure.credentials.disable_instance_discovery", "true")
        // The plugin only checks the AKS env triple for presence; values are read directly here.
        .environment("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token")
        .environment("AZURE_CLIENT_ID", CLIENT_ID)
        .environment("AZURE_TENANT_ID", TENANT_ID)
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
        // Round-trip through the fixture's own OAuth path to seed the blob: exchange the
        // federated token for an access token, then PUT a BlockBlob with that bearer. Using the
        // same code path the cluster uses keeps the test honest — if the OAuth fixture is broken,
        // seeding fails at @BeforeClass instead of producing a confusing test failure later.
        String bearer = fetchWorkloadIdentityBearer();
        putBlockBlob(bearer, OBJECT_KEY, NDJSON_CONTENT);
    }

    public void testAksWorkloadIdentityAuthQueryReturnsRows() throws IOException {
        putWorkloadIdentityDataSource(DATASOURCE_NAME, fixture.getAddress());
        putDataset(DATASET_NAME, DATASOURCE_NAME, "wasbs://" + ACCOUNT + ".blob.core.windows.net/" + CONTAINER + "/" + OBJECT_KEY);

        Map<String, Object> result = runEsql("FROM " + DATASET_NAME + " | STATS count = COUNT(*) | LIMIT 1");
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat("AKS workload identity query must return at least one stats row", values, hasSize(greaterThanOrEqualTo(1)));
        Number count = (Number) values.get(0).get(0);
        assertThat("AKS workload identity query must count both seeded NDJSON rows", count.intValue(), equalTo(2));
    }

    public void testAksWorkloadIdentityAuthRejectedWhenClusterSettingDisabled() throws IOException {
        try {
            setWorkloadIdentityCredentialsEnabled(false);
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> putWorkloadIdentityDataSource(DATASOURCE_NAME + "_disabled", fixture.getAddress())
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(
                org.apache.http.util.EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("esql.datasource.workload_identity.enabled")
            );
        } finally {
            setWorkloadIdentityCredentialsEnabled(true);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Blob seeding helpers
    // -----------------------------------------------------------------------------------------

    private static final Pattern ACCESS_TOKEN_PATTERN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

    @SuppressForbidden(reason = "test seeds a blob through the Azure fixture's loopback HTTPS endpoint")
    private static String fetchWorkloadIdentityBearer() throws Exception {
        URI tokenUri = URI.create(fixture.getOAuthTokenServiceAddress() + TENANT_ID + "/oauth2/v2.0/token");
        HttpsURLConnection conn = (HttpsURLConnection) tokenUri.toURL().openConnection();
        conn.setSSLSocketFactory(testSslContext().getSocketFactory());
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        String form = "client_id="
            + URLEncoder.encode(CLIENT_ID, StandardCharsets.UTF_8)
            + "&client_assertion="
            + URLEncoder.encode(fixture.getFederatedToken(), StandardCharsets.UTF_8)
            + "&client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
            + "&grant_type=client_credentials"
            + "&scope="
            + URLEncoder.encode("https://storage.azure.com/.default", StandardCharsets.UTF_8);
        try (OutputStream out = conn.getOutputStream()) {
            out.write(form.getBytes(StandardCharsets.UTF_8));
        }
        try (InputStream in = conn.getInputStream()) {
            String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Matcher m = ACCESS_TOKEN_PATTERN.matcher(body);
            assertTrue("OAuth fixture response missing access_token: " + body, m.find());
            return m.group(1);
        } finally {
            conn.disconnect();
        }
    }

    @SuppressForbidden(reason = "test seeds a blob through the Azure fixture's loopback HTTPS endpoint")
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

    private static void putWorkloadIdentityDataSource(String name, String endpoint) throws IOException {
        Request req = new Request("PUT", "/_query/data_source/" + name);
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject()
                .field("type", "azure")
                .startObject("settings")
                .field("auth", "workload_identity")
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

    private static void setWorkloadIdentityCredentialsEnabled(boolean enabled) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        try (XContentBuilder b = jsonBuilder()) {
            b.startObject().startObject("persistent").field("esql.datasource.workload_identity.enabled", enabled).endObject().endObject();
            req.setJsonEntity(Strings.toString(b));
        }
        Response r = client().performRequest(req);
        assertThat(r.getStatusLine().getStatusCode(), equalTo(200));
    }
}
