/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageServiceTests extends ESTestCase {

    public void testClientInitializer() throws Exception {
        final String clientName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final TimeValue connectTimeValue = TimeValue.timeValueNanos(randomIntBetween(0, 2000000));
        final TimeValue readTimeValue = TimeValue.timeValueNanos(randomIntBetween(0, 2000000));
        final String applicationName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final String endpoint = randomFrom("http://", "https://")
            + randomFrom("www.elastic.co", "www.googleapis.com", "localhost/api", "google.com/oauth")
            + ":"
            + randomIntBetween(1, 65535);
        final String projectIdName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final int maxRetries = randomIntBetween(0, 10);
        final Settings settings = Settings.builder()
            .put(
                GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                connectTimeValue.getStringRep()
            )
            .put(
                GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                readTimeValue.getStringRep()
            )
            .put(
                GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                applicationName
            )
            .put(GoogleCloudStorageClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint)
            .put(GoogleCloudStorageClientSettings.PROJECT_ID_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectIdName)
            .put(GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "HTTP")
            .put(GoogleCloudStorageClientSettings.PROXY_HOST_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "192.168.52.15")
            .put(GoogleCloudStorageClientSettings.PROXY_PORT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), 8080)
            .put(GoogleCloudStorageClientSettings.MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries)
            .build();
        SetOnce<Proxy> proxy = new SetOnce<>();
        final var clusterService = ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool());
        final GoogleCloudStorageService service = new GoogleCloudStorageService(clusterService, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
            @Override
            void notifyProxyIsSet(Proxy p) {
                proxy.set(p);
            }
        };
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));
        var statsCollector = new GcsRepositoryStatsCollector();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.client(projectIdForClusterClient(), "another_client", "repo", statsCollector)
        );
        assertThat(e.getMessage(), Matchers.startsWith("Unknown client name"));
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName) }
        );
        final var storage = service.client(projectIdForClusterClient(), clientName, "repo", statsCollector);
        assertThat(storage.getOptions().getApplicationName(), Matchers.containsString(applicationName));
        assertThat(storage.getOptions().getHost(), Matchers.is(endpoint));
        assertThat(storage.getOptions().getProjectId(), Matchers.is(projectIdName));
        assertThat(storage.getOptions().getTransportOptions(), Matchers.instanceOf(HttpTransportOptions.class));
        assertThat(storage.getOptions().getRetrySettings().getMaxAttempts(), equalTo(maxRetries + 1));
        assertThat(
            ((HttpTransportOptions) storage.getOptions().getTransportOptions()).getConnectTimeout(),
            Matchers.is((int) connectTimeValue.millis())
        );
        assertThat(
            ((HttpTransportOptions) storage.getOptions().getTransportOptions()).getReadTimeout(),
            Matchers.is((int) readTimeValue.millis())
        );
        assertThat(proxy.get().toString(), equalTo("HTTP @ /192.168.52.15:8080"));
    }

    public void testReinitClientSettings() throws Exception {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setFile("gcs.client.gcs1.credentials_file", serviceAccountFileContent("project_gcs11"));
        secureSettings1.setFile("gcs.client.gcs2.credentials_file", serviceAccountFileContent("project_gcs12"));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setFile("gcs.client.gcs1.credentials_file", serviceAccountFileContent("project_gcs21"));
        secureSettings2.setFile("gcs.client.gcs3.credentials_file", serviceAccountFileContent("project_gcs23"));
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        final var pluginServices = mock(Plugin.PluginServices.class);
        when(pluginServices.clusterService()).thenReturn(
            ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool())
        );
        when(pluginServices.projectResolver()).thenReturn(TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings1)) {
            plugin.createComponents(pluginServices);
            final GoogleCloudStorageService storageService = plugin.storageService.get();
            var statsCollector = new GcsRepositoryStatsCollector();
            final var client11 = storageService.client(projectIdForClusterClient(), "gcs1", "repo1", statsCollector);
            assertThat(client11.getOptions().getProjectId(), equalTo("project_gcs11"));
            final var client12 = storageService.client(projectIdForClusterClient(), "gcs2", "repo2", statsCollector);
            assertThat(client12.getOptions().getProjectId(), equalTo("project_gcs12"));
            // client 3 is missing
            final IllegalArgumentException e1 = expectThrows(
                IllegalArgumentException.class,
                () -> storageService.client(projectIdForClusterClient(), "gcs3", "repo3", statsCollector)
            );
            assertThat(e1.getMessage(), containsString("Unknown client name [gcs3]."));
            // update client settings
            plugin.reload(settings2);
            // old client 1 not changed
            assertThat(client11.getOptions().getProjectId(), equalTo("project_gcs11"));
            // new client 1 is changed
            final var client21 = storageService.client(projectIdForClusterClient(), "gcs1", "repo1", statsCollector);
            assertThat(client21.getOptions().getProjectId(), equalTo("project_gcs21"));
            // old client 2 not changed
            assertThat(client12.getOptions().getProjectId(), equalTo("project_gcs12"));
            // new client2 is gone
            final IllegalArgumentException e2 = expectThrows(
                IllegalArgumentException.class,
                () -> storageService.client(projectIdForClusterClient(), "gcs2", "repo2", statsCollector)
            );
            assertThat(e2.getMessage(), containsString("Unknown client name [gcs2]."));
            // client 3 emerged
            final var client23 = storageService.client(projectIdForClusterClient(), "gcs3", "repo3", statsCollector);
            assertThat(client23.getOptions().getProjectId(), equalTo("project_gcs23"));
        }
    }

    public void testClientsAreNotSharedAcrossRepositories() throws Exception {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setFile("gcs.client.gcs1.credentials_file", serviceAccountFileContent("test_project"));
        final Settings settings = Settings.builder().setSecureSettings(secureSettings1).build();
        final var pluginServices = mock(Plugin.PluginServices.class);
        when(pluginServices.clusterService()).thenReturn(
            ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool())
        );
        when(pluginServices.projectResolver()).thenReturn(TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings)) {
            plugin.createComponents(pluginServices);
            final GoogleCloudStorageService storageService = plugin.storageService.get();

            final MeteredStorage repo1Client = storageService.client(
                projectIdForClusterClient(),
                "gcs1",
                "repo1",
                new GcsRepositoryStatsCollector()
            );
            final MeteredStorage repo2Client = storageService.client(
                projectIdForClusterClient(),
                "gcs1",
                "repo2",
                new GcsRepositoryStatsCollector()
            );
            final MeteredStorage repo1ClientSecondInstance = storageService.client(
                projectIdForClusterClient(),
                "gcs1",
                "repo1",
                new GcsRepositoryStatsCollector()
            );

            assertNotSame(repo1Client, repo2Client);
            assertSame(repo1Client, repo1ClientSecondInstance);
        }
    }

    /**
     * Verifies that when no explicit credentials are configured and no credential files are found
     * (either because they genuinely don't exist, or because {@code File.isFile()} returns
     * {@code false} due to entitlement enforcement), the GCS service correctly falls back to
     * obtaining credentials from the GCP metadata server via Application Default Credentials.
     *
     * <p>This test exercises the code path that was broken when {@code NotEntitledException}
     * stopped extending {@code AccessControlException}: {@code DefaultCredentialsProvider}
     * explicitly catches {@code AccessControlException} when checking credential file paths,
     * silently skipping to the metadata server fallback. When {@code File.isFile()} threw a
     * {@code NotEntitledException} that no longer extended {@code AccessControlException}, the
     * exception propagated instead of being swallowed, preventing credential discovery entirely.
     *
     * <p>The fix — returning {@code false} from {@code File.isFile()} when access is denied —
     * causes {@code DefaultCredentialsProvider} to treat the credential file as absent and
     * naturally fall through to the metadata server. This test documents and protects that path.
     */
    public void testApplicationDefaultCredentialsFallbackToComputeEngine() throws IOException {
        final AtomicBoolean metadataServerContacted = new AtomicBoolean(false);

        // Mock HTTP transport that simulates the GCP metadata server.
        // DefaultCredentialsProvider uses this transport to detect GCE and obtain tokens
        // when no file-based credentials are found.
        final HttpTransportFactory mockMetadataTransportFactory = () -> new HttpTransport() {
            @Override
            protected LowLevelHttpRequest buildRequest(String method, String url) {
                metadataServerContacted.set(true);
                final byte[] responseBody = url.contains("token")
                    ? "{\"access_token\":\"test-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}".getBytes(UTF_8)
                    : new byte[0];
                return new LowLevelHttpRequest() {
                    @Override
                    public void addHeader(String name, String value) {}

                    @Override
                    public LowLevelHttpResponse execute() {
                        return new LowLevelHttpResponse() {
                            @Override
                            public InputStream getContent() {
                                return new ByteArrayInputStream(responseBody);
                            }

                            @Override
                            public String getContentEncoding() {
                                return null;
                            }

                            @Override
                            public long getContentLength() {
                                return responseBody.length;
                            }

                            @Override
                            public String getContentType() {
                                return "application/json";
                            }

                            @Override
                            public String getStatusLine() {
                                return "HTTP/1.1 200 OK";
                            }

                            @Override
                            public int getStatusCode() {
                                return 200;
                            }

                            @Override
                            public String getReasonPhrase() {
                                return "OK";
                            }

                            @Override
                            public int getHeaderCount() {
                                return 1;
                            }

                            @Override
                            public String getHeaderName(int index) {
                                return "Metadata-Flavor";
                            }

                            @Override
                            public String getHeaderValue(int index) {
                                return "Google";
                            }
                        };
                    }
                };
            }
        };

        final var clusterService = ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool());

        // Override createStorageOptions to use our mock transport for Application Default
        // Credentials, simulating what happens on a GCP VM when no credentials_file is set.
        final GoogleCloudStorageService service = new GoogleCloudStorageService(clusterService, TestProjectResolvers.DEFAULT_PROJECT_ONLY) {
            @Override
            StorageOptions createStorageOptions(
                GoogleCloudStorageClientSettings gcsClientSettings,
                HttpTransportOptions httpTransportOptions
            ) {
                assertNull("Test expects no explicit credential to be configured", gcsClientSettings.getCredential());
                try {
                    return StorageOptions.newBuilder()
                        .setCredentials(GoogleCredentials.getApplicationDefault(mockMetadataTransportFactory))
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to obtain Application Default Credentials from mock metadata server", e);
                }
            }
        };

        final Settings settings = Settings.builder()
            .put(GoogleCloudStorageClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace("default").getKey(), "http://unused")
            .build();
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));

        final var storage = service.client(projectIdForClusterClient(), "default", "repo", new GcsRepositoryStatsCollector());
        assertThat(storage.getOptions().getCredentials(), notNullValue());
        assertTrue(
            "GCP metadata server should have been contacted to obtain Application Default Credentials",
            metadataServerContacted.get()
        );
    }

    private byte[] serviceAccountFileContent(String projectId) throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final String encodedKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        final XContentBuilder serviceAccountBuilder = jsonBuilder().startObject()
            .field("type", "service_account")
            .field("project_id", projectId)
            .field("private_key_id", UUID.randomUUID().toString())
            .field("private_key", "-----BEGIN PRIVATE KEY-----\n" + encodedKey + "\n-----END PRIVATE KEY-----\n")
            .field("client_email", "integration_test@appspot.gserviceaccount.com")
            .field("client_id", "client_id")
            .endObject();
        return BytesReference.toBytes(BytesReference.bytes(serviceAccountBuilder));
    }

    public void testToTimeout() {
        assertEquals(-1, GoogleCloudStorageService.toTimeout(null).intValue());
        assertEquals(-1, GoogleCloudStorageService.toTimeout(TimeValue.ZERO).intValue());
        assertEquals(0, GoogleCloudStorageService.toTimeout(TimeValue.MINUS_ONE).intValue());
    }

    public void testGetDefaultProjectIdViaProxy() throws Exception {
        String proxyProjectId = randomAlphaOfLength(16);
        var proxyServer = new MockHttpProxyServer() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) {
                assertEquals(
                    "GET http://metadata.google.internal/computeMetadata/v1/project/project-id HTTP/1.1",
                    request.getRequestLine().toString()
                );
                response.setEntity(new StringEntity(proxyProjectId, ContentType.TEXT_PLAIN));
            }
        };
        try (proxyServer) {
            var proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(InetAddress.getLoopbackAddress(), proxyServer.getPort()));
            assertEquals(proxyProjectId, GoogleCloudStorageService.getDefaultProjectId(proxy));
        }
    }

    private ProjectId projectIdForClusterClient() {
        return randomBoolean() ? ProjectId.DEFAULT : null;
    }
}
