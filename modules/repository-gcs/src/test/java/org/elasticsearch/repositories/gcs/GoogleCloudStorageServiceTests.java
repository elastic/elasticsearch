/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
            .build();
        SetOnce<Proxy> proxy = new SetOnce<>();
        final GoogleCloudStorageService service = new GoogleCloudStorageService() {
            @Override
            void notifyProxyIsSet(Proxy p) {
                proxy.set(p);
            }
        };
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));
        GoogleCloudStorageOperationsStats statsCollector = new GoogleCloudStorageOperationsStats("bucket");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.client("another_client", "repo", statsCollector)
        );
        assertThat(e.getMessage(), Matchers.startsWith("Unknown client name"));
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName) }
        );
        final Storage storage = service.client(clientName, "repo", statsCollector);
        assertThat(storage.getOptions().getApplicationName(), Matchers.containsString(applicationName));
        assertThat(storage.getOptions().getHost(), Matchers.is(endpoint));
        assertThat(storage.getOptions().getProjectId(), Matchers.is(projectIdName));
        assertThat(storage.getOptions().getTransportOptions(), Matchers.instanceOf(HttpTransportOptions.class));
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
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings1)) {
            final GoogleCloudStorageService storageService = plugin.storageService;
            GoogleCloudStorageOperationsStats statsCollector = new GoogleCloudStorageOperationsStats("bucket");
            final Storage client11 = storageService.client("gcs1", "repo1", statsCollector);
            assertThat(client11.getOptions().getProjectId(), equalTo("project_gcs11"));
            final Storage client12 = storageService.client("gcs2", "repo2", statsCollector);
            assertThat(client12.getOptions().getProjectId(), equalTo("project_gcs12"));
            // client 3 is missing
            final IllegalArgumentException e1 = expectThrows(
                IllegalArgumentException.class,
                () -> storageService.client("gcs3", "repo3", statsCollector)
            );
            assertThat(e1.getMessage(), containsString("Unknown client name [gcs3]."));
            // update client settings
            plugin.reload(settings2);
            // old client 1 not changed
            assertThat(client11.getOptions().getProjectId(), equalTo("project_gcs11"));
            // new client 1 is changed
            final Storage client21 = storageService.client("gcs1", "repo1", statsCollector);
            assertThat(client21.getOptions().getProjectId(), equalTo("project_gcs21"));
            // old client 2 not changed
            assertThat(client12.getOptions().getProjectId(), equalTo("project_gcs12"));
            // new client2 is gone
            final IllegalArgumentException e2 = expectThrows(
                IllegalArgumentException.class,
                () -> storageService.client("gcs2", "repo2", statsCollector)
            );
            assertThat(e2.getMessage(), containsString("Unknown client name [gcs2]."));
            // client 3 emerged
            final Storage client23 = storageService.client("gcs3", "repo3", statsCollector);
            assertThat(client23.getOptions().getProjectId(), equalTo("project_gcs23"));
        }
    }

    public void testClientsAreNotSharedAcrossRepositories() throws Exception {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setFile("gcs.client.gcs1.credentials_file", serviceAccountFileContent("test_project"));
        final Settings settings = Settings.builder().setSecureSettings(secureSettings1).build();
        try (GoogleCloudStoragePlugin plugin = new GoogleCloudStoragePlugin(settings)) {
            final GoogleCloudStorageService storageService = plugin.storageService;

            final Storage repo1Client = storageService.client("gcs1", "repo1", new GoogleCloudStorageOperationsStats("bucket"));
            final Storage repo2Client = storageService.client("gcs1", "repo2", new GoogleCloudStorageOperationsStats("bucket"));
            final Storage repo1ClientSecondInstance = storageService.client(
                "gcs1",
                "repo1",
                new GoogleCloudStorageOperationsStats("bucket")
            );

            assertNotSame(repo1Client, repo2Client);
            assertSame(repo1Client, repo1ClientSecondInstance);
        }
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
            assertEquals(proxyProjectId, SocketAccess.doPrivilegedIOException(() -> GoogleCloudStorageService.getDefaultProjectId(proxy)));
        }
    }
}
