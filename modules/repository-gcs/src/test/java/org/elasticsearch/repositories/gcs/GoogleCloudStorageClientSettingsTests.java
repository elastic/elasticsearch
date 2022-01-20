/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROJECT_ID_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_HOST_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_PORT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROXY_TYPE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.getClientSettings;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.loadCredential;

public class GoogleCloudStorageClientSettingsTests extends ESTestCase {

    public void testLoadWithEmptySettings() {
        final Map<String, GoogleCloudStorageClientSettings> clientsSettings = GoogleCloudStorageClientSettings.load(Settings.EMPTY);
        assertEquals(1, clientsSettings.size());
        assertNotNull(clientsSettings.get("default"));
    }

    public void testLoad() throws Exception {
        final int nbClients = randomIntBetween(1, 5);
        final List<Setting<?>> deprecationWarnings = new ArrayList<>();
        final Tuple<Map<String, GoogleCloudStorageClientSettings>, Settings> randomClients = randomClients(nbClients, deprecationWarnings);
        final Map<String, GoogleCloudStorageClientSettings> expectedClientsSettings = randomClients.v1();

        final Map<String, GoogleCloudStorageClientSettings> actualClientsSettings = GoogleCloudStorageClientSettings.load(
            randomClients.v2()
        );
        assertEquals(expectedClientsSettings.size(), actualClientsSettings.size());

        for (final String clientName : expectedClientsSettings.keySet()) {
            final GoogleCloudStorageClientSettings actualClientSettings = actualClientsSettings.get(clientName);
            assertNotNull(actualClientSettings);
            final GoogleCloudStorageClientSettings expectedClientSettings = expectedClientsSettings.get(clientName);
            assertNotNull(expectedClientSettings);
            assertGoogleCredential(expectedClientSettings.getCredential(), actualClientSettings.getCredential());
            assertEquals(expectedClientSettings.getHost(), actualClientSettings.getHost());
            assertEquals(expectedClientSettings.getProjectId(), actualClientSettings.getProjectId());
            assertEquals(expectedClientSettings.getConnectTimeout(), actualClientSettings.getConnectTimeout());
            assertEquals(expectedClientSettings.getReadTimeout(), actualClientSettings.getReadTimeout());
            assertEquals(expectedClientSettings.getApplicationName(), actualClientSettings.getApplicationName());
        }

        if (deprecationWarnings.isEmpty() == false) {
            assertSettingDeprecationsAndWarnings(deprecationWarnings.toArray(new Setting<?>[0]));
        }
    }

    public void testLoadCredential() throws Exception {
        final List<Setting<?>> deprecationWarnings = new ArrayList<>();
        final Tuple<Map<String, GoogleCloudStorageClientSettings>, Settings> randomClient = randomClients(1, deprecationWarnings);
        final GoogleCloudStorageClientSettings expectedClientSettings = randomClient.v1().values().iterator().next();
        final String clientName = randomClient.v1().keySet().iterator().next();
        assertGoogleCredential(expectedClientSettings.getCredential(), loadCredential(randomClient.v2(), clientName));
    }

    public void testProjectIdDefaultsToCredentials() throws Exception {
        final String clientName = randomAlphaOfLength(5);
        final Tuple<ServiceAccountCredentials, byte[]> credentials = randomCredential(clientName);
        final ServiceAccountCredentials credential = credentials.v1();
        final GoogleCloudStorageClientSettings googleCloudStorageClientSettings = new GoogleCloudStorageClientSettings(
            credential,
            ENDPOINT_SETTING.getDefault(Settings.EMPTY),
            PROJECT_ID_SETTING.getDefault(Settings.EMPTY),
            CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY),
            new URI(""),
            PROXY_TYPE_SETTING.getDefault(Settings.EMPTY),
            PROXY_HOST_SETTING.getDefault(Settings.EMPTY),
            PROXY_PORT_SETTING.getDefault(Settings.EMPTY)
        );
        assertEquals(credential.getProjectId(), googleCloudStorageClientSettings.getProjectId());
    }

    public void testLoadsProxySettings() throws Exception {
        final String clientName = randomAlphaOfLength(5);
        final ServiceAccountCredentials credential = randomCredential(clientName).v1();
        final GoogleCloudStorageClientSettings googleCloudStorageClientSettings = new GoogleCloudStorageClientSettings(
            credential,
            ENDPOINT_SETTING.getDefault(Settings.EMPTY),
            PROJECT_ID_SETTING.getDefault(Settings.EMPTY),
            CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY),
            new URI(""),
            Proxy.Type.HTTP,
            "192.168.15.1",
            8080
        );
        assertEquals(
            new Proxy(Proxy.Type.HTTP, new InetSocketAddress(InetAddress.getByName("192.168.15.1"), 8080)),
            googleCloudStorageClientSettings.getProxy()
        );
    }

    /** Generates a given number of GoogleCloudStorageClientSettings along with the Settings to build them from **/
    private Tuple<Map<String, GoogleCloudStorageClientSettings>, Settings> randomClients(
        final int nbClients,
        final List<Setting<?>> deprecationWarnings
    ) throws Exception {
        final Map<String, GoogleCloudStorageClientSettings> expectedClients = new HashMap<>();

        final Settings.Builder settings = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();

        for (int i = 0; i < nbClients; i++) {
            final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            final GoogleCloudStorageClientSettings clientSettings = randomClient(clientName, settings, secureSettings, deprecationWarnings);
            expectedClients.put(clientName, clientSettings);
        }

        if (randomBoolean()) {
            final GoogleCloudStorageClientSettings clientSettings = randomClient("default", settings, secureSettings, deprecationWarnings);
            expectedClients.put("default", clientSettings);
        } else {
            expectedClients.put("default", getClientSettings(Settings.EMPTY, "default"));
        }

        return Tuple.tuple(expectedClients, settings.setSecureSettings(secureSettings).build());
    }

    /** Generates a random GoogleCloudStorageClientSettings along with the Settings to build it **/
    private static GoogleCloudStorageClientSettings randomClient(
        final String clientName,
        final Settings.Builder settings,
        final MockSecureSettings secureSettings,
        final List<Setting<?>> deprecationWarnings
    ) throws Exception {

        final Tuple<ServiceAccountCredentials, byte[]> credentials = randomCredential(clientName);
        final ServiceAccountCredentials credential = credentials.v1();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(), credentials.v2());

        String endpoint;
        if (randomBoolean()) {
            endpoint = randomFrom(
                "http://www.elastic.co",
                "http://metadata.google.com:88/oauth",
                "https://www.googleapis.com",
                "https://www.elastic.co:443",
                "http://localhost:8443",
                "https://www.googleapis.com/oauth/token"
            );
            settings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        } else {
            endpoint = ENDPOINT_SETTING.getDefault(Settings.EMPTY);
        }

        String projectId;
        if (randomBoolean()) {
            projectId = randomAlphaOfLength(5);
            settings.put(PROJECT_ID_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectId);
        } else {
            projectId = PROJECT_ID_SETTING.getDefault(Settings.EMPTY);
        }

        TimeValue connectTimeout;
        if (randomBoolean()) {
            connectTimeout = randomTimeout();
            settings.put(CONNECT_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), connectTimeout.getStringRep());
        } else {
            connectTimeout = CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY);
        }

        TimeValue readTimeout;
        if (randomBoolean()) {
            readTimeout = randomTimeout();
            settings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout.getStringRep());
        } else {
            readTimeout = READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY);
        }

        String applicationName;
        if (randomBoolean()) {
            applicationName = randomAlphaOfLength(5);
            settings.put(APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName).getKey(), applicationName);
            deprecationWarnings.add(APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName));
        } else {
            applicationName = APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY);
        }

        return new GoogleCloudStorageClientSettings(
            credential,
            endpoint,
            projectId,
            connectTimeout,
            readTimeout,
            applicationName,
            new URI(""),
            PROXY_TYPE_SETTING.getDefault(Settings.EMPTY),
            PROXY_HOST_SETTING.getDefault(Settings.EMPTY),
            PROXY_PORT_SETTING.getDefault(Settings.EMPTY)
        );
    }

    /** Generates a random GoogleCredential along with its corresponding Service Account file provided as a byte array **/
    private static Tuple<ServiceAccountCredentials, byte[]> randomCredential(final String clientName) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final ServiceAccountCredentials.Builder credentialBuilder = ServiceAccountCredentials.newBuilder();
        credentialBuilder.setClientId("id_" + clientName);
        credentialBuilder.setClientEmail(clientName);
        credentialBuilder.setProjectId("project_id_" + clientName);
        credentialBuilder.setPrivateKey(keyPair.getPrivate());
        credentialBuilder.setPrivateKeyId("private_key_id_" + clientName);
        credentialBuilder.setScopes(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL));
        final String encodedPrivateKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        final String serviceAccount = """
            {
              "type": "service_account",
              "project_id": "project_id_%s",
              "private_key_id": "private_key_id_%s",
              "private_key": "-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n",
              "client_email": "%s",
              "client_id": "id_%s"
            }""".formatted(clientName, clientName, encodedPrivateKey, clientName, clientName);
        return Tuple.tuple(credentialBuilder.build(), serviceAccount.getBytes(StandardCharsets.UTF_8));
    }

    private static TimeValue randomTimeout() {
        return randomFrom(TimeValue.MINUS_ONE, TimeValue.ZERO, TimeValue.parseTimeValue(randomPositiveTimeValue(), "test"));
    }

    private static void assertGoogleCredential(ServiceAccountCredentials expected, ServiceAccountCredentials actual) {
        if (expected != null) {
            assertEquals(expected.getServiceAccountUser(), actual.getServiceAccountUser());
            assertEquals(expected.getClientId(), actual.getClientId());
            assertEquals(expected.getClientEmail(), actual.getClientEmail());
            assertEquals(expected.getAccount(), actual.getAccount());
            assertEquals(expected.getProjectId(), actual.getProjectId());
            assertEquals(expected.getScopes(), actual.getScopes());
            assertEquals(expected.getPrivateKey(), actual.getPrivateKey());
            assertEquals(expected.getPrivateKeyId(), actual.getPrivateKeyId());
        } else {
            assertNull(actual);
        }
    }
}
