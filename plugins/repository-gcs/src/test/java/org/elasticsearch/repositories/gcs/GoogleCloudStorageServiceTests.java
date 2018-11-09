/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.gcs;

import com.google.auth.Credentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;

public class GoogleCloudStorageServiceTests extends ESTestCase {

    public void testClientInitializer() throws Exception {
        final String clientName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final TimeValue connectTimeValue = TimeValue.timeValueNanos(randomIntBetween(0, 2000000));
        final TimeValue readTimeValue = TimeValue.timeValueNanos(randomIntBetween(0, 2000000));
        final String applicationName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final String endpoint = randomFrom("http://", "https://")
                + randomFrom("www.elastic.co", "www.googleapis.com", "localhost/api", "google.com/oauth")
                + ":" + randomIntBetween(1, 65535);
        final String projectIdName = randomAlphaOfLength(randomIntBetween(1, 10)).toLowerCase(Locale.ROOT);
        final Settings settings = Settings.builder()
                .put(GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        connectTimeValue.getStringRep())
                .put(GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        readTimeValue.getStringRep())
                .put(GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        applicationName)
                .put(GoogleCloudStorageClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint)
                .put(GoogleCloudStorageClientSettings.PROJECT_ID_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectIdName)
                .build();
        final GoogleCloudStorageService service = new GoogleCloudStorageService();
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.client("another_client"));
        assertThat(e.getMessage(), Matchers.startsWith("Unknown client name"));
        assertSettingDeprecationsAndWarnings(
                new Setting<?>[] { GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName) });
        final Storage storage = service.client(clientName);
        assertThat(storage.getOptions().getApplicationName(), Matchers.containsString(applicationName));
        assertThat(storage.getOptions().getHost(), Matchers.is(endpoint));
        assertThat(storage.getOptions().getProjectId(), Matchers.is(projectIdName));
        assertThat(storage.getOptions().getTransportOptions(), Matchers.instanceOf(HttpTransportOptions.class));
        assertThat(((HttpTransportOptions) storage.getOptions().getTransportOptions()).getConnectTimeout(),
                Matchers.is((int) connectTimeValue.millis()));
        assertThat(((HttpTransportOptions) storage.getOptions().getTransportOptions()).getReadTimeout(),
                Matchers.is((int) readTimeValue.millis()));
        assertThat(storage.getOptions().getCredentials(), Matchers.nullValue(Credentials.class));
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
            final Storage client11 = storageService.client("gcs1");
            assertThat(client11.getOptions().getProjectId(), equalTo("project_gcs11"));
            final Storage client12 = storageService.client("gcs2");
            assertThat(client12.getOptions().getProjectId(), equalTo("project_gcs12"));
            // client 3 is missing
            final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> storageService.client("gcs3"));
            assertThat(e1.getMessage(), containsString("Unknown client name [gcs3]."));
            // update client settings
            plugin.reload(settings2);
            // old client 1 not changed
            assertThat(client11.getOptions().getProjectId(), equalTo("project_gcs11"));
            // new client 1 is changed
            final Storage client21 = storageService.client("gcs1");
            assertThat(client21.getOptions().getProjectId(), equalTo("project_gcs21"));
            // old client 2 not changed
            assertThat(client12.getOptions().getProjectId(), equalTo("project_gcs12"));
            // new client2 is gone
            final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> storageService.client("gcs2"));
            assertThat(e2.getMessage(), containsString("Unknown client name [gcs2]."));
            // client 3 emerged
            final Storage client23 = storageService.client("gcs3");
            assertThat(client23.getOptions().getProjectId(), equalTo("project_gcs23"));
        }
    }

    private byte[] serviceAccountFileContent(String projectId) throws Exception {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(1024);
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
}
