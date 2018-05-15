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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import java.util.Collections;
import java.util.Locale;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GoogleCloudStorageServiceTests extends ESTestCase {

    public void testClientInitializer() throws Exception {
        final String clientName = randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        final Environment environment = mock(Environment.class);
        final TimeValue connectTimeValue = TimeValue.timeValueNanos(randomIntBetween(0, 2000000));
        final TimeValue readTimeValue = TimeValue.timeValueNanos(randomIntBetween(0, 2000000));
        final String applicationName = randomAlphaOfLength(4);
        final String hostName = randomFrom("http://", "https://") + randomAlphaOfLength(4) + ":" + randomIntBetween(1, 65535);
        final String projectIdName = randomAlphaOfLength(4);
        final Settings settings = Settings.builder()
                .put(GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        connectTimeValue.getStringRep())
                .put(GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        readTimeValue.getStringRep())
                .put(GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                        applicationName)
                .put(GoogleCloudStorageClientSettings.ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), hostName)
                .put(GoogleCloudStorageClientSettings.PROJECT_ID_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectIdName)
                .build();
        when(environment.settings()).thenReturn(settings);
        final GoogleCloudStorageClientSettings clientSettings = GoogleCloudStorageClientSettings.getClientSettings(settings, clientName);
        final GoogleCloudStorageService service = new GoogleCloudStorageService(environment,
                Collections.singletonMap(clientName, clientSettings));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.createClient("another_client"));
        assertThat(e.getMessage(), Matchers.startsWith("Unknown client name"));
        assertSettingDeprecationsAndWarnings(
                new Setting<?>[] { GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName) });
        final Storage storage = service.createClient(clientName);
        assertThat(storage.getOptions().getApplicationName(), Matchers.containsString(applicationName));
        assertThat(storage.getOptions().getHost(), Matchers.is(hostName));
        assertThat(storage.getOptions().getProjectId(), Matchers.is(projectIdName));
        assertThat(storage.getOptions().getTransportOptions(), Matchers.instanceOf(HttpTransportOptions.class));
        assertThat(((HttpTransportOptions) storage.getOptions().getTransportOptions()).getConnectTimeout(),
                Matchers.is((int) connectTimeValue.millis()));
        assertThat(((HttpTransportOptions) storage.getOptions().getTransportOptions()).getReadTimeout(),
                Matchers.is((int) readTimeValue.millis()));
        assertThat(storage.getOptions().getCredentials(), Matchers.nullValue(Credentials.class));
    }

    public void testToTimeout() {
        assertEquals(-1, GoogleCloudStorageService.toTimeout(null).intValue());
        assertEquals(-1, GoogleCloudStorageService.toTimeout(TimeValue.ZERO).intValue());
        assertEquals(0, GoogleCloudStorageService.toTimeout(TimeValue.MINUS_ONE).intValue());
    }
}
