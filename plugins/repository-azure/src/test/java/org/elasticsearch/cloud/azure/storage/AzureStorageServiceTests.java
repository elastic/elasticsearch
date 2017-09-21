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

package org.elasticsearch.cloud.azure.storage;

import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.core.Base64;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.cloud.azure.storage.AzureStorageServiceImpl.blobNameFromUri;
import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.DEPRECATED_ACCOUNT_SETTING;
import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.DEPRECATED_DEFAULT_SETTING;
import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.DEPRECATED_KEY_SETTING;
import static org.elasticsearch.cloud.azure.storage.AzureStorageSettings.DEPRECATED_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.azure.AzureSettingsParserTests.getConcreteSetting;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AzureStorageServiceTests extends ESTestCase {

    @Deprecated
    static final Settings deprecatedSettings = Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .put("cloud.azure.storage.azure1.default", true)
            .put("cloud.azure.storage.azure2.account", "myaccount2")
            .put("cloud.azure.storage.azure2.key", "mykey2")
            .put("cloud.azure.storage.azure3.account", "myaccount3")
            .put("cloud.azure.storage.azure3.key", "mykey3")
            .put("cloud.azure.storage.azure3.timeout", "30s")
            .build();

    private MockSecureSettings buildSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", "mykey1");
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", "mykey2");
        secureSettings.setString("azure.client.azure3.account", "myaccount3");
        secureSettings.setString("azure.client.azure3.key", "mykey3");
        return secureSettings;
    }
    private Settings buildSettings() {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .build();
        return settings;
    }

    public void testReadSecuredSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", "mykey1");
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", "mykey2");
        secureSettings.setString("azure.client.azure3.account", "myaccount3");
        secureSettings.setString("azure.client.azure3.key", "mykey3");
        Settings settings = Settings.builder().setSecureSettings(secureSettings)
            .put("azure.client.azure3.endpoint_suffix", "my_endpoint_suffix").build();

        Map<String, AzureStorageSettings> loadedSettings = AzureStorageSettings.load(settings);
        assertThat(loadedSettings.keySet(), containsInAnyOrder("azure1","azure2","azure3","default"));

        assertThat(loadedSettings.get("azure1").getEndpointSuffix(), isEmptyString());
        assertThat(loadedSettings.get("azure2").getEndpointSuffix(), isEmptyString());
        assertThat(loadedSettings.get("azure3").getEndpointSuffix(), equalTo("my_endpoint_suffix"));
    }

    public void testCreateClientWithEndpointSuffix() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", Base64.encode("mykey1".getBytes(StandardCharsets.UTF_8)));
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", Base64.encode("mykey2".getBytes(StandardCharsets.UTF_8)));
        Settings settings = Settings.builder().setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", "my_endpoint_suffix").build();
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceImpl(settings, AzureStorageSettings.load(settings));
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getEndpoint().toString(), equalTo("https://myaccount1.blob.my_endpoint_suffix"));

        CloudBlobClient client2 = azureStorageService.getSelectedClient("azure2", LocationMode.PRIMARY_ONLY);
        assertThat(client2.getEndpoint().toString(), equalTo("https://myaccount2.blob.core.windows.net"));
    }

    public void testGetSelectedClientWithNoPrimaryAndSecondary() {
        try {
            new AzureStorageServiceMock(Settings.EMPTY);
            fail("we should have raised an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("If you want to use an azure repository, you need to define a client configuration."));
        }
    }

    public void testGetSelectedClientNonExisting() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(buildSettings());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            azureStorageService.getSelectedClient("azure4", LocationMode.PRIMARY_ONLY);
        });
        assertThat(e.getMessage(), is("Can not find named azure client [azure4]. Check your elasticsearch.yml."));
    }

    public void testGetSelectedClientGlobalTimeout() {
        Settings timeoutSettings = Settings.builder()
                .setSecureSettings(buildSecureSettings())
                .put(AzureStorageService.Storage.TIMEOUT_SETTING.getKey(), "10s")
                .put("azure.client.azure3.timeout", "30s")
                .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(10 * 1000));
        CloudBlobClient client3 = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));

        assertSettingDeprecationsAndWarnings(new Setting<?>[]{AzureStorageService.Storage.TIMEOUT_SETTING});
    }

    public void testGetSelectedClientDefaultTimeout() {
        Settings timeoutSettings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.timeout", "30s")
            .build();
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), nullValue());
        CloudBlobClient client3 = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));
    }

    public void testGetSelectedClientNoTimeout() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(buildSettings());
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(nullValue()));
    }

    public void testGetSelectedClientBackoffPolicy() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(buildSettings());
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testGetSelectedClientBackoffPolicyNbRetries() {
        Settings timeoutSettings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.max_retries", 7)
            .build();

        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(timeoutSettings);
        CloudBlobClient client1 = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testNoProxy() {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .build();
        AzureStorageServiceMock mock = new AzureStorageServiceMock(settings);
        assertThat(mock.storageSettings.get("azure1").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure2").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testProxyHttp() throws UnknownHostException {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "http")
            .build();
        AzureStorageServiceMock mock = new AzureStorageServiceMock(settings);
        Proxy azure1Proxy = mock.storageSettings.get("azure1").getProxy();

        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.type(), is(Proxy.Type.HTTP));
        assertThat(azure1Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        assertThat(mock.storageSettings.get("azure2").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testMultipleProxies() throws UnknownHostException {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "http")
            .put("azure.client.azure2.proxy.host", "127.0.0.1")
            .put("azure.client.azure2.proxy.port", 8081)
            .put("azure.client.azure2.proxy.type", "http")
            .build();
        AzureStorageServiceMock mock = new AzureStorageServiceMock(settings);
        Proxy azure1Proxy = mock.storageSettings.get("azure1").getProxy();
        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.type(), is(Proxy.Type.HTTP));
        assertThat(azure1Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        Proxy azure2Proxy = mock.storageSettings.get("azure2").getProxy();
        assertThat(azure2Proxy, notNullValue());
        assertThat(azure2Proxy.type(), is(Proxy.Type.HTTP));
        assertThat(azure2Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8081)));
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testProxySocks() throws UnknownHostException {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "socks")
            .build();
        AzureStorageServiceMock mock = new AzureStorageServiceMock(settings);
        Proxy azure1Proxy = mock.storageSettings.get("azure1").getProxy();
        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.type(), is(Proxy.Type.SOCKS));
        assertThat(azure1Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        assertThat(mock.storageSettings.get("azure2").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testProxyNoHost() {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .build();

        SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMock(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoPort() {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .build();

        SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMock(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoType() {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMock(settings));
        assertEquals("Azure Proxy port or host have been set but proxy type is not defined.", e.getMessage());
    }

    public void testProxyWrongHost() {
        Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .put("azure.client.azure1.proxy.host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMock(settings));
        assertEquals("Azure proxy host is unknown.", e.getMessage());
    }

    /**
     * This internal class just overload createClient method which is called by AzureStorageServiceImpl.doStart()
     */
    class AzureStorageServiceMock extends AzureStorageServiceImpl {
        AzureStorageServiceMock(Settings settings) {
            super(settings, AzureStorageSettings.load(settings));
        }

        // We fake the client here
        @Override
        void createClient(AzureStorageSettings azureStorageSettings) {
            this.clients.put(azureStorageSettings.getAccount(),
                    new CloudBlobClient(URI.create("https://" + azureStorageSettings.getName())));
        }
    }

    public void testBlobNameFromUri() throws URISyntaxException {
        String name = blobNameFromUri(new URI("https://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("https://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
    }

    // Deprecated settings. We still test them until we remove definitely the deprecated settings

    @Deprecated
    public void testGetSelectedClientWithNoSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .build());
        CloudBlobClient client = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            getConcreteSetting(DEPRECATED_ACCOUNT_SETTING, "azure1"),
            getConcreteSetting(DEPRECATED_KEY_SETTING, "azure1")
        });
    }

    @Deprecated
    public void testGetDefaultClientWithNoSecondary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .build());
        CloudBlobClient client = azureStorageService.getSelectedClient("default", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            getConcreteSetting(DEPRECATED_ACCOUNT_SETTING, "azure1"),
            getConcreteSetting(DEPRECATED_KEY_SETTING, "azure1")
        });
    }

    @Deprecated
    public void testGetSelectedClientPrimary() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(deprecatedSettings);
        CloudBlobClient client = azureStorageService.getSelectedClient("azure1", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
        assertDeprecatedWarnings();
    }

    @Deprecated
    public void testGetSelectedClientSecondary1() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(deprecatedSettings);
        CloudBlobClient client = azureStorageService.getSelectedClient("azure2", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure2")));
        assertDeprecatedWarnings();
    }

    @Deprecated
    public void testGetSelectedClientSecondary2() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(deprecatedSettings);
        CloudBlobClient client = azureStorageService.getSelectedClient("azure3", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure3")));
        assertDeprecatedWarnings();
    }

    @Deprecated
    public void testGetDefaultClientWithPrimaryAndSecondaries() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(deprecatedSettings);
        CloudBlobClient client = azureStorageService.getSelectedClient("default", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
        assertDeprecatedWarnings();
    }

    @Deprecated
    public void testGetSelectedClientDefault() {
        AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMock(deprecatedSettings);
        CloudBlobClient client = azureStorageService.getSelectedClient("default", LocationMode.PRIMARY_ONLY);
        assertThat(client.getEndpoint(), is(URI.create("https://azure1")));
        assertDeprecatedWarnings();
    }

    private void assertDeprecatedWarnings() {
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            getConcreteSetting(DEPRECATED_ACCOUNT_SETTING, "azure1"),
            getConcreteSetting(DEPRECATED_KEY_SETTING, "azure1"),
            getConcreteSetting(DEPRECATED_DEFAULT_SETTING, "azure1"),
            getConcreteSetting(DEPRECATED_ACCOUNT_SETTING, "azure2"),
            getConcreteSetting(DEPRECATED_KEY_SETTING, "azure2"),
            getConcreteSetting(DEPRECATED_ACCOUNT_SETTING, "azure3"),
            getConcreteSetting(DEPRECATED_KEY_SETTING, "azure3"),
            getConcreteSetting(DEPRECATED_TIMEOUT_SETTING, "azure3")
        });
    }
}
