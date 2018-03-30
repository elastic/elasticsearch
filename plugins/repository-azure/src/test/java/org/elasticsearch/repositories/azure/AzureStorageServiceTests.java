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

package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.core.Base64;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.repositories.azure.AzureStorageServiceImpl.blobNameFromUri;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AzureStorageServiceTests extends ESTestCase {

    private MockSecureSettings buildSecureSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", "mykey1");
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", "mykey2");
        secureSettings.setString("azure.client.azure3.account", "myaccount3");
        secureSettings.setString("azure.client.azure3.key", "mykey3");
        return secureSettings;
    }
    private Settings buildSettings() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .build();
        return settings;
    }

    public void testReadSecuredSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", "mykey1");
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", "mykey2");
        secureSettings.setString("azure.client.azure3.account", "myaccount3");
        secureSettings.setString("azure.client.azure3.key", "mykey3");
        final Settings settings = Settings.builder().setSecureSettings(secureSettings)
            .put("azure.client.azure3.endpoint_suffix", "my_endpoint_suffix").build();

        final Map<String, AzureStorageSettings> loadedSettings = AzureStorageSettings.load(settings);
        assertThat(loadedSettings.keySet(), containsInAnyOrder("azure1","azure2","azure3","default"));

        assertThat(loadedSettings.get("azure1").getEndpointSuffix(), isEmptyString());
        assertThat(loadedSettings.get("azure2").getEndpointSuffix(), isEmptyString());
        assertThat(loadedSettings.get("azure3").getEndpointSuffix(), equalTo("my_endpoint_suffix"));
    }

    public void testCreateClientWithEndpointSuffix() throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", Base64.encode("mykey1".getBytes(StandardCharsets.UTF_8)));
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", Base64.encode("mykey2".getBytes(StandardCharsets.UTF_8)));
        final Settings settings = Settings.builder().setSecureSettings(secureSettings)
            .put("azure.client.azure1.endpoint_suffix", "my_endpoint_suffix").build();
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings)) {
            final AzureStorageServiceImpl azureStorageService = (AzureStorageServiceImpl) plugin.azureStoreService;
            final CloudBlobClient client1 = azureStorageService.client("azure1").v1();
            assertThat(client1.getEndpoint().toString(), equalTo("https://myaccount1.blob.my_endpoint_suffix"));
            final CloudBlobClient client2 = azureStorageService.client("azure2").v1();
            assertThat(client2.getEndpoint().toString(), equalTo("https://myaccount2.blob.core.windows.net"));
        }
    }

    public void testReinitClientSettings() throws IOException {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString("azure.client.azure1.account", "myaccount11");
        secureSettings1.setString("azure.client.azure1.key", Base64.encode("mykey11".getBytes(StandardCharsets.UTF_8)));
        secureSettings1.setString("azure.client.azure2.account", "myaccount12");
        secureSettings1.setString("azure.client.azure2.key", Base64.encode("mykey12".getBytes(StandardCharsets.UTF_8)));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString("azure.client.azure1.account", "myaccount21");
        secureSettings2.setString("azure.client.azure1.key", Base64.encode("mykey21".getBytes(StandardCharsets.UTF_8)));
        secureSettings2.setString("azure.client.azure3.account", "myaccount23");
        secureSettings2.setString("azure.client.azure3.key", Base64.encode("mykey23".getBytes(StandardCharsets.UTF_8)));
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings1)) {
            final AzureStorageServiceImpl azureStorageService = (AzureStorageServiceImpl) plugin.azureStoreService;
            final CloudBlobClient client11 = azureStorageService.client("azure1").v1();
            assertThat(client11.getEndpoint().toString(), equalTo("https://myaccount11.blob.core.windows.net"));
            final CloudBlobClient client12 = azureStorageService.client("azure2").v1();
            assertThat(client12.getEndpoint().toString(), equalTo("https://myaccount12.blob.core.windows.net"));
            // client 3 is missing
            final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> azureStorageService.client("azure3"));
            assertThat(e1.getMessage(), is("Cannot find an azure client by the name [azure3]. Check your settings."));
            // update client settings
            plugin.reinit(settings2);
            // old client 1 not changed
            assertThat(client11.getEndpoint().toString(), equalTo("https://myaccount11.blob.core.windows.net"));
            // new client 1 is changed
            final CloudBlobClient client21 = azureStorageService.client("azure1").v1();
            assertThat(client21.getEndpoint().toString(), equalTo("https://myaccount21.blob.core.windows.net"));
            // old client 2 not changed
            assertThat(client12.getEndpoint().toString(), equalTo("https://myaccount12.blob.core.windows.net"));
            // new client2 is gone
            final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> azureStorageService.client("azure2"));
            assertThat(e2.getMessage(), is("Cannot find an azure client by the name [azure2]. Check your settings."));
            // client 3 emerged
            final CloudBlobClient client23 = azureStorageService.client("azure3").v1();
            assertThat(client23.getEndpoint().toString(), equalTo("https://myaccount23.blob.core.windows.net"));
        }
    }

    public void testReinitClientEmptySettings() throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", Base64.encode("mykey11".getBytes(StandardCharsets.UTF_8)));
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings)) {
            final AzureStorageServiceImpl azureStorageService = (AzureStorageServiceImpl) plugin.azureStoreService;
            final CloudBlobClient client11 = azureStorageService.client("azure1").v1();
            assertThat(client11.getEndpoint().toString(), equalTo("https://myaccount1.blob.core.windows.net"));
            // reinit with empty settings
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> plugin.reinit(Settings.EMPTY));
            assertThat(e.getMessage(), is("If you want to use an azure repository, you need to define a client configuration."));
            // existing client untouched
            assertThat(client11.getEndpoint().toString(), equalTo("https://myaccount1.blob.core.windows.net"));
            // new client also untouched
            final CloudBlobClient client21 = azureStorageService.client("azure1").v1();
            assertThat(client21.getEndpoint().toString(), equalTo("https://myaccount1.blob.core.windows.net"));
        }
    }

    public void testReinitClientWrongSettings() throws IOException {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString("azure.client.azure1.account", "myaccount1");
        secureSettings1.setString("azure.client.azure1.key", Base64.encode("mykey11".getBytes(StandardCharsets.UTF_8)));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString("azure.client.azure1.account", "myaccount1");
        // missing key
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        try (AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings1)) {
            final AzureStorageServiceImpl azureStorageService = (AzureStorageServiceImpl) plugin.azureStoreService;
            final CloudBlobClient client11 = azureStorageService.client("azure1").v1();
            assertThat(client11.getEndpoint().toString(), equalTo("https://myaccount1.blob.core.windows.net"));
            // reinit with empty settings
            plugin.reinit(settings2);
            // existing client untouched
            assertThat(client11.getEndpoint().toString(), equalTo("https://myaccount1.blob.core.windows.net"));
            // new client also untouched
            final SettingsException e = expectThrows(SettingsException.class, () -> azureStorageService.client("azure1"));
            assertThat(e.getMessage(), is("Invalid azure client [azure1] settings."));
        }
    }

    public void testGetSelectedClientWithNoPrimaryAndSecondary() {
        try {
            new AzureStorageServiceMockForSettings(Settings.EMPTY);
            fail("we should have raised an IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), is("If you want to use an azure repository, you need to define a client configuration."));
        }
    }

    public void testGetSelectedClientNonExisting() {
        final AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMockForSettings(buildSettings());
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            azureStorageService.client("azure4");
        });
        assertThat(e.getMessage(), is("Cannot find an azure client by the name [azure4]. Check your settings."));
    }

    public void testGetSelectedClientDefaultTimeout() {
        final Settings timeoutSettings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.timeout", "30s")
            .build();
        final AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMockForSettings(timeoutSettings);
        final CloudBlobClient client1 = azureStorageService.client("azure1").v1();
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), nullValue());
        final CloudBlobClient client3 = azureStorageService.client("azure3").v1();
        assertThat(client3.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(30 * 1000));
    }

    public void testGetSelectedClientNoTimeout() {
        final AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMockForSettings(buildSettings());
        final CloudBlobClient client1 = azureStorageService.client("azure1").v1();
        assertThat(client1.getDefaultRequestOptions().getTimeoutIntervalInMs(), is(nullValue()));
    }

    public void testGetSelectedClientBackoffPolicy() {
        final AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMockForSettings(buildSettings());
        final CloudBlobClient client1 = azureStorageService.client("azure1").v1();
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testGetSelectedClientBackoffPolicyNbRetries() {
        final Settings timeoutSettings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.max_retries", 7)
            .build();

        final AzureStorageServiceImpl azureStorageService = new AzureStorageServiceMockForSettings(timeoutSettings);
        final CloudBlobClient client1 = azureStorageService.client("azure1").v1();
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), is(notNullValue()));
        assertThat(client1.getDefaultRequestOptions().getRetryPolicyFactory(), instanceOf(RetryExponentialRetry.class));
    }

    public void testNoProxy() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .build();
        final AzureStorageServiceMockForSettings mock = new AzureStorageServiceMockForSettings(settings);
        assertThat(mock.storageSettings.get("azure1").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure2").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testProxyHttp() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "http")
            .build();
        final AzureStorageServiceMockForSettings mock = new AzureStorageServiceMockForSettings(settings);
        final Proxy azure1Proxy = mock.storageSettings.get("azure1").getProxy();

        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.type(), is(Proxy.Type.HTTP));
        assertThat(azure1Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        assertThat(mock.storageSettings.get("azure2").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testMultipleProxies() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "http")
            .put("azure.client.azure2.proxy.host", "127.0.0.1")
            .put("azure.client.azure2.proxy.port", 8081)
            .put("azure.client.azure2.proxy.type", "http")
            .build();
        final AzureStorageServiceMockForSettings mock = new AzureStorageServiceMockForSettings(settings);
        final Proxy azure1Proxy = mock.storageSettings.get("azure1").getProxy();
        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.type(), is(Proxy.Type.HTTP));
        assertThat(azure1Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        final Proxy azure2Proxy = mock.storageSettings.get("azure2").getProxy();
        assertThat(azure2Proxy, notNullValue());
        assertThat(azure2Proxy.type(), is(Proxy.Type.HTTP));
        assertThat(azure2Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8081)));
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testProxySocks() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "socks")
            .build();
        final AzureStorageServiceMockForSettings mock = new AzureStorageServiceMockForSettings(settings);
        final Proxy azure1Proxy = mock.storageSettings.get("azure1").getProxy();
        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.type(), is(Proxy.Type.SOCKS));
        assertThat(azure1Proxy.address(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        assertThat(mock.storageSettings.get("azure2").getProxy(), nullValue());
        assertThat(mock.storageSettings.get("azure3").getProxy(), nullValue());
    }

    public void testProxyNoHost() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMockForSettings(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoPort() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMockForSettings(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoType() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMockForSettings(settings));
        assertEquals("Azure Proxy port or host have been set but proxy type is not defined.", e.getMessage());
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .put("azure.client.azure1.proxy.host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> new AzureStorageServiceMockForSettings(settings));
        assertEquals("Azure proxy host is unknown.", e.getMessage());
    }

    /**
     * This internal class just overload createClient method which is called by AzureStorageServiceImpl.doStart()
     */
    class AzureStorageServiceMockForSettings extends AzureStorageServiceImpl {
        AzureStorageServiceMockForSettings(Settings settings) {
            super(settings);
        }

        // We fake the client here
        @Override
        protected CloudBlobClient createClient(AzureStorageSettings azureStorageSettings) {
            return new CloudBlobClient(URI.create("https://" + azureStorageSettings.getAccount()));
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
}
