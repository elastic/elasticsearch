/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import com.azure.storage.common.policy.RequestRetryOptions;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AzureStorageServiceTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(
            AzureStorageServiceTests.class.getName(),
            AzureRepositoryPlugin.executorBuilder(),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testReadSecuredSettings() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.endpoint_suffix", "my_endpoint_suffix")
            .build();

        final Map<String, AzureStorageSettings> loadedSettings = AzureStorageSettings.load(settings);
        assertThat(loadedSettings.keySet(), containsInAnyOrder("azure1", "azure2", "azure3", "default"));

        assertThat(loadedSettings.get("azure1").getEndpointSuffix(), is(emptyString()));
        assertThat(loadedSettings.get("azure2").getEndpointSuffix(), is(emptyString()));
        assertThat(loadedSettings.get("azure3").getEndpointSuffix(), equalTo("my_endpoint_suffix"));
    }

    private AzureRepositoryPlugin pluginWithSettingsValidation(Settings settings) {
        final AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings);
        new SettingsModule(settings, plugin.getSettings(), Collections.emptyList(), Collections.emptySet());
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null);
        return plugin;
    }

    private AzureStorageService storageServiceWithSettingsValidation(Settings settings) {
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            return plugin.azureStoreService.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testCreateClientWithEndpointSuffix() throws IOException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.endpoint_suffix", "my_endpoint_suffix")
            .build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureBlobServiceClient client1 = azureStorageService.client("azure1", LocationMode.PRIMARY_ONLY);
            assertThat(client1.getSyncClient().getAccountUrl(), equalTo("https://myaccount1.blob.my_endpoint_suffix"));

            AzureBlobServiceClient client2 = azureStorageService.client("azure2", LocationMode.PRIMARY_ONLY);
            assertThat(client2.getSyncClient().getAccountUrl(), equalTo("https://myaccount2.blob.core.windows.net"));
        }
    }

    public void testReinitClientSettings() throws IOException {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString("azure.client.azure1.account", "myaccount11");
        secureSettings1.setString("azure.client.azure1.key", encodeKey("mykey11"));
        secureSettings1.setString("azure.client.azure2.account", "myaccount12");
        secureSettings1.setString("azure.client.azure2.key", encodeKey("mykey12"));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString("azure.client.azure1.account", "myaccount21");
        secureSettings2.setString("azure.client.azure1.key", encodeKey("mykey21"));
        secureSettings2.setString("azure.client.azure3.account", "myaccount23");
        secureSettings2.setString("azure.client.azure3.key", encodeKey("mykey23"));
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings1)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();

            AzureBlobServiceClient client11 = azureStorageService.client("azure1", LocationMode.PRIMARY_ONLY);
            assertThat(client11.getSyncClient().getAccountUrl(), equalTo("https://myaccount11.blob.core.windows.net"));

            AzureBlobServiceClient client12 = azureStorageService.client("azure2", LocationMode.PRIMARY_ONLY);
            assertThat(client12.getSyncClient().getAccountUrl(), equalTo("https://myaccount12.blob.core.windows.net"));

            // client 3 is missing
            final SettingsException e1 = expectThrows(
                SettingsException.class,
                () -> azureStorageService.client("azure3", LocationMode.PRIMARY_ONLY)
            );
            assertThat(e1.getMessage(), is("Unable to find client with name [azure3]"));

            // update client settings
            plugin.reload(settings2);

            // old client 1 not changed
            assertThat(client11.getSyncClient().getAccountUrl(), equalTo("https://myaccount11.blob.core.windows.net"));

            // new client 1 is changed
            AzureBlobServiceClient client21 = azureStorageService.client("azure1", LocationMode.PRIMARY_ONLY);
            assertThat(client21.getSyncClient().getAccountUrl(), equalTo("https://myaccount21.blob.core.windows.net"));

            // old client 2 not changed
            assertThat(client12.getSyncClient().getAccountUrl(), equalTo("https://myaccount12.blob.core.windows.net"));

            // new client2 is gone
            final SettingsException e2 = expectThrows(
                SettingsException.class,
                () -> azureStorageService.client("azure2", LocationMode.PRIMARY_ONLY)
            );
            assertThat(e2.getMessage(), is("Unable to find client with name [azure2]"));

            // client 3 emerged
            AzureBlobServiceClient client23 = azureStorageService.client("azure3", LocationMode.PRIMARY_ONLY);
            assertThat(client23.getSyncClient().getAccountUrl(), equalTo("https://myaccount23.blob.core.windows.net"));
        }
    }

    public void testReinitClientEmptySettings() throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey11"));
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureBlobServiceClient client11 = azureStorageService.client("azure1", LocationMode.PRIMARY_ONLY);
            assertThat(client11.getSyncClient().getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
            // reinit with empty settings is okay
            plugin.reload(Settings.EMPTY);
            // existing client untouched
            assertThat(client11.getSyncClient().getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
            // client is no longer registered
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> azureStorageService.client("azure1", LocationMode.PRIMARY_ONLY)
            );
            assertThat(e.getMessage(), equalTo("Unable to find client with name [azure1]"));
        }
    }

    public void testReinitClientWrongSettings() throws IOException {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString("azure.client.azure1.account", "myaccount1");
        secureSettings1.setString("azure.client.azure1.key", encodeKey("mykey11"));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString("azure.client.azure1.account", "myaccount1");
        // missing key
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        final MockSecureSettings secureSettings3 = new MockSecureSettings();
        secureSettings3.setString("azure.client.azure1.account", "myaccount3");
        secureSettings3.setString("azure.client.azure1.key", encodeKey("mykey33"));
        secureSettings3.setString("azure.client.azure1.sas_token", encodeKey("mysasToken33"));
        final Settings settings3 = Settings.builder().setSecureSettings(secureSettings3).build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings1)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureBlobServiceClient client11 = azureStorageService.client("azure1", LocationMode.PRIMARY_ONLY);
            assertThat(client11.getSyncClient().getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
            final SettingsException e1 = expectThrows(SettingsException.class, () -> plugin.reload(settings2));
            assertThat(e1.getMessage(), is("Neither a secret key nor a shared access token was set."));
            final SettingsException e2 = expectThrows(SettingsException.class, () -> plugin.reload(settings3));
            assertThat(e2.getMessage(), is("Both a secret as well as a shared access token were set."));
            // existing client untouched
            assertThat(client11.getSyncClient().getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
        }
    }

    public void testNoProxy() {
        final Settings settings = Settings.builder().setSecureSettings(buildSecureSettings()).build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
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
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
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
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
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
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
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
        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoPort() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure Proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoType() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure Proxy port or host have been set but proxy type is not defined.", e.getMessage());
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "http"))
            .put("azure.client.azure1.proxy.host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure proxy host is unknown.", e.getMessage());
    }

    public void testDefaultTimeOut() throws Exception {
        final Settings settings = Settings.builder().setSecureSettings(buildSecureSettings()).build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");
            RequestRetryOptions retryOptions = azureStorageService.getRetryOptions(LocationMode.PRIMARY_ONLY, azureStorageSettings);
            assertThat(retryOptions.getTryTimeout(), equalTo(Integer.MAX_VALUE));
        }
    }

    public void testMillisecondsTimeOutIsRoundedUp() throws Exception {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.timeout", "200ms")
            .build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");
            RequestRetryOptions retryOptions = azureStorageService.getRetryOptions(LocationMode.PRIMARY_ONLY, azureStorageSettings);
            assertThat(retryOptions.getTryTimeout(), equalTo(1));
        }
    }

    public void testTimeoutConfiguration() throws Exception {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.timeout", "200s")
            .build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");
            RequestRetryOptions retryOptions = azureStorageService.getRetryOptions(LocationMode.PRIMARY_ONLY, azureStorageSettings);
            assertThat(retryOptions.getTryTimeout(), equalTo(200));
        }
    }

    public void testRetryConfigurationForSecondaryFallbackLocationMode() throws Exception {
        final String endpoint;
        if (randomBoolean()) {
            endpoint = "core.windows.net";
        } else {
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;"
                + "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
        }

        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");
            RequestRetryOptions retryOptions = azureStorageService.getRetryOptions(
                LocationMode.PRIMARY_THEN_SECONDARY,
                azureStorageSettings
            );
            assertThat(retryOptions.getSecondaryHost(), equalTo("https://myaccount1-secondary.blob.core.windows.net"));
        }
    }

    public void testRetryConfigurationForPrimaryFallbackLocationMode() throws Exception {
        final String endpoint;
        if (randomBoolean()) {
            endpoint = "core.windows.net";
        } else {
            endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net;"
                + "BlobSecondaryEndpoint=https://myaccount1-secondary.blob.core.windows.net";
        }

        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");
            RequestRetryOptions retryOptions = azureStorageService.getRetryOptions(
                LocationMode.SECONDARY_THEN_PRIMARY,
                azureStorageSettings
            );
            assertThat(retryOptions.getSecondaryHost(), equalTo("https://myaccount1.blob.core.windows.net"));
        }
    }

    public void testRetryConfigurationForLocationModeWithoutFallback() throws Exception {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.endpoint_suffix", "core.windows.net")
            .build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");
            LocationMode locationMode = randomFrom(LocationMode.PRIMARY_ONLY, LocationMode.SECONDARY_ONLY);
            RequestRetryOptions retryOptions = azureStorageService.getRetryOptions(locationMode, azureStorageSettings);

            assertThat(retryOptions.getSecondaryHost(), equalTo(null));
        }
    }

    public void testInvalidSettingsRetryConfigurationForLocationModeWithSecondaryFallback() throws Exception {
        final String endpoint = "ignored;BlobEndpoint=https://myaccount1.blob.core.windows.net";
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.endpoint_suffix", endpoint)
            .build();

        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService.get();
            AzureStorageSettings azureStorageSettings = azureStorageService.storageSettings.get("azure1");

            expectThrows(
                IllegalArgumentException.class,
                () -> azureStorageService.getRetryOptions(LocationMode.PRIMARY_THEN_SECONDARY, azureStorageSettings)
            );
        }
    }

    private static MockSecureSettings buildSecureSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey1"));
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", encodeKey("mykey2"));
        secureSettings.setString("azure.client.azure3.account", "myaccount3");
        secureSettings.setString("azure.client.azure3.key", encodeKey("mykey3"));
        return secureSettings;
    }

    private static String encodeKey(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
