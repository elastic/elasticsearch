/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class AwsS3ServiceImplTests extends ESTestCase {

    private final S3Service.CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider = Mockito.mock(
        S3Service.CustomWebIdentityTokenCredentialsProvider.class
    );

    public void testAWSCredentialsDefaultToInstanceProviders() {
        final String inexistentClientName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.EMPTY, inexistentClientName);
        final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(
            logger,
            clientSettings,
            webIdentityTokenCredentialsProvider
        );
        assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedAWSCredentialsProvider.class));
        var privilegedAWSCredentialsProvider = (S3Service.PrivilegedAWSCredentialsProvider) credentialsProvider;
        assertThat(privilegedAWSCredentialsProvider.getCredentialsProvider(), instanceOf(EC2ContainerCredentialsProviderWrapper.class));
    }

    public void testSupportsWebIdentityTokenCredentials() {
        Mockito.when(webIdentityTokenCredentialsProvider.getCredentials())
            .thenReturn(new BasicAWSCredentials("sts_access_key_id", "sts_secret_key"));
        Mockito.when(webIdentityTokenCredentialsProvider.isActive()).thenReturn(true);

        AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(
            logger,
            S3ClientSettings.getClientSettings(Settings.EMPTY, randomAlphaOfLength(8).toLowerCase(Locale.ROOT)),
            webIdentityTokenCredentialsProvider
        );
        assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedAWSCredentialsProvider.class));
        var privilegedAWSCredentialsProvider = (S3Service.PrivilegedAWSCredentialsProvider) credentialsProvider;
        assertThat(privilegedAWSCredentialsProvider.getCredentialsProvider(), instanceOf(AWSCredentialsProviderChain.class));
        AWSCredentials credentials = privilegedAWSCredentialsProvider.getCredentials();
        assertEquals("sts_access_key_id", credentials.getAWSAccessKeyId());
        assertEquals("sts_secret_key", credentials.getAWSSecretKey());
    }

    public void testAWSCredentialsFromKeystore() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientNamePrefix = "some_client_name_";
        final int clientsCount = randomIntBetween(0, 4);
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            secureSettings.setString("s3.client." + clientName + ".access_key", clientName + "_aws_access_key");
            secureSettings.setString("s3.client." + clientName + ".secret_key", clientName + "_aws_secret_key");
        }
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings);
        // no less, no more
        assertThat(allClientsSettings.size(), is(clientsCount + 1)); // including default
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            final S3ClientSettings someClientSettings = allClientsSettings.get(clientName);
            final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(
                logger,
                someClientSettings,
                webIdentityTokenCredentialsProvider
            );
            assertThat(credentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
            assertThat(credentialsProvider.getCredentials().getAWSAccessKeyId(), is(clientName + "_aws_access_key"));
            assertThat(credentialsProvider.getCredentials().getAWSSecretKey(), is(clientName + "_aws_secret_key"));
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(
            logger,
            defaultClientSettings,
            webIdentityTokenCredentialsProvider
        );
        assertThat(defaultCredentialsProvider, instanceOf(S3Service.PrivilegedAWSCredentialsProvider.class));
        var privilegedAWSCredentialsProvider = (S3Service.PrivilegedAWSCredentialsProvider) defaultCredentialsProvider;
        assertThat(privilegedAWSCredentialsProvider.getCredentialsProvider(), instanceOf(EC2ContainerCredentialsProviderWrapper.class));
    }

    public void testSetDefaultCredential() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String awsAccessKey = randomAlphaOfLength(8);
        final String awsSecretKey = randomAlphaOfLength(8);
        secureSettings.setString("s3.client.default.access_key", awsAccessKey);
        secureSettings.setString("s3.client.default.secret_key", awsSecretKey);
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings);
        assertThat(allClientsSettings.size(), is(1));
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(
            logger,
            defaultClientSettings,
            webIdentityTokenCredentialsProvider
        );
        assertThat(defaultCredentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
        assertThat(defaultCredentialsProvider.getCredentials().getAWSAccessKeyId(), is(awsAccessKey));
        assertThat(defaultCredentialsProvider.getCredentials().getAWSSecretKey(), is(awsSecretKey));
    }

    public void testCredentialsIncomplete() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final boolean missingOrMissing = randomBoolean();
        if (missingOrMissing) {
            secureSettings.setString("s3.client." + clientName + ".access_key", "aws_access_key");
        } else {
            secureSettings.setString("s3.client." + clientName + ".secret_key", "aws_secret_key");
        }
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Exception e = expectThrows(IllegalArgumentException.class, () -> S3ClientSettings.load(settings));
        if (missingOrMissing) {
            assertThat(e.getMessage(), containsString("Missing secret key for s3 client [" + clientName + "]"));
        } else {
            assertThat(e.getMessage(), containsString("Missing access key for s3 client [" + clientName + "]"));
        }
    }

    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(
            Settings.EMPTY,
            Protocol.HTTPS,
            null,
            -1,
            null,
            null,
            3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
            ClientConfiguration.DEFAULT_SOCKET_TIMEOUT
        );
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.host", "aws_proxy_host")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.read_timeout", "10s")
            .build();
        launchAWSConfigurationTest(
            settings,
            Protocol.HTTP,
            "aws_proxy_host",
            8080,
            "aws_proxy_username",
            "aws_proxy_password",
            3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
            10000
        );
    }

    public void testRepositoryMaxRetries() {
        final Settings settings = Settings.builder().put("s3.client.default.max_retries", 5).build();
        launchAWSConfigurationTest(settings, Protocol.HTTPS, null, -1, null, null, 5, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 50000);
    }

    public void testRepositoryThrottleRetries() {
        final boolean throttling = randomBoolean();

        final Settings settings = Settings.builder().put("s3.client.default.use_throttle_retries", throttling).build();
        launchAWSConfigurationTest(settings, Protocol.HTTPS, null, -1, null, null, 3, throttling, 50000);
    }

    private void launchAWSConfigurationTest(
        Settings settings,
        Protocol expectedProtocol,
        String expectedProxyHost,
        int expectedProxyPort,
        String expectedProxyUsername,
        String expectedProxyPassword,
        Integer expectedMaxRetries,
        boolean expectedUseThrottleRetries,
        int expectedReadTimeout
    ) {

        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default");
        final ClientConfiguration configuration = S3Service.buildConfiguration(clientSettings);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getMaxErrorRetry(), is(expectedMaxRetries));
        assertThat(configuration.useThrottledRetries(), is(expectedUseThrottleRetries));
        assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
    }

    public void testEndpointSetting() {
        final Settings settings = Settings.builder().put("s3.client.default.endpoint", "s3.endpoint").build();
        assertEndpoint(Settings.EMPTY, settings, "s3.endpoint");
    }

    private void assertEndpoint(Settings repositorySettings, Settings settings, String expectedEndpoint) {
        final String configName = S3Repository.CLIENT_NAME.get(repositorySettings);
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName);
        assertThat(clientSettings.endpoint, is(expectedEndpoint));
    }

    public void testLoggingCredentialsProviderCatchesErrors() {
        var mockProvider = Mockito.mock(AWSCredentialsProvider.class);
        String mockProviderErrorMessage = "mockProvider failed to generate credentials";
        Mockito.when(mockProvider.getCredentials()).thenThrow(new IllegalStateException(mockProviderErrorMessage));
        var mockLogger = Mockito.mock(Logger.class);

        var credentialsProvider = new S3Service.ErrorLoggingCredentialsProvider(mockProvider, mockLogger);
        var exception = expectThrows(IllegalStateException.class, credentialsProvider::getCredentials);
        assertEquals(mockProviderErrorMessage, exception.getMessage());

        var messageSupplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        var throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(mockLogger).error(messageSupplierCaptor.capture(), throwableCaptor.capture());

        assertThat(messageSupplierCaptor.getValue().get().toString(), startsWith("Unable to load credentials from"));
        assertThat(throwableCaptor.getValue().getMessage(), equalTo(mockProviderErrorMessage));
    }

    public void testLoggingCredentialsProviderCatchesErrorsOnRefresh() {
        var mockProvider = Mockito.mock(AWSCredentialsProvider.class);
        String mockProviderErrorMessage = "mockProvider failed to refresh";
        Mockito.doThrow(new IllegalStateException(mockProviderErrorMessage)).when(mockProvider).refresh();
        var mockLogger = Mockito.mock(Logger.class);

        var credentialsProvider = new S3Service.ErrorLoggingCredentialsProvider(mockProvider, mockLogger);
        var exception = expectThrows(IllegalStateException.class, credentialsProvider::refresh);
        assertEquals(mockProviderErrorMessage, exception.getMessage());

        var messageSupplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        var throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(mockLogger).error(messageSupplierCaptor.capture(), throwableCaptor.capture());

        assertThat(messageSupplierCaptor.getValue().get().toString(), startsWith("Unable to refresh"));
        assertThat(throwableCaptor.getValue().getMessage(), equalTo(mockProviderErrorMessage));
    }

}
