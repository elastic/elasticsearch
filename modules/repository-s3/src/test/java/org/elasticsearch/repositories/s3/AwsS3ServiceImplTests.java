/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.regions.Region;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class AwsS3ServiceImplTests extends ESTestCase {

    private final S3Service.CustomWebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider = Mockito.mock(
        S3Service.CustomWebIdentityTokenCredentialsProvider.class
    );

    /**
     * {@code webIdentityTokenCredentialsProvider} is not set up, so {@link S3Service#buildCredentials} should not use it and should instead
     * fall through to a {@link DefaultCredentialsProvider}.
     */
    public void testAwsCredentialsFallsThroughToDefaultCredentialsProvider() {
        final String nonExistentClientName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.EMPTY, nonExistentClientName);
        final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(
            logger,
            clientSettings,
            webIdentityTokenCredentialsProvider
        );
        assertThat(credentialsProvider, instanceOf(DefaultCredentialsProvider.class));
    }

    public void testSupportsWebIdentityTokenCredentials() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create("sts_access_key_id", "sts_secret_key");
        Mockito.when(webIdentityTokenCredentialsProvider.resolveCredentials()).thenReturn(credentials);
        // Mockito has difficulty with #resolveIdentity()'s generic return type. Using #thenAnswer (instead of #thenReturn) provides a
        // workaround.
        Answer<CompletableFuture<? extends AwsCredentialsIdentity>> answer = invocation -> CompletableFuture.completedFuture(credentials);
        Mockito.when(webIdentityTokenCredentialsProvider.resolveIdentity()).thenAnswer(answer);
        Mockito.when(webIdentityTokenCredentialsProvider.isActive()).thenReturn(true);

        AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(
            logger,
            S3ClientSettings.getClientSettings(Settings.EMPTY, randomAlphaOfLength(8).toLowerCase(Locale.ROOT)),
            webIdentityTokenCredentialsProvider
        );
        assertThat(credentialsProvider, instanceOf(AwsCredentialsProviderChain.class));
        AwsCredentials resolvedCredentials = credentialsProvider.resolveCredentials();
        assertEquals("sts_access_key_id", resolvedCredentials.accessKeyId());
        assertEquals("sts_secret_key", resolvedCredentials.secretAccessKey());
    }

    public void testAwsCredentialsFromKeystore() {
        /** Create a random number of clients that use basic access key + secret key credentials */
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
            final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(
                logger,
                someClientSettings,
                webIdentityTokenCredentialsProvider
            );
            assertThat(credentialsProvider, instanceOf(StaticCredentialsProvider.class));
            assertThat(credentialsProvider.resolveCredentials().accessKeyId(), is(clientName + "_aws_access_key"));
            assertThat(credentialsProvider.resolveCredentials().secretAccessKey(), is(clientName + "_aws_secret_key"));
        }

        /** Test that the default client, without basic access + secret keys, will fall back to using the DefaultCredentialsProvider */
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(
            logger,
            defaultClientSettings,
            webIdentityTokenCredentialsProvider
        );
        assertThat(defaultCredentialsProvider, instanceOf(DefaultCredentialsProvider.class));
    }

    public void testBasicAccessKeyAndSecretKeyCredentials() {
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
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(
            logger,
            defaultClientSettings,
            webIdentityTokenCredentialsProvider
        );
        assertThat(defaultCredentialsProvider, instanceOf(StaticCredentialsProvider.class));
        assertThat(defaultCredentialsProvider.resolveCredentials().accessKeyId(), is(awsAccessKey));
        assertThat(defaultCredentialsProvider.resolveCredentials().secretAccessKey(), is(awsSecretKey));
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
            null,
            -1,
            null,
            null,
            null,
            3,
            Math.toIntExact(S3ClientSettings.Defaults.READ_TIMEOUT.seconds())
        );
    }

    public void testAwsConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            // NB: URI #getHost returns null if host string contains underscores: don't do it. Underscores are invalid in URL host strings.
            .put("s3.client.default.proxy.host", "aws-proxy-host")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.proxy.scheme", "http")
            .put("s3.client.default.read_timeout", "10s")
            .build();
        launchAWSConfigurationTest(settings, "aws-proxy-host", 8080, "http", "aws_proxy_username", "aws_proxy_password", 3, 10000);
    }

    public void testRepositoryMaxRetries() {
        final Settings settings = Settings.builder().put("s3.client.default.max_retries", 5).build();
        launchAWSConfigurationTest(settings, null, -1, null, null, null, 5, 50000);
    }

    private void launchAWSConfigurationTest(
        Settings settings,
        String expectedProxyHost,
        int expectedProxyPort,
        String expectedHttpScheme,
        String expectedProxyUsername,
        String expectedProxyPassword,
        Integer expectedMaxRetries,
        int expectedReadTimeout
    ) {
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default");

        final var proxyClientConfiguration = S3Service.buildProxyConfiguration(clientSettings);
        if (proxyClientConfiguration.isPresent()) {
            final var proxyConfig = proxyClientConfiguration.get();
            assertThat(proxyConfig.username(), is(expectedProxyUsername));
            assertThat(proxyConfig.password(), is(expectedProxyPassword));
            assertThat(proxyConfig.scheme(), is(expectedHttpScheme));
            assertThat(proxyConfig.host(), is(expectedProxyHost));
            assertThat(proxyConfig.port(), is(expectedProxyPort));
        }

        final ClientOverrideConfiguration configuration = S3Service.buildConfiguration(clientSettings, false);
        assertThat(configuration.retryStrategy().get().maxAttempts(), is(expectedMaxRetries + 1));
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

    public void testEndPointAndRegionOverrides() throws IOException {
        try (
            S3Service s3Service = new S3Service(
                mock(Environment.class),
                Settings.EMPTY,
                mock(ResourceWatcherService.class),
                () -> Region.of("es-test-region")
            )
        ) {
            s3Service.start();
            final String endpointOverride = "http://first";
            final Settings settings = Settings.builder().put("endpoint", endpointOverride).build();
            final AmazonS3Reference reference = s3Service.client(new RepositoryMetadata("first", "s3", settings));

            assertEquals(endpointOverride, reference.client().serviceClientConfiguration().endpointOverride().get().toString());
            assertEquals("es-test-region", reference.client().serviceClientConfiguration().region().toString());

            reference.close();
            s3Service.doClose();
        }
    }

    public void testLoggingCredentialsProviderCatchesErrorsOnResolveCredentials() {
        var mockProvider = Mockito.mock(AwsCredentialsProvider.class);
        String mockProviderErrorMessage = "mockProvider failed to generate credentials";
        Mockito.when(mockProvider.resolveCredentials()).thenThrow(new IllegalStateException(mockProviderErrorMessage));
        var mockLogger = Mockito.mock(Logger.class);

        var credentialsProvider = new S3Service.ErrorLoggingCredentialsProvider(mockProvider, mockLogger);
        var exception = expectThrows(IllegalStateException.class, credentialsProvider::resolveCredentials);
        assertEquals(mockProviderErrorMessage, exception.getMessage());

        var messageSupplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        var throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(mockLogger).error(messageSupplierCaptor.capture(), throwableCaptor.capture());

        assertThat(messageSupplierCaptor.getValue().get().toString(), startsWith("Unable to load credentials from"));
        assertThat(throwableCaptor.getValue().getMessage(), equalTo(mockProviderErrorMessage));
    }

    public void testLoggingCredentialsProviderCatchesErrorsOnResolveIdentity() {
        // Set up #resolveIdentity() to return a future with an exception.
        var mockCredentialsProvider = Mockito.mock(AwsCredentialsProvider.class);
        String mockProviderErrorMessage = "mockProvider failed to generate credentials";
        Answer<CompletableFuture<? extends AwsCredentialsIdentity>> answer = invocation -> {
            CompletableFuture<AwsCredentialsIdentity> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException(mockProviderErrorMessage));
            return future;
        };
        Mockito.when(mockCredentialsProvider.resolveIdentity()).thenAnswer(answer);
        var mockLogger = Mockito.mock(Logger.class);
        var credentialsProvider = new S3Service.ErrorLoggingCredentialsProvider(mockCredentialsProvider, mockLogger);

        // The S3Service.ErrorLoggingCredentialsProvider should log the error.
        credentialsProvider.resolveIdentity();

        var messageSupplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        var throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(mockLogger).error(messageSupplierCaptor.capture(), throwableCaptor.capture());

        assertThat(messageSupplierCaptor.getValue().get().toString(), startsWith("Unable to resolve identity from"));
        assertThat(throwableCaptor.getValue().getMessage(), equalTo(mockProviderErrorMessage));
    }
}
