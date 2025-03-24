/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.mockito.Mockito;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class S3ClientSettingsTests extends ESTestCase {
    public void testThereIsADefaultClientByDefault() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.EMPTY);
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.credentials, nullValue());
        assertThat(defaultSettings.endpoint, is(emptyString()));
        assertThat(defaultSettings.proxyHost, is(emptyString()));
        assertThat(defaultSettings.proxyPort, is(80));
        assertThat(defaultSettings.proxyScheme, is(HttpScheme.HTTP));
        assertThat(defaultSettings.proxyUsername, is(emptyString()));
        assertThat(defaultSettings.proxyPassword, is(emptyString()));
        assertThat(defaultSettings.readTimeoutMillis, is(Math.toIntExact(S3ClientSettings.Defaults.READ_TIMEOUT.millis())));
        assertThat(defaultSettings.maxConnections, is(S3ClientSettings.Defaults.MAX_CONNECTIONS));
        assertThat(defaultSettings.maxRetries, is(S3ClientSettings.Defaults.RETRY_COUNT));
        assertThat(defaultSettings.throttleRetries, is(S3ClientSettings.Defaults.THROTTLE_RETRIES));
    }

    public void testDefaultClientSettingsCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.default.max_retries", 10).build()
        );
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(10));
    }

    public void testNondefaultClientCreatedBySettingItsSettings() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.another_client.max_retries", 10).build()
        );
        assertThat(settings.keySet(), contains("default", "another_client"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(S3ClientSettings.Defaults.RETRY_COUNT));

        final S3ClientSettings anotherClientSettings = settings.get("another_client");
        assertThat(anotherClientSettings.maxRetries, is(10));
    }

    public void testRejectionOfLoneAccessKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "aws_key");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build())
        );
        assertThat(e.getMessage(), is("Missing secret key for s3 client [default]"));
    }

    public void testRejectionOfLoneSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.secret_key", "aws_key");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build())
        );
        assertThat(e.getMessage(), is("Missing access key for s3 client [default]"));
    }

    public void testRejectionOfLoneSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.session_token", "aws_key");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build())
        );
        assertThat(e.getMessage(), is("Missing access key and secret key for s3 client [default]"));
    }

    public void testCredentialsTypeWithAccessKeyAndSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build());
        final S3ClientSettings defaultSettings = settings.get("default");
        AwsBasicCredentials credentials = (AwsBasicCredentials) defaultSettings.credentials;
        assertThat(credentials.accessKeyId(), is("access_key"));
        assertThat(credentials.secretAccessKey(), is("secret_key"));
    }

    public void testCredentialsTypeWithAccessKeyAndSecretKeyAndSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        secureSettings.setString("s3.client.default.session_token", "session_token");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build());
        final S3ClientSettings defaultSettings = settings.get("default");
        AwsSessionCredentials credentials = (AwsSessionCredentials) defaultSettings.credentials;
        assertThat(credentials.accessKeyId(), is("access_key"));
        assertThat(credentials.secretAccessKey(), is("secret_key"));
        assertThat(credentials.sessionToken(), is("session_token"));
    }

    public void testRefineWithRepoSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        secureSettings.setString("s3.client.default.session_token", "session_token");
        final S3ClientSettings baseSettings = S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build())
            .get("default");

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.EMPTY);
            assertSame(refinedSettings, baseSettings);
        }

        {
            final String endpoint = "some.host";
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("endpoint", endpoint).build());
            assertThat(refinedSettings.endpoint, is(endpoint));
            AwsSessionCredentials credentials = (AwsSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.accessKeyId(), is("access_key"));
            assertThat(credentials.secretAccessKey(), is("secret_key"));
            assertThat(credentials.sessionToken(), is("session_token"));
        }

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("path_style_access", true).build());
            assertThat(refinedSettings.pathStyleAccess, is(true));
            AwsSessionCredentials credentials = (AwsSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.accessKeyId(), is("access_key"));
            assertThat(credentials.secretAccessKey(), is("secret_key"));
            assertThat(credentials.sessionToken(), is("session_token"));
        }
    }

    public void testPathStyleAccessCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.path_style_access", true).build()
        );
        assertThat(settings.get("default").pathStyleAccess, is(false));
        assertThat(settings.get("other").pathStyleAccess, is(true));
    }

    public void testUseChunkedEncodingCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.disable_chunked_encoding", true).build()
        );
        assertThat(settings.get("default").disableChunkedEncoding, is(false));
        assertThat(settings.get("other").disableChunkedEncoding, is(true));
    }

    public void testRegionCanBeSet() throws IOException {
        final String region = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.region", region).build()
        );
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").region, is(region));
        try (var s3Service = new S3Service(Mockito.mock(Environment.class), Settings.EMPTY, Mockito.mock(ResourceWatcherService.class))) {
            S3Client other = s3Service.buildClient(settings.get("other"));
            assertThat(other.getSignerRegionOverride(), is(region));
        }
    }

    public void testSignerOverrideCanBeSet() {
        final String signerOverride = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.signer_override", signerOverride).build()
        );
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").signerOverride, is(signerOverride));
        ClientOverrideConfiguration defaultConfiguration = S3Service.buildConfiguration(settings.get("default"), false);
        assertThat(defaultConfiguration.getSignerOverride(), nullValue());
        ClientOverrideConfiguration configuration = S3Service.buildConfiguration(settings.get("other"), false);
        assertThat(configuration.getSignerOverride(), is(signerOverride));
    }

    public void testMaxConnectionsCanBeSet() {
        final int maxConnections = between(1, 100);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.max_connections", maxConnections).build()
        );
        assertThat(settings.get("default").maxConnections, is(S3ClientSettings.Defaults.MAX_CONNECTIONS));
        assertThat(settings.get("other").maxConnections, is(maxConnections));

        ApacheHttpClient.Builder defaultClientBuilder = S3Service.buildHttpClient(settings.get("default"));
        assertThat(defaultClientBuilder.getMaxConnections(), is(S3ClientSettings.Defaults.MAX_CONNECTIONS));

        ApacheHttpClient.Builder otherConfiguration = S3Service.buildHttpClient(settings.get("other"));
        assertThat(otherConfiguration.getMaxConnections(), is(maxConnections));

        // the default appears in the docs so let's make sure it doesn't change:
        assertEquals(50, S3ClientSettings.Defaults.MAX_CONNECTIONS);
    }

    public void testStatelessDefaultRetryPolicy() {
        final var s3ClientSettings = S3ClientSettings.load(Settings.EMPTY).get("default");
        final var clientConfiguration = S3Service.buildConfiguration(s3ClientSettings, true);
        assertThat(clientConfiguration.retryPolicy(), is(S3Service.RETRYABLE_403_RETRY_POLICY));
    }
}
