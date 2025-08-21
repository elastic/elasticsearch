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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;

import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.mockito.Mockito;

import java.util.Map;

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
        assertThat(defaultSettings.protocol, is(HttpScheme.HTTPS));
        assertThat(defaultSettings.endpoint, is(emptyString()));
        assertThat(defaultSettings.proxyHost, is(emptyString()));
        assertThat(defaultSettings.proxyPort, is(80));
        assertThat(defaultSettings.proxyScheme, is(HttpScheme.HTTP));
        assertThat(defaultSettings.proxyUsername, is(emptyString()));
        assertThat(defaultSettings.proxyPassword, is(emptyString()));
        assertThat(defaultSettings.readTimeoutMillis, is(Math.toIntExact(S3ClientSettings.Defaults.READ_TIMEOUT.millis())));
        assertThat(defaultSettings.maxConnections, is(S3ClientSettings.Defaults.MAX_CONNECTIONS));
        assertThat(defaultSettings.maxRetries, is(S3ClientSettings.Defaults.RETRY_COUNT));
    }

    public void testDefaultClientSettingsCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.default.max_retries", 10).build()
        );
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(10));
    }

    public void testNonDefaultClientCreatedBySettingItsSettings() {
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

    public void testRegionCanBeSet() {
        final String randomRegion = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.region", randomRegion).build()
        );

        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").region, is(randomRegion));

        try (
            var s3Service = new S3Service(
                Mockito.mock(Environment.class),
                ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                Mockito.mock(ResourceWatcherService.class),
                () -> null
            )
        ) {
            var otherSettings = settings.get("other");
            Region otherRegion = s3Service.getClientRegion(otherSettings);
            assertEquals(randomRegion, otherRegion.toString());

            // by default, we simply do not know the region (which S3Service maps to us-east-1 with cross-region access enabled)
            assertNull(s3Service.getClientRegion(settings.get("default")));
        }
    }

    public void testMaxConnectionsCanBeSet() {
        final int maxConnections = between(1, 100);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.max_connections", maxConnections).build()
        );
        assertThat(settings.get("default").maxConnections, is(S3ClientSettings.Defaults.MAX_CONNECTIONS));
        assertThat(settings.get("other").maxConnections, is(maxConnections));

        // the default appears in the docs so let's make sure it doesn't change:
        assertEquals(50, S3ClientSettings.Defaults.MAX_CONNECTIONS);
    }
}
