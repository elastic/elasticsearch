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

package org.elasticsearch.repositories.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.AmazonS3Client;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

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
        assertThat(defaultSettings.endpoint, is(emptyString()));
        assertThat(defaultSettings.protocol, is(Protocol.HTTPS));
        assertThat(defaultSettings.proxyHost, is(emptyString()));
        assertThat(defaultSettings.proxyPort, is(80));
        assertThat(defaultSettings.proxyUsername, is(emptyString()));
        assertThat(defaultSettings.proxyPassword, is(emptyString()));
        assertThat(defaultSettings.readTimeoutMillis, is(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT));
        assertThat(defaultSettings.maxRetries, is(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry()));
        assertThat(defaultSettings.throttleRetries, is(ClientConfiguration.DEFAULT_THROTTLE_RETRIES));
    }

    public void testDefaultClientSettingsCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.builder()
            .put("s3.client.default.max_retries", 10).build());
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(10));
    }

    public void testNondefaultClientCreatedBySettingItsSettings() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.builder()
            .put("s3.client.another_client.max_retries", 10).build());
        assertThat(settings.keySet(), contains("default", "another_client"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry()));

        final S3ClientSettings anotherClientSettings = settings.get("another_client");
        assertThat(anotherClientSettings.maxRetries, is(10));
    }

    public void testRejectionOfLoneAccessKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "aws_key");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build()));
        assertThat(e.getMessage(), is("Missing secret key for s3 client [default]"));
    }

    public void testRejectionOfLoneSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.secret_key", "aws_key");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build()));
        assertThat(e.getMessage(), is("Missing access key for s3 client [default]"));
    }

    public void testRejectionOfLoneSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.session_token", "aws_key");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build()));
        assertThat(e.getMessage(), is("Missing access key and secret key for s3 client [default]"));
    }

    public void testCredentialsTypeWithAccessKeyAndSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build());
        final S3ClientSettings defaultSettings = settings.get("default");
        S3BasicCredentials credentials = defaultSettings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
    }

    public void testCredentialsTypeWithAccessKeyAndSecretKeyAndSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        secureSettings.setString("s3.client.default.session_token", "session_token");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build());
        final S3ClientSettings defaultSettings = settings.get("default");
        S3BasicSessionCredentials credentials = (S3BasicSessionCredentials) defaultSettings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
        assertThat(credentials.getSessionToken(), is("session_token"));
    }

    public void testRefineWithRepoSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        secureSettings.setString("s3.client.default.session_token", "session_token");
        final S3ClientSettings baseSettings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).build()).get("default");

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.EMPTY);
            assertSame(refinedSettings, baseSettings);
        }

        {
            final String endpoint = "some.host";
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("endpoint", endpoint).build());
            assertThat(refinedSettings.endpoint, is(endpoint));
            S3BasicSessionCredentials credentials = (S3BasicSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
            assertThat(credentials.getAWSSecretKey(), is("secret_key"));
            assertThat(credentials.getSessionToken(), is("session_token"));
        }

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("path_style_access", true).build());
            assertThat(refinedSettings.pathStyleAccess, is(true));
            S3BasicSessionCredentials credentials = (S3BasicSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
            assertThat(credentials.getAWSSecretKey(), is("secret_key"));
            assertThat(credentials.getSessionToken(), is("session_token"));
        }
    }

    public void testPathStyleAccessCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.path_style_access", true).build());
        assertThat(settings.get("default").pathStyleAccess, is(false));
        assertThat(settings.get("other").pathStyleAccess, is(true));
    }

    public void testUseChunkedEncodingCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.disable_chunked_encoding", true).build());
        assertThat(settings.get("default").disableChunkedEncoding, is(false));
        assertThat(settings.get("other").disableChunkedEncoding, is(true));
    }

    public void testRegionCanBeSet() {
        final String region = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.region", region).build());
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").region, is(region));
        try (S3Service s3Service = new S3Service()) {
            AmazonS3Client other = (AmazonS3Client) s3Service.buildClient(settings.get("other"));
            assertThat(other.getSignerRegionOverride(), is(region));
        }
    }

    public void testSignerOverrideCanBeSet() {
        final String signerOverride = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.signer_override", signerOverride).build());
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").signerOverride, is(signerOverride));
        ClientConfiguration defaultConfiguration = S3Service.buildConfiguration(settings.get("default"));
        assertThat(defaultConfiguration.getSignerOverride(), nullValue());
        ClientConfiguration configuration = S3Service.buildConfiguration(settings.get("other"));
        assertThat(configuration.getSignerOverride(), is(signerOverride));
    }
}
