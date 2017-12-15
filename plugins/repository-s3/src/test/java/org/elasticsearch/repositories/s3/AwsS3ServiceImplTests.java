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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsS3ServiceImplTests extends ESTestCase {

    public void testAWSCredentialsWithSystemProviders() {
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.EMPTY, "default");
        AWSCredentialsProvider credentialsProvider =
            InternalAwsS3Service.buildCredentials(logger, deprecationLogger, clientSettings, Settings.EMPTY);
        assertThat(credentialsProvider, instanceOf(InternalAwsS3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testAwsCredsDefaultSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "aws_key");
        secureSettings.setString("s3.client.default.secret_key", "aws_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertCredentials(Settings.EMPTY, settings, "aws_key", "aws_secret");
    }

    public void testAwsCredsExplicitConfigSettings() {
        Settings repositorySettings = Settings.builder().put(InternalAwsS3Service.CLIENT_NAME.getKey(), "myconfig").build();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.myconfig.access_key", "aws_key");
        secureSettings.setString("s3.client.myconfig.secret_key", "aws_secret");
        secureSettings.setString("s3.client.default.access_key", "wrong_key");
        secureSettings.setString("s3.client.default.secret_key", "wrong_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertCredentials(repositorySettings, settings, "aws_key", "aws_secret");
    }

    public void testRepositorySettingsCredentialsDisallowed() {
        Settings repositorySettings = Settings.builder()
            .put(S3Repository.ACCESS_KEY_SETTING.getKey(), "aws_key")
            .put(S3Repository.SECRET_KEY_SETTING.getKey(), "aws_secret").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            assertCredentials(repositorySettings, Settings.EMPTY, "aws_key", "aws_secret"));
        assertThat(e.getMessage(), containsString("Setting [access_key] is insecure"));
    }

    public void testRepositorySettingsCredentialsMissingKey() {
        Settings repositorySettings = Settings.builder().put(S3Repository.SECRET_KEY_SETTING.getKey(), "aws_secret").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            assertCredentials(repositorySettings, Settings.EMPTY, "aws_key", "aws_secret"));
        assertThat(e.getMessage(), containsString("must be accompanied by setting [access_key]"));
    }

    public void testRepositorySettingsCredentialsMissingSecret() {
        Settings repositorySettings = Settings.builder().put(S3Repository.ACCESS_KEY_SETTING.getKey(), "aws_key").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            assertCredentials(repositorySettings, Settings.EMPTY, "aws_key", "aws_secret"));
        assertThat(e.getMessage(), containsString("must be accompanied by setting [secret_key]"));
    }

    private void assertCredentials(Settings singleRepositorySettings, Settings settings,
                                   String expectedKey, String expectedSecret) {
        String configName = InternalAwsS3Service.CLIENT_NAME.get(singleRepositorySettings);
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName);
        AWSCredentials credentials = InternalAwsS3Service.buildCredentials(logger, deprecationLogger,
            clientSettings, singleRepositorySettings).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(expectedKey));
        assertThat(credentials.getAWSSecretKey(), is(expectedSecret));
    }

    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(Settings.EMPTY, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null, 3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    }

    public void testAWSConfigurationWithAwsSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.host", "aws_proxy_host")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.read_timeout", "10s")
            .build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username",
            "aws_proxy_password", 3, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 10000);
    }

    public void testRepositoryMaxRetries() {
        Settings settings = Settings.builder()
            .put("s3.client.default.max_retries", 5)
            .build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTPS, null, -1, null,
            null, 5, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 50000);
    }

    public void testRepositoryThrottleRetries() {
        final boolean throttling = randomBoolean();

        Settings settings = Settings.builder().put("s3.client.default.use_throttle_retries", throttling).build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null, 3, throttling, 50000);
    }

    private void launchAWSConfigurationTest(Settings settings,
                                              Settings singleRepositorySettings,
                                              Protocol expectedProtocol,
                                              String expectedProxyHost,
                                              int expectedProxyPort,
                                              String expectedProxyUsername,
                                              String expectedProxyPassword,
                                              Integer expectedMaxRetries,
                                              boolean expectedUseThrottleRetries,
                                              int expectedReadTimeout) {

        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default");
        ClientConfiguration configuration = InternalAwsS3Service.buildConfiguration(clientSettings, singleRepositorySettings);

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
        Settings settings = Settings.builder()
            .put("s3.client.default.endpoint", "s3.endpoint")
            .build();
        assertEndpoint(Settings.EMPTY, settings, "s3.endpoint");
    }

    private void assertEndpoint(Settings repositorySettings, Settings settings, String expectedEndpoint) {
        String configName = InternalAwsS3Service.CLIENT_NAME.get(repositorySettings);
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName);
        assertThat(clientSettings.endpoint, is(expectedEndpoint));
    }

}
