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

package org.elasticsearch.cloud.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3Repository;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsS3ServiceImplTests extends ESTestCase {

    public void testAWSCredentialsWithSystemProviders() {
        AWSCredentialsProvider credentialsProvider =
            InternalAwsS3Service.buildCredentials(logger, deprecationLogger, Settings.EMPTY, Settings.EMPTY, "default");
        assertThat(credentialsProvider, instanceOf(InstanceProfileCredentialsProvider.class));
    }

    public void testAwsCredsDefaultSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "aws_key");
        secureSettings.setString("s3.client.default.secret_key", "aws_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        launchAWSCredentialsWithElasticsearchSettingsTest(Settings.EMPTY, settings, "aws_key", "aws_secret");
    }

    public void testAwsCredsExplicitConfigSettings() {
        Settings repositorySettings = Settings.builder().put(InternalAwsS3Service.CLIENT_NAME.getKey(), "myconfig").build();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.myconfig.access_key", "aws_key");
        secureSettings.setString("s3.client.myconfig.secret_key", "aws_secret");
        secureSettings.setString("s3.client.default.access_key", "wrong_key");
        secureSettings.setString("s3.client.default.secret_key", "wrong_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "aws_key", "aws_secret");
    }

    protected void launchAWSCredentialsWithElasticsearchSettingsTest(Settings singleRepositorySettings, Settings settings,
                                                                     String expectedKey, String expectedSecret) {
        String configName = InternalAwsS3Service.CLIENT_NAME.get(singleRepositorySettings);
        AWSCredentials credentials = InternalAwsS3Service.buildCredentials(logger, deprecationLogger, settings,
            singleRepositorySettings, configName).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(expectedKey));
        assertThat(credentials.getAWSSecretKey(), is(expectedSecret));
    }

    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(Settings.EMPTY, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null, null, 3, false,
            ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
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
            "aws_proxy_password", null, 3, false, 10000);
    }

    public void testRepositoryMaxRetries() {
        Settings repositorySettings = generateRepositorySettings(20);
        launchAWSConfigurationTest(Settings.EMPTY, repositorySettings, Protocol.HTTPS, null, -1, null, null, null, 20, false, 50000);
    }

    protected void launchAWSConfigurationTest(Settings settings,
                                              Settings singleRepositorySettings,
                                              Protocol expectedProtocol,
                                              String expectedProxyHost,
                                              int expectedProxyPort,
                                              String expectedProxyUsername,
                                              String expectedProxyPassword,
                                              String expectedSigner,
                                              Integer expectedMaxRetries,
                                              boolean expectedUseThrottleRetries,
                                              int expectedReadTimeout) {
        Integer maxRetries = S3Repository.Repository.MAX_RETRIES_SETTING.get(singleRepositorySettings);
        Boolean useThrottleRetries = S3Repository.Repository.USE_THROTTLE_RETRIES_SETTING.get(singleRepositorySettings);

        ClientConfiguration configuration = InternalAwsS3Service.buildConfiguration(logger, singleRepositorySettings, settings,
            "default", maxRetries, null, useThrottleRetries);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getSignerOverride(), is(expectedSigner));
        assertThat(configuration.getMaxErrorRetry(), is(expectedMaxRetries));
        assertThat(configuration.useThrottledRetries(), is(expectedUseThrottleRetries));
        assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
    }

    private static Settings generateRepositorySettings(Integer maxRetries) {
        Settings.Builder builder = Settings.builder();
        if (maxRetries != null) {
            builder.put(S3Repository.Repository.MAX_RETRIES_SETTING.getKey(), maxRetries);
        }
        return builder.build();
    }

    public void testDefaultEndpoint() {
        assertEndpoint(generateRepositorySettings(null), Settings.EMPTY, "");
    }

    public void testEndpointSetting() {
        Settings settings = Settings.builder()
            .put("s3.client.default.endpoint", "s3.endpoint")
            .build();
        assertEndpoint(generateRepositorySettings(null), settings, "s3.endpoint");
    }

    private void assertEndpoint(Settings repositorySettings, Settings settings,
                                  String expectedEndpoint) {
        String configName = InternalAwsS3Service.CLIENT_NAME.get(repositorySettings);
        String foundEndpoint = InternalAwsS3Service.findEndpoint(logger, repositorySettings, settings, configName);
        assertThat(foundEndpoint, is(expectedEndpoint));
    }

}
