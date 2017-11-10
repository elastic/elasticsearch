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
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsS3ServiceImplTests extends ESTestCase {

    public void testAWSCredentialsWithSystemProviders() {
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.EMPTY, "default");
        AWSCredentialsProvider credentialsProvider = InternalAwsS3Service.buildCredentials(logger, deprecationLogger, clientSettings, Settings.EMPTY);
        assertThat(credentialsProvider, instanceOf(InstanceProfileCredentialsProvider.class));
    }

    public void testAwsCredsDefaultSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "aws_key");
        secureSettings.setString("s3.client.default.secret_key", "aws_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "aws_key", "aws_secret");
    }

    public void testAwsCredsExplicitConfigSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        repositorySettings = Settings.builder().put(repositorySettings)
            .put(InternalAwsS3Service.CLIENT_NAME.getKey(), "myconfig").build();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.myconfig.access_key", "aws_key");
        secureSettings.setString("s3.client.myconfig.secret_key", "aws_secret");
        secureSettings.setString("s3.client.default.access_key", "wrong_key");
        secureSettings.setString("s3.client.default.secret_key", "wrong_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "aws_key", "aws_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsSettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "aws_key", "aws_secret");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{AwsS3Service.KEY_SETTING, AwsS3Service.SECRET_SETTING});
    }

    public void testAWSCredentialsWithElasticsearchS3SettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3_key")
            .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "s3_key", "s3_secret");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{AwsS3Service.CLOUD_S3.KEY_SETTING, AwsS3Service.CLOUD_S3.SECRET_SETTING});
    }

    public void testAWSCredentialsWithElasticsearchAwsAndS3SettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3_key")
            .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "s3_key", "s3_secret");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{
                AwsS3Service.KEY_SETTING,
                AwsS3Service.SECRET_SETTING,
                AwsS3Service.CLOUD_S3.KEY_SETTING,
                AwsS3Service.CLOUD_S3.SECRET_SETTING});
    }

    public void testAWSCredentialsWithElasticsearchRepositoriesSettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repositories_key", "repositories_secret");
         assertSettingDeprecationsAndWarnings(
                 new Setting<?>[]{S3Repository.Repositories.KEY_SETTING, S3Repository.Repositories.SECRET_SETTING});
    }

    public void testAWSCredentialsWithElasticsearchAwsAndRepositoriesSettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repositories_key", "repositories_secret");
         assertSettingDeprecationsAndWarnings(new Setting<?>[] {
                AwsS3Service.KEY_SETTING,
                AwsS3Service.SECRET_SETTING,
                S3Repository.Repositories.KEY_SETTING,
                S3Repository.Repositories.SECRET_SETTING});
    }

    public void testAWSCredentialsWithElasticsearchAwsAndS3AndRepositoriesSettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3_key")
            .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3_secret")
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repositories_key", "repositories_secret");
         assertSettingDeprecationsAndWarnings(new Setting<?>[] {
                AwsS3Service.KEY_SETTING,
                AwsS3Service.SECRET_SETTING,
                AwsS3Service.CLOUD_S3.KEY_SETTING,
                AwsS3Service.CLOUD_S3.SECRET_SETTING,
                S3Repository.Repositories.KEY_SETTING,
                S3Repository.Repositories.SECRET_SETTING});
    }

    public void testAWSCredentialsWithElasticsearchRepositoriesSettingsAndRepositorySettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repository_key", "repository_secret");
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            S3Repository.Repositories.KEY_SETTING,
            S3Repository.Repositories.SECRET_SETTING,
            S3Repository.Repository.KEY_SETTING,
            S3Repository.Repository.SECRET_SETTING},
            "Using s3 access/secret key from repository settings. Instead store these in named clients and the elasticsearch keystore for secure settings.");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndRepositoriesSettingsAndRepositorySettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repository_key", "repository_secret");
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            AwsS3Service.KEY_SETTING,
            AwsS3Service.SECRET_SETTING,
            S3Repository.Repositories.KEY_SETTING,
            S3Repository.Repositories.SECRET_SETTING,
            S3Repository.Repository.KEY_SETTING,
            S3Repository.Repository.SECRET_SETTING},
            "Using s3 access/secret key from repository settings. Instead store these in named clients and the elasticsearch keystore for secure settings.");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndS3AndRepositoriesSettingsAndRepositorySettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3_key")
            .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3_secret")
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repository_key", "repository_secret");
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            AwsS3Service.KEY_SETTING,
            AwsS3Service.SECRET_SETTING,
            AwsS3Service.CLOUD_S3.KEY_SETTING,
            AwsS3Service.CLOUD_S3.SECRET_SETTING,
            S3Repository.Repositories.KEY_SETTING,
            S3Repository.Repositories.SECRET_SETTING,
            S3Repository.Repository.KEY_SETTING,
            S3Repository.Repository.SECRET_SETTING},
            "Using s3 access/secret key from repository settings. Instead store these in named clients and the elasticsearch keystore for secure settings.");
    }

    protected void launchAWSCredentialsWithElasticsearchSettingsTest(Settings singleRepositorySettings, Settings settings,
                                                                     String expectedKey, String expectedSecret) {
        String configName = InternalAwsS3Service.CLIENT_NAME.get(singleRepositorySettings);
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName);
        AWSCredentials credentials = InternalAwsS3Service
            .buildCredentials(logger, deprecationLogger, clientSettings, singleRepositorySettings)
            .getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(expectedKey));
        assertThat(credentials.getAWSSecretKey(), is(expectedSecret));
    }

    public void testAWSDefaultConfiguration() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        launchAWSConfigurationTest(Settings.EMPTY, repositorySettings, Protocol.HTTPS, null, -1, null, null, null, 3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    }

    public void testAWSConfigurationWithAwsSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
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
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username",
            "aws_proxy_password", null, 3, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 10000);
    }

    public void testAWSConfigurationWithAwsSettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.PROTOCOL_SETTING.getKey(), "http")
            .put(AwsS3Service.PROXY_HOST_SETTING.getKey(), "aws_proxy_host")
            .put(AwsS3Service.PROXY_PORT_SETTING.getKey(), 8080)
            .put(AwsS3Service.PROXY_USERNAME_SETTING.getKey(), "aws_proxy_username")
            .put(AwsS3Service.PROXY_PASSWORD_SETTING.getKey(), "aws_proxy_password")
            .put(AwsS3Service.SIGNER_SETTING.getKey(), "AWS3SignerType")
            .put(AwsS3Service.READ_TIMEOUT.getKey(), "10s")
            .build();
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username",
            "aws_proxy_password", "AWS3SignerType", 3, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 10000);
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{
                AwsS3Service.PROXY_USERNAME_SETTING,
                AwsS3Service.PROXY_PASSWORD_SETTING,
                AwsS3Service.PROTOCOL_SETTING,
                AwsS3Service.PROXY_HOST_SETTING,
                AwsS3Service.PROXY_PORT_SETTING,
                AwsS3Service.SIGNER_SETTING,
                AwsS3Service.READ_TIMEOUT});
    }

    public void testAWSConfigurationWithAwsAndS3SettingsBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.PROTOCOL_SETTING.getKey(), "http")
            .put(AwsS3Service.PROXY_HOST_SETTING.getKey(), "aws_proxy_host")
            .put(AwsS3Service.PROXY_PORT_SETTING.getKey(), 8080)
            .put(AwsS3Service.PROXY_USERNAME_SETTING.getKey(), "aws_proxy_username")
            .put(AwsS3Service.PROXY_PASSWORD_SETTING.getKey(), "aws_proxy_password")
            .put(AwsS3Service.SIGNER_SETTING.getKey(), "AWS3SignerType")
            .put(AwsS3Service.READ_TIMEOUT.getKey(), "5s")
            .put(AwsS3Service.CLOUD_S3.PROTOCOL_SETTING.getKey(), "https")
            .put(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.getKey(), "s3_proxy_host")
            .put(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.getKey(), 8081)
            .put(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.getKey(), "s3_proxy_username")
            .put(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.getKey(), "s3_proxy_password")
            .put(AwsS3Service.CLOUD_S3.SIGNER_SETTING.getKey(), "NoOpSignerType")
            .put(AwsS3Service.CLOUD_S3.READ_TIMEOUT.getKey(), "10s")
            .build();
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTPS, "s3_proxy_host", 8081, "s3_proxy_username",
            "s3_proxy_password", "NoOpSignerType", 3, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 10000);
         assertSettingDeprecationsAndWarnings(new Setting<?>[] {
                AwsS3Service.PROXY_USERNAME_SETTING,
                AwsS3Service.PROXY_PASSWORD_SETTING,
                AwsS3Service.PROTOCOL_SETTING,
                AwsS3Service.PROXY_HOST_SETTING,
                AwsS3Service.PROXY_PORT_SETTING,
                AwsS3Service.SIGNER_SETTING,
                AwsS3Service.READ_TIMEOUT,
                AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING,
                AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING,
                AwsS3Service.CLOUD_S3.PROTOCOL_SETTING,
                AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING,
                AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING,
                AwsS3Service.CLOUD_S3.SIGNER_SETTING,
                AwsS3Service.CLOUD_S3.READ_TIMEOUT});
    }

    public void testGlobalMaxRetriesBackcompat() {
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.MAX_RETRIES_SETTING.getKey(), 10)
            .build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null,
            null, 10, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            S3Repository.Repositories.MAX_RETRIES_SETTING
        });
    }

    public void testRepositoryMaxRetries() {
        Settings settings = Settings.builder()
            .put("s3.client.default.max_retries", 5)
            .build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null,
            null, 5, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    }

    public void testRepositoryMaxRetriesBackcompat() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, 20);
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.MAX_RETRIES_SETTING.getKey(), 10)
            .build();
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTPS, null, -1, null,
            null, null, 20, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            S3Repository.Repositories.MAX_RETRIES_SETTING,
            S3Repository.Repository.MAX_RETRIES_SETTING
        });
    }

    public void testGlobalThrottleRetriesBackcompat() {
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.USE_THROTTLE_RETRIES_SETTING.getKey(), true)
            .build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null,
            null, 3, true, 50000);
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            S3Repository.Repositories.USE_THROTTLE_RETRIES_SETTING
        });
    }

    public void testRepositoryThrottleRetries() {
        Settings settings = Settings.builder()
            .put("s3.client.default.use_throttle_retries", true)
            .build();
        launchAWSConfigurationTest(settings, Settings.EMPTY, Protocol.HTTPS, null, -1, null, null,
            null, 3, true, 50000);
    }

    public void testRepositoryThrottleRetriesBackcompat() {
        Settings repositorySettings = Settings.builder()
            .put(S3Repository.Repository.USE_THROTTLE_RETRIES_SETTING.getKey(), true).build();
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.USE_THROTTLE_RETRIES_SETTING.getKey(), false)
            .build();
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTPS, null, -1, null, null,
            null, 3, true, 50000);
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            S3Repository.Repositories.USE_THROTTLE_RETRIES_SETTING,
            S3Repository.Repository.USE_THROTTLE_RETRIES_SETTING
        });
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

        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default");
        ClientConfiguration configuration = InternalAwsS3Service.buildConfiguration(logger, clientSettings, singleRepositorySettings, null);

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

    private static Settings generateRepositorySettings(String key, String secret, String region, String endpoint, Integer maxRetries) {
        Settings.Builder builder = Settings.builder();
        if (region != null) {
            builder.put(S3Repository.Repository.REGION_SETTING.getKey(), region);
        }
        if (endpoint != null) {
            builder.put(S3Repository.Repository.ENDPOINT_SETTING.getKey(), endpoint);
        }
        if (key != null) {
            builder.put(S3Repository.Repository.KEY_SETTING.getKey(), key);
        }
        if (secret != null) {
            builder.put(S3Repository.Repository.SECRET_SETTING.getKey(), secret);
        }
        if (maxRetries != null) {
            builder.put(S3Repository.Repository.MAX_RETRIES_SETTING.getKey(), maxRetries);
        }
        return builder.build();
    }

    public void testDefaultEndpoint() {
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, null, null), Settings.EMPTY, "");
    }

    public void testEndpointSetting() {
        Settings settings = Settings.builder()
            .put("s3.client.default.endpoint", "s3.endpoint")
            .build();
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings, "s3.endpoint");
    }

    public void testEndpointSettingBackcompat() {
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            Settings.EMPTY, "repository.endpoint");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{S3Repository.Repository.ENDPOINT_SETTING});
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.ENDPOINT_SETTING.getKey(), "repositories.endpoint")
            .build();
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings,
            "repositories.endpoint");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{S3Repository.Repositories.ENDPOINT_SETTING});
    }

    public void testRegionSettingBackcompat() {
        Settings settings = Settings.builder()
            .put(InternalAwsS3Service.REGION_SETTING.getKey(), randomFrom("eu-west", "eu-west-1"))
            .build();
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings,
            "s3-eu-west-1.amazonaws.com");
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), settings,
            "s3.eu-central-1.amazonaws.com");
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            settings, "repository.endpoint");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{
                        InternalAwsS3Service.REGION_SETTING,
                        S3Repository.Repository.REGION_SETTING,
                        S3Repository.Repository.ENDPOINT_SETTING},
                "Specifying region for an s3 repository is deprecated. Use endpoint " +
                        "to specify the region endpoint if the default behavior is not sufficient.");
    }

    public void testRegionAndEndpointSettingBackcompatPrecedence() {
        Settings settings = Settings.builder()
            .put(InternalAwsS3Service.REGION_SETTING.getKey(), randomFrom("eu-west", "eu-west-1"))
            .put(InternalAwsS3Service.CLOUD_S3.REGION_SETTING.getKey(), randomFrom("us-west", "us-west-1"))
            .build();
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings,
            "s3-us-west-1.amazonaws.com");
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), settings,
            "s3.eu-central-1.amazonaws.com");
        assertEndpoint(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            settings, "repository.endpoint");
         assertSettingDeprecationsAndWarnings(new Setting<?>[]{
                        InternalAwsS3Service.REGION_SETTING,
                        S3Repository.Repository.REGION_SETTING,
                        InternalAwsS3Service.CLOUD_S3.REGION_SETTING,
                        S3Repository.Repository.ENDPOINT_SETTING},
                "Specifying region for an s3 repository is deprecated. Use endpoint " +
                        "to specify the region endpoint if the default behavior is not sufficient.");
    }

    public void testInvalidRegion() {
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.REGION_SETTING.getKey(), "does-not-exist")
            .build();
        Settings repositorySettings = generateRepositorySettings("repository_key", "repository_secret", null, null, null);
        String configName = InternalAwsS3Service.CLIENT_NAME.get(repositorySettings);
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            InternalAwsS3Service.findEndpoint(logger, deprecationLogger, clientSettings, repositorySettings)
        );
        assertThat(e.getMessage(), containsString("No automatic endpoint could be derived from region"));
        assertSettingDeprecationsAndWarnings(
                new Setting<?>[]{S3Repository.Repositories.REGION_SETTING},
                "Specifying region for an s3 repository is deprecated. Use endpoint " +
                        "to specify the region endpoint if the default behavior is not sufficient.");
    }

    private void assertEndpoint(Settings repositorySettings, Settings settings,
                                  String expectedEndpoint) {
        String configName = InternalAwsS3Service.CLIENT_NAME.get(repositorySettings);
        S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName);
        String foundEndpoint = InternalAwsS3Service.findEndpoint(logger, deprecationLogger, clientSettings, repositorySettings);
        assertThat(foundEndpoint, is(expectedEndpoint));
    }

}
