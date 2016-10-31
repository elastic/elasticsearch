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
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.s3.S3Repository;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsS3ServiceImplTests extends ESTestCase {

    public void testAWSCredentialsWithSystemProviders() {
        AWSCredentialsProvider credentialsProvider = InternalAwsS3Service.buildCredentials(logger, Settings.EMPTY, Settings.EMPTY);
        assertThat(credentialsProvider, instanceOf(DefaultAWSCredentialsProviderChain.class));
    }

    public void testAWSCredentialsWithElasticsearchAwsSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "aws_key", "aws_secret");
    }

    public void testAWSCredentialsWithElasticsearchS3Settings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3_key")
            .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "s3_key", "s3_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndS3Settings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(AwsS3Service.CLOUD_S3.KEY_SETTING.getKey(), "s3_key")
            .put(AwsS3Service.CLOUD_S3.SECRET_SETTING.getKey(), "s3_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "s3_key", "s3_secret");
    }

    public void testAWSCredentialsWithElasticsearchRepositoriesSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repositories_key", "repositories_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndRepositoriesSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repositories_key", "repositories_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndS3AndRepositoriesSettings() {
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
    }

    public void testAWSCredentialsWithElasticsearchRepositoriesSettingsAndRepositorySettings() {
        Settings repositorySettings = generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repository_key", "repository_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndRepositoriesSettingsAndRepositorySettings() {
        Settings repositorySettings = generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsS3Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(S3Repository.Repositories.KEY_SETTING.getKey(), "repositories_key")
            .put(S3Repository.Repositories.SECRET_SETTING.getKey(), "repositories_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(repositorySettings, settings, "repository_key", "repository_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndS3AndRepositoriesSettingsAndRepositorySettings() {
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
    }

    protected void launchAWSCredentialsWithElasticsearchSettingsTest(Settings singleRepositorySettings, Settings settings,
                                                                     String expectedKey, String expectedSecret) {
        AWSCredentials credentials = InternalAwsS3Service.buildCredentials(logger, settings, singleRepositorySettings).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(expectedKey));
        assertThat(credentials.getAWSSecretKey(), is(expectedSecret));
    }

    public void testAWSDefaultConfiguration() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        launchAWSConfigurationTest(Settings.EMPTY, repositorySettings, Protocol.HTTPS, null, -1, null, null, null, 3, false);
    }

    public void testAWSConfigurationWithAwsSettings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.PROTOCOL_SETTING.getKey(), "http")
            .put(AwsS3Service.PROXY_HOST_SETTING.getKey(), "aws_proxy_host")
            .put(AwsS3Service.PROXY_PORT_SETTING.getKey(), 8080)
            .put(AwsS3Service.PROXY_USERNAME_SETTING.getKey(), "aws_proxy_username")
            .put(AwsS3Service.PROXY_PASSWORD_SETTING.getKey(), "aws_proxy_password")
            .put(AwsS3Service.SIGNER_SETTING.getKey(), "AWS3SignerType")
            .build();
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username",
            "aws_proxy_password", "AWS3SignerType", 3, false);
    }

    public void testAWSConfigurationWithAwsAndS3Settings() {
        Settings repositorySettings = generateRepositorySettings(null, null, "eu-central", null, null);
        Settings settings = Settings.builder()
            .put(AwsS3Service.PROTOCOL_SETTING.getKey(), "http")
            .put(AwsS3Service.PROXY_HOST_SETTING.getKey(), "aws_proxy_host")
            .put(AwsS3Service.PROXY_PORT_SETTING.getKey(), 8080)
            .put(AwsS3Service.PROXY_USERNAME_SETTING.getKey(), "aws_proxy_username")
            .put(AwsS3Service.PROXY_PASSWORD_SETTING.getKey(), "aws_proxy_password")
            .put(AwsS3Service.SIGNER_SETTING.getKey(), "AWS3SignerType")
            .put(AwsS3Service.CLOUD_S3.PROTOCOL_SETTING.getKey(), "https")
            .put(AwsS3Service.CLOUD_S3.PROXY_HOST_SETTING.getKey(), "s3_proxy_host")
            .put(AwsS3Service.CLOUD_S3.PROXY_PORT_SETTING.getKey(), 8081)
            .put(AwsS3Service.CLOUD_S3.PROXY_USERNAME_SETTING.getKey(), "s3_proxy_username")
            .put(AwsS3Service.CLOUD_S3.PROXY_PASSWORD_SETTING.getKey(), "s3_proxy_password")
            .put(AwsS3Service.CLOUD_S3.SIGNER_SETTING.getKey(), "NoOpSignerType")
            .build();
        launchAWSConfigurationTest(settings, repositorySettings, Protocol.HTTPS, "s3_proxy_host", 8081, "s3_proxy_username",
            "s3_proxy_password", "NoOpSignerType", 3, false);
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
                                              boolean expectedUseThrottleRetries) {
        Protocol protocol = S3Repository.getValue(singleRepositorySettings, settings,
            S3Repository.Repository.PROTOCOL_SETTING, S3Repository.Repositories.PROTOCOL_SETTING);
        Integer maxRetries = S3Repository.getValue(singleRepositorySettings, settings,
            S3Repository.Repository.MAX_RETRIES_SETTING, S3Repository.Repositories.MAX_RETRIES_SETTING);
        Boolean useThrottleRetries = S3Repository.getValue(singleRepositorySettings, settings,
            S3Repository.Repository.USE_THROTTLE_RETRIES_SETTING, S3Repository.Repositories.USE_THROTTLE_RETRIES_SETTING);

        ClientConfiguration configuration = InternalAwsS3Service.buildConfiguration(logger, settings, protocol, maxRetries, null,
            useThrottleRetries);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getSignerOverride(), is(expectedSigner));
        assertThat(configuration.getMaxErrorRetry(), is(expectedMaxRetries));
        assertThat(configuration.useThrottledRetries(), is(expectedUseThrottleRetries));
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
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, null, null), Settings.EMPTY, "");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), Settings.EMPTY,
            "s3.eu-central-1.amazonaws.com");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            Settings.EMPTY, "repository.endpoint");
    }

    public void testSpecificEndpoint() {
        Settings settings = Settings.builder()
            .put(InternalAwsS3Service.CLOUD_S3.ENDPOINT_SETTING.getKey(), "ec2.endpoint")
            .build();
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings,
            "ec2.endpoint");
        // Endpoint has precedence on region. Whatever region we set, we won't use it
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), settings,
            "ec2.endpoint");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            settings, "repository.endpoint");
    }

    public void testRegionWithAwsSettings() {
        Settings settings = Settings.builder()
            .put(InternalAwsS3Service.REGION_SETTING.getKey(), randomFrom("eu-west", "eu-west-1"))
            .build();
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings,
            "s3-eu-west-1.amazonaws.com");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), settings,
            "s3.eu-central-1.amazonaws.com");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            settings, "repository.endpoint");
    }

    public void testRegionWithAwsAndS3Settings() {
        Settings settings = Settings.builder()
            .put(InternalAwsS3Service.REGION_SETTING.getKey(), randomFrom("eu-west", "eu-west-1"))
            .put(InternalAwsS3Service.CLOUD_S3.REGION_SETTING.getKey(), randomFrom("us-west", "us-west-1"))
            .build();
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings,
            "s3-us-west-1.amazonaws.com");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), settings,
            "s3.eu-central-1.amazonaws.com");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            settings, "repository.endpoint");
    }

    public void testInvalidRegion() {
        Settings settings = Settings.builder()
            .put(InternalAwsS3Service.REGION_SETTING.getKey(), "does-not-exist")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, null, null), settings, null);
        });
        assertThat(e.getMessage(), containsString("No automatic endpoint could be derived from region"));

        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", "eu-central", null, null), settings,
            "s3.eu-central-1.amazonaws.com");
        launchAWSEndpointTest(generateRepositorySettings("repository_key", "repository_secret", null, "repository.endpoint", null),
            settings, "repository.endpoint");
    }

    protected void launchAWSEndpointTest(Settings singleRepositorySettings, Settings settings,
                                         String expectedEndpoint) {
        String region = S3Repository.getValue(singleRepositorySettings, settings,
            S3Repository.Repository.REGION_SETTING, S3Repository.Repositories.REGION_SETTING);
        String endpoint = S3Repository.getValue(singleRepositorySettings, settings,
            S3Repository.Repository.ENDPOINT_SETTING, S3Repository.Repositories.ENDPOINT_SETTING);

        String foundEndpoint = InternalAwsS3Service.findEndpoint(logger, settings, endpoint, region);
        assertThat(foundEndpoint, is(expectedEndpoint));
    }

}
