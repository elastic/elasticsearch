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
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AwsEc2ServiceImplTests extends ESTestCase {

    public void testAWSCredentialsWithSystemProviders() {
        AWSCredentialsProvider credentialsProvider = AwsEc2ServiceImpl.buildCredentials(logger, Settings.EMPTY);
        assertThat(credentialsProvider, instanceOf(DefaultAWSCredentialsProviderChain.class));
    }

    public void testAWSCredentialsWithElasticsearchAwsSettings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsEc2Service.SECRET_SETTING.getKey(), "aws_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(settings, "aws_key", "aws_secret");
    }

    public void testAWSCredentialsWithElasticsearchEc2Settings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.CLOUD_EC2.KEY_SETTING.getKey(), "ec2_key")
            .put(AwsEc2Service.CLOUD_EC2.SECRET_SETTING.getKey(), "ec2_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(settings, "ec2_key", "ec2_secret");
    }

    public void testAWSCredentialsWithElasticsearchAwsAndEc2Settings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.KEY_SETTING.getKey(), "aws_key")
            .put(AwsEc2Service.SECRET_SETTING.getKey(), "aws_secret")
            .put(AwsEc2Service.CLOUD_EC2.KEY_SETTING.getKey(), "ec2_key")
            .put(AwsEc2Service.CLOUD_EC2.SECRET_SETTING.getKey(), "ec2_secret")
            .build();
        launchAWSCredentialsWithElasticsearchSettingsTest(settings, "ec2_key", "ec2_secret");
    }

    protected void launchAWSCredentialsWithElasticsearchSettingsTest(Settings settings, String expectedKey, String expectedSecret) {
        AWSCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger, settings).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(expectedKey));
        assertThat(credentials.getAWSSecretKey(), is(expectedSecret));
    }

    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(Settings.EMPTY, Protocol.HTTPS, null, -1, null, null, null);
    }

    public void testAWSConfigurationWithAwsSettings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.PROTOCOL_SETTING.getKey(), "http")
            .put(AwsEc2Service.PROXY_HOST_SETTING.getKey(), "aws_proxy_host")
            .put(AwsEc2Service.PROXY_PORT_SETTING.getKey(), 8080)
            .put(AwsEc2Service.PROXY_USERNAME_SETTING.getKey(), "aws_proxy_username")
            .put(AwsEc2Service.PROXY_PASSWORD_SETTING.getKey(), "aws_proxy_password")
            .put(AwsEc2Service.SIGNER_SETTING.getKey(), "AWS3SignerType")
            .build();
        launchAWSConfigurationTest(settings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username", "aws_proxy_password",
            "AWS3SignerType");
    }

    public void testAWSConfigurationWithAwsAndEc2Settings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.PROTOCOL_SETTING.getKey(), "http")
            .put(AwsEc2Service.PROXY_HOST_SETTING.getKey(), "aws_proxy_host")
            .put(AwsEc2Service.PROXY_PORT_SETTING.getKey(), 8080)
            .put(AwsEc2Service.PROXY_USERNAME_SETTING.getKey(), "aws_proxy_username")
            .put(AwsEc2Service.PROXY_PASSWORD_SETTING.getKey(), "aws_proxy_password")
            .put(AwsEc2Service.SIGNER_SETTING.getKey(), "AWS3SignerType")
            .put(AwsEc2Service.CLOUD_EC2.PROTOCOL_SETTING.getKey(), "https")
            .put(AwsEc2Service.CLOUD_EC2.PROXY_HOST_SETTING.getKey(), "ec2_proxy_host")
            .put(AwsEc2Service.CLOUD_EC2.PROXY_PORT_SETTING.getKey(), 8081)
            .put(AwsEc2Service.CLOUD_EC2.PROXY_USERNAME_SETTING.getKey(), "ec2_proxy_username")
            .put(AwsEc2Service.CLOUD_EC2.PROXY_PASSWORD_SETTING.getKey(), "ec2_proxy_password")
            .put(AwsEc2Service.CLOUD_EC2.SIGNER_SETTING.getKey(), "NoOpSignerType")
            .build();
        launchAWSConfigurationTest(settings, Protocol.HTTPS, "ec2_proxy_host", 8081, "ec2_proxy_username", "ec2_proxy_password",
            "NoOpSignerType");
    }

    protected void launchAWSConfigurationTest(Settings settings,
                                              Protocol expectedProtocol,
                                              String expectedProxyHost,
                                              int expectedProxyPort,
                                              String expectedProxyUsername,
                                              String expectedProxyPassword,
                                              String expectedSigner) {
        ClientConfiguration configuration = AwsEc2ServiceImpl.buildConfiguration(logger, settings);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getSignerOverride(), is(expectedSigner));
    }

    public void testDefaultEndpoint() {
        String endpoint = AwsEc2ServiceImpl.findEndpoint(logger, Settings.EMPTY);
        assertThat(endpoint, nullValue());
    }

    public void testSpecificEndpoint() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.CLOUD_EC2.ENDPOINT_SETTING.getKey(), "ec2.endpoint")
            .build();
        String endpoint = AwsEc2ServiceImpl.findEndpoint(logger, settings);
        assertThat(endpoint, is("ec2.endpoint"));
    }

    public void testRegionWithAwsSettings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.REGION_SETTING.getKey(), randomFrom("eu-west", "eu-west-1"))
            .build();
        String endpoint = AwsEc2ServiceImpl.findEndpoint(logger, settings);
        assertThat(endpoint, is("ec2.eu-west-1.amazonaws.com"));
    }

    public void testRegionWithAwsAndEc2Settings() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.REGION_SETTING.getKey(), randomFrom("eu-west", "eu-west-1"))
            .put(AwsEc2Service.CLOUD_EC2.REGION_SETTING.getKey(), randomFrom("us-west", "us-west-1"))
            .build();
        String endpoint = AwsEc2ServiceImpl.findEndpoint(logger, settings);
        assertThat(endpoint, is("ec2.us-west-1.amazonaws.com"));
    }

    public void testInvalidRegion() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.REGION_SETTING.getKey(), "does-not-exist")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            AwsEc2ServiceImpl.findEndpoint(logger, settings);
        });
        assertThat(e.getMessage(), containsString("No automatic endpoint could be derived from region"));
    }
}
