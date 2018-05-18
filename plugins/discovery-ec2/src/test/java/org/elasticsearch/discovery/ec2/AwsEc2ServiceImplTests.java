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

package org.elasticsearch.discovery.ec2;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.ec2.AwsEc2Service;
import org.elasticsearch.discovery.ec2.AwsEc2ServiceImpl;
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
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        launchAWSCredentialsWithElasticsearchSettingsTest(settings, "aws_key", "aws_secret");
    }

    protected void launchAWSCredentialsWithElasticsearchSettingsTest(Settings settings, String expectedKey, String expectedSecret) {
        AWSCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger, settings).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(expectedKey));
        assertThat(credentials.getAWSSecretKey(), is(expectedSecret));
    }

    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(Settings.EMPTY, Protocol.HTTPS, null, -1, null, null,
            ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    }

    public void testAWSConfigurationWithAwsSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.proxy.username", "aws_proxy_username");
        secureSettings.setString("discovery.ec2.proxy.password", "aws_proxy_password");
        Settings settings = Settings.builder()
            .put("discovery.ec2.protocol", "http")
            .put("discovery.ec2.proxy.host", "aws_proxy_host")
            .put("discovery.ec2.proxy.port", 8080)
            .put("discovery.ec2.read_timeout", "10s")
            .setSecureSettings(secureSettings)
            .build();
        launchAWSConfigurationTest(settings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username", "aws_proxy_password", 10000);
    }

    protected void launchAWSConfigurationTest(Settings settings,
                                              Protocol expectedProtocol,
                                              String expectedProxyHost,
                                              int expectedProxyPort,
                                              String expectedProxyUsername,
                                              String expectedProxyPassword,
                                              int expectedReadTimeout) {
        ClientConfiguration configuration = AwsEc2ServiceImpl.buildConfiguration(logger, settings);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
    }

    public void testDefaultEndpoint() {
        String endpoint = AwsEc2ServiceImpl.findEndpoint(logger, Settings.EMPTY);
        assertThat(endpoint, nullValue());
    }

    public void testSpecificEndpoint() {
        Settings settings = Settings.builder()
            .put(AwsEc2Service.ENDPOINT_SETTING.getKey(), "ec2.endpoint")
            .build();
        String endpoint = AwsEc2ServiceImpl.findEndpoint(logger, settings);
        assertThat(endpoint, is("ec2.endpoint"));
    }
}
