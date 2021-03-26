/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsEc2ServiceImplTests extends ESTestCase {

    public void testAWSCredentialsWithSystemProviders() {
        final AWSCredentialsProvider credentialsProvider = AwsEc2ServiceImpl.buildCredentials(logger,
                Ec2ClientSettings.getClientSettings(Settings.EMPTY));
        assertThat(credentialsProvider, instanceOf(DefaultAWSCredentialsProviderChain.class));
    }

    public void testAWSCredentialsWithElasticsearchAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        final AWSCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is("aws_key"));
        assertThat(credentials.getAWSSecretKey(), is("aws_secret"));
    }

    public void testAWSSessionCredentialsWithElasticsearchAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        secureSettings.setString("discovery.ec2.session_token", "aws_session_token");
        final BasicSessionCredentials credentials = (BasicSessionCredentials) AwsEc2ServiceImpl.buildCredentials(logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is("aws_key"));
        assertThat(credentials.getAWSSecretKey(), is("aws_secret"));
        assertThat(credentials.getSessionToken(), is("aws_session_token"));
    }

    public void testDeprecationOfLoneAccessKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        final AWSCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is("aws_key"));
        assertThat(credentials.getAWSSecretKey(), is(""));
        assertSettingDeprecationsAndWarnings(new String[]{},
            "Setting [discovery.ec2.access_key] is set but [discovery.ec2.secret_key] is not, which will be unsupported in future");
    }

    public void testDeprecationOfLoneSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        final AWSCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())).getCredentials();
        assertThat(credentials.getAWSAccessKeyId(), is(""));
        assertThat(credentials.getAWSSecretKey(), is("aws_secret"));
        assertSettingDeprecationsAndWarnings(new String[]{},
            "Setting [discovery.ec2.secret_key] is set but [discovery.ec2.access_key] is not, which will be unsupported in future");
    }

    public void testRejectionOfLoneSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.session_token", "aws_session_token");
        SettingsException e = expectThrows(SettingsException.class, () -> AwsEc2ServiceImpl.buildCredentials(logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())));
        assertThat(e.getMessage(), is(
            "Setting [discovery.ec2.session_token] is set but [discovery.ec2.access_key] and [discovery.ec2.secret_key] are not"));
    }

    public void testAWSDefaultConfiguration() {
        launchAWSConfigurationTest(Settings.EMPTY, Protocol.HTTPS, null, -1, null, null,
            ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.proxy.username", "aws_proxy_username");
        secureSettings.setString("discovery.ec2.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
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
        final ClientConfiguration configuration = AwsEc2ServiceImpl.buildConfiguration(
            Ec2ClientSettings.getClientSettings(settings));

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
    }

}
