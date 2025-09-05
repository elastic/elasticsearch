/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.endpoints.Ec2EndpointParams;
import software.amazon.awssdk.services.ec2.endpoints.Ec2EndpointProvider;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Consumer;

import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.ACCESS_KEY_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.PROXY_HOST_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.PROXY_PASSWORD_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.PROXY_PORT_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.PROXY_SCHEME_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.PROXY_USERNAME_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.SECRET_KEY_SETTING;
import static org.elasticsearch.discovery.ec2.Ec2ClientSettings.SESSION_TOKEN_SETTING;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Mostly just testing that the various plugin settings (see reference docs) result in appropriate calls on the client builder, using mocks.
 */
public class Ec2DiscoveryPluginTests extends ESTestCase {

    public void testNodeAttributesDisabledByDefault() {
        assertTrue(Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(Settings.EMPTY).isEmpty());
    }

    public void testNodeAttributesDisabled() {
        assertTrue(
            Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(
                Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), false).build()
            ).isEmpty()
        );
    }

    public void testNodeAttributesEnabled() {
        final var availabilityZone = randomIdentifier();
        Ec2ImdsHttpFixture.runWithFixture(
            new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V2).availabilityZoneSupplier(() -> availabilityZone),
            ec2ImdsHttpFixture -> {
                try (var ignored = Ec2ImdsHttpFixture.withEc2MetadataServiceEndpointOverride(ec2ImdsHttpFixture.getAddress())) {
                    final var availabilityZoneNodeAttributeSettings = Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(
                        Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), true).build()
                    );
                    assertEquals(
                        availabilityZone,
                        availabilityZoneNodeAttributeSettings.get(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone")
                    );
                }
            }
        );
    }

    public void testDefaultEndpoint() {
        // Ec2ClientSettings#ENDPOINT_SETTING is not set, so the builder method shouldn't be called
        runPluginMockTest(Settings.builder(), plugin -> verify(plugin.ec2ClientBuilder, never()).endpointProvider(any()));
    }

    public void testSpecificEndpoint() {
        final var argumentCaptor = ArgumentCaptor.forClass(Ec2EndpointProvider.class);
        final var endpoint = randomIdentifier() + ".local";
        runPluginMockTest(
            Settings.builder().put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), endpoint),
            plugin -> verify(plugin.ec2ClientBuilder, times(1)).endpointProvider(argumentCaptor.capture())
        );
        assertEquals(endpoint, safeGet(argumentCaptor.getValue().resolveEndpoint(Ec2EndpointParams.builder().build())).url().toString());
    }

    public void testDefaultHttpSocketTimeout() {
        final var argumentCaptor = ArgumentCaptor.forClass(Duration.class);
        runPluginMockTest(Settings.builder(), plugin -> verify(plugin.httpClientBuilder, times(1)).socketTimeout(argumentCaptor.capture()));
        assertEquals(READ_TIMEOUT_SETTING.get(Settings.EMPTY).nanos(), argumentCaptor.getValue().toNanos());
    }

    public void testSpecificHttpSocketTimeout() {
        final var argumentCaptor = ArgumentCaptor.forClass(Duration.class);
        final var timeoutValue = TimeValue.timeValueMillis(between(0, 100000));
        runPluginMockTest(
            Settings.builder().put(READ_TIMEOUT_SETTING.getKey(), timeoutValue),
            plugin -> verify(plugin.httpClientBuilder, times(1)).socketTimeout(argumentCaptor.capture())
        );
        assertEquals(timeoutValue.nanos(), argumentCaptor.getValue().toNanos());
    }

    public void testDefaultProxyConfiguration() {
        runPluginMockTest(Settings.builder(), plugin -> verify(plugin.httpClientBuilder, never()).proxyConfiguration(any()));
    }

    public void testSpecificProxyConfiguration() {
        // generates a random proxy configuration (i.e. randomly setting/omitting all the settings) and verifies that the resulting
        // ProxyConfiguration is as expected with a sequence of assertions that match the configuration we generated

        final var argumentCaptor = ArgumentCaptor.forClass(ProxyConfiguration.class);

        final var proxySettings = Settings.builder();
        final var assertions = new ArrayList<Consumer<ProxyConfiguration>>();

        final var proxyHost = "proxy." + randomIdentifier() + ".host";
        proxySettings.put(PROXY_HOST_SETTING.getKey(), proxyHost);
        assertions.add(proxyConfiguration -> assertEquals(proxyHost, proxyConfiguration.host()));

        // randomly set, or not, the port
        if (randomBoolean()) {
            final var proxyPort = between(1, 65535);
            proxySettings.put(PROXY_PORT_SETTING.getKey(), proxyPort);
            assertions.add(proxyConfiguration -> assertEquals(proxyPort, proxyConfiguration.port()));
        } else {
            assertions.add(proxyConfiguration -> assertEquals((int) PROXY_PORT_SETTING.get(Settings.EMPTY), proxyConfiguration.port()));
        }

        // randomly set, or not, the scheme
        if (randomBoolean()) {
            final var proxyScheme = randomFrom("http", "https");
            proxySettings.put(PROXY_SCHEME_SETTING.getKey(), proxyScheme);
            assertions.add(proxyConfiguration -> assertEquals(proxyScheme, proxyConfiguration.scheme()));
        } else {
            assertions.add(
                proxyConfiguration -> assertEquals(PROXY_SCHEME_SETTING.get(Settings.EMPTY).getSchemeString(), proxyConfiguration.scheme())
            );
        }

        // randomly set, or not, the credentials
        if (randomBoolean()) {
            final var secureSettings = new MockSecureSettings();
            final var proxyUsername = randomSecretKey();
            final var proxyPassword = randomSecretKey();
            secureSettings.setString(PROXY_USERNAME_SETTING.getKey(), proxyUsername);
            secureSettings.setString(PROXY_PASSWORD_SETTING.getKey(), proxyPassword);
            assertions.add(proxyConfiguration -> assertEquals(proxyUsername, proxyConfiguration.username()));
            assertions.add(proxyConfiguration -> assertEquals(proxyPassword, proxyConfiguration.password()));
            proxySettings.setSecureSettings(secureSettings);
        } else {
            assertions.add(proxyConfiguration -> assertEquals("", proxyConfiguration.username()));
            assertions.add(proxyConfiguration -> assertEquals("", proxyConfiguration.password()));
        }

        // now verify
        runPluginMockTest(proxySettings, plugin -> verify(plugin.httpClientBuilder, times(1)).proxyConfiguration(argumentCaptor.capture()));
        final var proxyConfiguration = argumentCaptor.getValue();
        assertions.forEach(a -> a.accept(proxyConfiguration));
    }

    public void testCredentialsFromEnvironment() {
        final var argumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        runPluginMockTest(
            Settings.builder(),
            plugin -> verify(plugin.ec2ClientBuilder, times(1)).credentialsProvider(argumentCaptor.capture())
        );
        assertThat(argumentCaptor.getValue(), instanceOf(DefaultCredentialsProvider.class));
    }

    public void testPermanentCredentialsFromKeystore() {
        final var accessKey = randomSecretKey();
        final var secretKey = randomSecretKey();

        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING.getKey(), accessKey);
        secureSettings.setString(SECRET_KEY_SETTING.getKey(), secretKey);

        final var argumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);

        runPluginMockTest(
            Settings.builder().setSecureSettings(secureSettings),
            plugin -> verify(plugin.ec2ClientBuilder, times(1)).credentialsProvider(argumentCaptor.capture())
        );
        final var awsCredentials = asInstanceOf(AwsBasicCredentials.class, argumentCaptor.getValue().resolveCredentials());
        assertEquals(accessKey, awsCredentials.accessKeyId());
        assertEquals(secretKey, awsCredentials.secretAccessKey());
    }

    public void testSessionCredentialsFromKeystore() {
        final var accessKey = randomSecretKey();
        final var secretKey = randomSecretKey();
        final var sessionToken = randomSecretKey();

        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING.getKey(), accessKey);
        secureSettings.setString(SECRET_KEY_SETTING.getKey(), secretKey);
        secureSettings.setString(SESSION_TOKEN_SETTING.getKey(), sessionToken);

        final var argumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);

        runPluginMockTest(
            Settings.builder().setSecureSettings(secureSettings),
            plugin -> verify(plugin.ec2ClientBuilder, times(1)).credentialsProvider(argumentCaptor.capture())
        );
        final var awsCredentials = asInstanceOf(AwsSessionCredentials.class, argumentCaptor.getValue().resolveCredentials());
        assertEquals(accessKey, awsCredentials.accessKeyId());
        assertEquals(secretKey, awsCredentials.secretAccessKey());
        assertEquals(sessionToken, awsCredentials.sessionToken());
    }

    /**
     * Sets up a plugin with the given {@code settings}, using mocks, and then calls the {@code pluginConsumer} on it.
     */
    private static void runPluginMockTest(Settings.Builder settings, CheckedConsumer<Ec2DiscoveryPluginMock, Exception> pluginConsumer) {
        final var httpClientBuilder = mock(ApacheHttpClient.Builder.class);
        final var ec2ClientBuilder = mock(Ec2ClientBuilder.class);
        when(ec2ClientBuilder.build()).thenReturn(mock(Ec2Client.class));

        try (
            var plugin = new Ec2DiscoveryPluginMock(settings.build(), httpClientBuilder, ec2ClientBuilder);
            var ignored = plugin.ec2Service.client()
        ) {
            pluginConsumer.accept(plugin);
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    public void testLoneAccessKeyError() {
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCESS_KEY_SETTING.getKey(), randomSecretKey());
        final var settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertEquals(
            "Setting [discovery.ec2.access_key] is set but [discovery.ec2.secret_key] is not",
            expectThrows(SettingsException.class, () -> new Ec2DiscoveryPlugin(settings)).getMessage()
        );
    }

    public void testLoneSecretKeyError() {
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(SECRET_KEY_SETTING.getKey(), randomSecretKey());
        final var settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertEquals(
            "Setting [discovery.ec2.secret_key] is set but [discovery.ec2.access_key] is not",
            expectThrows(SettingsException.class, () -> new Ec2DiscoveryPlugin(settings)).getMessage()
        );
    }

    public void testLoneSessionTokenError() {
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(SESSION_TOKEN_SETTING.getKey(), randomSecretKey());
        final var settings = Settings.builder().setSecureSettings(secureSettings).build();
        assertEquals(
            "Setting [discovery.ec2.session_token] is set but [discovery.ec2.access_key] and [discovery.ec2.secret_key] are not",
            expectThrows(SettingsException.class, () -> new Ec2DiscoveryPlugin(settings)).getMessage()
        );
    }

    public void testReloadSettings() {
        final var httpClientBuilder = mock(ApacheHttpClient.Builder.class);
        final var ec2ClientBuilder = mock(Ec2ClientBuilder.class);

        final var accessKey1 = randomSecretKey();
        final var secretKey1 = randomSecretKey();
        final var secureSettings1 = new MockSecureSettings();
        secureSettings1.setString(ACCESS_KEY_SETTING.getKey(), accessKey1);
        secureSettings1.setString(SECRET_KEY_SETTING.getKey(), secretKey1);
        final var settings1 = Settings.builder().setSecureSettings(secureSettings1).build();

        try (var plugin = new Ec2DiscoveryPluginMock(settings1, httpClientBuilder, ec2ClientBuilder)) {
            final var client1 = mock(Ec2Client.class);
            when(ec2ClientBuilder.build()).thenReturn(client1);

            try (var clientReference = plugin.ec2Service.client()) {
                assertSame(client1, clientReference.client());
                final var argumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
                verify(plugin.ec2ClientBuilder, times(1)).credentialsProvider(argumentCaptor.capture());
                final var awsCredentials = argumentCaptor.getValue().resolveCredentials();
                assertEquals(accessKey1, awsCredentials.accessKeyId());
                assertEquals(secretKey1, awsCredentials.secretAccessKey());
            }
            verify(client1, never()).close(); // retaining client for future use

            final var accessKey2 = randomSecretKey();
            final var secretKey2 = randomSecretKey();
            final var secureSettings2 = new MockSecureSettings();
            secureSettings2.setString(ACCESS_KEY_SETTING.getKey(), accessKey2);
            secureSettings2.setString(SECRET_KEY_SETTING.getKey(), secretKey2);
            plugin.reload(Settings.builder().setSecureSettings(secureSettings2).build());

            verify(client1, times(1)).close(); // client released on reload

            final var client2 = mock(Ec2Client.class);
            when(ec2ClientBuilder.build()).thenReturn(client2);

            try (var clientReference = plugin.ec2Service.client()) {
                assertSame(client2, clientReference.client());
                final var argumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
                verify(plugin.ec2ClientBuilder, times(2)).credentialsProvider(argumentCaptor.capture());
                final var awsCredentials = argumentCaptor.getAllValues().get(1).resolveCredentials();
                assertEquals(accessKey2, awsCredentials.accessKeyId());
                assertEquals(secretKey2, awsCredentials.secretAccessKey());
            }
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    private static class Ec2DiscoveryPluginMock extends Ec2DiscoveryPlugin {
        final ApacheHttpClient.Builder httpClientBuilder;
        final Ec2ClientBuilder ec2ClientBuilder;

        Ec2DiscoveryPluginMock(Settings settings, ApacheHttpClient.Builder httpClientBuilder, Ec2ClientBuilder ec2ClientBuilder) {
            super(settings, new AwsEc2ServiceImpl() {
                @Override
                ApacheHttpClient.Builder getHttpClientBuilder() {
                    return httpClientBuilder;
                }

                @Override
                Ec2ClientBuilder getEc2ClientBuilder() {
                    return ec2ClientBuilder;
                }
            });
            this.httpClientBuilder = httpClientBuilder;
            this.ec2ClientBuilder = ec2ClientBuilder;
        }
    }

}
