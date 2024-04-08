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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.ec2.AbstractAmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@SuppressForbidden(reason = "Uses an HttpServer to emulate the Instance Metadata Service")
public class Ec2DiscoveryPluginTests extends ESTestCase {

    private Settings getNodeAttributes(Settings settings, String url, String tokenUrl) {
        final Settings realSettings = Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), true).put(settings).build();
        return Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(realSettings, url, tokenUrl);
    }

    private void assertNodeAttributes(Settings settings, String url, String tokenUrl, String expected) {
        final Settings additional = getNodeAttributes(settings, url, tokenUrl);
        if (expected == null) {
            assertTrue(additional.isEmpty());
        } else {
            assertEquals(expected, additional.get(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone"));
        }
    }

    public void testNodeAttributesDisabled() {
        final Settings settings = Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), false).build();
        assertNodeAttributes(settings, "bogus", "", null);
    }

    public void testNodeAttributes() throws Exception {
        try (var metadataServer = metadataServerWithoutToken()) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), "", "us-east-1c");
        }
    }

    public void testNodeAttributesBogusUrl() {
        final UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> getNodeAttributes(Settings.EMPTY, "bogus", ""));
        assertNotNull(e.getCause());
        final String msg = e.getCause().getMessage();
        assertTrue(msg, msg.contains("no protocol: bogus"));
    }

    public void testNodeAttributesEmpty() throws Exception {
        try (MetadataServer metadataServer = new MetadataServer("/metadata", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        })) {
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> getNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), "")
            );
            assertTrue(e.getMessage(), e.getMessage().contains("no ec2 metadata returned"));
        }
    }

    public void testNodeAttributesErrorLenient() throws Exception {
        try (var metadataServer = new MetadataServer("/metadata", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        })) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), "", null);
        }
    }

    public void testNodeAttributesWithToken() throws Exception {
        try (var metadataServer = new MetadataServer("/metadata", exchange -> {
            assertEquals("imdsv2-token", exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token"));
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("us-east-1c".getBytes(StandardCharsets.UTF_8));
            exchange.close();
        }, "/latest/api/token", exchange -> {
            assertEquals("PUT", exchange.getRequestMethod());
            assertEquals("10", exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token-ttl-seconds"));
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("imdsv2-token".getBytes(StandardCharsets.UTF_8));
            exchange.close();
        })) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), metadataServer.tokenUri(), "us-east-1c");
        }
    }

    public void testTokenMetadataApiIsMisbehaving() throws Exception {
        try (var metadataServer = new MetadataServer("/metadata", exchange -> {
            assertNull(exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token"));
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("us-east-1c".getBytes(StandardCharsets.UTF_8));
            exchange.close();
        }, "/latest/api/token", HttpExchange::close)) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), metadataServer.tokenUri(), "us-east-1c");
        }
    }

    public void testTokenMetadataApiDoesNotRespond() throws Exception {
        try (var metadataServer = new MetadataServer("/metadata", exchange -> {
            assertNull(exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token"));
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("us-east-1c".getBytes(StandardCharsets.UTF_8));
            exchange.close();
        }, "/latest/api/token", ex -> {
            // Intentionally don't close the connection, so the client has to time out
        })) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), metadataServer.tokenUri(), "us-east-1c");
        }
    }

    public void testTokenMetadataApiIsNotAvailable() throws Exception {
        try (var metadataServer = metadataServerWithoutToken()) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), metadataServer.tokenUri(), "us-east-1c");
        }
    }

    public void testBogusTokenMetadataUrl() throws Exception {
        try (var metadataServer = metadataServerWithoutToken();) {
            assertNodeAttributes(Settings.EMPTY, metadataServer.metadataUri(), "bogus", "us-east-1c");
        }
    }

    public void testDefaultEndpoint() throws IOException {
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(Settings.EMPTY)) {
            final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).endpoint;
            assertThat(endpoint, is(""));
        }
    }

    public void testSpecificEndpoint() throws IOException {
        final Settings settings = Settings.builder().put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2.endpoint").build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings)) {
            final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).endpoint;
            assertThat(endpoint, is("ec2.endpoint"));
        }
    }

    public void testClientSettingsReInit() throws IOException {
        final MockSecureSettings mockSecure1 = new MockSecureSettings();
        mockSecure1.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_1");
        mockSecure1.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_key_1");
        final boolean mockSecure1HasSessionToken = randomBoolean();
        if (mockSecure1HasSessionToken) {
            mockSecure1.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_1");
        }
        mockSecure1.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_1");
        mockSecure1.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_1");
        final Settings settings1 = Settings.builder()
            .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy_host_1")
            .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 881)
            .put(Ec2ClientSettings.PROXY_SCHEME_SETTING.getKey(), "http")
            .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_1")
            .setSecureSettings(mockSecure1)
            .build();
        final MockSecureSettings mockSecure2 = new MockSecureSettings();
        mockSecure2.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_2");
        mockSecure2.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_key_2");
        final boolean mockSecure2HasSessionToken = randomBoolean();
        if (mockSecure2HasSessionToken) {
            mockSecure2.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_2");
        }
        mockSecure2.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_2");
        mockSecure2.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_2");
        final Settings settings2 = Settings.builder()
            .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy_host_2")
            .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 882)
            .put(Ec2ClientSettings.PROXY_SCHEME_SETTING.getKey(), "http")
            .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_2")
            .setSecureSettings(mockSecure2)
            .build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings1)) {
            try (AmazonEc2Reference clientReference = plugin.ec2Service.client()) {
                {
                    final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
                    assertThat(credentials.getAWSAccessKeyId(), is("ec2_access_1"));
                    assertThat(credentials.getAWSSecretKey(), is("ec2_secret_key_1"));
                    if (mockSecure1HasSessionToken) {
                        assertThat(credentials, instanceOf(BasicSessionCredentials.class));
                        assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("ec2_session_token_1"));
                    } else {
                        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
                    }
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(881));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyProtocol(), is(Protocol.HTTP));
                    assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_1"));
                }
                // reload secure settings2
                plugin.reload(settings2);
                // client is not released, it is still using the old settings
                {
                    final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
                    if (mockSecure1HasSessionToken) {
                        assertThat(credentials, instanceOf(BasicSessionCredentials.class));
                        assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("ec2_session_token_1"));
                    } else {
                        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
                    }
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(881));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyProtocol(), is(Protocol.HTTP));
                    assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_1"));
                }
            }
            try (AmazonEc2Reference clientReference = plugin.ec2Service.client()) {
                final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
                assertThat(credentials.getAWSAccessKeyId(), is("ec2_access_2"));
                assertThat(credentials.getAWSSecretKey(), is("ec2_secret_key_2"));
                if (mockSecure2HasSessionToken) {
                    assertThat(credentials, instanceOf(BasicSessionCredentials.class));
                    assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("ec2_session_token_2"));
                } else {
                    assertThat(credentials, instanceOf(BasicAWSCredentials.class));
                }
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_2"));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_2"));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_2"));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(882));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyProtocol(), is(Protocol.HTTP));
                assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_2"));
            }
        }
    }

    private static class Ec2DiscoveryPluginMock extends Ec2DiscoveryPlugin {

        Ec2DiscoveryPluginMock(Settings settings) {
            super(settings, new AwsEc2ServiceImpl() {
                @Override
                AmazonEC2 buildClient(AWSCredentialsProvider credentials, ClientConfiguration configuration, String endpoint) {
                    return new AmazonEC2Mock(credentials, configuration, endpoint);
                }
            });
        }
    }

    private static class AmazonEC2Mock extends AbstractAmazonEC2 {

        String endpoint;
        final AWSCredentialsProvider credentials;
        final ClientConfiguration configuration;

        AmazonEC2Mock(AWSCredentialsProvider credentials, ClientConfiguration configuration, String endpoint) {
            this.credentials = credentials;
            this.configuration = configuration;
            this.endpoint = endpoint;
        }

        @Override
        public void shutdown() {}
    }

    @SuppressForbidden(reason = "Uses an HttpServer to emulate the Instance Metadata Service")
    private static MetadataServer metadataServerWithoutToken() throws IOException {
        return new MetadataServer("/metadata", exchange -> {
            assertNull(exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token"));
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("us-east-1c".getBytes(StandardCharsets.UTF_8));
            exchange.close();
        });
    }

    @SuppressForbidden(reason = "Uses an HttpServer to emulate the Instance Metadata Service")
    private static class MetadataServer implements AutoCloseable {

        private final HttpServer httpServer;

        private MetadataServer(String metadataPath, HttpHandler metadataHandler) throws IOException {
            this(metadataPath, metadataHandler, null, null);
        }

        private MetadataServer(String metadataPath, HttpHandler metadataHandler, String tokenPath, HttpHandler tokenHandler)
            throws IOException {
            httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            httpServer.createContext(metadataPath, metadataHandler);
            if (tokenPath != null && tokenHandler != null) {
                httpServer.createContext(tokenPath, tokenHandler);
            }
            httpServer.start();
        }

        @Override
        public void close() throws Exception {
            httpServer.stop(0);
        }

        private String metadataUri() {
            return "http://" + httpServer.getAddress().getHostString() + ":" + httpServer.getAddress().getPort() + "/metadata";
        }

        private String tokenUri() {
            return "http://" + httpServer.getAddress().getHostString() + ":" + httpServer.getAddress().getPort() + "/latest/api/token";
        }
    }
}
