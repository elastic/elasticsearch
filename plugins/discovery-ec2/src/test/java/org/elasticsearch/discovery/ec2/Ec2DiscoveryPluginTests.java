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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

public class Ec2DiscoveryPluginTests extends ESTestCase {

    private void assertNodeAttributes(Settings settings, String expected) {
        final var availabilityZoneNodeAttributeSettings = Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(
            Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), true).put(settings).build()
        );
        if (expected == null) {
            assertTrue(availabilityZoneNodeAttributeSettings.isEmpty());
        } else {
            assertEquals(expected, availabilityZoneNodeAttributeSettings.get(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone"));
        }
    }

    public void testNodeAttributesDisabled() {
        assertNodeAttributes(Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), false).build(), null);
    }

    public void testNodeAttributes() {
        final var availabilityZone = randomIdentifier();
        Ec2ImdsHttpFixture.runWithFixture(
            new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V2).availabilityZoneSupplier(() -> availabilityZone),
            ec2ImdsHttpFixture -> {
                try (var ignored = Ec2ImdsHttpFixture.withEc2MetadataServiceEndpointOverride(ec2ImdsHttpFixture.getAddress())) {
                    assertNodeAttributes(Settings.EMPTY, availabilityZone);
                }
            }
        );
    }

    // TODO NOMERGE do we really need these tests? They don't work with SDKv2, there is no notion of ClientConfiguration any more, so
    // really all we can test is that our settings infrastructure works. The end-to-end tests work ok, but we don't e.g. have any
    // end-to-end tests involving proxies.
    //
    // public void testDefaultEndpoint() throws IOException {
    // try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(Settings.EMPTY)) {
    // final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).endpoint;
    // assertThat(endpoint, is(""));
    // }
    // }
    //
    // public void testSpecificEndpoint() throws IOException {
    // final Settings settings = Settings.builder().put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2.endpoint").build();
    // try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings)) {
    // final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).endpoint;
    // assertThat(endpoint, is("ec2.endpoint"));
    // }
    // }

    // public void testClientSettingsReInit() throws IOException {
    // final MockSecureSettings mockSecure1 = new MockSecureSettings();
    // mockSecure1.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_1");
    // mockSecure1.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_key_1");
    // final boolean mockSecure1HasSessionToken = randomBoolean();
    // if (mockSecure1HasSessionToken) {
    // mockSecure1.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_1");
    // }
    // mockSecure1.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_1");
    // mockSecure1.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_1");
    // final Settings settings1 = Settings.builder()
    // .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy_host_1")
    // .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 881)
    // .put(Ec2ClientSettings.PROXY_SCHEME_SETTING.getKey(), "http")
    // .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_1")
    // .setSecureSettings(mockSecure1)
    // .build();
    // final MockSecureSettings mockSecure2 = new MockSecureSettings();
    // mockSecure2.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_2");
    // mockSecure2.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_key_2");
    // final boolean mockSecure2HasSessionToken = randomBoolean();
    // if (mockSecure2HasSessionToken) {
    // mockSecure2.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_2");
    // }
    // mockSecure2.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_2");
    // mockSecure2.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_2");
    // final Settings settings2 = Settings.builder()
    // .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy_host_2")
    // .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 882)
    // .put(Ec2ClientSettings.PROXY_SCHEME_SETTING.getKey(), "http")
    // .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_2")
    // .setSecureSettings(mockSecure2)
    // .build();
    // try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings1)) {
    // try (AmazonEc2Reference clientReference = plugin.ec2Service.client()) {
    // {
    // final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
    // assertThat(credentials.getAWSAccessKeyId(), is("ec2_access_1"));
    // assertThat(credentials.getAWSSecretKey(), is("ec2_secret_key_1"));
    // if (mockSecure1HasSessionToken) {
    // assertThat(credentials, instanceOf(BasicSessionCredentials.class));
    // assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("ec2_session_token_1"));
    // } else {
    // assertThat(credentials, instanceOf(BasicAWSCredentials.class));
    // }
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_1"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_1"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_1"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(881));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyProtocol(), is(Protocol.HTTP));
    // assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_1"));
    // }
    // // reload secure settings2
    // plugin.reload(settings2);
    // // client is not released, it is still using the old settings
    // {
    // final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
    // if (mockSecure1HasSessionToken) {
    // assertThat(credentials, instanceOf(BasicSessionCredentials.class));
    // assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("ec2_session_token_1"));
    // } else {
    // assertThat(credentials, instanceOf(BasicAWSCredentials.class));
    // }
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_1"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_1"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_1"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(881));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyProtocol(), is(Protocol.HTTP));
    // assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_1"));
    // }
    // }
    // try (AmazonEc2Reference clientReference = plugin.ec2Service.client()) {
    // final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
    // assertThat(credentials.getAWSAccessKeyId(), is("ec2_access_2"));
    // assertThat(credentials.getAWSSecretKey(), is("ec2_secret_key_2"));
    // if (mockSecure2HasSessionToken) {
    // assertThat(credentials, instanceOf(BasicSessionCredentials.class));
    // assertThat(((BasicSessionCredentials) credentials).getSessionToken(), is("ec2_session_token_2"));
    // } else {
    // assertThat(credentials, instanceOf(BasicAWSCredentials.class));
    // }
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_2"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_2"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_2"));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(882));
    // assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyProtocol(), is(Protocol.HTTP));
    // assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_2"));
    // }
    // }
    // }
    //
    // private static class Ec2DiscoveryPluginMock extends Ec2DiscoveryPlugin {
    //
    // Ec2DiscoveryPluginMock(Settings settings) {
    // super(settings, new AwsEc2ServiceImpl() {
    //
    // @Override
    // Ec2Client buildClient(AwsCredentialsProvider credentials, Ec2ClientSettings clientSettings) {
    // return super.buildClient(credentials, clientSettings);
    // }
    //
    // @Override
    // AmazonEC2 buildClient(AWSCredentialsProvider credentials, ClientConfiguration configuration, String endpoint) {
    // return new AmazonEC2Mock(credentials, configuration, endpoint);
    // }
    // });
    // }
    // }
    //
    // private static class AmazonEC2Mock extends AbstractAmazonEC2 {
    //
    // String endpoint;
    // final AWSCredentialsProvider credentials;
    // final ClientConfiguration configuration;
    //
    // AmazonEC2Mock(AWSCredentialsProvider credentials, ClientConfiguration configuration, String endpoint) {
    // this.credentials = credentials;
    // this.configuration = configuration;
    // this.endpoint = endpoint;
    // }
    //
    // @Override
    // public void shutdown() {}
    // }
    //
    // @SuppressForbidden(reason = "Uses an HttpServer to emulate the Instance Metadata Service")
    // private static MetadataServer metadataServerWithoutToken() throws IOException {
    // return new MetadataServer("/metadata", exchange -> {
    // assertNull(exchange.getRequestHeaders().getFirst("X-aws-ec2-metadata-token"));
    // exchange.sendResponseHeaders(200, 0);
    // exchange.getResponseBody().write("us-east-1c".getBytes(StandardCharsets.UTF_8));
    // exchange.close();
    // });
    // }
    //
    // @SuppressForbidden(reason = "Uses an HttpServer to emulate the Instance Metadata Service")
    // private static class MetadataServer implements AutoCloseable {
    //
    // private final HttpServer httpServer;
    //
    // private MetadataServer(String metadataPath, HttpHandler metadataHandler) throws IOException {
    // this(metadataPath, metadataHandler, null, null);
    // }
    //
    // private MetadataServer(String metadataPath, HttpHandler metadataHandler, String tokenPath, HttpHandler tokenHandler)
    // throws IOException {
    // httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    // httpServer.createContext(metadataPath, metadataHandler);
    // if (tokenPath != null && tokenHandler != null) {
    // httpServer.createContext(tokenPath, tokenHandler);
    // }
    // httpServer.start();
    // }
    //
    // @Override
    // public void close() throws Exception {
    // httpServer.stop(0);
    // }
    //
    // private String metadataUri() {
    // return "http://" + httpServer.getAddress().getHostString() + ":" + httpServer.getAddress().getPort() + "/metadata";
    // }
    //
    // private String tokenUri() {
    // return "http://" + httpServer.getAddress().getHostString() + ":" + httpServer.getAddress().getPort() + "/latest/api/token";
    // }
    // }
}
