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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class Ec2DiscoveryPluginTests extends ESTestCase {

    private Settings getNodeAttributes(Settings settings, String url) {
        final Settings realSettings = Settings.builder()
            .put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), true)
            .put(settings).build();
        return Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(realSettings, url);
    }

    private void assertNodeAttributes(Settings settings, String url, String expected) {
        final Settings additional = getNodeAttributes(settings, url);
        if (expected == null) {
            assertTrue(additional.isEmpty());
        } else {
            assertEquals(expected, additional.get(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone"));
        }
    }

    public void testNodeAttributesDisabled() {
        final Settings settings = Settings.builder()
            .put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), false).build();
        assertNodeAttributes(settings, "bogus", null);
    }

    public void testNodeAttributes() throws Exception {
        final Path zoneUrl = createTempFile();
        Files.write(zoneUrl, Arrays.asList("us-east-1c"));
        assertNodeAttributes(Settings.EMPTY, zoneUrl.toUri().toURL().toString(), "us-east-1c");
    }

    public void testNodeAttributesBogusUrl() {
        final UncheckedIOException e = expectThrows(UncheckedIOException.class, () ->
            getNodeAttributes(Settings.EMPTY, "bogus")
        );
        assertNotNull(e.getCause());
        final String msg = e.getCause().getMessage();
        assertTrue(msg, msg.contains("no protocol: bogus"));
    }

    public void testNodeAttributesEmpty() throws Exception {
        final Path zoneUrl = createTempFile();
        final IllegalStateException e = expectThrows(IllegalStateException.class, () ->
            getNodeAttributes(Settings.EMPTY, zoneUrl.toUri().toURL().toString())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("no ec2 metadata returned"));
    }

    public void testNodeAttributesErrorLenient() throws Exception {
        final Path dne = createTempDir().resolve("dne");
        assertNodeAttributes(Settings.EMPTY, dne.toUri().toURL().toString(), null);
    }

    public void testDefaultEndpoint() throws IOException {
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(Settings.EMPTY)) {
            final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).endpoint;
            assertThat(endpoint, nullValue());
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
        mockSecure1.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_1");
        final boolean mockSecure1HasSessionToken = randomBoolean();
        if (mockSecure1HasSessionToken) {
            mockSecure1.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_1");
        }
        mockSecure1.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_1");
        mockSecure1.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_1");
        final Settings settings1 = Settings.builder()
                .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy_host_1")
                .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 881)
                .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_1")
                .setSecureSettings(mockSecure1)
                .build();
        final MockSecureSettings mockSecure2 = new MockSecureSettings();
        mockSecure2.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_2");
        mockSecure2.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_2");
        final boolean mockSecure2HasSessionToken = randomBoolean();
        if (mockSecure2HasSessionToken) {
            mockSecure2.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_2");
        }
        mockSecure2.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_2");
        mockSecure2.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_2");
        final Settings settings2 = Settings.builder()
                .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy_host_2")
                .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 882)
                .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_2")
                .setSecureSettings(mockSecure2)
                .build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings1)) {
            try (AmazonEc2Reference clientReference = plugin.ec2Service.client()) {
                {
                    final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
                    assertThat(credentials.getAWSAccessKeyId(), is("ec2_access_1"));
                    assertThat(credentials.getAWSSecretKey(), is("ec2_secret_1"));
                    if (mockSecure1HasSessionToken) {
                        assertThat(credentials, instanceOf(BasicSessionCredentials.class));
                        assertThat(((BasicSessionCredentials)credentials).getSessionToken(), is("ec2_session_token_1"));
                    } else {
                        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
                    }
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(881));
                    assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_1"));
                }
                // reload secure settings2
                plugin.reload(settings2);
                // client is not released, it is still using the old settings
                {
                    final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
                    if (mockSecure1HasSessionToken) {
                        assertThat(credentials, instanceOf(BasicSessionCredentials.class));
                        assertThat(((BasicSessionCredentials)credentials).getSessionToken(), is("ec2_session_token_1"));
                    } else {
                        assertThat(credentials, instanceOf(BasicAWSCredentials.class));
                    }
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_1"));
                    assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(881));
                    assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_1"));
                }
            }
            try (AmazonEc2Reference clientReference = plugin.ec2Service.client()) {
                final AWSCredentials credentials = ((AmazonEC2Mock) clientReference.client()).credentials.getCredentials();
                assertThat(credentials.getAWSAccessKeyId(), is("ec2_access_2"));
                assertThat(credentials.getAWSSecretKey(), is("ec2_secret_2"));
                if (mockSecure2HasSessionToken) {
                    assertThat(credentials, instanceOf(BasicSessionCredentials.class));
                    assertThat(((BasicSessionCredentials)credentials).getSessionToken(), is("ec2_session_token_2"));
                } else {
                    assertThat(credentials, instanceOf(BasicAWSCredentials.class));
                }
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyUsername(), is("proxy_username_2"));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPassword(), is("proxy_password_2"));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyHost(), is("proxy_host_2"));
                assertThat(((AmazonEC2Mock) clientReference.client()).configuration.getProxyPort(), is(882));
                assertThat(((AmazonEC2Mock) clientReference.client()).endpoint, is("ec2_endpoint_2"));
            }
        }
    }
}
