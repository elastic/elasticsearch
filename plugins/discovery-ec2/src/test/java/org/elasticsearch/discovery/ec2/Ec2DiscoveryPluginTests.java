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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.elasticsearch.discovery.ec2.AwsEc2Service;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

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
            final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).getEndpoint();
            assertThat(endpoint, nullValue());
        }
    }

    public void testSpecificEndpoint() throws IOException {
        final Settings settings = Settings.builder().put(EC2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2.endpoint").build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings)) {
            final String endpoint = ((AmazonEC2Mock) plugin.ec2Service.client().client()).getEndpoint();
            assertThat(endpoint, is("ec2.endpoint"));
        }
    }
}
