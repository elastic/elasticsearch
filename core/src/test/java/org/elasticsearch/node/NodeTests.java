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
package org.elasticsearch.node;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class NodeTests extends ESTestCase {

    public void testNodeName() throws IOException {
        final String name = randomBoolean() ? randomAlphaOfLength(10) : null;
        Settings.Builder settings = baseSettings();
        if (name != null) {
            settings.put(Node.NODE_NAME_SETTING.getKey(), name);
        }
        try (Node node = new MockNode(settings.build(), Collections.singleton(getTestTransportPlugin()))) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            if (name == null) {
                assertThat(Node.NODE_NAME_SETTING.get(nodeSettings), equalTo(node.getNodeEnvironment().nodeId().substring(0, 7)));
            } else {
                assertThat(Node.NODE_NAME_SETTING.get(nodeSettings), equalTo(name));
            }
        }
    }

    public static class CheckPlugin extends Plugin {
        public static final BootstrapCheck CHECK = context -> BootstrapCheck.BootstrapCheckResult.success();

        @Override
        public List<BootstrapCheck> getBootstrapChecks() {
            return Collections.singletonList(CHECK);
        }
    }

    public void testLoadPluginBootstrapChecks() throws IOException {
        final String name = randomBoolean() ? randomAlphaOfLength(10) : null;
        Settings.Builder settings = baseSettings();
        if (name != null) {
            settings.put(Node.NODE_NAME_SETTING.getKey(), name);
        }
        AtomicBoolean executed = new AtomicBoolean(false);
        try (Node node = new MockNode(settings.build(), Arrays.asList(getTestTransportPlugin(), CheckPlugin.class)) {
            @Override
            protected void validateNodeBeforeAcceptingRequests(BootstrapContext context, BoundTransportAddress boundTransportAddress,
                                                               List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
                assertEquals(1, bootstrapChecks.size());
                assertSame(CheckPlugin.CHECK, bootstrapChecks.get(0));
                executed.set(true);
                throw new NodeValidationException("boom");
            }
        }) {
            expectThrows(NodeValidationException.class, () -> node.start());
            assertTrue(executed.get());
        }
    }

    public void testWarnIfPreRelease() {
        final Logger logger = mock(Logger.class);

        final int id = randomIntBetween(1, 9) * 1000000;
        final Version releaseVersion = Version.fromId(id + 99);
        final Version preReleaseVersion = Version.fromId(id + randomIntBetween(0, 98));

        Node.warnIfPreRelease(releaseVersion, false, logger);
        verifyNoMoreInteractions(logger);

        reset(logger);
        Node.warnIfPreRelease(releaseVersion, true, logger);
        verify(logger).warn(
            "version [{}] is a pre-release version of Elasticsearch and is not suitable for production", releaseVersion + "-SNAPSHOT");

        reset(logger);
        final boolean isSnapshot = randomBoolean();
        Node.warnIfPreRelease(preReleaseVersion, isSnapshot, logger);
        verify(logger).warn(
            "version [{}] is a pre-release version of Elasticsearch and is not suitable for production",
            preReleaseVersion + (isSnapshot ? "-SNAPSHOT" : ""));

    }

    public void testNodeAttributes() throws IOException {
        String attr = randomAlphaOfLength(5);
        Settings.Builder settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), Collections.singleton(getTestTransportPlugin()))) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            assertEquals(attr, Node.NODE_ATTRIBUTES.getAsMap(nodeSettings).get("test_attr"));
        }

        // leading whitespace not allowed
        attr = " leading";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), Collections.singleton(getTestTransportPlugin()))) {
            fail("should not allow a node attribute with leading whitespace");
        } catch (IllegalArgumentException e) {
            assertEquals("node.attr.test_attr cannot have leading or trailing whitespace [ leading]", e.getMessage());
        }

        // trailing whitespace not allowed
        attr = "trailing ";
        settings = baseSettings().put(Node.NODE_ATTRIBUTES.getKey() + "test_attr", attr);
        try (Node node = new MockNode(settings.build(), Collections.singleton(getTestTransportPlugin()))) {
            fail("should not allow a node attribute with trailing whitespace");
        } catch (IllegalArgumentException e) {
            assertEquals("node.attr.test_attr cannot have leading or trailing whitespace [trailing ]", e.getMessage());
        }
    }

    private static Settings.Builder baseSettings() {
        final Path tempDir = createTempDir();
        return Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", randomLong()))
                .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
                .put(Node.NODE_DATA_SETTING.getKey(), true);
    }


}
