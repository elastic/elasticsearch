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

package org.elasticsearch.tribe;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * This test doesn't extend {@link ESIntegTestCase} as the internal cluster ignores system properties
 * all the time, while we need to make the tribe node accept them in this case, so that we can verify that they are not read again as part
 * of the tribe client nodes initialization. Note that the started nodes will obey to the 'node.mode' settings as the internal cluster does.
 */
@SuppressForbidden(reason = "modifies system properties intentionally")
public class TribeUnitTests extends ESTestCase {

    private static List<Class<? extends Plugin>> classpathPlugins;
    private static Node tribe1;
    private static Node tribe2;

    @BeforeClass
    public static void createTribes() throws NodeValidationException {
        Settings baseSettings = Settings.builder()
            .put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .put("transport.type", getTestTransportType())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), 2)
            .build();

        classpathPlugins = Arrays.asList(TribeAwareTestZenDiscoveryPlugin.class, MockTribePlugin.class, getTestTransportPlugin());

        tribe1 = new MockNode(
            Settings.builder()
                .put(baseSettings)
                .put("cluster.name", "tribe1")
                .put("node.name", "tribe1_node")
                    .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
                .build(), classpathPlugins).start();
        tribe2 = new MockNode(
            Settings.builder()
                .put(baseSettings)
                .put("cluster.name", "tribe2")
                .put("node.name", "tribe2_node")
                    .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
                .build(), classpathPlugins).start();
    }

    @AfterClass
    public static void closeTribes() throws IOException {
        IOUtils.close(tribe1, tribe2);
        classpathPlugins = null;
        tribe1 = null;
        tribe2 = null;
    }

    public static class TribeAwareTestZenDiscoveryPlugin extends TestZenDiscovery.TestPlugin {

        public TribeAwareTestZenDiscoveryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Settings additionalSettings() {
            if (settings.getGroups("tribe", true).isEmpty()) {
                return super.additionalSettings();
            } else {
                return Settings.EMPTY;
            }
        }
    }

    public static class MockTribePlugin extends TribePlugin {

        public MockTribePlugin(Settings settings) {
            super(settings);
        }

        protected Function<Settings, Node> nodeBuilder(Path configPath) {
            return settings -> new MockNode(new Environment(settings, configPath), classpathPlugins);
        }

    }

    public void testThatTribeClientsIgnoreGlobalConfig() throws Exception {
        assertTribeNodeSuccessfullyCreated(getDataPath("elasticsearch.yml").getParent());
        assertWarnings("tribe nodes are deprecated in favor of cross-cluster search and will be removed in Elasticsearch 7.0.0");
    }

    private static void assertTribeNodeSuccessfullyCreated(Path configPath) throws Exception {
        // the tribe clients do need it to make sure they can find their corresponding tribes using the proper transport
        Settings settings = Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), false).put("node.name", "tribe_node")
                .put("transport.type", getTestTransportType())
                .put("tribe.t1.transport.type", getTestTransportType())
                .put("tribe.t2.transport.type", getTestTransportType())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();

        try (Node node = new MockNode(settings, classpathPlugins, configPath).start()) {
            try (Client client = node.client()) {
                assertBusy(() -> {
                    ClusterState state = client.admin().cluster().prepareState().clear().setNodes(true).get().getState();
                    assertThat(state.getClusterName().value(), equalTo("tribe_node_cluster"));
                    assertThat(state.getNodes().getSize(), equalTo(5));
                    for (DiscoveryNode discoveryNode : state.getNodes()) {
                        assertThat(discoveryNode.getName(), either(equalTo("tribe1_node")).or(equalTo("tribe2_node"))
                                .or(equalTo("tribe_node")).or(equalTo("tribe_node/t1")).or(equalTo("tribe_node/t2")));
                    }
                });
            }
        }
    }

}
