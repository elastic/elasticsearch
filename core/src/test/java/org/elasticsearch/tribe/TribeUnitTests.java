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

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * This test doesn't extend {@link ESIntegTestCase} as the internal cluster ignores system properties
 * all the time, while we need to make the tribe node accept them in this case, so that we can verify that they are not read again as part
 * of the tribe client nodes initialization. Note that the started nodes will obey to the 'node.mode' settings as the internal cluster does.
 */
public class TribeUnitTests extends ESTestCase {

    private static Node tribe1;
    private static Node tribe2;

    private static final String NODE_MODE = InternalTestCluster.configuredNodeMode();

    @BeforeClass
    public static void createTribes() {
        Settings baseSettings = Settings.builder()
            .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true)
            .put("http.enabled", false)
            .put("node.mode", NODE_MODE)
            .put("path.home", createTempDir()).build();

        tribe1 = NodeBuilder.nodeBuilder().settings(Settings.builder().put(baseSettings).put("cluster.name", "tribe1").put("node.name", "tribe1_node")).node();
        tribe2 = NodeBuilder.nodeBuilder().settings(Settings.builder().put(baseSettings).put("cluster.name", "tribe2").put("node.name", "tribe2_node")).node();
    }

    @AfterClass
    public static void closeTribes() {
        tribe1.close();
        tribe1 = null;
        tribe2.close();
        tribe2 = null;
    }

    public void testThatTribeClientsIgnoreGlobalSysProps() throws Exception {
        System.setProperty("es.cluster.name", "tribe_node_cluster");
        System.setProperty("es.tribe.t1.cluster.name", "tribe1");
        System.setProperty("es.tribe.t2.cluster.name", "tribe2");

        try {
            assertTribeNodeSuccesfullyCreated(Settings.EMPTY);
        } finally {
            System.clearProperty("es.cluster.name");
            System.clearProperty("es.tribe.t1.cluster.name");
            System.clearProperty("es.tribe.t2.cluster.name");
        }
    }

    public void testThatTribeClientsIgnoreGlobalConfig() throws Exception {
        Path pathConf = getDataPath("elasticsearch.yml").getParent();
        Settings settings = Settings.builder().put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true).put("path.conf", pathConf).build();
        assertTribeNodeSuccesfullyCreated(settings);
    }

    private static void assertTribeNodeSuccesfullyCreated(Settings extraSettings) throws Exception {
        //tribe node doesn't need the node.mode setting, as it's forced local internally anyways. The tribe clients do need it to make sure
        //they can find their corresponding tribes using the proper transport
        Settings settings = Settings.builder().put("http.enabled", false).put("node.name", "tribe_node")
                .put("tribe.t1.node.mode", NODE_MODE).put("tribe.t2.node.mode", NODE_MODE)
                .put("path.home", createTempDir()).put(extraSettings).build();

        try (Node node = NodeBuilder.nodeBuilder().settings(settings).node()) {
            try (Client client = node.client()) {
                assertBusy(new Runnable() {
                    @Override
                    public void run() {
                        ClusterState state = client.admin().cluster().prepareState().clear().setNodes(true).get().getState();
                        assertThat(state.getClusterName().value(), equalTo("tribe_node_cluster"));
                        assertThat(state.getNodes().getSize(), equalTo(5));
                        for (DiscoveryNode discoveryNode : state.getNodes()) {
                            assertThat(discoveryNode.getName(), either(equalTo("tribe1_node")).or(equalTo("tribe2_node")).or(equalTo("tribe_node"))
                                    .or(equalTo("tribe_node/t1")).or(equalTo("tribe_node/t2")));
                        }
                    }
                });
            }
        }
    }
}
