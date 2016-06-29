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
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * This test doesn't extend {@link ESIntegTestCase} as the internal cluster ignores system properties
 * all the time, while we need to make the tribe node accept them in this case, so that we can verify that they are not read again as part
 * of the tribe client nodes initialization. Note that the started nodes will obey to the 'node.mode' settings as the internal cluster does.
 */
@SuppressForbidden(reason = "modifies system properties intentionally")
public class TribeUnitTests extends ESTestCase {

    private static Node tribe1;
    private static Node tribe2;

    private static final String NODE_MODE = InternalTestCluster.configuredNodeMode();

    @BeforeClass
    public static void createTribes() {
        Settings baseSettings = Settings.builder()
            .put("http.enabled", false)
            .put(Node.NODE_MODE_SETTING.getKey(), NODE_MODE)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();

        tribe1 = new TribeClientNode(
            Settings.builder()
                .put(baseSettings)
                .put("cluster.name", "tribe1")
                .put("node.name", "tribe1_node")
                    .put(DiscoveryNodeService.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
                .build()).start();
        tribe2 = new TribeClientNode(
            Settings.builder()
                .put(baseSettings)
                .put("cluster.name", "tribe2")
                .put("node.name", "tribe2_node")
                    .put(DiscoveryNodeService.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
                .build()).start();
    }

    @AfterClass
    public static void closeTribes() throws IOException {
        IOUtils.close(tribe1, tribe2);
        tribe1 = null;
        tribe2 = null;
    }

    public void testThatTribeClientsIgnoreGlobalConfig() throws Exception {
        Path pathConf = getDataPath("elasticsearch.yml").getParent();
        Settings settings = Settings
            .builder()
            .put(Environment.PATH_CONF_SETTING.getKey(), pathConf)
            .build();
        assertTribeNodeSuccessfullyCreated(settings);
    }

    private static void assertTribeNodeSuccessfullyCreated(Settings extraSettings) throws Exception {
        //tribe node doesn't need the node.mode setting, as it's forced local internally anyways. The tribe clients do need it to make sure
        //they can find their corresponding tribes using the proper transport
        Settings settings = Settings.builder().put("http.enabled", false).put("node.name", "tribe_node")
                .put("tribe.t1.node.mode", NODE_MODE).put("tribe.t2.node.mode", NODE_MODE)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put(Node.NODE_MODE_SETTING.getKey(), NODE_MODE)
                .put(extraSettings).build();

        try (Node node = new Node(settings).start()) {
            try (Client client = node.client()) {
                assertBusy(new Runnable() {
                    @Override
                    public void run() {
                        ClusterState state = client.admin().cluster().prepareState().clear().setNodes(true).get().getState();
                        assertThat(state.getClusterName().value(), equalTo("tribe_node_cluster"));
                        assertThat(state.getNodes().getSize(), equalTo(5));
                        for (DiscoveryNode discoveryNode : state.getNodes()) {
                            assertThat(discoveryNode.getName(), either(equalTo("tribe1_node")).or(equalTo("tribe2_node"))
                                    .or(equalTo("tribe_node")).or(equalTo("tribe_node/t1")).or(equalTo("tribe_node/t2")));
                        }
                    }
                });
            }
        }
    }
}
