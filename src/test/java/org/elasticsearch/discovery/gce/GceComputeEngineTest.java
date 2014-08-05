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

package org.elasticsearch.discovery.gce;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.gce.mock.GceComputeServiceTwoNodesDifferentTagsMock;
import org.elasticsearch.discovery.gce.mock.GceComputeServiceTwoNodesSameTagsMock;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;

@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.TEST,
        numDataNodes = 0,
        numClientNodes = 0,
        transportClientRatio = 0.0)
public class GceComputeEngineTest extends ElasticsearchIntegrationTest {

    public static int getPort(int nodeOrdinal) {
        try {
            return PropertiesHelper.getAsInt("plugin.port")
                    + nodeOrdinal * 10;
        } catch (IOException e) {
        }

        return -1;
    }

    protected void checkNumberOfNodes(int expected) {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertNotNull(nodeInfos);
        assertNotNull(nodeInfos.getNodes());
        assertEquals(expected, nodeInfos.getNodes().length);
    }

    protected Settings settingsBuilder(int nodeOrdinal, Class<? extends GceComputeService> mock, Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("discovery.type", "gce")
                .put("cloud.gce.api.impl", mock)
                        // We need the network to make the mock working
                .put("node.mode", "network")
                        // Make the tests run faster
                .put("discovery.zen.join.timeout", "100ms")
                .put("discovery.zen.ping.timeout", "10ms")
                .put("discovery.initial_state_timeout", "300ms")
                        // We use a specific port for each node
                .put("transport.tcp.port", getPort(nodeOrdinal))
                        // We disable http
                .put("http.enabled", false)
                        // We force plugin loading
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put(settings)
                .put(super.nodeSettings(nodeOrdinal));

        return builder.build();
    }

    protected void startNode(int nodeOrdinal, Class<? extends GceComputeService> mock, Settings settings) {
        logger.info("--> start node #{}, mock [{}], settings [{}]", nodeOrdinal, mock.getSimpleName(), settings.getAsMap());
        internalCluster().startNode(settingsBuilder(
                nodeOrdinal,
                mock,
                settings));
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().masterNodeId(), notNullValue());
    }

    @Test @Ignore
    public void nodes_with_different_tags_and_no_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesDifferentTagsMock.class,
                ImmutableSettings.EMPTY);
        startNode(2,
                GceComputeServiceTwoNodesDifferentTagsMock.class,
                ImmutableSettings.EMPTY);

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    /**
     * We need to ignore this test from elasticsearch version 1.2.1 as
     * expected nodes running is 2 and this test will create 2 clusters with one node each.
     * @see org.elasticsearch.test.ElasticsearchIntegrationTest#ensureClusterSizeConsistency()
     * TODO Reactivate when it will be possible to set the number of running nodes
     */
    @Test @Ignore
    public void nodes_with_different_tags_and_one_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesDifferentTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", "elasticsearch").build());
        startNode(2,
                GceComputeServiceTwoNodesDifferentTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", "elasticsearch").build());

        // We expect having 1 nodes as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    /**
     * We need to ignore this test from elasticsearch version 1.2.1 as
     * expected nodes running is 2 and this test will create 2 clusters with one node each.
     * @see org.elasticsearch.test.ElasticsearchIntegrationTest#ensureClusterSizeConsistency()
     * TODO Reactivate when it will be possible to set the number of running nodes
     */
    @Test @Ignore
    public void nodes_with_different_tags_and_two_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesDifferentTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", Lists.newArrayList("elasticsearch", "dev")).build());
        startNode(2,
                GceComputeServiceTwoNodesDifferentTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", Lists.newArrayList("elasticsearch", "dev")).build());

        // We expect having 1 nodes as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    @Test @Ignore
    public void nodes_with_same_tags_and_no_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesSameTagsMock.class,
                ImmutableSettings.EMPTY);
        startNode(2,
                GceComputeServiceTwoNodesSameTagsMock.class,
                ImmutableSettings.EMPTY);

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    @Test @Ignore
    public void nodes_with_same_tags_and_one_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesSameTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", "elasticsearch").build());
        startNode(2,
                GceComputeServiceTwoNodesSameTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", "elasticsearch").build());

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    @Test @Ignore
    public void nodes_with_same_tags_and_two_tags_set() {
        startNode(1,
                GceComputeServiceTwoNodesSameTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", Lists.newArrayList("elasticsearch", "dev")).build());
        startNode(2,
                GceComputeServiceTwoNodesSameTagsMock.class,
                ImmutableSettings.settingsBuilder().put("discovery.gce.tags", Lists.newArrayList("elasticsearch", "dev")).build());

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }
}
