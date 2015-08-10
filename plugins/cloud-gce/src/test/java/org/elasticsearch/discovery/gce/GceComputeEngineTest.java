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

import com.google.common.collect.Lists;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cloud.gce.GceComputeService.Fields;
import org.elasticsearch.cloud.gce.GceComputeServiceTwoNodesDifferentTagsMock;
import org.elasticsearch.cloud.gce.GceComputeServiceTwoNodesOneZoneMock;
import org.elasticsearch.cloud.gce.GceComputeServiceTwoNodesSameTagsMock;
import org.elasticsearch.cloud.gce.GceComputeServiceTwoNodesTwoZonesMock;
import org.elasticsearch.cloud.gce.GceComputeServiceZeroNodeMock;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.cloud.gce.CloudGcePlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(
        scope = ESIntegTestCase.Scope.TEST,
        numDataNodes = 0,
        numClientNodes = 0,
        transportClientRatio = 0.0)
@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/12622")
public class GceComputeEngineTest extends ESIntegTestCase {

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

    protected Settings settingsBuilder(int nodeOrdinal, String mockClass, Settings settings) {
        Settings.Builder builder = Settings.settingsBuilder()
                .put("discovery.type", "gce")
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
                .extendArray("plugin.types", CloudGcePlugin.class.getName(), mockClass)
                .put(settings)
                .put(super.nodeSettings(nodeOrdinal));

        return builder.build();
    }

    protected void startNode(int nodeOrdinal, String mockClass, Settings settings) {
        logger.info("--> start node #{}, mock [{}], settings [{}]", nodeOrdinal, mockClass, settings.getAsMap());
        internalCluster().startNode(settingsBuilder(
            nodeOrdinal,
            mockClass,
            settings));
        assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().masterNodeId(), notNullValue());
    }

    @Test
    public void nodes_with_different_tags_and_no_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesDifferentTagsMock.Plugin.class.getName(),
                Settings.EMPTY);
        startNode(2,
                GceComputeServiceTwoNodesDifferentTagsMock.Plugin.class.getName(),
                Settings.EMPTY);

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    /**
     * We need to ignore this test from elasticsearch version 1.2.1 as
     * expected nodes running is 2 and this test will create 2 clusters with one node each.
     * @see ESIntegTestCase#ensureClusterSizeConsistency()
     * TODO Reactivate when it will be possible to set the number of running nodes
     */
    @Test
    public void nodes_with_different_tags_and_one_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesDifferentTagsMock.Plugin.class.getName(),
                Settings.settingsBuilder().put(Fields.TAGS, "elasticsearch").build());
        startNode(2,
                GceComputeServiceTwoNodesDifferentTagsMock.Plugin.class.getName(),
                Settings.settingsBuilder().put(Fields.TAGS, "elasticsearch").build());

        // We expect having 1 nodes as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    /**
     * We need to ignore this test from elasticsearch version 1.2.1 as
     * expected nodes running is 2 and this test will create 2 clusters with one node each.
     * @see ESIntegTestCase#ensureClusterSizeConsistency()
     * TODO Reactivate when it will be possible to set the number of running nodes
     */
    @Test
    public void nodes_with_different_tags_and_two_tag_set() {
        startNode(1,
            GceComputeServiceTwoNodesDifferentTagsMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.TAGS, Lists.newArrayList("elasticsearch", "dev")).build());
        startNode(2,
            GceComputeServiceTwoNodesDifferentTagsMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.TAGS, Lists.newArrayList("elasticsearch", "dev")).build());

        // We expect having 1 nodes as part of the cluster, let's test that
        checkNumberOfNodes(1);
    }

    @Test
    public void nodes_with_same_tags_and_no_tag_set() {
        startNode(1,
                GceComputeServiceTwoNodesSameTagsMock.Plugin.class.getName(),
                Settings.EMPTY);
        startNode(2,
                GceComputeServiceTwoNodesSameTagsMock.Plugin.class.getName(),
                Settings.EMPTY);

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    @Test
    public void nodes_with_same_tags_and_one_tag_set() {
        startNode(1,
            GceComputeServiceTwoNodesSameTagsMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.TAGS, "elasticsearch").build());
        startNode(2,
            GceComputeServiceTwoNodesSameTagsMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.TAGS, "elasticsearch").build());

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    @Test
    public void nodes_with_same_tags_and_two_tags_set() {
        startNode(1,
            GceComputeServiceTwoNodesSameTagsMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.TAGS, Lists.newArrayList("elasticsearch", "dev")).build());
        startNode(2,
                GceComputeServiceTwoNodesSameTagsMock.Plugin.class.getName(),
                Settings.settingsBuilder().put(Fields.TAGS, Lists.newArrayList("elasticsearch", "dev")).build());

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    @Test
    public void multiple_zones_and_two_nodes_in_same_zone() {
        startNode(1,
                GceComputeServiceTwoNodesOneZoneMock.Plugin.class.getName(),
                Settings.settingsBuilder().put(Fields.ZONE, Lists.newArrayList("us-central1-a", "us-central1-b",
                        "us-central1-f", "europe-west1-a", "europe-west1-b")).build());
        startNode(2,
            GceComputeServiceTwoNodesOneZoneMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.ZONE, Lists.newArrayList("us-central1-a", "us-central1-b",
                "us-central1-f", "europe-west1-a", "europe-west1-b")).build());

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    @Test
    public void multiple_zones_and_two_nodes_in_different_zones() {
        startNode(1,
                GceComputeServiceTwoNodesTwoZonesMock.Plugin.class.getName(),
                Settings.settingsBuilder().put(Fields.ZONE, Lists.newArrayList("us-central1-a", "us-central1-b",
                        "us-central1-f", "europe-west1-a", "europe-west1-b")).build());
        startNode(2,
                GceComputeServiceTwoNodesTwoZonesMock.Plugin.class.getName(),
                Settings.settingsBuilder().put(Fields.ZONE, Lists.newArrayList("us-central1-a", "us-central1-b",
                        "us-central1-f", "europe-west1-a", "europe-west1-b")).build());

        // We expect having 2 nodes as part of the cluster, let's test that
        checkNumberOfNodes(2);
    }

    /**
     * For issue https://github.com/elastic/elasticsearch-cloud-gce/issues/43
     */
    @Test
    public void zero_node_43() {
        startNode(1,
            GceComputeServiceZeroNodeMock.Plugin.class.getName(),
            Settings.settingsBuilder().put(Fields.ZONE, Lists.newArrayList("us-central1-a", "us-central1-b",
                "us-central1-f", "europe-west1-a", "europe-west1-b")).build());
    }

}
