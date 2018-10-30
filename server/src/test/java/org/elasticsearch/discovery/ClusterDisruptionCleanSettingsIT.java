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

package org.elasticsearch.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.store.IndicesStoreIntegrationIT;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class ClusterDisruptionCleanSettingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    /**
     * This test creates a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the target
     * node but already deleted on the source node. Search request should still work.
     */
    public void testSearchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String node_1 = internalCluster().startDataOnlyNode();

        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
            Settings.builder().put(indexSettings())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen("test");

        final String node_2 = internalCluster().startDataOnlyNode();
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(client().prepareIndex().setIndex("test").setType("_doc")
                .setSource("{\"int_field\":1}", XContentType.JSON));
        }
        indexRandom(true, indexRequestBuilderList);

        IndicesStoreIntegrationIT.relocateAndBlockCompletion(logger, "test", 0, node_1, node_2);
        // now search for the documents and see if we get a reply
        assertThat(client().prepareSearch().setSize(0).get().getHits().getTotalHits(), equalTo(100L));
    }

    /**
     * Tests that indices are properly deleted even if there is a master transition in between.
     * Test for https://github.com/elastic/elasticsearch/issues/11665
     */
    public void testIndicesDeleted() throws Exception {
        final Settings settings = Settings.builder()
            .put(AbstractDisruptionTestCase.DEFAULT_SETTINGS)
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s") // don't wait on isolated data node
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s") // wait till cluster state is committed
            .build();
        final String idxName = "test";
        final List<String> allMasterEligibleNodes = internalCluster().startMasterOnlyNodes(2, settings);
        final String dataNode = internalCluster().startDataOnlyNode(settings);
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));

        final String masterNode1 = internalCluster().getMasterName();
        NetworkDisruption networkDisruption =
            new NetworkDisruption(new NetworkDisruption.TwoPartitions(masterNode1, dataNode), new NetworkDisruption.NetworkUnresponsive());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        // We know this will time out due to the partition, we check manually below to not proceed until
        // the delete has been applied to the master node and the master eligible node.
        internalCluster().client(masterNode1).admin().indices().prepareDelete(idxName).setTimeout("0s").get();
        // Don't restart the master node until we know the index deletion has taken effect on master and the master eligible node.
        assertBusy(() -> {
            for (String masterNode : allMasterEligibleNodes) {
                final ClusterState masterState = internalCluster().clusterService(masterNode).state();
                assertTrue("index not deleted on " + masterNode, masterState.metaData().hasIndex(idxName) == false);
            }
        });
        internalCluster().restartNode(masterNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(client().admin().indices().prepareExists(idxName).get().isExists());
    }
}
