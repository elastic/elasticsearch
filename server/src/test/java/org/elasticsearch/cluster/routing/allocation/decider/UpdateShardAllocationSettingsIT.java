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
package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Set;

import static org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * An integration test for testing updating shard allocation/routing settings and
 * ensuring the updated settings take effect as expected.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UpdateShardAllocationSettingsIT extends ESIntegTestCase {

    /**
     * Tests that updating the {@link EnableAllocationDecider} related settings works as expected.
     */
    public void testEnableRebalance() throws InterruptedException {
        final String firstNode = internalCluster().startNode();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE))
                .get();
        // we test with 2 shards since otherwise it's pretty fragile if there are difference in the num or shards such that
        // all shards are relocated to the second node which is not what we want here. It's solely a test for the settings to take effect
        final int numShards = 2;
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)));
        assertAcked(prepareCreate("test_1").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)));
        ensureGreen();
        assertAllShardsOnNodes("test", firstNode);
        assertAllShardsOnNodes("test_1", firstNode);

        final String secondNode = internalCluster().startNode();
        // prevent via index setting but only on index test
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE))
                .get();
        client().admin().cluster().prepareReroute().get();
        ensureGreen();
        assertAllShardsOnNodes("test", firstNode);
        assertAllShardsOnNodes("test_1", firstNode);

        // now enable the index test to relocate since index settings override cluster settings
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                        randomBoolean() ? EnableAllocationDecider.Rebalance.PRIMARIES : EnableAllocationDecider.Rebalance.ALL))
                .get();
        logger.info("--> balance index [test]");
        client().admin().cluster().prepareReroute().get();
        ensureGreen("test");
        Set<String> test = assertAllShardsOnNodes("test", firstNode, secondNode);
        assertThat("index: [test] expected to be rebalanced on both nodes", test.size(), equalTo(2));

        // flip the cluster wide setting such that we can also balance for index
        // test_1 eventually we should have one shard of each index on each node
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(),
                        randomBoolean() ? EnableAllocationDecider.Rebalance.PRIMARIES : EnableAllocationDecider.Rebalance.ALL))
                .get();
        logger.info("--> balance index [test_1]");
        client().admin().cluster().prepareReroute().get();
        ensureGreen("test_1");
        Set<String> test_1 = assertAllShardsOnNodes("test_1", firstNode, secondNode);
        assertThat("index: [test_1] expected to be rebalanced on both nodes", test_1.size(), equalTo(2));

        test = assertAllShardsOnNodes("test", firstNode, secondNode);
        assertThat("index: [test] expected to be rebalanced on both nodes", test.size(), equalTo(2));
    }

    /**
     * Tests that updating the {@link SameShardAllocationDecider#CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING} setting works as expected.
     */
    public void testUpdateSameHostSetting() {
        internalCluster().startNodes(2);
        // same same_host to true, since 2 nodes are started on the same host,
        // only primaries should be assigned
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
            Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true)
        ).get();
        final String indexName = "idx";
        client().admin().indices().prepareCreate(indexName).setSettings(
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
        ).get();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertFalse("replica should be unassigned",
            clusterState.getRoutingTable().index(indexName).shardsWithState(ShardRoutingState.UNASSIGNED).isEmpty());
        // now, update the same_host setting to allow shards to be allocated to multiple nodes on
        // the same host - the replica should get assigned
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(
            Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), false)
        ).get();
        clusterState = client().admin().cluster().prepareState().get().getState();
        assertTrue("all shards should be assigned",
            clusterState.getRoutingTable().index(indexName).shardsWithState(ShardRoutingState.UNASSIGNED).isEmpty());
    }
}
