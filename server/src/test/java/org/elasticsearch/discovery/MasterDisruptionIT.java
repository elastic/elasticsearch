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

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockMasterServiceOnMaster;
import org.elasticsearch.test.disruption.IntermittentLongGCDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SingleNodeDisruption;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests relating to the loss of the master.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MasterDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * Test that cluster recovers from a long GC on master that causes other nodes to elect a new one
     */
    public void testMasterNodeGCs() throws Exception {
        List<String> nodes = startCluster(3);

        String oldMasterNode = internalCluster().getMasterName();
        // a very long GC, but it's OK as we remove the disruption when it has had an effect
        SingleNodeDisruption masterNodeDisruption = new IntermittentLongGCDisruption(random(), oldMasterNode, 100, 200, 30000, 60000);
        internalCluster().setDisruptionScheme(masterNodeDisruption);
        masterNodeDisruption.startDisrupting();

        Set<String> oldNonMasterNodesSet = new HashSet<>(nodes);
        oldNonMasterNodesSet.remove(oldMasterNode);

        List<String> oldNonMasterNodes = new ArrayList<>(oldNonMasterNodesSet);

        logger.info("waiting for nodes to de-elect master [{}]", oldMasterNode);
        for (String node : oldNonMasterNodesSet) {
            assertDifferentMaster(node, oldMasterNode);
        }

        logger.info("waiting for nodes to elect a new master");
        ensureStableCluster(2, oldNonMasterNodes.get(0));

        // restore GC
        masterNodeDisruption.stopDisrupting();
        final TimeValue waitTime = new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + masterNodeDisruption.expectedTimeToHeal().millis());
        ensureStableCluster(3, waitTime, false, oldNonMasterNodes.get(0));

        // make sure all nodes agree on master
        String newMaster = internalCluster().getMasterName();
        assertThat(newMaster, not(equalTo(oldMasterNode)));
        assertMaster(newMaster, nodes);
    }

    /**
     * This test isolates the master from rest of the cluster, waits for a new master to be elected, restores the partition
     * and verifies that all node agree on the new cluster state
     */
    public void testIsolateMasterAndVerifyClusterStateConsensus() throws Exception {
        final List<String> nodes = startCluster(3);

        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                ));

        ensureGreen();
        String isolatedNode = internalCluster().getMasterName();
        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        networkDisruption.startDisrupting();

        String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // make sure cluster reforms
        ensureStableCluster(2, nonIsolatedNode);

        // make sure isolated need picks up on things.
        assertNoMaster(isolatedNode, TimeValue.timeValueSeconds(40));

        // restore isolation
        networkDisruption.stopDisrupting();

        for (String node : nodes) {
            ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()),
                    true, node);
        }

        logger.info("issue a reroute");
        // trigger a reroute now, instead of waiting for the background reroute of RerouteService
        assertAcked(client().admin().cluster().prepareReroute());
        // and wait for it to finish and for the cluster to stabilize
        ensureGreen("test");

        // verify all cluster states are the same
        // use assert busy to wait for cluster states to be applied (as publish_timeout has low value)
        assertBusy(() -> {
            ClusterState state = null;
            for (String node : nodes) {
                ClusterState nodeState = getNodeClusterState(node);
                if (state == null) {
                    state = nodeState;
                    continue;
                }
                // assert nodes are identical
                try {
                    assertEquals("unequal versions", state.version(), nodeState.version());
                    assertEquals("unequal node count", state.nodes().getSize(), nodeState.nodes().getSize());
                    assertEquals("different masters ", state.nodes().getMasterNodeId(), nodeState.nodes().getMasterNodeId());
                    assertEquals("different meta data version", state.metaData().version(), nodeState.metaData().version());
                    assertEquals("different routing", state.routingTable().toString(), nodeState.routingTable().toString());
                } catch (AssertionError t) {
                    fail("failed comparing cluster state: " + t.getMessage() + "\n" +
                            "--- cluster state of node [" + nodes.get(0) + "]: ---\n" + state +
                            "\n--- cluster state [" + node + "]: ---\n" + nodeState);
                }

            }
        });
    }

    /**
     * Verify that the proper block is applied when nodes lose their master
     */
    public void testVerifyApiBlocksDuringPartition() throws Exception {
        internalCluster().startNodes(3);

        // Makes sure that the get request can be executed on each node locally:
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
        ));

        // Everything is stable now, it is now time to simulate evil...
        // but first make sure we have no initializing shards and all is green
        // (waiting for green here, because indexing / search in a yellow index is fine as long as no other nodes go down)
        ensureGreen("test");

        TwoPartitions partitions = TwoPartitions.random(random(), internalCluster().getNodeNames());
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);

        assertEquals(1, partitions.getMinoritySide().size());
        final String isolatedNode = partitions.getMinoritySide().iterator().next();
        assertEquals(2, partitions.getMajoritySide().size());
        final String nonIsolatedNode = partitions.getMajoritySide().iterator().next();

        // Simulate a network issue between the unlucky node and the rest of the cluster.
        networkDisruption.startDisrupting();


        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, NoMasterBlockService.NO_MASTER_BLOCK_WRITES, TimeValue.timeValueSeconds(30));


        logger.info("wait until elected master has been removed and a new 2 node cluster was from (via [{}])", isolatedNode);
        ensureStableCluster(2, nonIsolatedNode);

        for (String node : partitions.getMajoritySide()) {
            ClusterState nodeState = getNodeClusterState(node);
            boolean success = true;
            if (nodeState.nodes().getMasterNode() == null) {
                success = false;
            }
            if (!nodeState.blocks().global().isEmpty()) {
                success = false;
            }
            if (!success) {
                fail("node [" + node + "] has no master or has blocks, despite of being on the right side of the partition. State dump:\n"
                        + nodeState);
            }
        }


        networkDisruption.stopDisrupting();

        // Wait until the master node sees al 3 nodes again.
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()));

        logger.info("Verify no master block with {} set to {}", NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all");
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all"))
                .get();

        networkDisruption.startDisrupting();


        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, NoMasterBlockService.NO_MASTER_BLOCK_ALL, TimeValue.timeValueSeconds(30));

        // make sure we have stable cluster & cross partition recoveries are canceled by the removal of the missing node
        // the unresponsive partition causes recoveries to only time out after 15m (default) and these will cause
        // the test to fail due to unfreed resources
        ensureStableCluster(2, nonIsolatedNode);

    }

    public void testMappingTimeout() throws Exception {
        startCluster(3);
        createIndex("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put("index.routing.allocation.exclude._name", internalCluster().getMasterName())
        .build());

        // create one field
        index("test", "1", "{ \"f\": 1 }");

        ensureGreen();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(
            Settings.builder().put("indices.mapping.dynamic_timeout", "1ms")));

        ServiceDisruptionScheme disruption = new BlockMasterServiceOnMaster(random());
        setDisruptionScheme(disruption);

        disruption.startDisrupting();

        BulkRequestBuilder bulk = client().prepareBulk();
        bulk.add(client().prepareIndex("test").setId("2").setSource("{ \"f\": 1 }", XContentType.JSON));
        bulk.add(client().prepareIndex("test").setId("3").setSource("{ \"g\": 1 }", XContentType.JSON));
        bulk.add(client().prepareIndex("test").setId("4").setSource("{ \"f\": 1 }", XContentType.JSON));
        BulkResponse bulkResponse = bulk.get();
        assertTrue(bulkResponse.hasFailures());

        disruption.stopDisrupting();

        assertBusy(() -> {
            IndicesStatsResponse stats = client().admin().indices().prepareStats("test").clear().get();
            for (ShardStats shardStats : stats.getShards()) {
                assertThat(shardStats.getShardRouting().toString(),
                    shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(shardStats.getSeqNoStats().getLocalCheckpoint()));
            }
        });

    }
}
