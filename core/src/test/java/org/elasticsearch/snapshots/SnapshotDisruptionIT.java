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

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Tests for snapshot/restore disruptions and cancellations.  Tests in this class rely on the
 * custom disruption schemes, which require the MockTransportService test plugin, hence they cannot
 * be put in the {@link DedicatedClusterSnapshotRestoreIT} or {@link SharedClusterSnapshotRestoreIT}
 * test suites.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class SnapshotDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    /**
     * This test ensures that if a node that holds a primary that is being snapshotted leaves the cluster,
     * when it returns, the node aborts the snapshotting so a replica can be assigned to it.
     * Before, the node would continue snapshotting, which means it would continue to hold a reference to
     * the Store for the shard.  Holding this reference would prevent the replica from being allocated to
     * the same node, until the snapshot completes.  The solution here was to abort the snapshot when one
     * of the nodes participating as a primary source for the snapshot leaves the cluster.
     *
     * See https://github.com/elastic/elasticsearch/issues/20876
     */
    public void testSnapshotCanceledWithPartionedNodeThatRejoinsCluster() throws Exception {
        final int numNodes = 2;
        final int numPrimaries = 2;
        final int numReplicas = 1;
        final int numDocs = 100;
        final String repo = "test-repo";
        final String index = "test-idx";
        final String snapshot = "test-snap";
        final Settings sharedSettings = Settings.builder()
                                            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "zen")
                                            .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s")
                                            .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1")
                                            .build();

        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodesAsync(numNodes, sharedSettings).get();
        assertAcked(prepareCreate(index, numNodes,
            Settings.builder().put("number_of_shards", numPrimaries).put("number_of_replicas", numReplicas)));

        logger.info("--> indexing some data");
        Client client = client();
        for (int i = 0; i < numDocs; i++) {
            index(index, "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> creating repository");
        PutRepositoryResponse putRepositoryResponse =
            client.admin().cluster().preparePutRepository(repo).setType("fs").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                // snapshot painfully slow, so the node has time to leave the cluster and rejoin while still snapshotting
                .put("max_snapshot_bytes_per_sec", "100b")
            ).get();
        assertTrue(putRepositoryResponse.isAcknowledged());

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot(repo, snapshot)
            .setWaitForCompletion(false)
            .execute();

        logger.info("--> waiting for snapshot to be in progress on all nodes");
        assertBusy(() -> {
            for (String node : internalCluster().nodesInclude(index)) {
                final Client nodeClient = client(node);
                SnapshotsInProgress snapshotsInProgress = nodeClient.admin().cluster().prepareState().get()
                                                              .getState().custom(SnapshotsInProgress.TYPE);
                assertNotNull(snapshotsInProgress);
                assertEquals(1, snapshotsInProgress.entries().size());
                assertEquals(snapshot, snapshotsInProgress.entries().get(0).snapshot().getSnapshotId().getName());
                ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = snapshotsInProgress.entries().get(0).shards();
                assertEquals(numPrimaries, shards.size());
                for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> cursor : shards) {
                    assertEquals(SnapshotsInProgress.State.INIT, cursor.value.state());
                }
            }
        }, 10, TimeUnit.SECONDS);

        // Pick a node with a primary shard and cause it to be blocked from the cluster
        String masterNode = internalCluster().getMasterName();
        Tuple<String, ShardId> nodeWithPrimary = nodeWithPrimary(index, masterNode);
        String blockedNode = nodeWithPrimary.v1();
        ShardId shardId = nodeWithPrimary.v2();
        logger.info("--> partition node [{}] from the rest of the cluster", blockedNode);
        Set<String> otherNodes = Sets.newHashSet(nodes);
        otherNodes = Sets.difference(otherNodes, Collections.singleton(blockedNode));
        NetworkDisruption partition = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(blockedNode), otherNodes),
            new NetworkDisruption.NetworkDisconnect()
        );
        internalCluster().setDisruptionScheme(partition);
        partition.startDisrupting();
        ensureStableCluster(numNodes - 1, TimeValue.timeValueSeconds(30), false, masterNode);

        logger.info("--> ensuring a new primary node is assigned for the shard");
        String newPrimaryNode = internalCluster().clusterService(masterNode).state()
                                    .getRoutingTable().index(index)
                                    .shard(shardId.id())
                                    .primaryShard()
                                    .currentNodeId();
        assertNotEquals(blockedNode, nodeIdToName(newPrimaryNode, masterNode));

        logger.info("--> blocked node [{}] rejoining the cluster, assigning it the replica for [{}]", blockedNode, shardId);
        partition.stopDisrupting();
        // when the node rejoins, all shards assigned to it should be properly started
        // (which implies the snapshot on the node was properly canceled)
        ensureGreen();
        assertAcked(client.admin().cluster().prepareDeleteSnapshot(repo, snapshot).get());
    }

    private static Tuple<String, ShardId> nodeWithPrimary(final String indexName, final String masterNode) {
        IndexRoutingTable indexRoutingTable = internalCluster().clusterService(internalCluster().getMasterName())
                                                  .state().getRoutingTable().index(indexName);
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            String nodeName = nodeIdToName(indexShardRoutingTable.primaryShard().currentNodeId(), masterNode);
            if (masterNode.equals(nodeName) == false) {
                return Tuple.tuple(nodeName, indexShardRoutingTable.shardId());
            }
        }
        fail("No nodes with primary shard for the index " + indexName + " found");
        return null;
    }

    private static String nodeIdToName(String nodeId, String masterName) {
        return internalCluster().clusterService(masterName).state().nodes().get(nodeId).getName();
    }
}
