/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

// Inspired by StatelessTranslogIT.
// TODO consider generalizing this and sharing with StatelessTranslogIT.
public class StatelessReshardDisruptionBaseIT extends AbstractStatelessPluginIntegTestCase {
    void induceFailure(Failure failure, Index index, String coordinator) throws Exception {
        int clusterSize = internalCluster().size();

        String masterNode = internalCluster().getMasterName();

        var clusterState = internalCluster().clusterService(masterNode).state();
        var project = clusterState.metadata().projectFor(index);
        var indexMetadata = project.index(index);

        switch (failure) {
            // TODO restart masters
            case RESTART -> {
                String nodeToRestart = randomOtherDataNode(coordinator);
                logger.info("--> restarting node [{}]", nodeToRestart);
                internalCluster().restartNode(nodeToRestart);
                ensureStableCluster(clusterSize);
            }
            case REPLACE_FAILED_NODE -> {
                boolean searchNode = randomBoolean();
                if (searchNode) {
                    String nodeToReplace = randomOtherSearchNode(clusterState, coordinator).get();
                    logger.info("--> replacing search node [{}]", nodeToReplace);
                    internalCluster().stopNode(nodeToReplace);
                    startSearchNode();
                } else {
                    String nodeToReplace = randomOtherIndexingNode(clusterState, coordinator).get();
                    logger.info("--> replacing index node [{}]", nodeToReplace);
                    internalCluster().stopNode(nodeToReplace);
                    startIndexNode();
                }
                ensureStableCluster(clusterSize);
            }
            case LOCAL_FAIL_SHARD -> {
                try {
                    IndexShard indexShard = findIndexShard(index, randomIntBetween(0, indexMetadata.getNumberOfShards()));
                    var listener = ClusterServiceUtils.addTemporaryStateListener(
                        cs -> cs.routingTable(project.id())
                            .index(index.getName())
                            .shard(indexShard.shardId().id())
                            .primaryShard()
                            .unassigned()
                    );
                    logger.info("--> failing shard {}", indexShard.shardId());
                    indexShard.failShard("broken", new Exception("boom local"));
                    // ensureGreen may succeed before the cluster state reflects the failed shard
                    safeAwait(listener);
                    ensureGreen(index.getName());
                } catch (AssertionError | AlreadyClosedException e) {
                    // Unlucky, shard does not exist yet or is already closed.
                }
            }
            case RELOCATE_SHARD -> {
                ensureGreen(index.getName());
                var shardId = randomIntBetween(0, indexMetadata.getNumberOfShards());
                var shardRoutingTable = clusterState.routingTable(project.id()).index(index.getName()).shard(shardId);
                if (shardRoutingTable == null) {
                    // target shard doesn't exist yet, relocate source instead
                    assert shardId != 0;
                    shardId = 0;
                    shardRoutingTable = clusterState.routingTable(project.id()).index(index.getName()).shard(shardId);
                }
                boolean relocatePrimary = randomBoolean();
                // We run with one replica so there will only one unpromotable shard to relocate here.
                String fromNodeId = null;
                if (relocatePrimary) {
                    fromNodeId = shardRoutingTable.primaryShard().currentNodeId();
                } else {
                    var assignedUnpromotableShards = shardRoutingTable.assignedUnpromotableShards();
                    if (assignedUnpromotableShards.isEmpty() == false) {
                        fromNodeId = assignedUnpromotableShards.get(0).currentNodeId();
                    }
                }

                // Shard is not currently allocated.
                if (fromNodeId == null) {
                    return;
                }

                String fromNodeName = clusterState.nodes().get(fromNodeId).getName();
                Optional<String> toNode = relocatePrimary
                    ? randomOtherIndexingNode(clusterState, coordinator, fromNodeName)
                    : randomOtherSearchNode(clusterState, coordinator, fromNodeName);

                if (toNode.isPresent()) {
                    logger.info("--> relocating shard {} from {} to {}", shardId, fromNodeName, toNode.get());
                    try {
                        assertAcked(
                            client().execute(
                                TransportClusterRerouteAction.TYPE,
                                new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setRetryFailed(false)
                                    .add(new MoveAllocationCommand(index.getName(), shardId, fromNodeName, toNode.get(), project.id()))
                            ).actionGet(SAFE_AWAIT_TIMEOUT)
                        );
                        if (randomBoolean()) {
                            // We want to have a mix of successful and failed relocations.
                            // To ensure we get successful ones we wait for the relocation to complete sometimes.
                            // We are leveraging `waitForNoRelocatingShards` functionality of `ensureYellow for` this.
                            // Yellow is fine because maybe we have just started resharding.
                            ensureYellow(index.getName());
                        }
                    } catch (Exception e) {
                        // Since we don't always wait for relocations to complete it's possible that this shard is already being relocated
                        // from the node.
                        assertTrue(e.getMessage().contains("failed to find it on node"));
                    }
                }
            }
        }
    }

    enum Failure {
        RESTART,
        REPLACE_FAILED_NODE,
        LOCAL_FAIL_SHARD,
        RELOCATE_SHARD,
    }

    private static String randomOtherDataNode(String coordinator) {
        return Stream.generate(() -> internalCluster().getRandomDataNodeName())
            .filter(nodeName -> nodeName.equals(coordinator) == false)
            .findFirst()
            .get();
    }

    private static Optional<String> randomOtherSearchNode(ClusterState clusterState, String... excluded) {
        return nonCoordinatorNodeWithRole(clusterState, DiscoveryNodeRole.SEARCH_ROLE, excluded);
    }

    private static Optional<String> randomOtherIndexingNode(ClusterState clusterState, String... excluded) {
        return nonCoordinatorNodeWithRole(clusterState, DiscoveryNodeRole.INDEX_ROLE, excluded);
    }

    private static Optional<String> nonCoordinatorNodeWithRole(ClusterState clusterState, DiscoveryNodeRole role, String... excluded) {
        var excludedNodes = Stream.of(excluded).collect(Collectors.toSet());

        return clusterState.nodes()
            .getDataNodes()
            .values()
            .stream()
            .filter(dn -> dn.hasRole(role.roleName()))
            .filter(dn -> excludedNodes.contains(dn.getName()) == false)
            .map(DiscoveryNode::getName)
            .findFirst();
    }
}
