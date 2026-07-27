/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

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
            // TODO restart master node
            case ISOLATE_NODE -> {
                String isolatedNode = randomOtherDataNode(coordinator);
                logger.info("--> isolating node [{}]", isolatedNode);

                Set<String> isolatedSide = Set.of(isolatedNode);
                Set<String> otherSide = Arrays.stream(internalCluster().getNodeNames())
                    .filter(name -> name.equals(isolatedNode) == false)
                    .collect(Collectors.toSet());
                var disruption = new NetworkDisruption(new TwoPartitions(isolatedSide, otherSide), NetworkDisruption.DISCONNECT);
                disruption.applyToCluster(internalCluster());
                disruption.startDisrupting();
                try {
                    // waitForRelocation may succeed before the cluster state reflects the isolated node
                    awaitClusterState(
                        masterNode,
                        cs -> cs.nodes().getNodes().values().stream().noneMatch(n -> n.getName().equals(isolatedNode))
                    );
                    // We can't use `ensureGreen` here since it requires all nodes to be available and we just isolated a node.
                    // So we craft the health request manually.
                    ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, index.getName())
                        // We have custom logic to keep the index green during resharding to this should work.
                        .waitForStatus(ClusterHealthStatus.GREEN)
                        .waitForEvents(Priority.LANGUID)
                        .waitForNoRelocatingShards(true)
                        // Target shards may be uninitialized.
                        .waitForNoInitializingShards(false);
                    ClusterHealthResponse actionGet = client(coordinator).admin().cluster().health(healthRequest).actionGet();
                    if (actionGet.isTimedOut()) {
                        assertThat("timed out waiting for green state during network disruption", actionGet.isTimedOut(), equalTo(false));
                    }
                } finally {
                    disruption.stopDisrupting();
                    NetworkDisruption.ensureFullyConnectedCluster(internalCluster());
                    ensureStableCluster(clusterSize);
                }
                ensureGreen(index.getName());
            }
            case LOCAL_FAIL_SHARD -> {
                try {
                    int shardIdToFail = randomIntBetween(0, indexMetadata.getNumberOfShards());
                    boolean isIndexShard = randomBoolean();
                    IndexShard shard;
                    if (isIndexShard) {
                        shard = findIndexShard(index, shardIdToFail);
                    } else {
                        shard = findSearchShard(index, shardIdToFail);
                    }
                    var listener = ClusterServiceUtils.addTemporaryStateListener(cs -> {
                        var shardRoutingTable = cs.routingTable(project.id()).index(index.getName()).shard(shard.shardId().id());

                        return isIndexShard
                            ? shardRoutingTable.primaryShard().unassigned()
                            : shardRoutingTable.unpromotableShards().get(0).unassigned();
                    });
                    logger.info("--> failing {} shard {}", isIndexShard ? "index" : "search", shard.shardId());
                    shard.failShard("broken", new Exception("boom local"));
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
        ISOLATE_NODE,
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
