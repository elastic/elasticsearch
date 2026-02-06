/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class NotPreferredAllocationRebalancingIT extends ESIntegTestCase {

    private static final Set<String> NOT_PREFERRED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private static boolean ALLOCATION_DISABLED = false;

    @After
    public void clearNotPreferredNodesAndAllocationDisabled() {
        NOT_PREFERRED_NODES.clear();
        ALLOCATION_DISABLED = false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(NotPreferredPlugin.class);
        return plugins;
    }

    public void testAllocatorDoesNotMoveShardsToNotPreferredNode() {
        final Settings settings = Settings.builder()
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false)
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
            )
            .put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 1.0f)
            .build();

        /* set up a node that has a few indices, hotspot it against a not preferred node,
        and see that it doesn't move any shards onto it */
        final String sourceNode = internalCluster().startNode(settings);
        final String sourceNodeId = getNodeId(sourceNode);

        final int numberOfNodes = randomIntBetween(5, 20);
        final int numberOfIndices = numberOfNodes * 2;
        final int numberOfNotPreferredNodes = randomIntBetween(2, numberOfNodes - 2);

        Set<String> indexNames = new HashSet<>(numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            final var indexName = randomIdentifier();
            indexNames.add(indexName);
            createIndex(indexName, 1, 0);
        }

        // check that the indices are on the only source hotspot node, beforehand
        ClusterState clusterState = clusterService().state();
        for (String indexName : indexNames) {
            assertEquals(
                sourceNodeId,
                clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
            );
        }

        // turn off allocation, and add a bunch of nodes
        ALLOCATION_DISABLED = true;
        List<String> nodeNames = internalCluster().startNodes(numberOfNodes, settings);
        Set<String> nodeIds = new HashSet<>(nodeNames.stream().map(nodeName -> getNodeId(nodeName)).collect(Collectors.toSet()));
        Set<String> notPreferredNodeIds = new HashSet<>(randomSubsetOf(numberOfNotPreferredNodes, nodeIds));
        Set<String> preferredNodeIds = Sets.difference(nodeIds, notPreferredNodeIds);
        Set<String> preferredAndSourceNodeIDS = Sets.union(preferredNodeIds, Set.of(sourceNodeId));

        // check that all the shards are still assigned to the source node
        clusterState = clusterService().state();
        for (String indexName : indexNames) {
            assertEquals(
                sourceNodeId,
                clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
            );
        }

        // set up not preferred nodes, then turn back on allocation
        for (String nodeId : notPreferredNodeIds) {
            NOT_PREFERRED_NODES.add(nodeId);
        }
        ALLOCATION_DISABLED = false;

        // run reroute
        safeGet(
            client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );

        // spin on checking that enough rebalancing has happened, while asserting that no shards have been moved to the not preferred nodes
        awaitClusterState(state -> {
            var projectRoutingTable = state.routingTable(ProjectId.DEFAULT);
            int countOnPreferred = 0;
            for (String indexName : indexNames) {
                var shardRoutingTable = projectRoutingTable.index(indexName).shard(0);
                String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
                assertTrue("Not preferred nodes should not have any assignments", notPreferredNodeIds.contains(primaryNodeId) == false);
                assertTrue("Preferred shards should only have the assignments", preferredAndSourceNodeIDS.contains(primaryNodeId));
                if (preferredNodeIds.contains(primaryNodeId)) {
                    countOnPreferred++;
                }
            }

            // wait until at least half of the shards have been moved off the source node
            return countOnPreferred > numberOfIndices / 2.0;
        });
    }

    public static class NotPreferredPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {

            return List.of(new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    Set<String> nodeIds = NOT_PREFERRED_NODES;
                    if (nodeIds.contains(node.nodeId())) {
                        return Decision.NOT_PREFERRED;
                    } else {
                        return Decision.YES;
                    }
                }
            }, new AllocationDecider() {
                @Override
                public Decision canRebalance(RoutingAllocation allocation) {
                    if (ALLOCATION_DISABLED) {
                        return Decision.NO;
                    } else {
                        return Decision.YES;
                    }
                }
            });
        }
    }
}
