/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.Builder;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.equalTo;

public class RandomAllocationDeciderTests extends ESAllocationTestCase {
    /* This test will make random allocation decision on a growing and shrinking
     * cluster leading to a random distribution of the shards. After a certain
     * amount of iterations the test allows allocation unless the same shard is
     * already allocated on a node and balances the cluster to gain optimal
     * balance.*/
    public void testRandomDecisions() {
        RandomAllocationDecider randomAllocationDecider = new RandomAllocationDecider(random());
        AllocationService strategy = new AllocationService(
            new AllocationDeciders(
                new HashSet<>(
                    Arrays.asList(
                        new SameShardAllocationDecider(createBuiltInClusterSettings()),
                        new ReplicaAfterPrimaryActiveAllocationDecider(),
                        randomAllocationDecider
                    )
                )
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        int indices = scaledRandomIntBetween(1, 20);
        Builder metaBuilder = Metadata.builder();
        int maxNumReplicas = 1;
        int totalNumShards = 0;
        for (int i = 0; i < indices; i++) {
            int replicas = scaledRandomIntBetween(0, 6);
            maxNumReplicas = Math.max(maxNumReplicas, replicas + 1);
            int numShards = scaledRandomIntBetween(1, 20);
            totalNumShards += numShards * (replicas + 1);
            metaBuilder.put(
                IndexMetadata.builder("INDEX_" + i).settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(replicas)
            );

        }
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        for (int i = 0; i < indices; i++) {
            routingTableBuilder.addAsNew(metadata.index("INDEX_" + i));
        }

        RoutingTable initialRoutingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();
        int numIters = scaledRandomIntBetween(5, 15);
        int nodeIdCounter = 0;
        int atMostNodes = scaledRandomIntBetween(Math.max(1, maxNumReplicas), 15);
        final boolean frequentNodes = randomBoolean();
        for (int i = 0; i < numIters; i++) {
            logger.info("Start iteration [{}]", i);
            ClusterState.Builder stateBuilder = ClusterState.builder(clusterState);
            DiscoveryNodes.Builder newNodesBuilder = DiscoveryNodes.builder(clusterState.nodes());

            if (clusterState.nodes().getSize() <= atMostNodes && (nodeIdCounter == 0 || (frequentNodes ? frequently() : rarely()))) {
                int numNodes = scaledRandomIntBetween(1, 3);
                for (int j = 0; j < numNodes; j++) {
                    logger.info("adding node [{}]", nodeIdCounter);
                    newNodesBuilder.add(newNode("NODE_" + (nodeIdCounter++)));
                }
            }

            boolean nodesRemoved = false;
            if (nodeIdCounter > 1 && rarely()) {
                int nodeId = scaledRandomIntBetween(0, nodeIdCounter - 2);
                final String node = "NODE_" + nodeId;
                boolean safeToRemove = true;
                RoutingNode routingNode = clusterState.getRoutingNodes().node(node);
                for (ShardRouting shard : routingNode != null ? routingNode : Collections.<ShardRouting>emptyList()) {
                    if (shard.active() && shard.primary()) {
                        // make sure there is an active replica to prevent from going red
                        if (clusterState.routingTable().shardRoutingTable(shard.shardId()).activeShards().size() <= 1) {
                            safeToRemove = false;
                            break;
                        }
                    }
                }
                if (safeToRemove) {
                    logger.info("removing node [{}]", nodeId);
                    newNodesBuilder.remove(node);
                    nodesRemoved = true;
                } else {
                    logger.debug("not removing node [{}] as it holds a primary with no replacement", nodeId);
                }
            }

            stateBuilder.nodes(newNodesBuilder.build());
            clusterState = stateBuilder.build();
            if (nodesRemoved) {
                clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
            } else {
                clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
            }
            if (shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size() > 0) {
                clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            }
        }
        logger.info("Fill up nodes such that every shard can be allocated");
        if (clusterState.nodes().getSize() < maxNumReplicas) {
            ClusterState.Builder stateBuilder = ClusterState.builder(clusterState);
            DiscoveryNodes.Builder newNodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
            for (int j = 0; j < (maxNumReplicas - clusterState.nodes().getSize()); j++) {
                logger.info("adding node [{}]", nodeIdCounter);
                newNodesBuilder.add(newNode("NODE_" + (nodeIdCounter++)));
            }
            stateBuilder.nodes(newNodesBuilder.build());
            clusterState = stateBuilder.build();
        }

        randomAllocationDecider.alwaysSayYes = true;
        logger.info("now say YES to everything");
        int iterations = 0;
        do {
            iterations++;
            clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
            if (shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size() > 0) {
                clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            }

        } while (shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size() != 0
            || shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.UNASSIGNED).size() != 0 && iterations < 200);
        logger.info("Done Balancing after [{}] iterations. State:\n{}", iterations, clusterState);
        // we stop after 200 iterations if it didn't stabelize by then something is likely to be wrong
        assertThat("max num iteration exceeded", iterations, Matchers.lessThan(200));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(0));
        int shards = shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size();
        assertThat(shards, equalTo(totalNumShards));
        final int numNodes = clusterState.nodes().getSize();
        final int upperBound = (int) Math.round(((shards / numNodes) * 1.10));
        final int lowerBound = (int) Math.round(((shards / numNodes) * 0.90));
        for (int i = 0; i < nodeIdCounter; i++) {
            if (clusterState.getRoutingNodes().node("NODE_" + i) == null) {
                continue;
            }
            assertThat(
                clusterState.getRoutingNodes().node("NODE_" + i).size(),
                Matchers.anyOf(
                    Matchers.anyOf(equalTo((shards / numNodes) + 1), equalTo((shards / numNodes) - 1), equalTo((shards / numNodes))),
                    Matchers.allOf(Matchers.greaterThanOrEqualTo(lowerBound), Matchers.lessThanOrEqualTo(upperBound))
                )
            );
        }
    }

    public static final class RandomAllocationDecider extends AllocationDecider {

        private final Random random;

        public RandomAllocationDecider(Random random) {
            this.random = random;
        }

        public boolean alwaysSayYes = false;

        @Override
        public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
            return getRandomDecision();
        }

        private Decision getRandomDecision() {
            if (alwaysSayYes) {
                return Decision.YES;
            }
            return switch (random.nextInt(10)) {
                case 9, 8, 7, 6, 5 -> Decision.NO;
                case 4 -> Decision.THROTTLE;
                case 3, 2, 1 -> Decision.YES;
                default -> Decision.ALWAYS;
            };
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return getRandomDecision();
        }

        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return getRandomDecision();
        }

    }

}
