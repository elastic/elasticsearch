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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * This class of tests exercise various scenarios of
 * primary shard allocation and assert the cluster health
 * has the correct status based on those allocation decisions.
 */
public class DecisionsImpactOnClusterHealthTests extends ESAllocationTestCase {

    public void testPrimaryShardNoDecisionOnIndexCreation() throws IOException {
        final String indexName = "test-idx";
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
        AllocationDecider decider = new TestAllocateDecision(Decision.NO);
        // if deciders say NO to allocating a primary shard, then the cluster health should be RED
        runAllocationTest(settings, indexName, decider, ClusterHealthStatus.RED);
    }

    public void testPrimaryShardThrottleDecisionOnIndexCreation() throws IOException {
        final String indexName = "test-idx";
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
        AllocationDecider decider = new TestAllocateDecision(Decision.THROTTLE);
        // if deciders THROTTLE allocating a primary shard, stay in YELLOW state
        runAllocationTest(settings, indexName, decider, ClusterHealthStatus.YELLOW);
    }

    public void testPrimaryShardYesDecisionOnIndexCreation() throws IOException {
        final String indexName = "test-idx";
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
        AllocationDecider decider = new TestAllocateDecision(Decision.YES) {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                if (node.getByShardId(shardRouting.shardId()) == null) {
                    return Decision.YES;
                } else {
                    return Decision.NO;
                }
            }
        };
        // if deciders say YES to allocating primary shards, stay in YELLOW state
        ClusterState clusterState = runAllocationTest(settings, indexName, decider, ClusterHealthStatus.YELLOW);
        // make sure primaries are initialized
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            assertTrue(indexRoutingTable.shard(i).primaryShard().initializing());
        }
    }

    private ClusterState runAllocationTest(
        final Settings settings,
        final String indexName,
        final AllocationDecider allocationDecider,
        final ClusterHealthStatus expectedStatus
    ) throws IOException {

        final String clusterName = "test-cluster";
        final AllocationService allocationService = newAllocationService(settings, allocationDecider);

        logger.info("Building initial routing table");
        final int numShards = randomIntBetween(1, 5);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(indexName).settings(settings(Version.CURRENT)).numberOfShards(numShards).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index(indexName))
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName(clusterName))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        logger.info("--> adding nodes");
        // we need at least as many nodes as shards for the THROTTLE case, because
        // once a shard has been throttled on a node, that node no longer accepts
        // any allocations on it
        final DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        for (int i = 0; i < numShards; i++) {
            discoveryNodes.add(newNode("node" + i));
        }
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).build();

        logger.info("--> do the reroute");
        routingTable = allocationService.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("--> assert cluster health");
        ClusterStateHealth health = new ClusterStateHealth(clusterState);
        assertThat(health.getStatus(), equalTo(expectedStatus));

        return clusterState;
    }

    private static AllocationService newAllocationService(Settings settings, AllocationDecider decider) {
        return new AllocationService(
            new AllocationDeciders(List.of(decider, new ReplicaAfterPrimaryActiveAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

}
