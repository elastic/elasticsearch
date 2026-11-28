/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.PlacementAspects;
import org.elasticsearch.cluster.RoutingDerivedPlacementModel;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Compares the routing-derived placement model, directly and through {@link ClusterInfoSimulator}, which is built on it, with an
 * oracle: the aggregates recomputed from scratch for the final placement.
 */
public class RoutingDerivedPlacementModelTests extends ESTestCase {

    private static final long TOTAL_DISK = 1_000_000_000L;
    private static final int WRITE_THREADS = 8;

    public void testMatchesOracleAfterMovesBeforeAndAfterThePoll() {
        final int nodeCount = between(3, 6);
        final int shardCount = between(5, 40);
        final String[] nodes = new String[nodeCount];
        for (int n = 0; n < nodeCount; n++) {
            nodes[n] = "node-" + n;
        }

        final DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        final Map<String, DiscoveryNodeRole> roles = new HashMap<>();
        for (String node : nodes) {
            roles.put(node, randomBoolean() ? DiscoveryNodeRole.INDEX_ROLE : DiscoveryNodeRole.SEARCH_ROLE);
            discoveryNodes.add(DiscoveryNodeUtils.builder(node).roles(Set.of(roles.get(node))).build());
        }

        final ShardId[] shardIds = new ShardId[shardCount];
        final int[] pollPlacement = new int[shardCount];
        final int[] currentPlacement = new int[shardCount];
        final long[] size = new long[shardCount];
        final double[] writeLoad = new double[shardCount];
        final BoostedAndUnboostedCacheRequirements[] cache = new BoostedAndUnboostedCacheRequirements[shardCount];
        final Metadata.Builder metadata = Metadata.builder();
        final RoutingTable.Builder routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        for (int s = 0; s < shardCount; s++) {
            final IndexMetadata index = IndexMetadata.builder("index-" + s).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
            metadata.put(index, false);
            shardIds[s] = new ShardId(index.getIndex(), 0);
            pollPlacement[s] = between(0, nodeCount - 1);
            // some shards moved after the poll and before the computation started
            currentPlacement[s] = randomBoolean() ? pollPlacement[s] : between(0, nodeCount - 1);
            size[s] = between(1, 1000);
            writeLoad[s] = randomDoubleBetween(0.0, 1.0, true);
            cache[s] = new BoostedAndUnboostedCacheRequirements(between(1, 100), between(1, 100));
            routingTable.add(
                IndexRoutingTable.builder(index.getIndex())
                    .addShard(shardRoutingBuilder(shardIds[s], nodes[currentPlacement[s]], true, STARTED).build())
            );
        }

        // the polled node values are what a node holding exactly the poll placement would report
        final Map<String, DiskUsage> disk = new HashMap<>();
        final Map<String, NodeCacheSizeAndCommitments> nodeCache = new HashMap<>();
        final Map<String, NodeUsageStatsForThreadPools> threadPools = new HashMap<>();
        final Map<ClusterInfo.NodeAndShard, String> dataPath = new HashMap<>();
        final Map<String, Long> shardSizes = new HashMap<>();
        final Map<ShardId, Double> shardWriteLoads = new HashMap<>();
        final Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCache = new HashMap<>();
        for (int n = 0; n < nodeCount; n++) {
            long used = 1000;
            long boosted = 1000;
            long unboosted = 1000;
            double load = 0;
            for (int s = 0; s < shardCount; s++) {
                if (pollPlacement[s] == n) {
                    used += size[s];
                    boosted += cache[s].boostedCacheRequirementInBytes();
                    unboosted += cache[s].unboostedCacheRequirementInBytes();
                    load += writeLoad[s] / WRITE_THREADS;
                }
            }
            disk.put(nodes[n], new DiskUsage(nodes[n], nodes[n], "/data", TOTAL_DISK, TOTAL_DISK - used));
            nodeCache.put(nodes[n], new NodeCacheSizeAndCommitments(TOTAL_DISK, boosted, unboosted));
            threadPools.put(nodes[n], writePool(nodes[n], (float) (1.0 + load), 0));
        }
        for (int s = 0; s < shardCount; s++) {
            dataPath.put(new ClusterInfo.NodeAndShard(nodes[pollPlacement[s]], shardIds[s]), "/data");
            shardSizes.put(shardIdentifierFromRouting(shardIds[s], true), size[s]);
            shardWriteLoads.put(shardIds[s], writeLoad[s]);
            shardCache.put(shardIds[s], cache[s]);
        }
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .leastAvailableSpaceUsage(disk)
            .mostAvailableSpaceUsage(disk)
            .shardSizes(shardSizes)
            .dataPath(dataPath)
            .nodeCacheSizeAndCommitments(nodeCache)
            .shardCacheRequirements(shardCache)
            .nodeUsageStatsForThreadPools(threadPools)
            .shardWriteLoads(shardWriteLoads)
            .build();

        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        final RoutingAllocation heuristicAllocation = TestRoutingAllocationFactory.forClusterState(state)
            .clusterInfo(clusterInfo)
            .mutable();
        final RoutingAllocation modelAllocation = TestRoutingAllocationFactory.forClusterState(state).clusterInfo(clusterInfo).mutable();

        final var simulator = new ClusterInfoSimulator(heuristicAllocation);
        final var usedBytes = new PlacementAspects.UsedBytes(modelAllocation);
        final var cacheCommitment = new PlacementAspects.CacheCommitment(clusterInfo);
        final var writeLoadAspect = new PlacementAspects.WriteLoad(clusterInfo);
        final var model = new RoutingDerivedPlacementModel(modelAllocation, usedBytes, cacheCommitment, writeLoadAspect);

        // moves during the computation
        for (int move = between(0, 3 * shardCount); move > 0; move--) {
            final int s = between(0, shardCount - 1);
            final int target = randomValueOtherThan(currentPlacement[s], () -> between(0, nodeCount - 1));
            currentPlacement[s] = target;
            final var heuristicTarget = relocate(heuristicAllocation, shardIds[s], nodes[target], size[s]);
            simulator.simulateShardStarted(heuristicTarget);
            heuristicAllocation.routingNodes().startShard(heuristicTarget, heuristicAllocation.changes(), 0L);
            final var modelTarget = relocate(modelAllocation, shardIds[s], nodes[target], size[s]);
            model.shardStarted(modelTarget);
            modelAllocation.routingNodes().startShard(modelTarget, modelAllocation.changes(), 0L);
        }

        // oracle: what a node holding exactly the final placement would report
        final var heuristicResult = simulator.getClusterInfo();
        final var modelDisk = usedBytes.simulated();
        final var modelCache = cacheCommitment.simulated();
        final var modelThreadPools = writeLoadAspect.simulated();
        for (int n = 0; n < nodeCount; n++) {
            long used = 1000;
            long boosted = 1000;
            long unboosted = 1000;
            double load = 0;
            for (int s = 0; s < shardCount; s++) {
                if (currentPlacement[s] == n) {
                    used += size[s];
                    boosted += cache[s].boostedCacheRequirementInBytes();
                    unboosted += cache[s].unboostedCacheRequirementInBytes();
                    load += writeLoad[s] / WRITE_THREADS;
                }
            }
            final String node = nodes[n];
            final long expectedFree = TOTAL_DISK - used;
            final var expectedCache = new NodeCacheSizeAndCommitments(TOTAL_DISK, boosted, unboosted);
            final float expectedUtilisation = (float) (1.0 + load);

            assertThat(modelDisk.get(node).freeBytes(), equalTo(expectedFree));
            assertThat(modelCache.get(node), equalTo(expectedCache));
            assertThat((double) writeUtilisation(modelThreadPools.get(node)), closeTo(expectedUtilisation, 1e-3));

            assertThat(heuristicResult.getNodeMostAvailableDiskUsages().get(node).freeBytes(), equalTo(expectedFree));
            assertThat(heuristicResult.getNodeCacheSizeAndCommitments().get(node), equalTo(expectedCache));
            assertThat(
                (double) writeUtilisation(heuristicResult.getNodeUsageStatsForThreadPools().get(node)),
                closeTo(expectedUtilisation, 1e-3)
            );
        }
    }

    private static ShardRouting relocate(RoutingAllocation allocation, ShardId shardId, String targetNodeId, long expectedSize) {
        final ShardRouting started = allocation.routingNodes().assignedShards(shardId).getFirst();
        return allocation.routingNodes()
            .relocateShard(
                started,
                targetNodeId,
                expectedSize,
                MoveReason.CANNOT_REMAIN,
                allocation.changes(),
                RecoveryPriority.RELOCATION_CAN_REMAIN_NO
            )
            .v2();
    }

    private static NodeUsageStatsForThreadPools writePool(String nodeId, float utilisation, long queueLatencyMillis) {
        return new NodeUsageStatsForThreadPools(
            nodeId,
            Map.of(
                ThreadPool.Names.WRITE,
                new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(WRITE_THREADS, utilisation, queueLatencyMillis)
            )
        );
    }

    private static float writeUtilisation(NodeUsageStatsForThreadPools stats) {
        return stats.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE).averageThreadPoolUtilization();
    }
}
