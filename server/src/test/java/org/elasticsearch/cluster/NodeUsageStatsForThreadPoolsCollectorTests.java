/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPoolsCollector.CollectedUsageStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class NodeUsageStatsForThreadPoolsCollectorTests extends ESTestCase {

    /**
     * Verifies that the collector merges the per-node shard write loads into a single map, passing the values through unchanged.
     */
    public void testShardWriteLoadsAreMergedAcrossNodes() {
        final DiscoveryNode node1 = DiscoveryNodeUtils.create("node-1");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("node-2");
        final ClusterState clusterState = createClusterStateWithNodes(node1, node2);

        final ShardId shard1 = new ShardId("index", "uuid", 0);
        final ShardId shard2 = new ShardId("index", "uuid", 1);
        final double shard1WriteLoad = 4.0;
        final double shard2WriteLoad = 5.0;

        final NodeUsageStatsForThreadPoolsAction.Response response = createSuccessfulResponse(
            List.of(
                nodeResponseWithRandomThreadPoolUsage(node1, 8, Map.of(shard1, shard1WriteLoad)),
                nodeResponseWithRandomThreadPoolUsage(node2, 20, Map.of(shard2, shard2WriteLoad))
            )
        );

        final Client client = clientReturning(response);
        final NodeUsageStatsForThreadPoolsCollector collector = new NodeUsageStatsForThreadPoolsCollector();

        final CollectedUsageStats collected = safeAwait(l -> collector.collectUsageStats(client, clusterState, true, l));

        assertThat(collected.shardWriteLoads(), equalTo(Map.of(shard1, shard1WriteLoad, shard2, shard2WriteLoad)));
    }

    /**
     * Verifies that when a node fails to respond to a poll, the previously collected node usage stats and shard
     * write loads for that node are retained (not zeroed or dropped), while nodes that do respond are updated.
     */
    public void testFailedNodeStatsAndShardLoadsAreRetainedFromLastSuccessfulPoll() {
        final DiscoveryNode node1 = DiscoveryNodeUtils.create("node-1");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("node-2");
        final ClusterState clusterState = createClusterStateWithNodes(node1, node2);

        final ShardId shard1 = new ShardId("index", "uuid", 0);
        final ShardId shard2 = new ShardId("index", "uuid", 1);
        final int node1NumWriteThreads = 8;
        final int node2NumWriteThreads = 20;
        final double shard1WriteLoadFirstPoll = 4.0;
        final double shard2WriteLoadFirstPoll = 5.0;
        final double shard1WriteLoadSecondPoll = 6.0;

        final NodeUsageStatsForThreadPoolsCollector collector = new NodeUsageStatsForThreadPoolsCollector();

        // First poll: both nodes respond successfully.
        final NodeUsageStatsForThreadPoolsAction.Response firstFullResponse = createSuccessfulResponse(
            List.of(
                nodeResponseWithRandomThreadPoolUsage(node1, node1NumWriteThreads, Map.of(shard1, shard1WriteLoadFirstPoll)),
                nodeResponseWithRandomThreadPoolUsage(node2, node2NumWriteThreads, Map.of(shard2, shard2WriteLoadFirstPoll))
            )
        );
        final CollectedUsageStats firstCollectedStats = safeAwait(
            l -> collector.collectUsageStats(clientReturning(firstFullResponse), clusterState, true, l)
        );
        assertThat(firstCollectedStats.nodeUsageStats().keySet(), equalTo(Set.of(node1.getId(), node2.getId())));
        assertThat(
            firstCollectedStats.shardWriteLoads(),
            equalTo(Map.of(shard1, shard1WriteLoadFirstPoll, shard2, shard2WriteLoadFirstPoll))
        );

        // Second poll: node-2 fails to respond; node-1 reports updated values.
        final NodeUsageStatsForThreadPoolsAction.Response secondPartialResponse = new NodeUsageStatsForThreadPoolsAction.Response(
            ClusterName.DEFAULT,
            List.of(nodeResponseWithRandomThreadPoolUsage(node1, node1NumWriteThreads, Map.of(shard1, shard1WriteLoadSecondPoll))),
            List.of(new FailedNodeException(node2.getId(), "simulated failure", new RuntimeException("boom")))
        );
        final CollectedUsageStats secondCollectedStats = safeAwait(
            l -> collector.collectUsageStats(clientReturning(secondPartialResponse), clusterState, true, l)
        );

        assertThat(secondCollectedStats.nodeUsageStats().keySet(), equalTo(Set.of(node1.getId(), node2.getId())));

        // node-1's thread pool stats and shard load reflect the new poll.
        assertThat(
            secondCollectedStats.nodeUsageStats().get(node1.getId()),
            not(equalTo(firstCollectedStats.nodeUsageStats().get(node1.getId())))
        );
        assertThat(
            secondCollectedStats.nodeUsageStats().get(node1.getId()),
            equalTo(secondCollectedStats.nodeUsageStats().get(node1.getId()))
        );
        assertThat(secondCollectedStats.shardWriteLoads().get(shard1), equalTo(shard1WriteLoadSecondPoll));

        // node-2's last known stats and shard load were returned.
        assertThat(
            secondCollectedStats.nodeUsageStats().get(node2.getId()),
            equalTo(firstCollectedStats.nodeUsageStats().get(node2.getId()))
        );
        assertThat(
            secondCollectedStats.nodeUsageStats().get(node2.getId()),
            equalTo(firstCollectedStats.nodeUsageStats().get(node2.getId()))
        );
        assertThat(secondCollectedStats.shardWriteLoads().get(shard2), equalTo(shard2WriteLoadFirstPoll));
    }

    /**
     * Verifies that when shard write loads are not requested, any shard write loads cached from an earlier poll are discarded rather than
     * returned as stale values, even for a node that fails to respond (whose cached thread pool stats are still returned).
     */
    public void testCachedShardWriteLoadsAreDiscardedWhenNotRequested() {
        final DiscoveryNode node1 = DiscoveryNodeUtils.create("node-1");
        final ClusterState clusterState = createClusterStateWithNodes(node1);
        final ShardId shard1 = new ShardId("index", "uuid", 0);
        final double shard1WriteLoad = 4.0;
        final NodeUsageStatsForThreadPoolsCollector collector = new NodeUsageStatsForThreadPoolsCollector();

        // First poll requests shard write loads, which are cached.
        final NodeUsageStatsForThreadPoolsAction.Response firstFullResponse = createSuccessfulResponse(
            List.of(nodeResponseWithRandomThreadPoolUsage(node1, 8, Map.of(shard1, shard1WriteLoad)))
        );
        final CollectedUsageStats firstCollectedStats = safeAwait(
            l -> collector.collectUsageStats(clientReturning(firstFullResponse), clusterState, true, l)
        );
        assertThat(firstCollectedStats.shardWriteLoads(), equalTo(Map.of(shard1, shard1WriteLoad)));

        // Second poll does not request shard write loads, and node-1 fails to respond.
        final NodeUsageStatsForThreadPoolsAction.Response secondFailedResponse = new NodeUsageStatsForThreadPoolsAction.Response(
            ClusterName.DEFAULT,
            List.of(),
            List.of(new FailedNodeException(node1.getId(), "simulated failure", new RuntimeException("boom")))
        );
        final CollectedUsageStats secondCollectedStats = safeAwait(
            l -> collector.collectUsageStats(clientReturning(secondFailedResponse), clusterState, false, l)
        );

        // node-1's last known thread pool stats are returned, but its cached shard write loads are not.
        assertThat(secondCollectedStats.nodeUsageStats(), equalTo(firstCollectedStats.nodeUsageStats()));
        assertThat(secondCollectedStats.shardWriteLoads(), equalTo(Map.of()));
    }

    private static ClusterState createClusterStateWithNodes(DiscoveryNode... nodes) {
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        final ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
            stateBuilder.putCompatibilityVersions(node.getId(), TransportVersion.current(), Map.of());
        }
        nodesBuilder.masterNodeId(nodes[0].getId()).localNodeId(nodes[0].getId());
        return stateBuilder.nodes(nodesBuilder).build();
    }

    private static NodeUsageStatsForThreadPoolsAction.NodeResponse nodeResponseWithRandomThreadPoolUsage(
        DiscoveryNode node,
        int writeThreads,
        Map<ShardId, Double> shardWriteLoads
    ) {
        return new NodeUsageStatsForThreadPoolsAction.NodeResponse(
            node,
            new NodeUsageStatsForThreadPools(
                node.getId(),
                Map.of(ThreadPool.Names.WRITE, new ThreadPoolUsageStats(writeThreads, randomFloat(), randomLongBetween(0, 1000)))
            ),
            shardWriteLoads
        );
    }

    private static NodeUsageStatsForThreadPoolsAction.Response createSuccessfulResponse(
        List<NodeUsageStatsForThreadPoolsAction.NodeResponse> nodeResponses
    ) {
        return new NodeUsageStatsForThreadPoolsAction.Response(ClusterName.DEFAULT, nodeResponses, List.of());
    }

    private static Client clientReturning(NodeUsageStatsForThreadPoolsAction.Response response) {
        final Client client = mock(Client.class);
        doAnswer(invocation -> {
            final ActionListener<NodeUsageStatsForThreadPoolsAction.Response> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());
        return client;
    }
}
