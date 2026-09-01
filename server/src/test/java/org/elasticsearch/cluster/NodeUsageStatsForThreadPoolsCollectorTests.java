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
     * Verifies that the collector converts each shard's write-thread-pool utilization into thread time by
     * multiplying by the reporting node's total write thread pool thread count (see
     * {@link org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator#calculateUtilizationForWriteLoad}).
     */
    public void testShardWriteLoadConversionFromUtilizationToThreadTime() {
        final DiscoveryNode node1 = DiscoveryNodeUtils.create("node-1");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("node-2");
        final ClusterState clusterState = createClusterStateWithNodes(node1, node2);

        final ShardId shard1 = new ShardId("index", "uuid", 0);
        final ShardId shard2 = new ShardId("index", "uuid", 1);
        final int node1WriteThreads = 8;
        final int node2WriteThreads = 20;
        final double shard1Utilization = 0.5;
        final double shard2Utilization = 0.25;

        final NodeUsageStatsForThreadPoolsAction.Response response = createSuccessfulResponse(
            List.of(
                nodeResponseWithRandomThreadPoolUsage(node1, node1WriteThreads, Map.of(shard1, shard1Utilization)),
                nodeResponseWithRandomThreadPoolUsage(node2, node2WriteThreads, Map.of(shard2, shard2Utilization))
            )
        );

        final Client client = clientReturning(response);
        final NodeUsageStatsForThreadPoolsCollector collector = new NodeUsageStatsForThreadPoolsCollector();

        final CollectedUsageStats collected = safeAwait(l -> collector.collectUsageStats(client, clusterState, l));

        assertThat(
            collected.shardWriteLoadUtilizations(),
            equalTo(Map.of(shard1, shard1Utilization * node1WriteThreads, shard2, shard2Utilization * node2WriteThreads))
        );
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
        final double shard1UtilizationFirstPoll = 0.5;
        final double shard2UtilizationFirstPoll = 0.25;
        final double shard1UtilizationSecondPoll = 0.75;

        final NodeUsageStatsForThreadPoolsCollector collector = new NodeUsageStatsForThreadPoolsCollector();

        // First poll: both nodes respond successfully.
        final NodeUsageStatsForThreadPoolsAction.Response firstFullResponse = createSuccessfulResponse(
            List.of(
                nodeResponseWithRandomThreadPoolUsage(node1, node1NumWriteThreads, Map.of(shard1, shard1UtilizationFirstPoll)),
                nodeResponseWithRandomThreadPoolUsage(node2, node2NumWriteThreads, Map.of(shard2, shard2UtilizationFirstPoll))
            )
        );
        final CollectedUsageStats firstCollectedResponse = safeAwait(
            l -> collector.collectUsageStats(clientReturning(firstFullResponse), clusterState, l)
        );
        assertThat(firstCollectedResponse.nodeUsageStats().keySet(), equalTo(Set.of(node1.getId(), node2.getId())));
        assertThat(
            firstCollectedResponse.shardWriteLoadUtilizations(),
            equalTo(
                Map.of(shard1, shard1UtilizationFirstPoll * node1NumWriteThreads, shard2, shard2UtilizationFirstPoll * node2NumWriteThreads)
            )
        );

        // Second poll: node-2 fails to respond; node-1 reports updated values.
        final NodeUsageStatsForThreadPoolsAction.Response secondPartialResponse = new NodeUsageStatsForThreadPoolsAction.Response(
            ClusterName.DEFAULT,
            List.of(nodeResponseWithRandomThreadPoolUsage(node1, node1NumWriteThreads, Map.of(shard1, shard1UtilizationSecondPoll))),
            List.of(new FailedNodeException(node2.getId(), "simulated failure", new RuntimeException("boom")))
        );
        final CollectedUsageStats secondCollectedResponse = safeAwait(
            l -> collector.collectUsageStats(clientReturning(secondPartialResponse), clusterState, l)
        );

        assertThat(secondCollectedResponse.nodeUsageStats().keySet(), equalTo(Set.of(node1.getId(), node2.getId())));

        // node-1's thread pool stats and shard load reflect the new poll.
        assertThat(
            secondCollectedResponse.nodeUsageStats().get(node1.getId()),
            not(equalTo(firstCollectedResponse.nodeUsageStats().get(node1.getId())))
        );
        assertThat(
            secondCollectedResponse.nodeUsageStats().get(node1.getId()),
            equalTo(secondCollectedResponse.nodeUsageStats().get(node1.getId()))
        );
        assertThat(
            secondCollectedResponse.shardWriteLoadUtilizations().get(shard1),
            equalTo(shard1UtilizationSecondPoll * node1NumWriteThreads)
        );

        // node-2's last known stats and shard load were returned.
        assertThat(
            secondCollectedResponse.nodeUsageStats().get(node2.getId()),
            equalTo(firstCollectedResponse.nodeUsageStats().get(node2.getId()))
        );
        assertThat(
            secondCollectedResponse.nodeUsageStats().get(node2.getId()),
            equalTo(firstCollectedResponse.nodeUsageStats().get(node2.getId()))
        );
        assertThat(
            secondCollectedResponse.shardWriteLoadUtilizations().get(shard2),
            equalTo(shard2UtilizationFirstPoll * node2NumWriteThreads)
        );
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
