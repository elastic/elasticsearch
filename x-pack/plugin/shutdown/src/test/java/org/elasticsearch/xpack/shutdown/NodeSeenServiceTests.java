/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import static org.elasticsearch.xpack.shutdown.NodeSeenService.RemoveSigtermShutdownTask;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class NodeSeenServiceTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<RemoveSigtermShutdownTask> newMockTaskQueue(ClusterService clusterService) {
        final var masterServiceTaskQueue = mock(MasterServiceTaskQueue.class);
        when(clusterService.<RemoveSigtermShutdownTask>createTaskQueue(eq("shutdown-sigterm-cleaner"), eq(Priority.NORMAL), any()))
            .thenReturn(masterServiceTaskQueue);
        return masterServiceTaskQueue;
    }

    public void testCleanIfRemoved() {
        ClusterService clusterService = mock(ClusterService.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        when(clusterService.threadPool()).thenReturn(mockThreadPool);
        MasterServiceTaskQueue<RemoveSigtermShutdownTask> taskQueue = newMockTaskQueue(clusterService);
        NodeSeenService nodeSeenService = new NodeSeenService(clusterService);
        var master = TestDiscoveryNode.create("node1", randomNodeId());
        var other = TestDiscoveryNode.create("node2", randomNodeId());
        var another = TestDiscoveryNode.create("node3", randomNodeId());

        nodeSeenService.clusterChanged(
            new ClusterChangedEvent(
                this.getTestName(),
                createClusterState(new NodesShutdownMetadata(Map.of(another.getId(), createShutdown(another.getId(), 0))), master, other),
                createClusterState(null, master, other, another)
            )
        );

        Mockito.verify(taskQueue, times(1))
            .submitTask(eq("sigterm nodes left cluster"), any(NodeSeenService.RemoveSigtermShutdownTask.class), isNull());
    }

    public void testDontCleanIfPresent() {
        ClusterService clusterService = mock(ClusterService.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.absoluteTimeInMillis()).thenReturn(120_000L);
        when(clusterService.threadPool()).thenReturn(mockThreadPool);
        MasterServiceTaskQueue<RemoveSigtermShutdownTask> taskQueue = newMockTaskQueue(clusterService);
        NodeSeenService nodeSeenService = new NodeSeenService(clusterService);
        var master = TestDiscoveryNode.create("node1", randomNodeId());
        var other = TestDiscoveryNode.create("node2", randomNodeId());
        var another = TestDiscoveryNode.create("node3", randomNodeId());

        nodeSeenService.clusterChanged(
            new ClusterChangedEvent(
                this.getTestName(),
                createClusterState(
                    new NodesShutdownMetadata(
                        Map.of(another.getId(), createShutdown(another.getId(), 0), other.getId(), createShutdown(other.getId(), 20L))
                    ),
                    master,
                    other,
                    another
                ),
                createClusterState(null, master, other, another)
            )
        );

        Mockito.verify(taskQueue, never()).submitTask(any(), any(), any());
    }

    private static SingleNodeShutdownMetadata createShutdown(String name, long startedAt) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(name)
            .setType(Type.SIGTERM)
            .setReason("test")
            .setStartedAtMillis(startedAt)
            .build();
    }

    private static ClusterState createClusterState(NodesShutdownMetadata shutdown, DiscoveryNode masterNode, DiscoveryNode... nodes) {
        var routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (shutdown != null) {
            metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, shutdown);
        }
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (masterNode != null) {
            nodesBuilder.masterNodeId(masterNode.getId());
            nodesBuilder.localNodeId(masterNode.getId());
            nodesBuilder.add(masterNode);
        }
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder)
            .build();
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }
}
