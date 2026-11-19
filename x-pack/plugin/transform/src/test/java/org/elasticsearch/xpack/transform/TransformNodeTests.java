/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformNodeTests extends ESTestCase {
    private static final String SHUTTING_DOWN_ID = "shuttingDownNodeId";
    private static final String NOT_SHUTTING_DOWN_ID = "notShuttingDownId";

    /**
     * When the local node is shutting down
     * Then we return true
     */
    public void testIsShuttingDown() {
        var isShuttingDown = new TransformNode(clusterState(SHUTTING_DOWN_ID)).isShuttingDown();
        assertTrue(isShuttingDown.isPresent());
        assertTrue(isShuttingDown.get());
    }

    /**
     * When the local node is not shutting down
     * Then we return false
     */
    public void testIsNotShuttingDown() {
        var isShuttingDown = new TransformNode(clusterState(NOT_SHUTTING_DOWN_ID)).isShuttingDown();
        assertTrue(isShuttingDown.isPresent());
        assertFalse(isShuttingDown.get());
    }

    /**
     * When the local node is null
     * Then we return empty
     */
    public void testMissingLocalId() {
        var isShuttingDown = new TransformNode(clusterState(null)).isShuttingDown();
        assertFalse(isShuttingDown.isPresent());
    }

    /**
     * When the cluster state is empty
     * Then we return empty
     */
    public void testClusterStateMissing() {
        var isShuttingDown = new TransformNode(Optional::empty).isShuttingDown();
        assertFalse(isShuttingDown.isPresent());
    }

    /**
     * When there is a local node
     * Then return its id
     */
    public void testNodeId() {
        var nodeId = new TransformNode(clusterState(SHUTTING_DOWN_ID)).nodeId();
        assertThat(nodeId, equalTo(SHUTTING_DOWN_ID));
    }

    /**
     * When the local node is null
     * Then return "null"
     */
    public void testNodeIdMissing() {
        var nodeId = new TransformNode(Optional::empty).nodeId();
        assertThat(nodeId, equalTo(String.valueOf((String) null)));
    }

    /**
     * When tasks are registered and deregistered
     * Then the registry reflects only the currently registered tasks
     */
    public void testTransformTaskRegistry() {
        var transformNode = new TransformNode(Optional::empty);
        assertThat(transformNode.getTransformTasks(), empty());

        var task1 = mockTask("transform-1");
        var task2 = mockTask("transform-2");
        transformNode.registerTransform(task1);
        transformNode.registerTransform(task2);
        assertThat(transformNode.getTransformTasks(), containsInAnyOrder(task1, task2));

        transformNode.deregisterTransform("transform-1");
        assertThat(transformNode.getTransformTasks(), contains(task2));

        // deregistering an unknown transform is a no-op
        transformNode.deregisterTransform("never-registered");
        assertThat(transformNode.getTransformTasks(), contains(task2));

        transformNode.deregisterTransform("transform-2");
        assertThat(transformNode.getTransformTasks(), empty());
    }

    /**
     * When a task is re-registered under the same transform id
     * Then it replaces the previous entry
     */
    public void testRegisterReplacesPreviousTask() {
        var transformNode = new TransformNode(Optional::empty);
        var oldTask = mockTask("transform-1");
        var newTask = mockTask("transform-1");
        transformNode.registerTransform(oldTask);
        transformNode.registerTransform(newTask);
        assertThat(transformNode.getTransformTasks(), contains(newTask));
    }

    private TransformTask mockTask(String transformId) {
        var task = mock(TransformTask.class);
        when(task.getTransformId()).thenReturn(transformId);
        return task;
    }

    private Supplier<Optional<ClusterState>> clusterState(String nodeId) {
        var nodesShutdownMetadata = new NodesShutdownMetadata(
            Map.of(
                SHUTTING_DOWN_ID,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(SHUTTING_DOWN_ID)
                    .setNodeEphemeralId(SHUTTING_DOWN_ID)
                    .setReason("shutdown for a unit test")
                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setGracePeriod(null)
                    .build()
            )
        );

        var nodes = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(SHUTTING_DOWN_ID)).localNodeId(nodeId).masterNodeId(nodeId);

        if (SHUTTING_DOWN_ID.equals(nodeId) == false && nodeId != null) {
            nodes.add(DiscoveryNodeUtils.create(nodeId));
        }

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .nodes(nodes)
            .build();

        return () -> Optional.of(state);
    }
}
