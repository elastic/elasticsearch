/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
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

    private Supplier<Optional<ClusterState>> clusterState(String nodeId) {
        var clusterState = mock(ClusterState.class);

        var nodeShutdowns = mock(NodesShutdownMetadata.class);
        when(nodeShutdowns.contains(anyString())).thenReturn(SHUTTING_DOWN_ID.equals(nodeId));

        var metadata = mock(Metadata.class);
        when(metadata.nodeShutdowns()).thenReturn(nodeShutdowns);
        when(clusterState.metadata()).thenReturn(metadata);

        var discoveryNodes = mock(DiscoveryNodes.class);

        when(discoveryNodes.getLocalNodeId()).thenReturn(nodeId);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        return () -> Optional.of(clusterState);
    }
}
