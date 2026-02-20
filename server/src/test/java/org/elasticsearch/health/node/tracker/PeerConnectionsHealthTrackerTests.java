/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.PeerConnectionsHealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeerConnectionsHealthTrackerTests extends ESTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode1;
    private DiscoveryNode remoteNode2;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        localNode = DiscoveryNodeUtils.builder("local").name("local_name").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
        remoteNode1 = DiscoveryNodeUtils.builder("remote_1").name("remote_1_name").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
        remoteNode2 = DiscoveryNodeUtils.builder("remote_2").name("remote_2_name").roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build();
    }

    public void testHealthyWhenAllNodesConnected() {
        setupClusterState(localNode, remoteNode1, remoteNode2);
        when(transportService.nodeConnected(remoteNode1)).thenReturn(true);
        when(transportService.nodeConnected(remoteNode2)).thenReturn(true);

        var tracker = new PeerConnectionsHealthTracker(transportService, clusterService);
        PeerConnectionsHealthInfo result = tracker.determineCurrentHealth();

        assertThat(result, equalTo(PeerConnectionsHealthInfo.HEALTHY));
        assertThat(result.disconnectedPeers(), hasSize(0));
    }

    public void testDetectsDisconnectedPeer() {
        setupClusterState(localNode, remoteNode1, remoteNode2);
        when(transportService.nodeConnected(remoteNode1)).thenReturn(true);
        when(transportService.nodeConnected(remoteNode2)).thenReturn(false);

        var tracker = new PeerConnectionsHealthTracker(transportService, clusterService);
        PeerConnectionsHealthInfo result = tracker.determineCurrentHealth();

        assertThat(result.hasDisconnectedPeers(), equalTo(true));
        assertThat(result.disconnectedPeers(), hasSize(1));
        assertThat(result.disconnectedPeers(), contains(remoteNode2.getId()));
    }

    public void testDetectsMultipleDisconnectedPeers() {
        setupClusterState(localNode, remoteNode1, remoteNode2);
        when(transportService.nodeConnected(remoteNode1)).thenReturn(false);
        when(transportService.nodeConnected(remoteNode2)).thenReturn(false);

        var tracker = new PeerConnectionsHealthTracker(transportService, clusterService);
        PeerConnectionsHealthInfo result = tracker.determineCurrentHealth();

        assertThat(result.hasDisconnectedPeers(), equalTo(true));
        assertThat(result.disconnectedPeers(), hasSize(2));
    }

    public void testHealthyWhenSingleNodeCluster() {
        setupClusterState(localNode);

        var tracker = new PeerConnectionsHealthTracker(transportService, clusterService);
        PeerConnectionsHealthInfo result = tracker.determineCurrentHealth();

        assertThat(result, equalTo(PeerConnectionsHealthInfo.HEALTHY));
    }

    public void testCheckHealthChangedDetectsTransition() {
        setupClusterState(localNode, remoteNode1);
        when(transportService.nodeConnected(remoteNode1)).thenReturn(true);

        var tracker = new PeerConnectionsHealthTracker(transportService, clusterService);

        boolean changed = tracker.checkHealthChanged();
        assertThat(changed, equalTo(true));

        changed = tracker.checkHealthChanged();
        assertThat(changed, equalTo(false));

        when(transportService.nodeConnected(remoteNode1)).thenReturn(false);
        changed = tracker.checkHealthChanged();
        assertThat(changed, equalTo(true));
    }

    private void setupClusterState(DiscoveryNode... nodes) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(nodes[0].getId()).masterNodeId(nodes[0].getId());
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder).build();
        when(clusterService.state()).thenReturn(clusterState);
    }
}
