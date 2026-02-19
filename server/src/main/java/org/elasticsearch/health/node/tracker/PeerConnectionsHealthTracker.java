/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.PeerConnectionsHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Determines the health of peer-to-peer transport connections from this node to other nodes in the cluster.
 * A peer is considered disconnected if it is present in the cluster state but not currently connected via
 * the transport layer.
 */
public class PeerConnectionsHealthTracker extends HealthTracker<PeerConnectionsHealthInfo> {

    private final TransportService transportService;
    private final ClusterService clusterService;

    public PeerConnectionsHealthTracker(TransportService transportService, ClusterService clusterService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
    }

    @Override
    protected PeerConnectionsHealthInfo determineCurrentHealth() {
        var clusterState = clusterService.state();
        var localNode = clusterState.nodes().getLocalNode();
        if (localNode == null) {
            return PeerConnectionsHealthInfo.HEALTHY;
        }

        List<String> disconnectedPeers = new ArrayList<>();
        for (DiscoveryNode node : clusterState.nodes()) {
            if (node.equals(localNode)) {
                continue;
            }
            if (transportService.nodeConnected(node) == false) {
                disconnectedPeers.add(node.getId());
            }
        }

        if (disconnectedPeers.isEmpty()) {
            return PeerConnectionsHealthInfo.HEALTHY;
        }
        return new PeerConnectionsHealthInfo(disconnectedPeers);
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, PeerConnectionsHealthInfo healthInfo) {
        builder.peerConnectionsHealthInfo(healthInfo);
    }
}
