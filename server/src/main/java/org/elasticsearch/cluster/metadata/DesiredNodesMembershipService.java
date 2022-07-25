/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.HashSet;
import java.util.Set;

public class DesiredNodesMembershipService implements ClusterStateListener {
    private final Set<DesiredNode> members;
    private String latestHistoryId = null;

    DesiredNodesMembershipService() {
        this.members = new HashSet<>();
    }

    public static DesiredNodesMembershipService create(ClusterService clusterService) {
        var tracker = new DesiredNodesMembershipService();
        clusterService.addListener(tracker);
        return tracker;
    }

    @Override
    public synchronized void clusterChanged(ClusterChangedEvent event) {
        final var clusterState = event.state();
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        if (desiredNodes == null) {
            members.clear();
            return;
        }

        if (event.nodesChanged()) {
            final var nodesDelta = event.nodesDelta();

            for (DiscoveryNode addedNode : nodesDelta.addedNodes()) {
                final var desiredNode = desiredNodes.find(addedNode.getExternalId());
                if (desiredNode != null) {
                    members.add(desiredNode);
                }
            }
        } else if (event.changedCustomMetadataSet().contains(DesiredNodesMetadata.TYPE) || latestHistoryId == null) {
            if (desiredNodes.historyID().equals(latestHistoryId) == false) {
                members.clear();
            }
            latestHistoryId = desiredNodes.historyID();

            final Set<DesiredNode> removedDesiredNodes = new HashSet<>(members);
            desiredNodes.nodes().forEach(removedDesiredNodes::remove);

            for (DiscoveryNode node : clusterState.nodes()) {
                final var desiredNode = desiredNodes.find(node.getExternalId());
                if (desiredNode != null) {
                    members.add(desiredNode);
                }
            }

            members.removeAll(removedDesiredNodes);
        }
    }

    public synchronized boolean isMember(DesiredNode desiredNode) {
        return members.contains(desiredNode);
    }

    // visible for testing
    synchronized int trackedMembersCount() {
        return members.size();
    }
}
