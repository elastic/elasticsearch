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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InternalDesiredNodesMembershipService implements ClusterStateListener, DesiredNodesMembershipService {
    private final ClusterService clusterService;
    private volatile Set<DesiredNode> members;
    private String latestHistoryId = null;

    public InternalDesiredNodesMembershipService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.members = new HashSet<>();
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final var clusterState = event.state();
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        if (desiredNodes == null) {
            members = Collections.emptySet();
            return;
        }

        if (event.nodesChanged()) {
            final var nodesDelta = event.nodesDelta();

            final Set<DesiredNode> updatedMembers = new HashSet<>(members);
            for (DiscoveryNode addedNode : nodesDelta.addedNodes()) {
                final var desiredNode = desiredNodes.find(addedNode.getExternalId());
                if (desiredNode != null) {
                    updatedMembers.add(desiredNode);
                }
            }
            members = Collections.unmodifiableSet(updatedMembers);
        } else if (event.changedCustomMetadataSet().contains(DesiredNodesMetadata.TYPE) || latestHistoryId == null) {
            final Set<DesiredNode> updatedMembers = desiredNodes.historyID().equals(latestHistoryId)
                ? new HashSet<>(members)
                : new HashSet<>();
            latestHistoryId = desiredNodes.historyID();

            final Map<String, DesiredNode> removedDesiredNodes = updatedMembers.stream()
                .collect(Collectors.toMap(DesiredNode::externalId, Function.identity()));
            for (DesiredNode desiredNode : desiredNodes) {
                removedDesiredNodes.remove(desiredNode.externalId());
            }

            for (DiscoveryNode node : clusterState.nodes()) {
                final var desiredNode = desiredNodes.find(node.getExternalId());
                if (desiredNode != null) {
                    updatedMembers.add(desiredNode);
                }
            }

            removedDesiredNodes.values().forEach(updatedMembers::remove);
            members = Collections.unmodifiableSet(updatedMembers);
        }
    }

    @Override
    public DesiredNodes.MembershipInformation getMembershipInformation() {
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());

        return new DesiredNodes.MembershipInformation(desiredNodes, members);
    }
}
