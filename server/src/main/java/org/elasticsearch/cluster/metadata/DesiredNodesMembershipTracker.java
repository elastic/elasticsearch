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
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.HashSet;
import java.util.Set;

public class DesiredNodesMembershipTracker implements ClusterStateListener {

    public static final Setting<TimeValue> LEFT_NODE_GRACE_PERIOD = Setting.timeSetting(
        "desired_nodes.left_node_grace_period",
        TimeValue.timeValueMinutes(15),
        Setting.Property.NodeScope
    );

    private final Set<DesiredNode> members;
    private final Cache<String, DesiredNode> quarantinedMembers;
    private String latestHistoryId = null;

    DesiredNodesMembershipTracker(Settings settings) {
        TimeValue leftNodeQuarantinePeriod = LEFT_NODE_GRACE_PERIOD.get(settings);
        this.members = new HashSet<>();
        this.quarantinedMembers = CacheBuilder.<String, DesiredNode>builder().setExpireAfterWrite(leftNodeQuarantinePeriod).build();
    }

    public static DesiredNodesMembershipTracker create(Settings settings, ClusterService clusterService) {
        var tracker = new DesiredNodesMembershipTracker(settings);
        clusterService.addListener(tracker);
        return tracker;
    }

    @Override
    public synchronized void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            final var clusterState = event.state();
            final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
            if (desiredNodes == null) {
                return;
            }

            if (event.nodesChanged()) {
                final var nodesDelta = event.nodesDelta();

                for (DiscoveryNode addedNode : nodesDelta.addedNodes()) {
                    final var desiredNode = desiredNodes.find(addedNode.getExternalId());
                    if (desiredNode != null) {
                        addAsMember(desiredNode);
                    }
                }

                for (DiscoveryNode removedNode : nodesDelta.removedNodes()) {
                    final var desiredNode = desiredNodes.find(removedNode.getExternalId());
                    moveToQuarantineIfMember(desiredNode);
                }
            } else if (event.changedCustomMetadataSet().contains(DesiredNodesMetadata.TYPE)) {
                if (desiredNodes.historyID().equals(latestHistoryId) == false) {
                    members.clear();
                    quarantinedMembers.invalidateAll();
                }
                latestHistoryId = desiredNodes.historyID();

                final Set<DesiredNode> unknownDesiredNodes = new HashSet<>(members);
                for (DiscoveryNode node : clusterState.nodes()) {
                    final var desiredNode = desiredNodes.find(node.getExternalId());
                    if (desiredNode != null) {
                        addAsMember(desiredNode);
                        unknownDesiredNodes.remove(desiredNode);
                    }
                }

                for (DesiredNode unknownNode : unknownDesiredNodes) {
                    moveToQuarantineIfMember(unknownNode);
                }
            }
        } else if (event.previousState().nodes().isLocalNodeElectedMaster()) {
            members.clear();
            quarantinedMembers.invalidateAll();
        } else {
            assert members.isEmpty();
            assert quarantinedMembers.count() == 0;
        }
    }

    private void addAsMember(DesiredNode desiredNode) {
        members.add(desiredNode);
        quarantinedMembers.invalidate(desiredNode.externalId());
    }

    private void moveToQuarantineIfMember(DesiredNode desiredNode) {
        if (desiredNode != null && members.remove(desiredNode)) {
            quarantinedMembers.put(desiredNode.externalId(), desiredNode);
        }
    }

    public synchronized boolean isMember(DesiredNode desiredNode) {
        return members.contains(desiredNode) || isQuarantined(desiredNode);
    }

    // visible for testing
    synchronized boolean isQuarantined(DesiredNode desiredNode) {
        return quarantinedMembers.get(desiredNode.externalId()) != null;
    }

    // visible for testing
    synchronized int trackedMembers() {
        return members.size() + quarantinedMembers.count();
    }
}
