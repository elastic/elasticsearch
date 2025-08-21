/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.Map;
import java.util.stream.Collectors;

public class PerNodeShardSnapshotCounter {

    private final int shardSnapshotPerNodeLimit;
    private final Map<String, Integer> perNodeCounts;

    public PerNodeShardSnapshotCounter(
        int shardSnapshotPerNodeLimit,
        SnapshotsInProgress snapshotsInProgress,
        DiscoveryNodes nodes,
        boolean isStateless
    ) {
        this.shardSnapshotPerNodeLimit = shardSnapshotPerNodeLimit;
        if (this.shardSnapshotPerNodeLimit <= 0) {
            this.perNodeCounts = Map.of();
        } else {
            final var perNodeCounts = nodes.getDataNodes()
                .values()
                .stream()
                .filter(node -> isStateless == false || node.hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()))
                .filter(node -> snapshotsInProgress.isNodeIdForRemoval(node.getId()) == false)
                .collect(Collectors.toMap(DiscoveryNode::getId, node -> 0));

            snapshotsInProgress.asStream().forEach(entry -> {
                if (entry.state().completed() || entry.isClone()) {
                    return;
                }
                for (var shardSnapshotStatus : entry.shards().values()) {
                    if (isRunningOnDataNode(shardSnapshotStatus)) {
                        perNodeCounts.computeIfPresent(shardSnapshotStatus.nodeId(), (nodeId, count) -> count + 1);
                    }
                }
            });
            this.perNodeCounts = perNodeCounts;
        }
    }

    public boolean tryStartShardSnapshotOnNode(String nodeId) {
        if (enabled() == false) {
            return true;
        }
        final Integer count = perNodeCounts.get(nodeId);
        if (count == null) {
            return false;
        }
        if (count < shardSnapshotPerNodeLimit) {
            perNodeCounts.put(nodeId, count + 1);
            return true;
        } else {
            return false;
        }
    }

    public boolean completeShardSnapshotOnNode(String nodeId) {
        if (enabled() == false) {
            return true;
        }
        final Integer count = perNodeCounts.get(nodeId);
        if (count == null) {
            return false;
        }
        if (count <= 0) {
            return false;
        }
        perNodeCounts.put(nodeId, count - 1);
        return true;
    }

    public boolean hasCapacityOnAnyNode() {
        return enabled() == false || perNodeCounts.values().stream().anyMatch(count -> count < shardSnapshotPerNodeLimit);
    }

    @Override
    public String toString() {
        return "PerNodeShardSnapshotCounter{"
            + "shardSnapshotPerNodeLimit="
            + shardSnapshotPerNodeLimit
            + ", perNodeCounts="
            + perNodeCounts
            + '}';
    }

    private boolean enabled() {
        return shardSnapshotPerNodeLimit > 0;
    }

    private static boolean isRunningOnDataNode(SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus) {
        return shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.INIT
            // Aborted shard snapshot may still be running on the data node unless it was assigned-queued, i.e. never actually started
            || (shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.ABORTED
                && shardSnapshotStatus.isAbortedAssignedQueued() == false);
    }
}
