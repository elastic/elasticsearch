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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PerNodeShardSnapshotCounter {

    private final int shardSnapshotPerNodeLimit;
    private final Map<String, Integer> perNodeCounts;
    private final Map<Snapshot, Set<ShardId>> limitedShardsBySnapshot;

    private PerNodeShardSnapshotCounter(
        Map<Snapshot, Set<ShardId>> limitedShardsBySnapshot,
        Map<String, Integer> perNodeCounts,
        int shardSnapshotPerNodeLimit
    ) {
        this.limitedShardsBySnapshot = limitedShardsBySnapshot;
        this.perNodeCounts = perNodeCounts;
        this.shardSnapshotPerNodeLimit = shardSnapshotPerNodeLimit;
    }

    public static PerNodeShardSnapshotCounter create(
        SnapshotsInProgress snapshotsInProgress,
        DiscoveryNodes nodes,
        Map<Snapshot, Set<ShardId>> limitedShardsBySnapshot,
        int shardSnapshotPerNodeLimit,
        boolean isStateless
    ) {
        final Map<String, Integer> perNodeCounts;
        if (shardSnapshotPerNodeLimit <= 0) {
            perNodeCounts = Map.of();
        } else {
            perNodeCounts = nodes.getDataNodes()
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
        }
        return new PerNodeShardSnapshotCounter(limitedShardsBySnapshot, perNodeCounts, shardSnapshotPerNodeLimit);
    }

    public boolean tryStartShardSnapshotOnNode(String nodeId, Snapshot snapshot, ShardId shardId) {
        if (enabled() == false) {
            return true;
        }
        final Integer count = perNodeCounts.get(nodeId);
        if (count == null) {
            return false;
        }
        if (count < shardSnapshotPerNodeLimit) {
            perNodeCounts.put(nodeId, count + 1);
            final var shards = limitedShardsBySnapshot.get(snapshot);
            if (shards != null) {
                shards.remove(shardId);
                if (shards.isEmpty()) {
                    limitedShardsBySnapshot.remove(snapshot);
                }
            }
            return true;
        } else {
            addLimitedShardSnapshot(snapshot, shardId);
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

    public void addLimitedShardSnapshot(Snapshot snapshot, ShardId shardId) {
        if (enabled() == false) {
            return;
        }
        final var added = limitedShardsBySnapshot.computeIfAbsent(snapshot, ignore -> new HashSet<>()).add(shardId);
        assert added : "shard " + shardId + " snapshot [" + snapshot + "] is already limited in " + limitedShardsBySnapshot;
    }

    public boolean isShardLimitedForRepo(ProjectId projectId, String repository, ShardId shardId) {
        if (enabled() == false) {
            return false;
        }
        for (var snapshot : limitedShardsBySnapshot.keySet()) {
            if (snapshot.getProjectId().equals(projectId) && snapshot.getRepository().equals(repository)) {
                if (limitedShardsBySnapshot.get(snapshot).contains(shardId)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isShardLimitedForSnapshot(Snapshot snapshot, ShardId shardId) {
        if (enabled() == false) {
            return false;
        }
        return limitedShardsBySnapshot.computeIfAbsent(snapshot, ignore -> Set.of()).contains(shardId);
    }

    public boolean hasAnyLimitedShardsForSnapshot(Snapshot snapshot) {
        return limitedShardsBySnapshot.computeIfAbsent(snapshot, ignore -> Set.of()).isEmpty() == false;
    }

    public boolean removeLimitedShardForSnapshot(Snapshot snapshot, ShardId shardId) {
        if (enabled() == false) {
            return false;
        }
        final var shards = limitedShardsBySnapshot.get(snapshot);
        if (shards != null) {
            final var removed = shards.remove(shardId);
            if (shards.isEmpty()) {
                limitedShardsBySnapshot.remove(snapshot);
            }
            return removed;
        }
        return false;
    }

    @Override
    public String toString() {
        return "PerNodeShardSnapshotCounter{"
            + "shardSnapshotPerNodeLimit="
            + shardSnapshotPerNodeLimit
            + ", perNodeCounts="
            + perNodeCounts
            + ", limitedShardsBySnapshot="
            + limitedShardsBySnapshot
            + '}';
    }

    private boolean enabled() {
        return shardSnapshotPerNodeLimit > 0;
    }

    private static boolean isRunningOnDataNode(SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus) {
        return shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.INIT
            // Aborted shard snapshot is still be running on the data node until FAILED
            || shardSnapshotStatus.state() == SnapshotsInProgress.ShardState.ABORTED;
    }
}
