/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Requests reroutes for pending snapshot restores when indexing-node storage information changes in a way
 * that may admit a previously THROTTLEd restore.
 */
public class SnapshotRestoreStorageMonitor {
    private static final Logger logger = LogManager.getLogger(SnapshotRestoreStorageMonitor.class);

    /**
     * Operator-only; registered by StatelessPlugin. Temporary with restore disk capacity checks.
     * Rate-limits retries while snapshot restores remain unassigned; structural storage changes always
     * reroute immediately and are never subject to this interval.
     */
    public static final Setting<TimeValue> REROUTE_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.snapshot_restore.reroute_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.ZERO,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private final Supplier<ClusterState> clusterState;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;
    private volatile TimeValue rerouteInterval;
    // Accessed only by onNewInfo callbacks. InternalClusterInfoService serializes these callbacks and
    // safely publishes their writes through the synchronized refresh handoff, even when the callback thread changes.
    private Map<String, Storage> nodeStorage = Map.of();
    private long lastRerouteTimeMillis;

    private record Storage(String path, ClusterInfo.ReservedSpace reservations) {}

    public SnapshotRestoreStorageMonitor(
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillisSupplier,
        Supplier<ClusterState> clusterState,
        RerouteService rerouteService
    ) {
        this.clusterState = clusterState;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
        clusterSettings.initializeAndWatchIfRegistered(REROUTE_INTERVAL_SETTING, value -> this.rerouteInterval = value);
    }

    public void onNewInfo(ClusterInfo info) {
        var state = clusterState.get();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            nodeStorage = Map.of();
            return;
        }
        Map<String, Storage> nodeStorageNow = new HashMap<>();
        for (var node : state.nodes()) {
            if (node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
                var disk = info.getNodeMostAvailableDiskUsages().get(node.getId());
                if (disk != null) {
                    nodeStorageNow.put(node.getId(), new Storage(disk.path(), info.getReservedSpace(node.getId(), disk.path())));
                }
            }
        }
        boolean structuralChange = nodeStorageNow.equals(nodeStorage) == false;
        nodeStorage = Collections.unmodifiableMap(nodeStorageNow);

        boolean hasUnassignedSnapshotPrimary = state.getRoutingNodes()
            .unassigned()
            .stream()
            .anyMatch(shard -> shard.primary() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT);
        if (hasUnassignedSnapshotPrimary == false) {
            return;
        }

        long now = currentTimeMillisSupplier.getAsLong();
        boolean intervalElapsed = (now - lastRerouteTimeMillis) >= rerouteInterval.millis();
        if (structuralChange) {
            reroute("snapshot restore storage updated", now);
        } else if (intervalElapsed && nodeStorageNow.isEmpty() == false) {
            reroute("snapshot restore storage retry", now);
        }
    }

    private void reroute(String reason, long now) {
        lastRerouteTimeMillis = now;
        rerouteService.reroute(
            reason,
            Priority.NORMAL,
            ActionListener.wrap(ignored -> {}, e -> logger.debug("reroute after snapshot restore storage update failed", e))
        );
    }
}
