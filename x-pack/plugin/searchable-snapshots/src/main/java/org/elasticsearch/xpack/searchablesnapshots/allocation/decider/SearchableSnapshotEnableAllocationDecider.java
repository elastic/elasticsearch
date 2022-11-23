/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

public class SearchableSnapshotEnableAllocationDecider extends AllocationDecider {

    static final String NAME = "searchable_snapshots_enable";

    /**
     * This setting describes whether searchable snapshots are allocated during rolling restarts. For now, whether a rolling restart is
     * ongoing is determined by cluster.routing.allocation.enable=primaries. Notice that other values for that setting except "all" mean
     * that no searchable snapshots are allocated anyway.
     */
    public static final Setting<Boolean> SEARCHABLE_SNAPSHOTS_ALLOCATE_ON_ROLLING_RESTART = Setting.boolSetting(
        "xpack.searchable.snapshot.allocate_on_rolling_restart",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    static {
        // TODO xpack.searchable.snapshot.allocate_on_rolling_restart was only temporary, remove it in the next major
        assert Version.CURRENT.major == Version.V_7_17_0.major + 1;
    }

    private volatile EnableAllocationDecider.Allocation enableAllocation;
    private volatile boolean allocateOnRollingRestart;

    public SearchableSnapshotEnableAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.enableAllocation = EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.get(settings);
        this.allocateOnRollingRestart = SEARCHABLE_SNAPSHOTS_ALLOCATE_ON_ROLLING_RESTART.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
            this::setEnableAllocation
        );
        clusterSettings.addSettingsUpdateConsumer(SEARCHABLE_SNAPSHOTS_ALLOCATE_ON_ROLLING_RESTART, this::setAllocateOnRollingRestart);
    }

    private void setEnableAllocation(EnableAllocationDecider.Allocation allocation) {
        this.enableAllocation = allocation;
    }

    private void setAllocateOnRollingRestart(boolean allocateOnRollingRestart) {
        this.allocateOnRollingRestart = allocateOnRollingRestart;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (allocation.isSimulating()) {
            return allocation.decision(Decision.YES, NAME, "allocation is always enabled when simulating");
        }

        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (indexMetadata.isSearchableSnapshot()) {
            EnableAllocationDecider.Allocation enableAllocationCopy = this.enableAllocation;
            boolean allocateOnRollingRestartCopy = this.allocateOnRollingRestart;
            if (enableAllocationCopy == EnableAllocationDecider.Allocation.PRIMARIES) {
                if (allocateOnRollingRestartCopy == false) {
                    return allocation.decision(
                        Decision.NO,
                        NAME,
                        "no allocations of searchable snapshots allowed during rolling restart due to [%s=%s] and [%s=false]",
                        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                        enableAllocationCopy,
                        SEARCHABLE_SNAPSHOTS_ALLOCATE_ON_ROLLING_RESTART.getKey()
                    );
                } else {
                    return allocation.decision(
                        Decision.YES,
                        NAME,
                        "allocate on rolling restart enabled [%s=true]",
                        SEARCHABLE_SNAPSHOTS_ALLOCATE_ON_ROLLING_RESTART.getKey()
                    );
                }
            } else {
                return allocation.decision(
                    Decision.YES,
                    NAME,
                    "decider only active during rolling restarts [%s=primaries]",
                    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()
                );
            }
        } else {
            return allocation.decision(Decision.YES, NAME, "decider only applicable for indices backed by searchable snapshots");
        }
    }
}
