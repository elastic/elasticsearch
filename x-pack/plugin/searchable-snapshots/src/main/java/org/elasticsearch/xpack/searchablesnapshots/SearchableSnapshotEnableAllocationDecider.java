/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
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
     * This setting indicates the behavior of searchable snapshots when cluster.routing.allocation.enable=primaries
     *
     */
    public static final Setting<EnableAllocationDecider.Allocation> SEARCHABLE_SNAPSHOTS_ALLOCATION_ENABLE_PRIMARIES_SETTING =
        new Setting<>("xpack.searchable.snapshot.allocation.enable.primaries", EnableAllocationDecider.Allocation.NONE.toString(),
            SearchableSnapshotEnableAllocationDecider::parseSetting,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private static EnableAllocationDecider.Allocation parseSetting(String value) {
        EnableAllocationDecider.Allocation allocation = EnableAllocationDecider.Allocation.parse(value);
        if (allocation == EnableAllocationDecider.Allocation.ALL || allocation == EnableAllocationDecider.Allocation.NEW_PRIMARIES) {
            throw new IllegalArgumentException("[" + SEARCHABLE_SNAPSHOTS_ALLOCATION_ENABLE_PRIMARIES_SETTING.getKey() + "=" + allocation
                 + "] is not valid");
        }
        return allocation;
    }

    private volatile EnableAllocationDecider.Allocation enableAllocation;
    private volatile EnableAllocationDecider.Allocation primariesAllocation;

    public SearchableSnapshotEnableAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.enableAllocation = EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.get(settings);
        this.primariesAllocation = SEARCHABLE_SNAPSHOTS_ALLOCATION_ENABLE_PRIMARIES_SETTING.get(settings);
        assertSettingValue();
        clusterSettings.addSettingsUpdateConsumer(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
            this::setEnableAllocation);
        clusterSettings.addSettingsUpdateConsumer(SEARCHABLE_SNAPSHOTS_ALLOCATION_ENABLE_PRIMARIES_SETTING,
            this::setPrimariesAllocation);
    }

    public void assertSettingValue() {
        assert this.primariesAllocation != EnableAllocationDecider.Allocation.ALL && this.primariesAllocation != EnableAllocationDecider.Allocation.NEW_PRIMARIES;
    }

    private void setPrimariesAllocation(EnableAllocationDecider.Allocation allocation) {
        this.primariesAllocation = allocation;
        assertSettingValue();
    }

    private void setEnableAllocation(EnableAllocationDecider.Allocation allocation) {
        this.enableAllocation = allocation;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexMetadata.getSettings())) {
            EnableAllocationDecider.Allocation enableAllocation = this.enableAllocation;
            EnableAllocationDecider.Allocation primariesAllocation = this.primariesAllocation;
            if (enableAllocation == EnableAllocationDecider.Allocation.PRIMARIES) {
                if (primariesAllocation == EnableAllocationDecider.Allocation.NONE) {
                    return allocation.decision(Decision.NO, NAME,
                        "no allocations of searchable snapshots allowed due to [%s=%s] and [%s=%s]",
                        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                        enableAllocation,
                        SEARCHABLE_SNAPSHOTS_ALLOCATION_ENABLE_PRIMARIES_SETTING.getKey(),
                        primariesAllocation
                    );
                } else {
                    return allocation.decision(Decision.YES, NAME, "decider relies on generic enable decider");
                }
            } else {
                return allocation.decision(Decision.YES, NAME, "decider only active when [%s=primaries]",
                    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey());
            }
        } else {
            return allocation.decision(Decision.YES, NAME, "decider only applicable for indices backed by searchable snapshots");
        }
    }
}
