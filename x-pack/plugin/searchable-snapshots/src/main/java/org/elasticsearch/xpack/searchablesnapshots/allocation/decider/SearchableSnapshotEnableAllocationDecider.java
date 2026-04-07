/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

public class SearchableSnapshotEnableAllocationDecider extends AllocationDecider {

    static final String NAME = "searchable_snapshots_enable";

    private volatile EnableAllocationDecider.Allocation enableAllocation;

    public SearchableSnapshotEnableAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.enableAllocation = EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING,
            this::setEnableAllocation
        );
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
        if (allocation.isSimulating()) {
            return allocation.decision(Decision.YES, NAME, "allocation is always enabled when simulating");
        }

        final IndexMetadata indexMetadata = allocation.metadata().indexMetadata(shardRouting.index());
        if (indexMetadata.isSearchableSnapshot()) {
            EnableAllocationDecider.Allocation enableAllocationCopy = this.enableAllocation;
            if (enableAllocationCopy == EnableAllocationDecider.Allocation.PRIMARIES) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "no allocations of searchable snapshots allowed during rolling restart due to [%s=%s]",
                    EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                    enableAllocationCopy
                );
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
