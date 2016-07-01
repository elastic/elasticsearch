/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

/**
 * This {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider} prevents shards that
 * are currently been snapshotted to be moved to other nodes.
 */
public class SnapshotInProgressAllocationDecider extends AllocationDecider {

    public static final String NAME = "snapshot_in_progress";

    /**
     * Disables relocation of shards that are currently being snapshotted.
     */
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED_SETTING =
        Setting.boolSetting("cluster.routing.allocation.snapshot.relocation_enabled", false,
            Property.Dynamic, Property.NodeScope);

    private volatile boolean enableRelocation = false;

    /**
     * Creates a new {@link org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider} instance
     */
    public SnapshotInProgressAllocationDecider() {
        this(Settings.Builder.EMPTY_SETTINGS);
    }

    /**
     * Creates a new {@link org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider} instance from
     * given settings
     *
     * @param settings {@link org.elasticsearch.common.settings.Settings} to use
     */
    public SnapshotInProgressAllocationDecider(Settings settings) {
        this(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    @Inject
    public SnapshotInProgressAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        enableRelocation = CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED_SETTING,
                this::setEnableRelocation);
    }

    private void setEnableRelocation(boolean enableRelocation) {
        this.enableRelocation = enableRelocation;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced to the given allocation. The default is
     * {@link Decision#ALWAYS}.
     */
    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canMove(shardRouting, allocation);
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#ALWAYS}.
     */
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canMove(shardRouting, allocation);
    }

    private Decision canMove(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (!enableRelocation && shardRouting.primary()) {
            // Only primary shards are snapshotted

            SnapshotsInProgress snapshotsInProgress = allocation.custom(SnapshotsInProgress.TYPE);
            if (snapshotsInProgress == null) {
                // Snapshots are not running
                return allocation.decision(Decision.YES, NAME, "no snapshots are currently running");
            }

            for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
                SnapshotsInProgress.ShardSnapshotStatus shardSnapshotStatus = snapshot.shards().get(shardRouting.shardId());
                if (shardSnapshotStatus != null && !shardSnapshotStatus.state().completed() && shardSnapshotStatus.nodeId() != null &&
                        shardSnapshotStatus.nodeId().equals(shardRouting.currentNodeId())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Preventing snapshotted shard [{}] to be moved from node [{}]",
                                shardRouting.shardId(), shardSnapshotStatus.nodeId());
                    }
                    return allocation.decision(Decision.NO, NAME, "snapshot for shard [%s] is currently running on node [%s]",
                            shardRouting.shardId(), shardSnapshotStatus.nodeId());
                }
            }
        }
        return allocation.decision(Decision.YES, NAME, "the shard is not primary or relocation is disabled");
    }

}
