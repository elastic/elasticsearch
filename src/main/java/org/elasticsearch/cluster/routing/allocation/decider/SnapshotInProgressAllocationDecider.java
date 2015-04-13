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

import org.elasticsearch.cluster.metadata.SnapshotMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * This {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider} prevents shards that
 * are currently been snapshotted to be moved to other nodes.
 */
public class SnapshotInProgressAllocationDecider extends AllocationDecider {

    public static final String NAME = "snapshot_in_progress";

    /**
     * Disables relocation of shards that are currently being snapshotted.
     */
    public static final String CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED = "cluster.routing.allocation.snapshot.relocation_enabled";

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean newEnableRelocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED, enableRelocation);
            if (newEnableRelocation != enableRelocation) {
                logger.info("updating [{}] from [{}], to [{}]", CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED, enableRelocation, newEnableRelocation);
                enableRelocation = newEnableRelocation;
            }
        }
    }

    private volatile boolean enableRelocation = false;

    /**
     * Creates a new {@link org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider} instance
     */
    public SnapshotInProgressAllocationDecider() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    /**
     * Creates a new {@link org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider} instance from given settings
     *
     * @param settings {@link org.elasticsearch.common.settings.Settings} to use
     */
    public SnapshotInProgressAllocationDecider(Settings settings) {
        this(settings, new NodeSettingsService(settings));
    }

    @Inject
    public SnapshotInProgressAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        enableRelocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_SNAPSHOT_RELOCATION_ENABLED, enableRelocation);
        nodeSettingsService.addListener(new ApplySettings());
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

            SnapshotMetaData snapshotMetaData = allocation.metaData().custom(SnapshotMetaData.TYPE);
            if (snapshotMetaData == null) {
                // Snapshots are not running
                return allocation.decision(Decision.YES, NAME, "no snapshots are currently running");
            }

            for (SnapshotMetaData.Entry snapshot : snapshotMetaData.entries()) {
                SnapshotMetaData.ShardSnapshotStatus shardSnapshotStatus = snapshot.shards().get(shardRouting.shardId());
                if (shardSnapshotStatus != null && !shardSnapshotStatus.state().completed() && shardSnapshotStatus.nodeId() != null && shardSnapshotStatus.nodeId().equals(shardRouting.currentNodeId())) {
                    logger.trace("Preventing snapshotted shard [{}] to be moved from node [{}]", shardRouting.shardId(), shardSnapshotStatus.nodeId());
                    return allocation.decision(Decision.NO, NAME, "snapshot for shard [%s] is currently running on node [%s]",
                            shardRouting.shardId(), shardSnapshotStatus.nodeId());
                }
            }
        }
        return allocation.decision(Decision.YES, NAME, "shard not primary or relocation disabled");
    }

}
