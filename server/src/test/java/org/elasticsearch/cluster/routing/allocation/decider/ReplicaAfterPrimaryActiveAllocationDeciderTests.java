/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ReplicaAfterPrimaryActiveAllocationDeciderTests extends ESTestCase {

    /** Cheap shards prototype: only with the setting on, and only unpromotable copies, may be allocated without an active primary. */
    public void testUnpromotableWithoutPrimarySetting() {
        final ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        final var decider = new ReplicaAfterPrimaryActiveAllocationDecider(clusterSettings);
        final var allocation = RoutingAllocation.immutable(
            new AllocationDeciders(List.of(decider)),
            ClusterState.EMPTY_STATE,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
        final ShardId shardId = new ShardId("idx", "_na_", 0);
        final ShardRouting search = TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            ShardRoutingState.UNASSIGNED,
            ShardRouting.Role.SEARCH_ONLY
        );
        final ShardRouting replica = TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED);

        // default off: no replica of any kind without an active primary
        assertThat(decider.canAllocate(search, allocation).type(), equalTo(Decision.Type.NO));
        assertThat(decider.canAllocate(replica, allocation).type(), equalTo(Decision.Type.NO));

        clusterSettings.applySettings(
            Settings.builder()
                .put(ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING.getKey(), true)
                .build()
        );
        assertThat(decider.canAllocate(search, allocation).type(), equalTo(Decision.Type.YES));
        assertThat(
            "promotable replicas still need an active primary",
            decider.canAllocate(replica, allocation).type(),
            equalTo(Decision.Type.NO)
        );
    }
}
