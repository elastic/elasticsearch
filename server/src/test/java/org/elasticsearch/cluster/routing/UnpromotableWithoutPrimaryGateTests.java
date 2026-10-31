/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.junit.After;

import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cheap shards prototype: the relaxations in {@link RoutingNodes} and {@link IndexShardRoutingTable} for unpromotable copies without an
 * active primary apply only while {@link ReplicaAfterPrimaryActiveAllocationDecider#UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING} is on;
 * with it off the original assertions hold.
 */
public class UnpromotableWithoutPrimaryGateTests extends ESAllocationTestCase {

    @After
    public void resetGate() {
        ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY = false;
    }

    public void testGateFollowsSetting() {
        final boolean initial = randomBoolean();
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING.getKey(), initial)
                .build(),
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
        ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY = initial == false;
        new ReplicaAfterPrimaryActiveAllocationDecider(clusterSettings);
        assertThat("initial value applied", ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY, equalTo(initial));
        clusterSettings.applySettings(
            Settings.builder()
                .put(ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY_ENABLED_SETTING.getKey(), initial == false)
                .build()
        );
        assertThat(ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY, equalTo(initial == false));
    }

    public void testRoutingTableAssertionOnlyRelaxedWithSettingOn() {
        final ShardId shardId = new ShardId("idx", "_na_", 0);
        final ShardRouting primary = shardRoutingBuilder(shardId, null, true, UNASSIGNED).withRole(ShardRouting.Role.INDEX_ONLY).build();
        final ShardRouting search = shardRoutingBuilder(shardId, "node-2", false, STARTED).withRole(ShardRouting.Role.SEARCH_ONLY).build();
        final ShardRouting replica = shardRoutingBuilder(shardId, "node-2", false, STARTED).build();

        ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY = false;
        assertFalse(IndexShardRoutingTable.Builder.noAssignedReplicaWithoutActivePrimary(List.of(primary, search)));
        assertFalse(IndexShardRoutingTable.Builder.noAssignedReplicaWithoutActivePrimary(List.of(primary, replica)));

        ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY = true;
        assertTrue(IndexShardRoutingTable.Builder.noAssignedReplicaWithoutActivePrimary(List.of(primary, search)));
        assertFalse(IndexShardRoutingTable.Builder.noAssignedReplicaWithoutActivePrimary(List.of(primary, replica)));
    }

    public void testRecoveryAccountingOnlyRelaxedWithSettingOn() {
        ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY = true;
        final var projectId = randomProjectIdOrDefault();
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var shardId = new ShardId(indexMetadata.getIndex(), 0);
        final var indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRoutingBuilder(shardId, null, true, UNASSIGNED).withRole(ShardRouting.Role.INDEX_ONLY).build())
            .addShard(shardRoutingBuilder(shardId, "node-2", false, INITIALIZING).withRole(ShardRouting.Role.SEARCH_ONLY).build())
            .build();
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, false)).build())
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).build())
            .routingTable(GlobalRoutingTableTestHelper.routingTable(projectId, RoutingTable.builder().add(indexRoutingTable).build()))
            .build();

        // setting on: the search copy recovers without a primary, so there is an incoming but no outgoing recovery
        final var routingNodes = RoutingNodes.mutable(clusterState.globalRoutingTable(), clusterState.nodes());
        assertThat(routingNodes.getIncomingRecoveries("node-2"), equalTo(1));
        assertThat(routingNodes.getOutgoingRecoveries("node-1"), equalTo(0));
        final var copy = routingNodes.node("node-2").getByShardId(shardId);
        routingNodes.startShard(copy, RoutingChangesObserver.NOOP, copy.getExpectedShardSize());
        assertThat(routingNodes.getIncomingRecoveries("node-2"), equalTo(0));

        // setting off: the original assertion (a peer-recovering copy needs an assigned primary) holds again
        ReplicaAfterPrimaryActiveAllocationDecider.UNPROMOTABLE_WITHOUT_PRIMARY = false;
        expectThrows(AssertionError.class, () -> RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()));
    }
}
