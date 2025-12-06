/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;

public class ResizeAllocationDeciderTests extends ESAllocationTestCase {

    private AllocationService strategy;
    private ProjectId projectId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        strategy = new AllocationService(
            new AllocationDeciders(Collections.singleton(new ResizeAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

    private ClusterState createInitialClusterState(boolean startShards) {
        Metadata.Builder metaBuilder = Metadata.builder();
        projectId = randomUniqueProjectId();
        metaBuilder.put(
            ProjectMetadata.builder(projectId)
                .put(
                    IndexMetadata.builder("source")
                        .settings(settings(IndexVersion.current()))
                        .numberOfShards(2)
                        .numberOfReplicas(0)
                        .setRoutingNumShards(16)
                )
        );
        Metadata metadata = metaBuilder.build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        var prevRoutingTable = clusterState.globalRoutingTable().routingTable(projectId);
        var reroute = strategy.reroute(clusterState, "reroute", ActionListener.noop()).globalRoutingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(reroute).build();
        var routingTable = reroute.routingTable(projectId);

        assertEquals(prevRoutingTable.index("source").size(), 2);
        assertEquals(prevRoutingTable.index("source").shard(0).shard(0).state(), UNASSIGNED);
        assertEquals(prevRoutingTable.index("source").shard(1).shard(0).state(), UNASSIGNED);

        assertEquals(routingTable.index("source").size(), 2);

        assertEquals(routingTable.index("source").shard(0).shard(0).state(), INITIALIZING);
        assertEquals(routingTable.index("source").shard(1).shard(0).state(), INITIALIZING);

        if (startShards) {
            clusterState = startShardsAndReroute(
                strategy,
                clusterState,
                routingTable.index("source").shard(0).shard(0),
                routingTable.index("source").shard(1).shard(0)
            );
            routingTable = clusterState.globalRoutingTable().routingTable(projectId);
            assertEquals(routingTable.index("source").size(), 2);
            assertEquals(routingTable.index("source").shard(0).shard(0).state(), STARTED);
            assertEquals(routingTable.index("source").shard(1).shard(0).state(), STARTED);

        }
        return clusterState;
    }

    public void testNonResizeRouting() {
        ClusterState clusterState = createInitialClusterState(true);
        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState, null, null, 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting("non-resize", 0, null, true, ShardRoutingState.UNASSIGNED);
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));
        assertEquals(
            Decision.ALWAYS,
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
        );
    }

    public void testShrink() { // we don't handle shrink yet
        ClusterState clusterState = createInitialClusterState(true);

        Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        metaBuilder.put(
            ProjectMetadata.builder(clusterState.metadata().getProject(projectId))
                .put(
                    IndexMetadata.builder("target")
                        .settings(
                            settings(IndexVersion.current()).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
                                .put(IndexMetadata.SETTING_INDEX_UUID, "target_uuid")
                                .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, IndexMetadata.INDEX_UUID_NA_VALUE)
                        )
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                )
        );
        includeAdditionalProjects(randomIntBetween(1, 3), metaBuilder);
        Metadata metadata = metaBuilder.build();
        GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew);

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        Index idx = clusterState.metadata().getProject(projectId).index("target").getIndex();

        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState, null, null, 0);
        ShardRouting shardRouting = shardRoutingBuilder(new ShardId(idx, 0), null, true, ShardRoutingState.UNASSIGNED).withRecoverySource(
            RecoverySource.LocalShardsRecoverySource.INSTANCE
        ).build();
        assertEquals(Decision.ALWAYS, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));
        assertEquals(
            Decision.ALWAYS,
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
        );
        assertEquals(
            Decision.ALWAYS,
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"), routingAllocation)
        );
    }

    public void testSourceNotActive() {
        ClusterState clusterState = createInitialClusterState(false);
        Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        metaBuilder.put(
            IndexMetadata.builder("target")
                .settings(
                    settings(IndexVersion.current()).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, IndexMetadata.INDEX_UUID_NA_VALUE)
                )
                .numberOfShards(4)
                .numberOfReplicas(0)
        );
        includeAdditionalProjects(randomIntBetween(1, 3), metaBuilder);
        Metadata metadata = metaBuilder.build();
        GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew);
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();

        Index idx = clusterState.metadata().getProject(projectId).index("target").getIndex();

        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState, null, null, 0);
        int shardId = randomIntBetween(0, 3);
        int sourceShardId = IndexMetadata.selectSplitShard(shardId, clusterState.metadata().getProject(projectId).index("source"), 4).id();
        ShardRouting shardRouting = shardRoutingBuilder(new ShardId(idx, shardId), null, true, ShardRoutingState.UNASSIGNED)
            .withRecoverySource(RecoverySource.LocalShardsRecoverySource.INSTANCE)
            .build();
        assertEquals(Decision.NO, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));
        assertEquals(
            Decision.NO,
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
        );
        assertEquals(
            Decision.NO,
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"), routingAllocation)
        );

        routingAllocation.debugDecision(true);
        assertEquals(
            "source primary shard [[source][" + sourceShardId + "]] is not active",
            resizeAllocationDecider.canAllocate(shardRouting, routingAllocation).getExplanation()
        );
        assertEquals(
            "source primary shard [[source][" + sourceShardId + "]] is not active",
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node0"), routingAllocation)
                .getExplanation()
        );
        assertEquals(
            "source primary shard [[source][" + sourceShardId + "]] is not active",
            resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
                .getExplanation()
        );
    }

    public void testSourcePrimaryActive() {
        ClusterState clusterState = createInitialClusterState(true);
        Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        metaBuilder.put(
            IndexMetadata.builder("target")
                .settings(
                    settings(IndexVersion.current()).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
                        .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY, IndexMetadata.INDEX_UUID_NA_VALUE)
                )
                .numberOfShards(4)
                .numberOfReplicas(0)
        );
        includeAdditionalProjects(randomIntBetween(1, 3), metaBuilder);
        Metadata metadata = metaBuilder.build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).build();
        final GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.updateRoutingTable(
            clusterState,
            RoutingTable.Builder::addAsNew
        );
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        Index idx = clusterState.metadata().getProject(projectId).index("target").getIndex();

        ResizeAllocationDecider resizeAllocationDecider = new ResizeAllocationDecider();
        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState, null, null, 0);
        int shardId = randomIntBetween(0, 3);
        int sourceShardId = IndexMetadata.selectSplitShard(shardId, clusterState.metadata().getProject(projectId).index("source"), 4).id();
        ShardRouting shardRouting = shardRoutingBuilder(new ShardId(idx, shardId), null, true, ShardRoutingState.UNASSIGNED)
            .withRecoverySource(RecoverySource.LocalShardsRecoverySource.INSTANCE)
            .build();
        assertEquals(Decision.YES, resizeAllocationDecider.canAllocate(shardRouting, routingAllocation));

        String allowedNode = clusterState.routingTable(projectId).index("source").shard(sourceShardId).primaryShard().currentNodeId();

        if ("node1".equals(allowedNode)) {
            assertEquals(
                Decision.YES,
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
            );
            assertEquals(
                Decision.NO,
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"), routingAllocation)
            );
        } else {
            assertEquals(
                Decision.NO,
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
            );
            assertEquals(
                Decision.YES,
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"), routingAllocation)
            );
        }

        routingAllocation.debugDecision(true);
        assertEquals("source primary is active", resizeAllocationDecider.canAllocate(shardRouting, routingAllocation).getExplanation());

        if ("node1".equals(allowedNode)) {
            assertEquals(
                "source primary is allocated on this node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
                    .getExplanation()
            );
            assertEquals(
                "source primary is allocated on another node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"), routingAllocation)
                    .getExplanation()
            );
        } else {
            assertEquals(
                "source primary is allocated on another node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node1"), routingAllocation)
                    .getExplanation()
            );
            assertEquals(
                "source primary is allocated on this node",
                resizeAllocationDecider.canAllocate(shardRouting, clusterState.getRoutingNodes().node("node2"), routingAllocation)
                    .getExplanation()
            );
        }
    }

    public void testGetForcedInitialShardAllocationToNodes() {
        final int additionalProjects = randomIntBetween(0, 5);

        projectId = additionalProjects == 0 ? Metadata.DEFAULT_PROJECT_ID : randomUniqueProjectId();
        var source = IndexMetadata.builder("source")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, "uuid-1"))
            .build();
        var target = IndexMetadata.builder("target")
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), "source")
                    .put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), "uuid-1")
                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid-2")
            )
            .build();
        final Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(ProjectMetadata.builder(projectId).put(source, false).put(target, false));

        includeAdditionalProjects(additionalProjects, metadataBuilder);

        final Metadata metadata = metadataBuilder.build();
        var clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, (rtb, index) -> {
                if (index == source) {
                    rtb.add(
                        IndexRoutingTable.builder(source.getIndex())
                            .addShard(
                                shardRoutingBuilder(new ShardId(source.getIndex(), 0), "node-1", true, STARTED).withRecoverySource(null)
                                    .build()
                            )
                    );
                } else {
                    rtb.addAsNew(index);
                }
            }))
            .build();

        var decider = new ResizeAllocationDecider();
        var allocation = new RoutingAllocation(new AllocationDeciders(List.of(decider)), clusterState, null, null, 0);

        var localRecoveryShard = ShardRouting.newUnassigned(
            new ShardId(target.getIndex(), 0),
            true,
            RecoverySource.LocalShardsRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
            ShardRouting.Role.DEFAULT
        );
        assertThat(decider.getForcedInitialShardAllocationToNodes(localRecoveryShard, allocation), equalTo(Optional.of(Set.of("node-1"))));

        var newShard = ShardRouting.newUnassigned(
            new ShardId(target.getIndex(), 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
            ShardRouting.Role.DEFAULT
        );
        assertThat(decider.getForcedInitialShardAllocationToNodes(newShard, allocation), equalTo(Optional.empty()));
    }

    private static void includeAdditionalProjects(int projectCount, Metadata.Builder metadataBuilder) {
        for (int i = 0; i < projectCount; i++) {
            final ProjectMetadata.Builder project = ProjectMetadata.builder(randomUniqueProjectId());
            for (String index : randomSubsetOf(List.of("source", "target", "index-" + i))) {
                final Settings.Builder indexSettings = indexSettings(IndexVersion.current(), randomIntBetween(1, 5), randomIntBetween(0, 2))
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomUUID());
                project.put(IndexMetadata.builder(index).settings(indexSettings).build(), false);
            }
            metadataBuilder.put(project);
        }
    }
}
