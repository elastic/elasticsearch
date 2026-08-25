/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.health;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Multi-project tests for the {@code shards_availability} indicator.
 * Due to the combinatorial explosion introduced by multi-project clusters, this test suite
 * relies on randomisation to generate healthy / non-healthy clusters and assert the indicator
 * responds as expected.
 */
public class MultiProjectShardsAvailabilityHealthIndicatorServiceTests extends ESTestCase {

    /**
     * Randomly generated a green only cluster, and asserts that the indicator aggregates as expected. There are
     * six different possible ways that a cluster can be green:
     * <ol>
     *     <li>
     *         All primaries and replicas are started or relocating
     *     </li>
     *     <li>
     *         An index is still initialising
     *     </li>
     *     <li>
     *         A primary index is started, but the replica is still initialising (and we haven't exceeded the
     *         {@code health.shards_availability.replica_unassigned_buffer_time})
     *     </li>
     *     <li>
     *         A primary is unassigned because it's node is restarting
     *     </li>
     *     <li>
     *         A mounted / searchable-snapshot primary is unassigned, but the original index is still assigned
     *         in the same project
     *     </li>
     *     <li>
     *         After a resharding event, if shard 0 (the source) is fully started but shard 1 (the target) is unassigned
     *         with a {@code RESHARD_SPLIT} recovery
     *     </li>
     * </ol>
     */
    public void testShouldBeGreenWhenEachProjectHasARandomGreenCase() {
        int projectCount = randomIntBetween(1, 6);
        Map<ProjectId, List<IndexSetup>> projects = new HashMap<>();
        Map<String, SingleNodeShutdownMetadata> shutdowns = new HashMap<>();
        // Records the project cases used in this test for reproducibility and debugging
        List<String> projectCases = new ArrayList<>();

        int startedPrimaries = 0;
        int startedReplicas = 0;
        int creatingPrimaries = 0;
        int creatingReplicas = 0;
        int restartingPrimaries = 0;
        int unassignedPrimaries = 0;

        Set<ProjectId> projectIds = new HashSet<>();
        while (projectIds.size() < projectCount) {
            projectIds.add(randomUniqueProjectId());
        }

        for (ProjectId projectId : projectIds) {
            GreenCase greenCase = randomFrom(GreenCase.values());
            projectCases.add(projectId + "=" + greenCase);
            List<IndexSetup> indices = new ArrayList<>();
            switch (greenCase) {
                // Both the primary (and maybe replica shards) are either started or relocating.
                case ALL_SHARDS_STARTING_OR_RELOCATING -> {
                    int replicaCount = randomBoolean() ? 1 : 0;
                    var metadata = indexMetadata(randomIndexName(), 1, replicaCount);
                    var shardId = new ShardId(metadata.getIndex(), 0);
                    var builder = IndexRoutingTable.builder(metadata.getIndex());
                    builder.addShard(shardRouting(shardId, true, randomFrom(STARTED, RELOCATING)));
                    startedPrimaries++;
                    if (replicaCount == 1) {
                        builder.addShard(shardRouting(shardId, false, randomFrom(STARTED, RELOCATING)));
                        startedReplicas++;
                    }
                    indices.add(new IndexSetup(metadata, builder.build()));
                }
                // The primary (and maybe replica) shards are still initialising
                // TODO - RANDOMISE
                case INITIALIZING_SHARDS -> {
                    var metadata = indexMetadata(randomIndexName(), 1, 1);
                    var shardId = new ShardId(metadata.getIndex(), 0);
                    var created = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
                    indices.add(
                        new IndexSetup(
                            metadata,
                            IndexRoutingTable.builder(metadata.getIndex())
                                .addShard(
                                    shardRouting(shardId, true, UNASSIGNED, RecoverySource.EmptyStoreRecoverySource.INSTANCE, created)
                                )
                                .addShard(shardRouting(shardId, false, UNASSIGNED, RecoverySource.PeerRecoverySource.INSTANCE, created))
                                .build()
                        )
                    );
                    creatingPrimaries++;
                    creatingReplicas++;
                }
                // The primary is started, but the replica is still unassigned. As long as this is within the
                // health.shards_availability.replica_unassigned_buffer_time then the indicator is green.
                // When this grace period is exceeded, the indicator goes red since the shard has no remaining
                // searchable copy.
                case REPLICA_UNASSIGNED_WITHIN_GRACE_WINDOW -> {
                    var metadata = indexMetadata(randomIndexName(), 1, 1);
                    var shardId = new ShardId(metadata.getIndex(), 0);
                    // Set a big grace window so that it doesn't expire before the test ends
                    var withinGrace = new TimeValue(
                        System.currentTimeMillis() + TimeValue.timeValueHours(1).millis(),
                        TimeUnit.MILLISECONDS
                    );
                    indices.add(
                        new IndexSetup(
                            metadata,
                            IndexRoutingTable.builder(metadata.getIndex())
                                .addShard(shardRouting(shardId, true, STARTED))
                                .addShard(
                                    shardRouting(
                                        shardId,
                                        false,
                                        UNASSIGNED,
                                        RecoverySource.PeerRecoverySource.INSTANCE,
                                        unassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, withinGrace)
                                    )
                                )
                                .build()
                        )
                    );
                    startedPrimaries++;
                    creatingReplicas++;
                }
                // The primary (and maybe replica) is unassigned because its node is restarting
                // TODO -Randomise
                case RESTARTING_NODE -> {
                    var nodeId = randomNodeId();
                    shutdowns.put(nodeId, restartShutdown(nodeId, 60));
                    var metadata = indexMetadata(randomIndexName(), 1, 0);
                    var shardId = new ShardId(metadata.getIndex(), 0);
                    indices.add(
                        new IndexSetup(
                            metadata,
                            IndexRoutingTable.builder(metadata.getIndex()).addShard(restartingPrimary(shardId, nodeId)).build()
                        )
                    );
                    restartingPrimaries++;
                }
                /*
                 * The mounted / searchable-snapshot primary is unassigned, but the original index
                 * is still assigned in the same project. Searchable snapshots do not use
                 * replicas, so both indices are primaries-only. The mounted shard is counted
                 * as unassigned_primaries in the details object.
                 */
                case SEARCHABLE_SNAPSHOT -> {
                    var originalName = randomIndexName();
                    var mountedName = randomIndexName();
                    var original = indexMetadata(originalName, 1, 0);
                    var mounted = indexMetadata(
                        mountedName,
                        1,
                        0,
                        Settings.builder()
                            .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_INDEX_NAME_SETTING_KEY, originalName)
                            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE)
                            .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, randomBoolean())
                            .build()
                    );
                    indices.add(
                        new IndexSetup(
                            original,
                            IndexRoutingTable.builder(original.getIndex())
                                .addShard(shardRouting(new ShardId(original.getIndex(), 0), true, STARTED))
                                .build()
                        )
                    );
                    indices.add(
                        new IndexSetup(
                            mounted,
                            IndexRoutingTable.builder(mounted.getIndex())
                                .addShard(
                                    shardRouting(
                                        new ShardId(mounted.getIndex(), 0),
                                        true,
                                        UNASSIGNED,
                                        RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                        unassignedInfo(UnassignedInfo.Reason.NODE_LEFT, TimeValue.timeValueMillis(0))
                                    )
                                )
                                .build()
                        )
                    );
                    startedPrimaries++;
                    unassignedPrimaries++;
                }
                // After resharding, shard 0 (the source) is fully started and still serves the data but
                // shard 1 (the target) is unassigned with RESHARD_SPLIT recovery
                case RESHARD -> {
                    var metadata = indexMetadata(randomIndexName(), 2, 1);
                    var index = metadata.getIndex();
                    var sourceId = new ShardId(index, 0);
                    var targetId = new ShardId(index, 1);
                    var reshardAdded = unassignedInfo(UnassignedInfo.Reason.RESHARD_ADDED, TimeValue.timeValueMinutes(5));
                    indices.add(
                        new IndexSetup(
                            metadata,
                            IndexRoutingTable.builder(index)
                                .addShard(shardRouting(sourceId, true, STARTED))
                                .addShard(shardRouting(sourceId, false, STARTED))
                                .addShard(
                                    shardRouting(
                                        targetId,
                                        true,
                                        UNASSIGNED,
                                        new RecoverySource.ReshardSplitRecoverySource(sourceId),
                                        reshardAdded
                                    )
                                )
                                .addShard(
                                    shardRouting(targetId, false, UNASSIGNED, RecoverySource.PeerRecoverySource.INSTANCE, reshardAdded)
                                )
                                .build()
                        )
                    );
                    startedPrimaries++;
                    startedReplicas++;
                    creatingPrimaries++;
                    creatingReplicas++;
                }
            }
            projects.put(projectId, indices);
        }

        var service = createStatelessIndicator(
            Settings.builder()
                .put(ShardsAvailabilityHealthIndicatorService.PRIMARY_INACTIVE_BUFFER_TIME.getKey(), "20s")
                .put(ShardsAvailabilityHealthIndicatorService.REPLICA_INACTIVE_BUFFER_TIME.getKey(), "20s")
                .build(),
            clusterState(projects, shutdowns)
        );
        var result = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        var details = ((SimpleHealthIndicatorDetails) result.details()).details();
        String assignment = "projects " + projectCases;

        // TODO - Assert symptom and diagnosis

        assertThat(assignment, result.status(), equalTo(GREEN));
        assertThat(assignment, result.impacts(), empty());
        assertThat(assignment, details.get("started_primaries"), equalTo(startedPrimaries));
        assertThat(assignment, details.get("started_replicas"), equalTo(startedReplicas));
        assertThat(assignment, details.get("creating_primaries"), equalTo(creatingPrimaries));
        assertThat(assignment, details.get("creating_replicas"), equalTo(creatingReplicas));
        assertThat(assignment, details.get("restarting_primaries"), equalTo(restartingPrimaries));
        assertThat(assignment, details.get("unassigned_primaries"), equalTo(unassignedPrimaries));
        assertThat(assignment, details.get("unassigned_replicas"), equalTo(0));
        assertThat(assignment, details.get("indices_with_unavailable_primaries"), nullValue());
        assertThat(assignment, details.get("indices_with_unavailable_replicas"), nullValue());
    }

    private enum GreenCase {
        ALL_SHARDS_STARTING_OR_RELOCATING,
        INITIALIZING_SHARDS,
        REPLICA_UNASSIGNED_WITHIN_GRACE_WINDOW,
        RESTARTING_NODE,
        SEARCHABLE_SNAPSHOT,
        RESHARD
    }

    private record IndexSetup(IndexMetadata metadata, IndexRoutingTable routing) {}

    private static IndexMetadata indexMetadata(String name, int shards, int replicas) {
        return indexMetadata(name, shards, replicas, Settings.EMPTY);
    }

    private static IndexMetadata indexMetadata(String name, int shards, int replicas, Settings extra) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(extra).build())
            .numberOfShards(shards)
            .numberOfReplicas(replicas)
            .build();
    }

    private static ShardRouting shardRouting(ShardId shardId, boolean primary, ShardRoutingState state) {
        return shardRouting(
            shardId,
            primary,
            state,
            primary ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
    }

    private static ShardRouting shardRouting(
        ShardId shardId,
        boolean primary,
        ShardRoutingState state,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo
    ) {
        var routing = newUnassigned(
            shardId,
            primary,
            recoverySource,
            unassignedInfo,
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
        );
        if (state == UNASSIGNED) {
            return routing;
        }
        routing = routing.initialize(randomNodeId(), null, 0);
        routing = routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        if (state == RELOCATING) {
            return routing.relocate(
                randomNodeId(),
                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
            );
        }
        return routing;
    }

    private static ShardRouting restartingPrimary(ShardId shardId, String nodeId) {
        // Must go STARTED then moveToUnassigned: a never-started primary is "creating", not "restarting".
        // unassignedTimeNanos is System.nanoTime() (not wall-clock) so the 60s allocation delay has not elapsed.
        return shardRouting(shardId, true, STARTED).moveToUnassigned(
            new UnassignedInfo(
                UnassignedInfo.Reason.NODE_RESTARTING,
                null,
                null,
                -1,
                System.nanoTime(),
                0,
                false,
                UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                Set.of(),
                nodeId
            ),
            ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED
        );
    }

    private static UnassignedInfo unassignedInfo(UnassignedInfo.Reason reason, TimeValue unassignedTime) {
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        String lastAllocatedNodeId = reason == UnassignedInfo.Reason.NODE_RESTARTING ? randomNodeId() : null;
        return new UnassignedInfo(
            reason,
            null,
            null,
            failedAllocations,
            unassignedTime.nanos(),
            unassignedTime.millis(),
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Set.of(),
            lastAllocatedNodeId
        );
    }

    private static SingleNodeShutdownMetadata restartShutdown(String nodeId, int allocationDelaySeconds) {
        return SingleNodeShutdownMetadata.builder()
            .setNodeId(nodeId)
            .setNodeEphemeralId(nodeId)
            .setType(SingleNodeShutdownMetadata.Type.RESTART)
            .setReason("test")
            .setNodeSeen(true)
            .setStartedAtMillis(System.currentTimeMillis())
            .setAllocationDelay(TimeValue.timeValueSeconds(allocationDelaySeconds))
            .build();
    }

    private static ClusterState clusterState(Map<ProjectId, List<IndexSetup>> projects, Map<String, SingleNodeShutdownMetadata> shutdowns) {
        var metadataBuilder = Metadata.builder();
        var globalRoutingTableBuilder = GlobalRoutingTable.builder();
        for (var entry : projects.entrySet()) {
            var projectMetadata = ProjectMetadata.builder(entry.getKey());
            var routingTable = RoutingTable.builder();
            for (IndexSetup index : entry.getValue()) {
                projectMetadata.put(index.metadata(), false);
                routingTable.add(index.routing());
            }
            metadataBuilder.put(projectMetadata.build());
            globalRoutingTableBuilder.put(entry.getKey(), routingTable.build());
        }
        metadataBuilder.putCustom(
            NodesShutdownMetadata.TYPE,
            shutdowns.isEmpty() ? NodesShutdownMetadata.EMPTY : new NodesShutdownMetadata(shutdowns)
        );
        return ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(metadataBuilder.build())
            .routingTable(globalRoutingTableBuilder.build())
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    private static StatelessShardsAvailabilityHealthIndicatorService createStatelessIndicator(
        Settings nodeSettings,
        ClusterState clusterState
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        var allocationService = mock(AllocationService.class);
        when(
            allocationService.explainShardAllocation(
                any(ShardRouting.class),
                any(ClusterState.class),
                any(RoutingAllocation.DebugMode.class)
            )
        ).thenReturn(ShardAllocationDecision.NOT_TAKEN);
        return new StatelessShardsAvailabilityHealthIndicatorService(
            clusterService,
            allocationService,
            new SystemIndices(List.of()),
            TestProjectResolvers.allProjects()
        );
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }
}
