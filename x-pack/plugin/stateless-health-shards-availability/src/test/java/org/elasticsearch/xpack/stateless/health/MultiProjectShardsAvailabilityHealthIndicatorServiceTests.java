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
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_CHECK_ALLOCATION_EXPLAIN_API;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.DIAGNOSIS_WAIT_FOR_INITIALIZATION;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.READ_ONLY_PRIMARY_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.health.Diagnosis.Resource.Type.INDEX;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.xpack.stateless.health.StatelessShardsAvailabilityHealthIndicatorService.ALL_REPLICAS_UNASSIGNED_IMPACT_ID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a multi-project test suite for the `shards_availability` indicator. This indicator has a stateful and a
 * stateless implementation, with unit tests for both, {@code StatefulShardsAvailabilityHealthIndicatorServiceTests}
 * and {@link StatelessShardsAvailabilityHealthIndicatorServiceTests}. Since the multi-project behaviour is an extension
 * of the stateless implementation, which is itself a wrapper of the stateful core, this test suite has handpicked tests
 * from both, extended to cover multi-project behaviour.
 */
public class MultiProjectShardsAvailabilityHealthIndicatorServiceTests extends ESTestCase {

    private static final Settings NO_GRACE_PERIOD_SETTINGS = Settings.builder()
        .put(ShardsAvailabilityHealthIndicatorService.PRIMARY_INACTIVE_BUFFER_TIME.getKey(), TimeValue.ZERO)
        .put(ShardsAvailabilityHealthIndicatorService.REPLICA_INACTIVE_BUFFER_TIME.getKey(), TimeValue.ZERO)
        .build();

    /**
     * Available shards keep the indicator green even when they are spread across multiple projects.
     * Relocating shards are still considered available and are counted in the started_* detail fields
     * together with fully started shards.
     */
    public void testShouldBeGreenWhenAllPrimariesAndReplicasAreStartedOrRelocating() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            final var replicatedIndexName = randomIndexName();
            final var unreplicatedIndexName = randomIndexName();
            projectIndexRoutes.put(
                projectId,
                List.of(
                    index(replicatedIndexName, randomStartedOrRelocating(), randomStartedOrRelocating()),
                    index(unreplicatedIndexName, randomStartedOrRelocating())
                )
            );
        }

        var service = createStatelessIndicator(clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has all shards available.",
                    new SimpleHealthIndicatorDetails(
                        detailsWithDefaults(Map.of("started_primaries", 2 * projectCount, "started_replicas", projectCount))
                    ),
                    List.of(),
                    List.of()
                )
            )
        );
    }

    /**
     * Indices that expect no replicas stay green.
     */
    public void testShouldBeGreenWhenThereAreNoReplicasExpected() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);;
        Set<ProjectId> primariesOnlyProjects = randomProjects(projectIds);
        int replicatedCount = projectCount - primariesOnlyProjects.size();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            final var indexName = randomIndexName();
            if (primariesOnlyProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        // Adds some noise to the cluster by shutting down an unrelated node, but this should have no effect
        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes, restartShutdown("node-0", 60)));

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has all shards available.",
                    new SimpleHealthIndicatorDetails(
                        detailsWithDefaults(Map.of("started_primaries", projectCount, "started_replicas", replicatedCount))
                    ),
                    List.of(),
                    List.of()
                )
            )
        );
    }


    private static Set<ProjectId> randomProjectIds(int projectCount) {
        Set<ProjectId> projectIds = new HashSet<>();
        while (projectIds.size() < projectCount) {
            projectIds.add(randomUniqueProjectId());
        }
        return projectIds;
    }

    private static Set<ProjectId> randomProjects(Set<ProjectId> projectIds) {
        return new HashSet<>(randomSubsetOf(randomIntBetween(1, projectIds.size()), projectIds));
    }

    private static NodesShutdownMetadata restartShutdown(String nodeId, int allocationDelaySeconds) {
        return nodeShutdown(nodeId, SingleNodeShutdownMetadata.Type.RESTART, allocationDelaySeconds);
    }

    private static NodesShutdownMetadata nodeShutdown(String nodeId, SingleNodeShutdownMetadata.Type type, Integer allocationDelaySeconds) {
        return new NodesShutdownMetadata(
            Map.of(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setNodeEphemeralId(nodeId)
                    .setType(type)
                    .setReason("test")
                    .setNodeSeen(true)
                    .setStartedAtMillis(System.currentTimeMillis())
                    .setAllocationDelay(allocationDelaySeconds != null ? TimeValue.timeValueSeconds(allocationDelaySeconds) : null)
                    .build()
            )
        );
    }

    private static ShardRoutingState randomStartedOrRelocating() {
        return randomFrom(STARTED, RELOCATING);
    }

    private static IndexRoutingTable index(String name, ShardRoutingState primaryState, ShardRoutingState... replicaStates) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(replicaStates.length)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        var builder = IndexRoutingTable.builder(index);
        builder.addShard(shardRouting(shardId, true, primaryState));
        for (ShardRoutingState replicaState : replicaStates) {
            builder.addShard(shardRouting(shardId, false, replicaState));
        }
        return builder.build();
    }

    private static ShardRouting shardRouting(ShardId shardId, boolean primary, ShardRoutingState state) {
        assert state == STARTED || state == RELOCATING : state;
        return TestShardRouting.newShardRouting(
            shardId,
            randomNodeId(),
            state == RELOCATING ? randomNodeId() : null,
            primary,
            state
        );
    }

    private static ClusterState clusterState(Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes) {
        return clusterState(projectIndexRoutes, Map.of(), NodesShutdownMetadata.EMPTY);
    }

    private static ClusterState clusterState(
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes,
        NodesShutdownMetadata nodesShutdownMetadata
    ) {
        return clusterState(projectIndexRoutes, Map.of(), nodesShutdownMetadata);
    }

    private static ClusterState clusterState(
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes,
        Map<String, Integer> indexPriorities,
        NodesShutdownMetadata nodesShutdownMetadata
    ) {
        var metadataBuilder = Metadata.builder();
        var globalRoutingTableBuilder = GlobalRoutingTable.builder();
        for (var entry : projectIndexRoutes.entrySet()) {
            ProjectId projectId = entry.getKey();
            var projectMetadata = ProjectMetadata.builder(projectId);
            var routingTable = RoutingTable.builder();
            for (IndexRoutingTable indexRouting : entry.getValue()) {
                var settings = Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexRouting.getIndex().getUUID());
                Integer priority = indexPriorities.get(indexRouting.getIndex().getName());
                if (priority != null) {
                    settings.put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), priority);
                }
                projectMetadata.put(
                    IndexMetadata.builder(indexRouting.getIndex().getName())
                        .settings(settings.build())
                        .numberOfShards(indexRouting.size())
                        .numberOfReplicas(indexRouting.shard(0).size() - 1)
                        .build(),
                    false
                );
                routingTable.add(indexRouting);
            }
            metadataBuilder.put(projectMetadata.build());
            globalRoutingTableBuilder.put(projectId, routingTable.build());
        }
        metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata);
        return ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(metadataBuilder.build())
            .routingTable(globalRoutingTableBuilder.build())
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    private static StatelessShardsAvailabilityHealthIndicatorService createStatelessIndicator(ClusterState clusterState) {
        return createStatelessIndicator(Settings.EMPTY, clusterState);
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

    private static Map<String, Object> detailsWithDefaults(Map<String, Object> override) {
        return Map.of(
            "unassigned_primaries",
            override.getOrDefault("unassigned_primaries", 0),
            "initializing_primaries",
            override.getOrDefault("initializing_primaries", 0),
            "creating_primaries",
            override.getOrDefault("creating_primaries", 0),
            "restarting_primaries",
            override.getOrDefault("restarting_primaries", 0),
            "started_primaries",
            override.getOrDefault("started_primaries", 0),
            "unassigned_replicas",
            override.getOrDefault("unassigned_replicas", 0),
            "initializing_replicas",
            override.getOrDefault("initializing_replicas", 0),
            "creating_replicas",
            override.getOrDefault("creating_replicas", 0),
            "restarting_replicas",
            override.getOrDefault("restarting_replicas", 0),
            "started_replicas",
            override.getOrDefault("started_replicas", 0)
        );
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }
}
