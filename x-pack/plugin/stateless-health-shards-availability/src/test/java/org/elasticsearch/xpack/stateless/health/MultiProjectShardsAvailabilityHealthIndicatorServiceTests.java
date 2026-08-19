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

    private static final Settings PRIMARY_GRACE_PERIOD_SETTINGS = Settings.builder()
        .put(ShardsAvailabilityHealthIndicatorService.PRIMARY_INACTIVE_BUFFER_TIME.getKey(), "20s")
        .build();

    private static final Settings REPLICA_GRACE_PERIOD_SETTINGS = Settings.builder()
        .put(ShardsAvailabilityHealthIndicatorService.REPLICA_INACTIVE_BUFFER_TIME.getKey(), "20s")
        .build();

    private static final Settings BOTH_GRACE_PERIOD_SETTINGS = Settings.builder()
        .put(ShardsAvailabilityHealthIndicatorService.PRIMARY_INACTIVE_BUFFER_TIME.getKey(), "20s")
        .put(ShardsAvailabilityHealthIndicatorService.REPLICA_INACTIVE_BUFFER_TIME.getKey(), "20s")
        .build();

    /**
     * Available shards keep the indicator green even when they are spread across multiple projects.
     * Relocating shards are still considered available and are counted in the started_* detail fields
     * together with fully started shards.
     */
    public void testShouldBeGreenWhenAllPrimariesAndReplicasAreStartedOrRelocating() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);

        final var replicatedIndexName = randomIndexName();
        final var unreplicatedIndexName = randomIndexName();
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
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
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> primariesOnlyProjects = randomProjects(projectIds);
        int replicatedCount = projectCount - primariesOnlyProjects.size();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
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

    /**
     * Newly created unassigned primaries are provisionally unavailable and keep the indicator green
     */
    public void testShouldBeGreenWhenAllPrimariesAreCreating() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            projectIndexRoutes.put(projectId, List.of(creatingIndex(indexName)));
        }

        List<String> creatingIndices = prefixedIndexNames(projectIds, indexName);
        Map<String, Object> details = new HashMap<>(detailsWithDefaults(Map.of("creating_primaries", projectCount)));
        details.put("indices_with_provisionally_unavailable_primaries", String.join(", ", creatingIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    projectCount == 1
                        ? "This cluster has 1 creating primary shard."
                        : "This cluster has " + projectCount + " creating primary shards.",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, creatingIndices))))
                )
            )
        );
    }

    /**
     * An unassigned replica of a still-creating primary is provisionally unavailable
     */
    public void testShouldBeGreenWhenUnassignedReplicaBelongsToCreatingPrimary() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(creatingIndex(indexName, UNASSIGNED)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of(
                    "creating_primaries",
                    affectedCount,
                    "creating_replicas",
                    affectedCount,
                    "started_primaries",
                    greenCount,
                    "started_replicas",
                    greenCount
                )
            )
        );
        details.put("indices_with_provisionally_unavailable_primaries", String.join(", ", affectedIndices));
        details.put("indices_with_provisionally_unavailable_replicas", String.join(", ", affectedIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has "
                        + countPhrase(affectedCount, "creating primary shard", "creating primary shards")
                        + ", "
                        + countPhrase(affectedCount, "creating replica shard", "creating replica shards")
                        + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, affectedIndices))))
                )
            )
        );
    }

    /**
     * Creating primaries together with creating replicas are provisionally unavailable and keep the indicator green.
     */
    public void testShouldBeGreenWhenThereAreInitializingPrimariesAndReplicas() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(creatingIndex(indexName, UNASSIGNED)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of(
                    "creating_primaries",
                    affectedCount,
                    "creating_replicas",
                    affectedCount,
                    "started_primaries",
                    greenCount,
                    "started_replicas",
                    greenCount
                )
            )
        );
        details.put("indices_with_provisionally_unavailable_primaries", String.join(", ", affectedIndices));
        details.put("indices_with_provisionally_unavailable_replicas", String.join(", ", affectedIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has "
                        + countPhrase(affectedCount, "creating primary shard", "creating primary shards")
                        + ", "
                        + countPhrase(affectedCount, "creating replica shard", "creating replica shards")
                        + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, affectedIndices))))
                )
            )
        );
    }

    /**
     * A still-creating primary is provisionally unavailable, and so is its unassigned replica, even with
     * a zero grace period. Failed allocations on that primary are not new initialization and turn the indicator red.
     */
    public void testShouldBeGreenWhenUnassignedNewInitialization() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;

        boolean isAcceptable = randomBoolean();
        UnassignedInfo primaryUnassignedInfo = isAcceptable
            ? new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
            : unassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, randomTimeValue());

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(creatingIndex(indexName, primaryUnassignedInfo, UNASSIGNED)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                isAcceptable
                    ? Map.of(
                        "creating_primaries",
                        affectedCount,
                        "creating_replicas",
                        affectedCount,
                        "started_primaries",
                        greenCount,
                        "started_replicas",
                        greenCount
                    )
                    : Map.of(
                        "unassigned_primaries",
                        affectedCount,
                        "unassigned_replicas",
                        affectedCount,
                        "started_primaries",
                        greenCount,
                        "started_replicas",
                        greenCount
                    )
            )
        );
        details.put(
            isAcceptable ? "indices_with_provisionally_unavailable_primaries" : "indices_with_unavailable_primaries",
            String.join(", ", affectedIndices)
        );
        details.put(
            isAcceptable ? "indices_with_provisionally_unavailable_replicas" : "indices_with_unavailable_replicas",
            String.join(", ", affectedIndices)
        );

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    isAcceptable ? GREEN : RED,
                    "This cluster has "
                        + countPhrase(
                            affectedCount,
                            isAcceptable ? "creating primary shard" : "unavailable primary shard",
                            isAcceptable ? "creating primary shards" : "unavailable primary shards"
                        )
                        + ", "
                        + countPhrase(
                            affectedCount,
                            isAcceptable ? "creating replica shard" : "unavailable replica shard",
                            isAcceptable ? "creating replica shards" : "unavailable replica shards"
                        )
                        + ".",
                    new SimpleHealthIndicatorDetails(details),
                    isAcceptable
                        ? List.of()
                        : List.of(primaryUnassignedImpact(affectedIndices), allReplicasUnassignedImpact(affectedIndices)),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, affectedIndices))))
                )
            )
        );
    }

    /**
     * Pre-handoff reshard targets keep {@link RecoverySource.Type#RESHARD_SPLIT}, including across failed recoveries.
     * Those stay provisionally GREEN rather than RED while the source shard still serves the data, even with a zero
     * grace period.
     */
    public void testHealthWhileReshardSplitTargetShardsInactive() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        boolean targetPrimaryInitializing = randomBoolean();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(reshardSplitIndex(indexName, targetPrimaryInitializing)));
            } else {
                projectIndexRoutes.put(projectId, List.of(twoShardIndex(indexName, STARTED, STARTED, STARTED, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> countOverrides = new HashMap<>(
            Map.of(
                "started_primaries",
                affectedCount + 2 * greenCount,
                "started_replicas",
                affectedCount + 2 * greenCount,
                "creating_replicas",
                affectedCount
            )
        );
        if (targetPrimaryInitializing) {
            countOverrides.put("initializing_primaries", affectedCount);
        } else {
            countOverrides.put("creating_primaries", affectedCount);
        }
        Map<String, Object> details = new HashMap<>(detailsWithDefaults(countOverrides));
        details.put("indices_with_provisionally_unavailable_primaries", String.join(", ", affectedIndices));
        details.put("indices_with_provisionally_unavailable_replicas", String.join(", ", affectedIndices));

        String symptom = targetPrimaryInitializing
            ? "This cluster has "
                + countPhrase(affectedCount, "creating replica shard", "creating replica shards")
                + ", "
                + countPhrase(affectedCount, "initializing primary shard", "initializing primary shards")
                + "."
            : "This cluster has "
                + countPhrase(affectedCount, "creating primary shard", "creating primary shards")
                + ", "
                + countPhrase(affectedCount, "creating replica shard", "creating replica shards")
                + ".";
        List<Diagnosis> diagnoses = new ArrayList<>();
        diagnoses.add(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, affectedIndices))));
        if (targetPrimaryInitializing) {
            diagnoses.add(new Diagnosis(DIAGNOSIS_WAIT_FOR_INITIALIZATION, List.of(new Diagnosis.Resource(INDEX, affectedIndices))));
        }

        var result = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes)).calculate(
            true,
            HealthInfo.EMPTY_HEALTH_INFO
        );
        assertThat(result.status(), equalTo(GREEN));
        assertThat(result.symptom(), equalTo(symptom));
        assertThat(result.details(), equalTo(new SimpleHealthIndicatorDetails(details)));
        assertThat(result.impacts(), equalTo(List.of()));
        assertThat(result.diagnosisList(), containsInAnyOrder(diagnoses.toArray(Diagnosis[]::new)));
    }

    /**
     * An unreplicated primary unassigned because its node is restarting, still within the restart allocation delay,
     * keeps the indicator green.
     */
    public void testShouldBeGreenWhenThereAreRestartingPrimaries() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        String restartingNodeId = "node-0";

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithRestartingPrimary(indexName, restartingNodeId)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, restartShutdown(restartingNodeId, 60))
        );
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has " + countPhrase(affectedCount, "restarting primary shard", "restarting primary shards") + ".",
                    new SimpleHealthIndicatorDetails(
                        detailsWithDefaults(Map.of("restarting_primaries", affectedCount, "started_primaries", greenCount))
                    ),
                    List.of(),
                    List.of()
                )
            )
        );
    }

    /**
     * A replica unassigned because its node is restarting, still within the restart allocation delay, keeps the indicator green.
     */
    public void testShouldBeGreenWhenThereAreRestartingReplicas() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        String restartingNodeId = "node-0";

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithRestartingReplica(indexName, restartingNodeId)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, restartShutdown(restartingNodeId, 60))
        );
        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has " + countPhrase(affectedCount, "restarting replica shard", "restarting replica shards") + ".",
                    new SimpleHealthIndicatorDetails(
                        detailsWithDefaults(
                            Map.of("started_primaries", projectCount, "started_replicas", greenCount, "restarting_replicas", affectedCount)
                        )
                    ),
                    List.of(allReplicasUnassignedImpact(affectedIndices)),
                    List.of()
                )
            )
        );
    }

    /**
     * An unassigned frozen/mounted index stays green when the original index is still available in the same project.
     */
    public void testShouldBeGreenWhenFrozenIndexIsUnassignedAndOriginalIsAvailable() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        int unavailableMountedPrimaries = randomBoolean() ? 1 : randomIntBetween(2, 3);
        final var originalIndex = randomIndexName();
        final var restoredIndex = randomIndexName();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(
                    projectId,
                    List.of(
                        unreplicatedIndex(restoredIndex, unavailableMountedPrimaries, UNASSIGNED),
                        unreplicatedIndex(originalIndex, unavailableMountedPrimaries, STARTED)
                    )
                );
            } else {
                projectIndexRoutes.put(
                    projectId,
                    List.of(
                        unreplicatedIndex(restoredIndex, unavailableMountedPrimaries, STARTED),
                        unreplicatedIndex(originalIndex, unavailableMountedPrimaries, STARTED)
                    )
                );
            }
        }

        int unassignedPrimaries = unavailableMountedPrimaries * affectedCount;
        int startedPrimaries = unavailableMountedPrimaries * (affectedCount + 2 * greenCount);
        List<String> restoredIndices = prefixedIndexNames(affectedProjects, restoredIndex);
        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, Map.of(), Map.of(restoredIndex, searchableSnapshotSettings(originalIndex)))
        );
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    GREEN,
                    "This cluster has "
                        + countPhrase(unassignedPrimaries, "unavailable primary shard", "unavailable primary shards")
                        + "."
                        + (unassignedPrimaries == 1
                            ? " This is a mounted shard and the original shard is available, so there are no data availability problems."
                            : " These are mounted shards and the original shards are available, "
                                + "so there are no data availability problems."),
                    new SimpleHealthIndicatorDetails(
                        detailsWithDefaults(Map.of("unassigned_primaries", unassignedPrimaries, "started_primaries", startedPrimaries))
                    ),
                    List.of(),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, restoredIndices))))
                )
            )
        );
    }

    /**
     * Unavailable replicas in any project turn the indicator yellow when each of those projects still has
     * a started replica copy.
     */
    public void testShouldBeYellowWhenSomeReplicaCopiesRemain() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        int startedCopies = randomIntBetween(1, 2);
        int unavailableCopies = randomIntBetween(1, 2);
        ShardRoutingState unavailableState = randomFrom(UNASSIGNED, INITIALIZING);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            List<ShardRoutingState> replicaStates = new ArrayList<>();
            int startedForProject = affectedProjects.contains(projectId) ? startedCopies : startedCopies + unavailableCopies;
            int unavailableForProject = affectedProjects.contains(projectId) ? unavailableCopies : 0;
            for (int i = 0; i < startedForProject; i++) {
                replicaStates.add(STARTED);
            }
            for (int i = 0; i < unavailableForProject; i++) {
                replicaStates.add(unavailableState);
            }
            projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, replicaStates.toArray(ShardRoutingState[]::new))));
        }

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedReplicaAvailabilityResult(
                    YELLOW,
                    affectedProjects,
                    indexName,
                    Map.of(
                        "started_primaries",
                        projectCount,
                        "started_replicas",
                        startedCopies * projectCount + unavailableCopies * greenCount,
                        unavailableState == INITIALIZING ? "initializing_replicas" : "unassigned_replicas",
                        unavailableCopies * affectedCount
                    ),
                    new HealthIndicatorImpact(
                        ShardsAvailabilityHealthIndicatorService.NAME,
                        REPLICA_UNASSIGNED_IMPACT_ID,
                        2,
                        "Searches might be slower than usual. Fewer redundant copies of the data exist on "
                            + indexImpactPhrase(prefixedIndexNames(affectedProjects, indexName))
                            + ".",
                        List.of(ImpactArea.SEARCH)
                    ),
                    unavailableState
                )
            )
        );
    }

    /**
     * An inactive unreplicated primary whose unassignment reason is an expected transient event stays green while
     * the primary grace period is still running. Other reasons turn the indicator red immediately.
     */
    public void testPrimaryInactiveWithinGracePeriod() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
        boolean primaryInitializing = randomBoolean();
        ShardRoutingState primaryState = primaryInitializing ? INITIALIZING : UNASSIGNED;
        UnassignedInfo unassignedInfo = unassignedInfo(reason, unassignedTimeWithinGracePeriod());

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactivePrimary(indexName, primaryState, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        var service = createStatelessIndicator(PRIMARY_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedInactivePrimaryResult(
                    affectedProjects,
                    indexName,
                    affectedCount,
                    greenCount,
                    primaryInitializing,
                    reason.isExpectedTransient()
                )
            )
        );
    }

    /**
     * An inactive replica whose unassignment reason is an expected transient event stays green while the replica
     * grace period is still running. Other reasons are treated as unavailable immediately; stateless reports RED
     * when that replica is the only copy.
     */
    public void testReplicaInactiveWithinGracePeriod() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
        boolean replicaInitializing = randomBoolean();
        ShardRoutingState replicaState = replicaInitializing ? INITIALIZING : UNASSIGNED;
        UnassignedInfo unassignedInfo = unassignedInfo(reason, unassignedTimeWithinGracePeriod());

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactiveReplica(indexName, replicaState, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(REPLICA_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedInactiveReplicaResult(
                    affectedProjects,
                    indexName,
                    affectedCount,
                    greenCount,
                    projectCount,
                    replicaInitializing,
                    reason.isExpectedTransient()
                )
            )
        );
    }

    /**
     * Documents current behavior for master directly cancelled recoveries ({@link UnassignedInfo.Reason#RECOVERY_CANCELLED}).
     * Direct cancellation is still disabled by default.
     */
    public void testRecoveryCancelledPrimaryGracePeriodBehavior() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        int failedAllocations = randomBoolean() ? 0 : randomIntBetween(1, 5);
        UnassignedInfo unassignedInfo = recoveryCancelledUnassignedInfo(failedAllocations);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactivePrimary(indexName, UNASSIGNED, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        var service = createStatelessIndicator(PRIMARY_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(expectedInactivePrimaryResult(affectedProjects, indexName, affectedCount, greenCount, false, failedAllocations == 0))
        );
    }

    /**
     * Documents current behavior for master directly cancelled recoveries ({@link UnassignedInfo.Reason#RECOVERY_CANCELLED}).
     * Direct cancellation is still disabled by default.
     */
    public void testRecoveryCancelledReplicaGracePeriodBehavior() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        int failedAllocations = randomBoolean() ? 0 : randomIntBetween(1, 5);
        UnassignedInfo unassignedInfo = recoveryCancelledUnassignedInfo(failedAllocations);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactiveReplica(indexName, UNASSIGNED, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(REPLICA_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedInactiveReplicaResult(
                    affectedProjects,
                    indexName,
                    affectedCount,
                    greenCount,
                    projectCount,
                    false,
                    failedAllocations == 0
                )
            )
        );
    }

    /**
     * Once the primary grace period expires, an inactive unreplicated primary is treated as unavailable even for an
     * expected transient reason.
     */
    public void testShouldBeRedWhenPrimaryGracePeriodExpires() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        boolean primaryInitializing = randomBoolean();
        ShardRoutingState primaryState = primaryInitializing ? INITIALIZING : UNASSIGNED;
        UnassignedInfo unassignedInfo = unassignedInfo(randomUnassignedInfoReason(true), expiredUnassignedTime());

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactivePrimary(indexName, primaryState, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        var service = createStatelessIndicator(PRIMARY_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(expectedInactivePrimaryResult(affectedProjects, indexName, affectedCount, greenCount, primaryInitializing, false))
        );
    }

    /**
     * Once the replica grace period expires, an inactive replica is treated as unavailable even for an expected
     * transient reason
     */
    public void testShouldBeRedWhenReplicaGracePeriodExpires() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        boolean replicaInitializing = randomBoolean();
        ShardRoutingState replicaState = replicaInitializing ? INITIALIZING : UNASSIGNED;
        UnassignedInfo unassignedInfo = unassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, expiredUnassignedTime());

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactiveReplica(indexName, replicaState, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(REPLICA_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedInactiveReplicaResult(
                    affectedProjects,
                    indexName,
                    affectedCount,
                    greenCount,
                    projectCount,
                    replicaInitializing,
                    false
                )
            )
        );
    }

    /**
     * A primary that became inactive only moments ago is usually given a short grace period before the indicator
     * turns red. That grace period does not apply when the last allocation status is DECIDERS_NO.
     */
    public void testShouldBeRedWhenPrimaryAllocationFailureBlocksGracePeriod() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        UnassignedInfo unassignedInfo = unassignedInfo(
            randomUnassignedInfoReason(true),
            UnassignedInfo.AllocationStatus.DECIDERS_NO,
            unassignedTimeWithinGracePeriod()
        );

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactivePrimary(indexName, UNASSIGNED, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        var service = createStatelessIndicator(PRIMARY_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(expectedInactivePrimaryResult(affectedProjects, indexName, affectedCount, greenCount, false, false))
        );
    }

    /**
     * A replica that became inactive only moments ago is usually given a short grace period. That grace period does
     * not apply when the last allocation status is DECIDERS_NO. Stateless reports RED when that replica is the only copy.
     */
    public void testShouldBeRedWhenReplicaAllocationFailureBlocksGracePeriod() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        UnassignedInfo unassignedInfo = unassignedInfo(
            randomUnassignedInfoReason(true),
            UnassignedInfo.AllocationStatus.DECIDERS_NO,
            unassignedTimeWithinGracePeriod()
        );

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInactiveReplica(indexName, UNASSIGNED, unassignedInfo)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(REPLICA_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(expectedInactiveReplicaResult(affectedProjects, indexName, affectedCount, greenCount, projectCount, false, false))
        );
    }

    /**
     * An inactive shard with no unassigned info cannot use the grace period. Relocating replicas initialize on the
     * destination node in that state. Stateless reports RED when that replica is the only copy.
     */
    public void testShouldBeRedWhenReplicaMissingUnassignedInfo() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithInitializingReplicaMissingUnassignedInfo(indexName)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(REPLICA_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(expectedInactiveReplicaResult(affectedProjects, indexName, affectedCount, greenCount, projectCount, true, false))
        );
    }

    /**
     * Mixes creating and unavailable primaries and replicas across the same indices
     */
    public void testMixedGraceAndNonGracePrimaryAndReplicaState() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;

        TimeValue recentUnassignedTime = new TimeValue(
            System.currentTimeMillis() + TimeValue.timeValueHours(randomIntBetween(1, 10)).millis(),
            TimeUnit.MILLISECONDS
        );
        TimeValue expiredUnassignedTime = new TimeValue(
            System.currentTimeMillis() - TimeValue.timeValueSeconds(randomIntBetween(21, 200)).millis(),
            TimeUnit.MILLISECONDS
        );
        UnassignedInfo.Reason replicaReason1a = randomFrom(UnassignedInfo.Reason.values());
        UnassignedInfo.Reason replicaReason1b = randomFrom(UnassignedInfo.Reason.values());
        UnassignedInfo.Reason replicaReason1c = randomFrom(UnassignedInfo.Reason.values());
        boolean replicaExpired1a = randomBoolean();
        boolean replicaExpired1b = randomBoolean();
        boolean replicaExpired1c = randomBoolean();
        UnassignedInfo.Reason primaryReason2 = randomFrom(UnassignedInfo.Reason.values());
        UnassignedInfo.Reason primaryReason3 = randomFrom(UnassignedInfo.Reason.values());
        boolean primaryExpired2 = randomBoolean();
        boolean primaryExpired3 = randomBoolean();

        final var replicaIndexName = randomIndexName();
        final var primaryIndexName2 = randomIndexName();
        final var primaryIndexName3 = randomIndexName();
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(
                    projectId,
                    List.of(
                        indexWithStartedPrimaryAndUnassignedReplicas(
                            replicaIndexName,
                            unassignedInfo(replicaReason1a, replicaExpired1a ? expiredUnassignedTime : recentUnassignedTime),
                            unassignedInfo(replicaReason1b, replicaExpired1b ? expiredUnassignedTime : recentUnassignedTime),
                            unassignedInfo(replicaReason1c, replicaExpired1c ? expiredUnassignedTime : recentUnassignedTime)
                        ),
                        indexWithInactivePrimary(
                            primaryIndexName2,
                            UNASSIGNED,
                            unassignedInfo(primaryReason2, primaryExpired2 ? expiredUnassignedTime : recentUnassignedTime)
                        ),
                        indexWithInactivePrimary(
                            primaryIndexName3,
                            UNASSIGNED,
                            unassignedInfo(primaryReason3, primaryExpired3 ? expiredUnassignedTime : recentUnassignedTime)
                        )
                    )
                );
            } else {
                projectIndexRoutes.put(
                    projectId,
                    List.of(
                        index(replicaIndexName, STARTED, STARTED, STARTED, STARTED),
                        index(primaryIndexName2, STARTED),
                        index(primaryIndexName3, STARTED)
                    )
                );
            }
        }

        int unavailablePrimaryCount = 0;
        List<String> unavailablePrimaryIndexNames = new ArrayList<>();
        List<String> creatingPrimaryIndexNames = new ArrayList<>();
        if (isNonProvisionallyUnavailable(primaryReason2, primaryExpired2)) {
            unavailablePrimaryCount++;
            unavailablePrimaryIndexNames.add(primaryIndexName2);
        } else {
            creatingPrimaryIndexNames.add(primaryIndexName2);
        }
        if (isNonProvisionallyUnavailable(primaryReason3, primaryExpired3)) {
            unavailablePrimaryCount++;
            unavailablePrimaryIndexNames.add(primaryIndexName3);
        } else {
            creatingPrimaryIndexNames.add(primaryIndexName3);
        }
        int creatingPrimaryCount = 2 - unavailablePrimaryCount;
        int unavailableReplicaCount = 0;
        if (isNonProvisionallyUnavailable(replicaReason1a, replicaExpired1a)) {
            unavailableReplicaCount++;
        }
        if (isNonProvisionallyUnavailable(replicaReason1b, replicaExpired1b)) {
            unavailableReplicaCount++;
        }
        if (isNonProvisionallyUnavailable(replicaReason1c, replicaExpired1c)) {
            unavailableReplicaCount++;
        }
        int creatingReplicaCount = 3 - unavailableReplicaCount;

        int totalUnavailablePrimaries = unavailablePrimaryCount * affectedCount;
        int totalCreatingPrimaries = creatingPrimaryCount * affectedCount;
        int totalUnavailableReplicas = unavailableReplicaCount * affectedCount;
        int totalCreatingReplicas = creatingReplicaCount * affectedCount;
        HealthStatus expectedStatus = totalUnavailablePrimaries > 0 || totalUnavailableReplicas > 0 ? RED : GREEN;

        List<String> symptomParts = new ArrayList<>();
        if (totalUnavailablePrimaries > 0) {
            symptomParts.add(countPhrase(totalUnavailablePrimaries, "unavailable primary shard", "unavailable primary shards"));
        }
        if (totalCreatingPrimaries > 0) {
            symptomParts.add(countPhrase(totalCreatingPrimaries, "creating primary shard", "creating primary shards"));
        }
        if (totalUnavailableReplicas > 0) {
            symptomParts.add(countPhrase(totalUnavailableReplicas, "unavailable replica shard", "unavailable replica shards"));
        }
        if (totalCreatingReplicas > 0) {
            symptomParts.add(countPhrase(totalCreatingReplicas, "creating replica shard", "creating replica shards"));
        }

        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of(
                    "unassigned_primaries",
                    totalUnavailablePrimaries,
                    "creating_primaries",
                    totalCreatingPrimaries,
                    "started_primaries",
                    affectedCount + 3 * greenCount,
                    "unassigned_replicas",
                    totalUnavailableReplicas,
                    "creating_replicas",
                    totalCreatingReplicas,
                    "started_replicas",
                    3 * greenCount
                )
            )
        );
        List<String> unavailablePrimaryIndices = prefixedIndexNames(affectedProjects, unavailablePrimaryIndexNames);
        List<String> creatingPrimaryIndices = prefixedIndexNames(affectedProjects, creatingPrimaryIndexNames);
        List<String> replicaIndices = prefixedIndexNames(affectedProjects, replicaIndexName);
        if (unavailablePrimaryIndices.isEmpty() == false) {
            details.put("indices_with_unavailable_primaries", String.join(", ", unavailablePrimaryIndices));
        }
        if (creatingPrimaryIndices.isEmpty() == false) {
            details.put("indices_with_provisionally_unavailable_primaries", String.join(", ", creatingPrimaryIndices));
        }
        if (totalUnavailableReplicas > 0) {
            details.put("indices_with_unavailable_replicas", String.join(", ", replicaIndices));
        }
        if (totalCreatingReplicas > 0) {
            details.put("indices_with_provisionally_unavailable_replicas", String.join(", ", replicaIndices));
        }

        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        if (unavailablePrimaryIndices.isEmpty() == false) {
            impacts.add(primaryUnassignedImpact(unavailablePrimaryIndices));
        }
        if (totalUnavailableReplicas > 0) {
            impacts.add(allReplicasUnassignedImpact(replicaIndices));
        }

        List<String> diagnosisIndices = prefixedIndexNames(
            affectedProjects,
            List.of(primaryIndexName2, primaryIndexName3, replicaIndexName)
        );
        var service = createStatelessIndicator(BOTH_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, 100, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    expectedStatus,
                    "This cluster has " + String.join(", ", symptomParts) + ".",
                    new SimpleHealthIndicatorDetails(details),
                    impacts,
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, diagnosisIndices))))
                )
            )
        );
    }

    /**
     * Once the restart allocation delay expires, a restarting replica is treated as unavailable. The stateless health api reports RED
     * when that replica is the only copy.
     */
    public void testShouldBeRedWhenRestartingReplicasReachedAllocationDelay() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        String restartingNodeId = "node-0";
        long expiredUnassignedTimeNanos = System.nanoTime() - TimeValue.timeValueSeconds(randomIntBetween(60, 180)).nanos();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(
                    projectId,
                    List.of(indexWithRestartingReplica(indexName, restartingNodeId, expiredUnassignedTimeNanos))
                );
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of("started_primaries", projectCount, "started_replicas", greenCount, "unassigned_replicas", affectedCount)
            )
        );
        details.put("indices_with_unavailable_replicas", String.join(", ", affectedIndices));

        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, restartShutdown(restartingNodeId, 60))
        );
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(affectedCount, "unavailable replica shard", "unavailable replica shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(allReplicasUnassignedImpact(affectedIndices)),
                    List.of(
                        new Diagnosis(DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS, List.of(new Diagnosis.Resource(INDEX, affectedIndices)))
                    )
                )
            )
        );
    }

    /**
     * Once the restart allocation delay expires, a restarting unreplicated primary is treated as unavailable.
     */
    public void testShouldBeRedWhenRestartingPrimariesReachedAllocationDelayAndNoReplicas() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        String restartingNodeId = "node-0";
        long expiredUnassignedTimeNanos = System.nanoTime() - TimeValue.timeValueSeconds(randomIntBetween(60, 120)).nanos();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(
                    projectId,
                    List.of(indexWithRestartingPrimary(indexName, restartingNodeId, expiredUnassignedTimeNanos))
                );
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(Map.of("unassigned_primaries", affectedCount, "started_primaries", greenCount))
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", affectedIndices));

        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, restartShutdown(restartingNodeId, 60))
        );
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(affectedCount, "unavailable primary shard", "unavailable primary shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(primaryUnassignedImpact(affectedIndices)),
                    List.of(
                        new Diagnosis(DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS, List.of(new Diagnosis.Resource(INDEX, affectedIndices)))
                    )
                )
            )
        );
    }

    /**
     * A shard unassigned because its node is restarting is normally ignored while a matching RESTART shutdown allocation delay is still
     * running. Without a matching RESTART shutdown for that node, the indicator treats the unreplicated primary as unavailable.
     */
    public void testRestartingPrimaryHasNoMatchingRestartShutdown() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        String restartingNodeId = "node-0";

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithRestartingPrimary(indexName, restartingNodeId)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(Map.of("unassigned_primaries", affectedCount, "started_primaries", greenCount))
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", affectedIndices));

        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, mismatchedRestartShutdowns(restartingNodeId))
        );
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(affectedCount, "unavailable primary shard", "unavailable primary shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(primaryUnassignedImpact(affectedIndices)),
                    List.of(
                        new Diagnosis(DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS, List.of(new Diagnosis.Resource(INDEX, affectedIndices)))
                    )
                )
            )
        );
    }

    /**
     * Without a matching RESTART shutdown, a restarting replica is treated as unavailable. The stateless health api reports RED
     * when that replica is the only copy.
     */
    public void testRestartingReplicaHasNoMatchingRestartShutdown() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        String restartingNodeId = "node-0";

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(indexWithRestartingReplica(indexName, restartingNodeId)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of("started_primaries", projectCount, "started_replicas", greenCount, "unassigned_replicas", affectedCount)
            )
        );
        details.put("indices_with_unavailable_replicas", String.join(", ", affectedIndices));

        var service = createStatelessIndicator(
            NO_GRACE_PERIOD_SETTINGS,
            clusterState(projectIndexRoutes, mismatchedRestartShutdowns(restartingNodeId))
        );
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(affectedCount, "unavailable replica shard", "unavailable replica shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(allReplicasUnassignedImpact(affectedIndices)),
                    List.of(
                        new Diagnosis(DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS, List.of(new Diagnosis.Resource(INDEX, affectedIndices)))
                    )
                )
            )
        );
    }

    /**
     * An initializing replica in any project, with no other started replica copy of that shard, is treated as
     * all replicas unassigned. Stateless reports RED for unassigned replicas
     */
    public void testShouldBeRedWhenReplicaIsInitializing() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> redProjects = randomProjects(projectIds);
        int redCount = redProjects.size();
        List<String> redIndices = prefixedIndexNames(redProjects, indexName);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (redProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, INITIALIZING)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of("started_primaries", projectCount, "started_replicas", projectCount - redCount, "initializing_replicas", redCount)
            )
        );
        details.put("indices_with_unavailable_replicas", String.join(", ", redIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(redCount, "initializing replica shard", "initializing replica shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            ALL_REPLICAS_UNASSIGNED_IMPACT_ID,
                            1,
                            "Not all data is searchable. No searchable copies of the data exist on " + indexImpactPhrase(redIndices) + ".",
                            List.of(ImpactArea.SEARCH)
                        )
                    ),
                    List.of(new Diagnosis(DIAGNOSIS_WAIT_FOR_INITIALIZATION, List.of(new Diagnosis.Resource(INDEX, redIndices))))
                )
            )
        );
    }

    /**
     * An initializing primary in any project turns the indicator red
     */
    public void testShouldBeRedWhenPrimaryIsInitializing() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> redProjects = randomProjects(projectIds);
        int redCount = redProjects.size();
        List<String> redIndices = prefixedIndexNames(redProjects, indexName);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (redProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(index(indexName, INITIALIZING)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(Map.of("started_primaries", projectCount - redCount, "initializing_primaries", redCount))
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", redIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(redCount, "initializing primary shard", "initializing primary shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to " + indexImpactPhrase(redIndices) + ". Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(new Diagnosis(DIAGNOSIS_WAIT_FOR_INITIALIZATION, List.of(new Diagnosis.Resource(INDEX, redIndices))))
                )
            )
        );
    }

    /**
     * An unassigned primary and replica in any project turns the indicator red
     */
    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicas() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> redProjects = randomProjects(projectIds);
        int redCount = redProjects.size();
        List<String> redIndices = prefixedIndexNames(redProjects, indexName);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (redProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(index(indexName, UNASSIGNED, UNASSIGNED)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, STARTED)));
            }
        }

        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of(
                    "started_primaries",
                    projectCount - redCount,
                    "started_replicas",
                    projectCount - redCount,
                    "unassigned_primaries",
                    redCount,
                    "unassigned_replicas",
                    redCount
                )
            )
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", redIndices));
        details.put("indices_with_unavailable_replicas", String.join(", ", redIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has "
                        + countPhrase(redCount, "unavailable primary shard", "unavailable primary shards")
                        + ", "
                        + countPhrase(redCount, "unavailable replica shard", "unavailable replica shards")
                        + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to " + indexImpactPhrase(redIndices) + ". Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        ),
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            ALL_REPLICAS_UNASSIGNED_IMPACT_ID,
                            1,
                            "Not all data is searchable. No searchable copies of the data exist on " + indexImpactPhrase(redIndices) + ".",
                            List.of(ImpactArea.SEARCH)
                        )
                    ),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, redIndices))))
                )
            )
        );
    }

    /**
     * Every replica copy of an index is unavailable
     */
    public void testShouldBeRedWhenAllReplicaCopiesAreUnavailable() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        int replicaCount = randomIntBetween(1, 3);
        ShardRoutingState unavailableState = randomFrom(UNASSIGNED, INITIALIZING);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            ShardRoutingState[] replicaStates = new ShardRoutingState[replicaCount];
            Arrays.fill(replicaStates, affectedProjects.contains(projectId) ? unavailableState : STARTED);
            projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED, replicaStates)));
        }

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedReplicaAvailabilityResult(
                    RED,
                    affectedProjects,
                    indexName,
                    Map.of(
                        "started_primaries",
                        projectCount,
                        "started_replicas",
                        replicaCount * greenCount,
                        unavailableState == INITIALIZING ? "initializing_replicas" : "unassigned_replicas",
                        replicaCount * affectedCount
                    ),
                    allReplicasUnassignedImpact(prefixedIndexNames(affectedProjects, indexName)),
                    unavailableState
                )
            )
        );
    }

    /**
     * With two shards and one replica each, an unassigned replica on a single shard still means all copies of that shard
     * are gone, so the index has all replicas unassigned.
     */
    public void testShouldBeRedWhenOneShardHasAllReplicasUnassigned() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        ShardRoutingState unavailableState = randomFrom(UNASSIGNED, INITIALIZING);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(twoShardIndex(indexName, STARTED, STARTED, STARTED, unavailableState)));
            } else {
                projectIndexRoutes.put(projectId, List.of(twoShardIndex(indexName, STARTED, STARTED, STARTED, STARTED)));
            }
        }

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                expectedReplicaAvailabilityResult(
                    RED,
                    affectedProjects,
                    indexName,
                    Map.of(
                        "started_primaries",
                        2 * projectCount,
                        "started_replicas",
                        projectCount + greenCount,
                        unavailableState == INITIALIZING ? "initializing_replicas" : "unassigned_replicas",
                        affectedCount
                    ),
                    allReplicasUnassignedImpact(prefixedIndexNames(affectedProjects, indexName)),
                    unavailableState
                )
            )
        );
    }

    /**
     * An unassigned unreplicated primary in any project turns the indicator red
     */
    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndNoReplicas() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        Set<ProjectId> redProjects = randomProjects(projectIds);
        int redCount = redProjects.size();
        List<String> redIndices = prefixedIndexNames(redProjects, indexName);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (redProjects.contains(projectId)) {
                projectIndexRoutes.put(projectId, List.of(index(indexName, UNASSIGNED)));
            } else {
                projectIndexRoutes.put(projectId, List.of(index(indexName, STARTED)));
            }
        }

        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(Map.of("started_primaries", projectCount - redCount, "unassigned_primaries", redCount))
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", redIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + countPhrase(redCount, "unavailable primary shard", "unavailable primary shards") + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to " + indexImpactPhrase(redIndices) + ". Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, redIndices))))
                )
            )
        );
    }

    /**
     * Unassigned primaries and unassigned replicas on different indices in the same projects. The stateless health api reports RED
     * for both the missing primaries and the yellow indices whose only replica copy is gone.
     */
    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicasOnDifferentIndices() {
        int projectCount = randomIntBetween(1, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        Set<ProjectId> affectedProjects = randomProjects(projectIds);
        int affectedCount = affectedProjects.size();
        int greenCount = projectCount - affectedCount;
        final var redIndexName = randomIndexName();
        final var yellowIndexName1 = randomIndexName();
        final var yellowIndexName2 = randomIndexName();
        Map<String, Integer> indexPriorities = Map.of(redIndexName, 3, yellowIndexName1, 5, yellowIndexName2, 8);

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            if (affectedProjects.contains(projectId)) {
                projectIndexRoutes.put(
                    projectId,
                    List.of(
                        index(redIndexName, UNASSIGNED),
                        index(yellowIndexName1, STARTED, UNASSIGNED),
                        index(yellowIndexName2, STARTED, UNASSIGNED)
                    )
                );
            } else {
                projectIndexRoutes.put(
                    projectId,
                    List.of(
                        index(redIndexName, STARTED),
                        index(yellowIndexName1, STARTED, STARTED),
                        index(yellowIndexName2, STARTED, STARTED)
                    )
                );
            }
        }

        List<String> redIndices = prefixedIndexNames(affectedProjects, redIndexName);
        List<String> yellowIndices = prefixedIndexNamesByPriority(affectedProjects, List.of(yellowIndexName2, yellowIndexName1));
        List<String> diagnosisIndices = prefixedIndexNamesByPriority(
            affectedProjects,
            List.of(yellowIndexName2, yellowIndexName1, redIndexName)
        );
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of(
                    "started_primaries",
                    3 * projectCount - affectedCount,
                    "started_replicas",
                    2 * greenCount,
                    "unassigned_primaries",
                    affectedCount,
                    "unassigned_replicas",
                    2 * affectedCount
                )
            )
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", redIndices));
        details.put("indices_with_unavailable_replicas", String.join(", ", yellowIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes, indexPriorities));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has "
                        + countPhrase(affectedCount, "unavailable primary shard", "unavailable primary shards")
                        + ", "
                        + countPhrase(2 * affectedCount, "unavailable replica shard", "unavailable replica shards")
                        + ".",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to " + indexImpactPhrase(redIndices) + ". Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        ),
                        allReplicasUnassignedImpact(yellowIndices)
                    ),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, diagnosisIndices))))
                )
            )
        );
    }

    /**
     * A red project (unassigned primary), a yellow project (some replica copies still started), and green
     * projects. Cluster status is RED; yellow replica impact is kept because that index has no unavailable primary.
     */
    public void testShouldBeRedWhenOneProjectHasUnassignedPrimaryAndAnotherHasUnavailableReplicas() {
        int projectCount = randomIntBetween(3, 5);
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        final var indexName = randomIndexName();
        List<ProjectId> remaining = new ArrayList<>(projectIds);
        ProjectId redProject = randomFrom(remaining);
        remaining.remove(redProject);
        ProjectId yellowProject = randomFrom(remaining);
        remaining.remove(yellowProject);
        int greenCount = remaining.size();

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        projectIndexRoutes.put(redProject, List.of(index(indexName, UNASSIGNED)));
        projectIndexRoutes.put(yellowProject, List.of(index(indexName, STARTED, STARTED, UNASSIGNED)));
        for (ProjectId greenProject : remaining) {
            projectIndexRoutes.put(greenProject, List.of(index(indexName, STARTED, STARTED, STARTED)));
        }

        List<String> redIndices = prefixedIndexNames(Set.of(redProject), indexName);
        List<String> yellowIndices = prefixedIndexNames(Set.of(yellowProject), indexName);
        List<String> diagnosisIndices = prefixedIndexNames(Set.of(redProject, yellowProject), indexName);
        Map<String, Object> details = new HashMap<>(
            detailsWithDefaults(
                Map.of(
                    "started_primaries",
                    projectCount - 1,
                    "unassigned_primaries",
                    1,
                    "started_replicas",
                    1 + 2 * greenCount,
                    "unassigned_replicas",
                    1
                )
            )
        );
        details.put("indices_with_unavailable_primaries", String.join(", ", redIndices));
        details.put("indices_with_unavailable_replicas", String.join(", ", yellowIndices));

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has 1 unavailable primary shard, 1 unavailable replica shard.",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(primaryUnassignedImpact(redIndices), replicaUnassignedImpact(yellowIndices)),
                    List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, diagnosisIndices))))
                )
            )
        );
    }

    /**
     * Details and impact index lists keep the 10 highest-priority {@code projectId/indexName} values
     * and append {@code , ...} when more than 10 project-indices are unavailable.
     */
    public void testTruncatesUnavailableIndexListsInDetailsAndImpacts() {
        int projectCount = 3;
        Set<ProjectId> projectIds = randomProjectIds(projectCount);
        Set<String> indexNames = new HashSet<>();
        while (indexNames.size() < 5) {
            indexNames.add(randomIndexName());
        }
        List<String> names = new ArrayList<>(indexNames);
        String highPriorityIndex = names.get(0);
        List<String> lowPriorityIndices = names.subList(1, names.size());
        Map<String, Integer> indexPriorities = new HashMap<>();
        indexPriorities.put(highPriorityIndex, 100);
        for (String lowPriorityIndex : lowPriorityIndices) {
            indexPriorities.put(lowPriorityIndex, 1);
        }

        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
        for (ProjectId projectId : projectIds) {
            List<IndexRoutingTable> indices = new ArrayList<>();
            for (String indexName : names) {
                indices.add(index(indexName, UNASSIGNED));
            }
            projectIndexRoutes.put(projectId, indices);
        }

        int unavailableCount = projectCount * names.size();
        List<String> allUnavailableIndices = new ArrayList<>(prefixedIndexNames(projectIds, highPriorityIndex));
        allUnavailableIndices.addAll(prefixedIndexNames(projectIds, lowPriorityIndices));
        String truncatedIndices = String.join(", ", allUnavailableIndices.subList(0, 10)) + ", ...";
        Map<String, Object> details = new HashMap<>(detailsWithDefaults(Map.of("unassigned_primaries", unavailableCount)));
        details.put("indices_with_unavailable_primaries", truncatedIndices);

        var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes, indexPriorities));
        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    ShardsAvailabilityHealthIndicatorService.NAME,
                    RED,
                    "This cluster has " + unavailableCount + " unavailable primary shards.",
                    new SimpleHealthIndicatorDetails(details),
                    List.of(
                        new HealthIndicatorImpact(
                            ShardsAvailabilityHealthIndicatorService.NAME,
                            PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to "
                                + unavailableCount
                                + " indices ["
                                + truncatedIndices
                                + "]. Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, allUnavailableIndices)))
                    )
                )
            )
        );
    }

    /**
     * An unassigned frozen/mounted index is red when the original index is missing or also unavailable.
     */
    public void testShouldBeRedWhenFrozenIndexIsUnassignedAndOriginalIsUnavailable() {
        final var originalIndex = randomIndexName();
        final var restoredIndex = randomIndexName();
        Map<String, Settings> restoredSettings = Map.of(restoredIndex, searchableSnapshotSettings(originalIndex));

        // The restored/mounted index is unassigned and the original index is not in the project at all
        {
            int projectCount = randomIntBetween(1, 5);
            Set<ProjectId> projectIds = randomProjectIds(projectCount);
            Set<ProjectId> affectedProjects = randomProjects(projectIds);
            int affectedCount = affectedProjects.size();
            int greenCount = projectCount - affectedCount;

            Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
            for (ProjectId projectId : projectIds) {
                if (affectedProjects.contains(projectId)) {
                    projectIndexRoutes.put(projectId, List.of(index(restoredIndex, UNASSIGNED)));
                } else {
                    projectIndexRoutes.put(projectId, List.of(index(restoredIndex, STARTED)));
                }
            }

            List<String> restoredIndices = prefixedIndexNames(affectedProjects, restoredIndex);
            Map<String, Object> details = new HashMap<>(
                detailsWithDefaults(Map.of("unassigned_primaries", affectedCount, "started_primaries", greenCount))
            );
            var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes, Map.of(), restoredSettings));
            assertThat(
                service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
                equalTo(
                    new HealthIndicatorResult(
                        ShardsAvailabilityHealthIndicatorService.NAME,
                        RED,
                        "This cluster has " + countPhrase(affectedCount, "unavailable primary shard", "unavailable primary shards") + ".",
                        new SimpleHealthIndicatorDetails(details),
                        List.of(readOnlyPrimaryUnassignedImpact(restoredIndices)),
                        List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, restoredIndices))))
                    )
                )
            );
        }
        // Both the mounted index and the original index exist but their primaries are unassigned
        {
            int projectCount = randomIntBetween(1, 5);
            Set<ProjectId> projectIds = randomProjectIds(projectCount);
            Set<ProjectId> affectedProjects = randomProjects(projectIds);
            int affectedCount = affectedProjects.size();
            int greenCount = projectCount - affectedCount;

            Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes = new HashMap<>();
            for (ProjectId projectId : projectIds) {
                if (affectedProjects.contains(projectId)) {
                    projectIndexRoutes.put(projectId, List.of(index(restoredIndex, UNASSIGNED), index(originalIndex, UNASSIGNED)));
                } else {
                    projectIndexRoutes.put(projectId, List.of(index(restoredIndex, STARTED), index(originalIndex, STARTED)));
                }
            }

            List<String> originalIndices = prefixedIndexNames(affectedProjects, originalIndex);
            List<String> restoredIndices = prefixedIndexNames(affectedProjects, restoredIndex);
            List<String> diagnosisIndices = prefixedIndexNames(affectedProjects, List.of(originalIndex, restoredIndex));
            Map<String, Object> details = new HashMap<>(
                detailsWithDefaults(Map.of("unassigned_primaries", 2 * affectedCount, "started_primaries", 2 * greenCount))
            );
            details.put("indices_with_unavailable_primaries", String.join(", ", originalIndices));
            var service = createStatelessIndicator(NO_GRACE_PERIOD_SETTINGS, clusterState(projectIndexRoutes, Map.of(), restoredSettings));
            assertThat(
                service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
                equalTo(
                    new HealthIndicatorResult(
                        ShardsAvailabilityHealthIndicatorService.NAME,
                        RED,
                        "This cluster has "
                            + countPhrase(2 * affectedCount, "unavailable primary shard", "unavailable primary shards")
                            + ".",
                        new SimpleHealthIndicatorDetails(details),
                        List.of(primaryUnassignedImpact(originalIndices), readOnlyPrimaryUnassignedImpact(restoredIndices)),
                        List.of(
                            new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, diagnosisIndices)))
                        )
                    )
                )
            );
        }
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

    private static String countPhrase(int count, String singular, String plural) {
        return count == 1 ? "1 " + singular : count + " " + plural;
    }

    private static String indexImpactPhrase(List<String> indices) {
        return indices.size() == 1 ? "1 index [" + indices.get(0) + "]" : indices.size() + " indices [" + String.join(", ", indices) + "]";
    }

    private static HealthIndicatorImpact allReplicasUnassignedImpact(List<String> indices) {
        return new HealthIndicatorImpact(
            ShardsAvailabilityHealthIndicatorService.NAME,
            ALL_REPLICAS_UNASSIGNED_IMPACT_ID,
            1,
            "Not all data is searchable. No searchable copies of the data exist on " + indexImpactPhrase(indices) + ".",
            List.of(ImpactArea.SEARCH)
        );
    }

    private static HealthIndicatorImpact primaryUnassignedImpact(List<String> indices) {
        return new HealthIndicatorImpact(
            ShardsAvailabilityHealthIndicatorService.NAME,
            PRIMARY_UNASSIGNED_IMPACT_ID,
            1,
            "Cannot add data to " + indexImpactPhrase(indices) + ". Searches might return incomplete results.",
            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
        );
    }

    private static HealthIndicatorImpact replicaUnassignedImpact(List<String> indices) {
        return new HealthIndicatorImpact(
            ShardsAvailabilityHealthIndicatorService.NAME,
            REPLICA_UNASSIGNED_IMPACT_ID,
            2,
            "Searches might be slower than usual. Fewer redundant copies of the data exist on " + indexImpactPhrase(indices) + ".",
            List.of(ImpactArea.SEARCH)
        );
    }

    private static HealthIndicatorImpact readOnlyPrimaryUnassignedImpact(List<String> indices) {
        return new HealthIndicatorImpact(
            ShardsAvailabilityHealthIndicatorService.NAME,
            READ_ONLY_PRIMARY_UNASSIGNED_IMPACT_ID,
            1,
            "Searching " + indexImpactPhrase(indices) + " might return incomplete results.",
            List.of(ImpactArea.SEARCH)
        );
    }

    private static HealthIndicatorResult expectedReplicaAvailabilityResult(
        HealthStatus status,
        Set<ProjectId> affectedProjects,
        String indexName,
        Map<String, Object> countOverrides,
        HealthIndicatorImpact replicaImpact,
        ShardRoutingState unavailableState
    ) {
        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> details = new HashMap<>(detailsWithDefaults(countOverrides));
        details.put("indices_with_unavailable_replicas", String.join(", ", affectedIndices));
        int unavailableCount = (Integer) countOverrides.get(
            unavailableState == INITIALIZING ? "initializing_replicas" : "unassigned_replicas"
        );
        String symptomKind = unavailableState == INITIALIZING ? "initializing replica shard" : "unavailable replica shard";
        String symptomKindPlural = unavailableState == INITIALIZING ? "initializing replica shards" : "unavailable replica shards";
        return new HealthIndicatorResult(
            ShardsAvailabilityHealthIndicatorService.NAME,
            status,
            "This cluster has " + countPhrase(unavailableCount, symptomKind, symptomKindPlural) + ".",
            new SimpleHealthIndicatorDetails(details),
            List.of(replicaImpact),
            List.of(
                new Diagnosis(
                    unavailableState == INITIALIZING ? DIAGNOSIS_WAIT_FOR_INITIALIZATION : ACTION_CHECK_ALLOCATION_EXPLAIN_API,
                    List.of(new Diagnosis.Resource(INDEX, affectedIndices))
                )
            )
        );
    }

    private static HealthIndicatorResult expectedInactiveReplicaResult(
        Set<ProjectId> affectedProjects,
        String indexName,
        int affectedCount,
        int greenCount,
        int projectCount,
        boolean initializing,
        boolean provisionallyGreen
    ) {
        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> countOverrides = new HashMap<>(Map.of("started_primaries", projectCount, "started_replicas", greenCount));
        String symptomKind;
        String symptomKindPlural;
        if (initializing) {
            countOverrides.put("initializing_replicas", affectedCount);
            symptomKind = "initializing replica shard";
            symptomKindPlural = "initializing replica shards";
        } else if (provisionallyGreen) {
            countOverrides.put("creating_replicas", affectedCount);
            symptomKind = "creating replica shard";
            symptomKindPlural = "creating replica shards";
        } else {
            countOverrides.put("unassigned_replicas", affectedCount);
            symptomKind = "unavailable replica shard";
            symptomKindPlural = "unavailable replica shards";
        }
        Map<String, Object> details = new HashMap<>(detailsWithDefaults(countOverrides));
        details.put(
            provisionallyGreen ? "indices_with_provisionally_unavailable_replicas" : "indices_with_unavailable_replicas",
            String.join(", ", affectedIndices)
        );
        return new HealthIndicatorResult(
            ShardsAvailabilityHealthIndicatorService.NAME,
            provisionallyGreen ? GREEN : RED,
            "This cluster has " + countPhrase(affectedCount, symptomKind, symptomKindPlural) + ".",
            new SimpleHealthIndicatorDetails(details),
            provisionallyGreen ? List.of() : List.of(allReplicasUnassignedImpact(affectedIndices)),
            List.of(
                new Diagnosis(
                    initializing ? DIAGNOSIS_WAIT_FOR_INITIALIZATION : ACTION_CHECK_ALLOCATION_EXPLAIN_API,
                    List.of(new Diagnosis.Resource(INDEX, affectedIndices))
                )
            )
        );
    }

    private static HealthIndicatorResult expectedInactivePrimaryResult(
        Set<ProjectId> affectedProjects,
        String indexName,
        int affectedCount,
        int greenCount,
        boolean initializing,
        boolean provisionallyGreen
    ) {
        List<String> affectedIndices = prefixedIndexNames(affectedProjects, indexName);
        Map<String, Object> countOverrides = new HashMap<>(Map.of("started_primaries", greenCount));
        String symptomKind;
        String symptomKindPlural;
        if (initializing) {
            countOverrides.put("initializing_primaries", affectedCount);
            symptomKind = "initializing primary shard";
            symptomKindPlural = "initializing primary shards";
        } else if (provisionallyGreen) {
            countOverrides.put("creating_primaries", affectedCount);
            symptomKind = "creating primary shard";
            symptomKindPlural = "creating primary shards";
        } else {
            countOverrides.put("unassigned_primaries", affectedCount);
            symptomKind = "unavailable primary shard";
            symptomKindPlural = "unavailable primary shards";
        }
        Map<String, Object> details = new HashMap<>(detailsWithDefaults(countOverrides));
        details.put(
            provisionallyGreen ? "indices_with_provisionally_unavailable_primaries" : "indices_with_unavailable_primaries",
            String.join(", ", affectedIndices)
        );
        return new HealthIndicatorResult(
            ShardsAvailabilityHealthIndicatorService.NAME,
            provisionallyGreen ? GREEN : RED,
            "This cluster has " + countPhrase(affectedCount, symptomKind, symptomKindPlural) + ".",
            new SimpleHealthIndicatorDetails(details),
            provisionallyGreen ? List.of() : List.of(primaryUnassignedImpact(affectedIndices)),
            List.of(
                new Diagnosis(
                    initializing ? DIAGNOSIS_WAIT_FOR_INITIALIZATION : ACTION_CHECK_ALLOCATION_EXPLAIN_API,
                    List.of(new Diagnosis.Resource(INDEX, affectedIndices))
                )
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

    private static IndexRoutingTable creatingIndex(String name, ShardRoutingState... replicaStates) {
        return creatingIndex(name, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null), replicaStates);
    }

    private static IndexRoutingTable creatingIndex(String name, UnassignedInfo primaryUnassignedInfo, ShardRoutingState... replicaStates) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(replicaStates.length)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        var builder = IndexRoutingTable.builder(index);
        builder.addShard(
            newUnassigned(
                shardId,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                primaryUnassignedInfo,
                ShardRouting.Role.DEFAULT,
                ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
            )
        );
        for (ShardRoutingState replicaState : replicaStates) {
            builder.addShard(shardRouting(shardId, false, replicaState));
        }
        return builder.build();
    }

    private static IndexRoutingTable unreplicatedIndex(String name, int numberOfShards, ShardRoutingState primaryState) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0)
            .build();
        var index = indexMetadata.getIndex();
        var builder = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < numberOfShards; shard++) {
            builder.addShard(shardRouting(new ShardId(index, shard), true, primaryState));
        }
        return builder.build();
    }

    private static IndexRoutingTable twoShardIndex(
        String name,
        ShardRoutingState primary0,
        ShardRoutingState replica0,
        ShardRoutingState primary1,
        ShardRoutingState replica1
    ) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(2)
            .numberOfReplicas(1)
            .build();
        var index = indexMetadata.getIndex();
        var builder = IndexRoutingTable.builder(index);
        builder.addShard(shardRouting(new ShardId(index, 0), true, primary0));
        builder.addShard(shardRouting(new ShardId(index, 1), true, primary1));
        builder.addShard(shardRouting(new ShardId(index, 0), false, replica0));
        builder.addShard(shardRouting(new ShardId(index, 1), false, replica1));
        return builder.build();
    }

    /**
     * Two-shard index mid-reshard. Shard 0, the source, is started. Shard 1 is an inactive {@code RESHARD_SPLIT} target.
     */
    private static IndexRoutingTable reshardSplitIndex(String name, boolean targetPrimaryInitializing) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(2)
            .numberOfReplicas(1)
            .build();
        var index = indexMetadata.getIndex();
        var sourceShardId = new ShardId(index, 0);
        var targetShardId = new ShardId(index, 1);
        // 5 minutes ago: proves GREEN is from RESHARD_SPLIT, not the inactive grace window.
        TimeValue unassignedAt = new TimeValue(System.currentTimeMillis() - TimeValue.timeValueMinutes(5).millis(), TimeUnit.MILLISECONDS);
        UnassignedInfo targetPrimaryUnassignedInfo = randomBoolean()
            ? unassignedInfo(UnassignedInfo.Reason.RESHARD_ADDED, unassignedAt)
            : unassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, unassignedAt);
        var targetShardPrimary = newUnassigned(
            targetShardId,
            true,
            new RecoverySource.ReshardSplitRecoverySource(sourceShardId),
            targetPrimaryUnassignedInfo,
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
        );
        var targetShardReplica = newUnassigned(
            targetShardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            unassignedInfo(UnassignedInfo.Reason.RESHARD_ADDED, unassignedAt),
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
        );
        if (targetPrimaryInitializing) {
            targetShardPrimary = targetShardPrimary.initialize(randomNodeId(), null, 0);
        }
        return IndexRoutingTable.builder(index)
            .addShard(shardRouting(sourceShardId, true, STARTED))
            .addShard(shardRouting(sourceShardId, false, STARTED))
            .addShard(targetShardPrimary)
            .addShard(targetShardReplica)
            .build();
    }

    private static IndexRoutingTable indexWithRestartingReplica(String name, String restartingNodeId) {
        return indexWithRestartingReplica(name, restartingNodeId, System.nanoTime());
    }

    private static IndexRoutingTable indexWithRestartingReplica(String name, String restartingNodeId, long unassignedTimeNanos) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        return IndexRoutingTable.builder(index)
            .addShard(shardRouting(shardId, true, STARTED))
            .addShard(restartingShard(shardId, false, restartingNodeId, unassignedTimeNanos))
            .build();
    }

    private static IndexRoutingTable indexWithRestartingPrimary(String name, String restartingNodeId) {
        return indexWithRestartingPrimary(name, restartingNodeId, System.nanoTime());
    }

    private static IndexRoutingTable indexWithRestartingPrimary(String name, String restartingNodeId, long unassignedTimeNanos) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        return IndexRoutingTable.builder(index).addShard(restartingShard(shardId, true, restartingNodeId, unassignedTimeNanos)).build();
    }

    private static IndexRoutingTable indexWithInactiveReplica(String name, ShardRoutingState replicaState, UnassignedInfo unassignedInfo) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        return IndexRoutingTable.builder(index)
            .addShard(shardRouting(shardId, true, STARTED))
            .addShard(inactiveShard(shardId, false, replicaState, unassignedInfo))
            .build();
    }

    private static IndexRoutingTable indexWithInactivePrimary(String name, ShardRoutingState primaryState, UnassignedInfo unassignedInfo) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        return IndexRoutingTable.builder(index).addShard(inactiveShard(shardId, true, primaryState, unassignedInfo)).build();
    }

    private static IndexRoutingTable indexWithStartedPrimaryAndUnassignedReplicas(String name, UnassignedInfo... replicaInfos) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(replicaInfos.length)
            .build();
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);
        var builder = IndexRoutingTable.builder(index).addShard(shardRouting(shardId, true, STARTED));
        for (UnassignedInfo replicaInfo : replicaInfos) {
            builder.addShard(inactiveShard(shardId, false, UNASSIGNED, replicaInfo));
        }
        return builder.build();
    }

    private static IndexRoutingTable indexWithInitializingReplicaMissingUnassignedInfo(String name) {
        var indexMetadata = IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        var shardId = new ShardId(indexMetadata.getIndex(), 0);
        var relocatingTargetReplica = TestShardRouting.newShardRouting(
            shardId,
            randomNodeId(),
            randomNodeId(),
            false,
            ShardRoutingState.INITIALIZING
        );
        assertNull(relocatingTargetReplica.unassignedInfo());
        return IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting(shardId, true, STARTED))
            .addShard(relocatingTargetReplica)
            .build();
    }

    /**
     * Matches the original suite's INITIALIZING / UNAVAILABLE construction: ExistingStore recovery for primaries
     * so {@code isUnassignedDueToNewInitialization} is false, and UNASSIGNED shards go through started then
     * {@code moveToUnassigned} so the provided unassigned info is the current one.
     */
    private static ShardRouting inactiveShard(ShardId shardId, boolean primary, ShardRoutingState state, UnassignedInfo unassignedInfo) {
        var routing = newUnassigned(
            shardId,
            primary,
            primary ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            unassignedInfo,
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
        );
        routing = routing.initialize(randomNodeId(), null, 0);
        if (state == INITIALIZING) {
            return routing;
        }
        return routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
            .moveToUnassigned(unassignedInfo, ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED);
    }

    private static TimeValue unassignedTimeWithinGracePeriod() {
        return new TimeValue(System.currentTimeMillis() + TimeValue.timeValueHours(1).millis(), TimeUnit.MILLISECONDS);
    }

    private static TimeValue expiredUnassignedTime() {
        return new TimeValue(System.currentTimeMillis() - TimeValue.timeValueSeconds(30).millis(), TimeUnit.MILLISECONDS);
    }

    private static UnassignedInfo.Reason randomUnassignedInfoReason(boolean expectedTransient) {
        return randomFrom(
            Arrays.stream(UnassignedInfo.Reason.values()).filter(reason -> reason.isExpectedTransient() == expectedTransient).toList()
        );
    }

    private static boolean isNonProvisionallyUnavailable(UnassignedInfo.Reason reason, boolean expired) {
        return reason.isExpectedTransient() == false || expired;
    }

    private static UnassignedInfo unassignedInfo(UnassignedInfo.Reason reason, TimeValue unassignedTime) {
        return unassignedInfo(reason, UnassignedInfo.AllocationStatus.NO_ATTEMPT, unassignedTime);
    }

    private static UnassignedInfo unassignedInfo(
        UnassignedInfo.Reason reason,
        UnassignedInfo.AllocationStatus allocationStatus,
        TimeValue unassignedTime
    ) {
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        String lastAllocatedNodeId = reason == UnassignedInfo.Reason.NODE_RESTARTING ? "last-allocated-test-node" : null;
        return new UnassignedInfo(
            reason,
            null,
            null,
            failedAllocations,
            unassignedTime.nanos(),
            unassignedTime.millis(),
            false,
            allocationStatus,
            Set.of(),
            lastAllocatedNodeId
        );
    }

    private static UnassignedInfo recoveryCancelledUnassignedInfo(int failedAllocations) {
        TimeValue unassignedTime = unassignedTimeWithinGracePeriod();
        return new UnassignedInfo(
            UnassignedInfo.Reason.RECOVERY_CANCELLED,
            null,
            null,
            failedAllocations,
            unassignedTime.nanos(),
            unassignedTime.millis(),
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Set.of(),
            null
        );
    }

    private static ShardRouting restartingShard(ShardId shardId, boolean primary, String nodeId, long unassignedTimeNanos) {
        return shardRouting(shardId, primary, STARTED).moveToUnassigned(
            new UnassignedInfo(
                UnassignedInfo.Reason.NODE_RESTARTING,
                null,
                null,
                0,
                unassignedTimeNanos,
                0L,
                false,
                UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                Set.of(),
                nodeId
            ),
            ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED
        );
    }

    private static NodesShutdownMetadata restartShutdown(String nodeId, int allocationDelaySeconds) {
        return nodeShutdown(nodeId, SingleNodeShutdownMetadata.Type.RESTART, allocationDelaySeconds);
    }

    /**
     * Empty shutdowns, a non-RESTART shutdown type, or a RESTART shutdown for a different node — all make
     * {@code shutdowns.get(nodeId, RESTART)} return null.
     */
    private static NodesShutdownMetadata mismatchedRestartShutdowns(String restartingNodeId) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> NodesShutdownMetadata.EMPTY;
            case 1 -> nodeShutdown(restartingNodeId, SingleNodeShutdownMetadata.Type.REMOVE, null);
            default -> restartShutdown("other-node", 60);
        };
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

    private static ShardRouting shardRouting(ShardId shardId, boolean primary, ShardRoutingState state) {
        var routing = newUnassigned(
            shardId,
            primary,
            primary ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
        );
        if (state == UNASSIGNED) {
            return routing;
        }
        routing = routing.initialize(randomNodeId(), null, 0);
        if (state == INITIALIZING) {
            return routing;
        }
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

    private static List<String> prefixedIndexNames(Collection<ProjectId> projectIds, String indexName) {
        return projectIds.stream().map(projectId -> projectId.id() + "/" + indexName).sorted().toList();
    }

    private static List<String> prefixedIndexNames(Collection<ProjectId> projectIds, List<String> indexNames) {
        List<String> names = new ArrayList<>();
        for (ProjectId projectId : projectIds) {
            for (String indexName : indexNames) {
                names.add(projectId.id() + "/" + indexName);
            }
        }
        names.sort(null);
        return names;
    }

    private static List<String> prefixedIndexNamesByPriority(Collection<ProjectId> projectIds, List<String> indexNamesHighestFirst) {
        List<String> names = new ArrayList<>();
        for (String indexName : indexNamesHighestFirst) {
            names.addAll(prefixedIndexNames(projectIds, indexName));
        }
        return names;
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
        Map<String, Integer> indexPriorities
    ) {
        return clusterState(projectIndexRoutes, indexPriorities, Map.of(), NodesShutdownMetadata.EMPTY);
    }

    private static ClusterState clusterState(
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes,
        Map<String, Integer> indexPriorities,
        NodesShutdownMetadata nodesShutdownMetadata
    ) {
        return clusterState(projectIndexRoutes, indexPriorities, Map.of(), nodesShutdownMetadata);
    }

    private static ClusterState clusterState(
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes,
        Map<String, Integer> indexPriorities,
        Map<String, Settings> extraIndexSettings
    ) {
        return clusterState(projectIndexRoutes, indexPriorities, extraIndexSettings, NodesShutdownMetadata.EMPTY);
    }

    private static ClusterState clusterState(
        Map<ProjectId, List<IndexRoutingTable>> projectIndexRoutes,
        Map<String, Integer> indexPriorities,
        Map<String, Settings> extraIndexSettings,
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
                Settings extraSettings = extraIndexSettings.get(indexRouting.getIndex().getName());
                if (extraSettings != null) {
                    settings.put(extraSettings);
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

    private static Settings searchableSnapshotSettings(String originalIndex) {
        return Settings.builder()
            .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_INDEX_NAME_SETTING_KEY, originalIndex)
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE)
            .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, randomBoolean())
            .build();
    }
}
