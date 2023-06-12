/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ShardAllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.RESTART;
import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_CHECK_ALLOCATION_EXPLAIN_API;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_ENABLE_INDEX_ROUTING_ALLOCATION;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_ENABLE_TIERS_LOOKUP;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_NODE_CAPACITY;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_TIER_CAPACITY_LOOKUP;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.ACTION_RESTORE_FROM_SNAPSHOT;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.DIAGNOSIS_WAIT_FOR_INITIALIZATION;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.NAME;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.AVAILABLE;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.CREATING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.INITIALIZING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.RESTARTING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.UNAVAILABLE;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.health.Diagnosis.Resource.Type.FEATURE_STATE;
import static org.elasticsearch.health.Diagnosis.Resource.Type.INDEX;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardsAvailabilityHealthIndicatorServiceTests extends ESTestCase {

    public void testShouldBeGreenWhenAllPrimariesAndReplicasAreStarted() {
        var clusterState = createClusterStateWith(
            List.of(
                index("replicated-index", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), AVAILABLE)),
                index("unreplicated-index", new ShardAllocation(randomNodeId(), AVAILABLE))
            ),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has all shards available.",
                    Map.of("started_primaries", 2, "started_replicas", 1),
                    emptyList(),
                    emptyList()
                )
            )
        );
    }

    public void testShouldBeYellowWhenReplicaIsInitializing() {
        var clusterState = createClusterStateWith(
            List.of(
                index("replicated-index", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), INITIALIZING))
            ),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    "This cluster has 1 initializing replica shard.",
                    Map.of("started_primaries", 1, "initializing_replicas", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID,
                            2,
                            "Searches might be slower than usual. Fewer redundant copies of the data exist on 1 index [replicated-index].",
                            List.of(ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(
                            DIAGNOSIS_WAIT_FOR_INITIALIZATION,
                            List.of(new Diagnosis.Resource(INDEX, List.of("replicated-index")))
                        )
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenPrimaryIsInitializing() {
        var clusterState = createClusterStateWith(
            List.of(index("unreplicated-index", new ShardAllocation(randomNodeId(), INITIALIZING))),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        HealthIndicatorResult calculate = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertThat(
            calculate,
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 initializing primary shard.",
                    Map.of("initializing_primaries", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to 1 index [unreplicated-index]. Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(
                            DIAGNOSIS_WAIT_FOR_INITIALIZATION,
                            List.of(new Diagnosis.Resource(INDEX, List.of("unreplicated-index")))
                        )
                    )
                )
            )
        );
    }

    public void testShouldBeGreenWhenAllPrimariesAreCreating() {
        var clusterState = createClusterStateWith(
            List.of(index("unreplicated-index", new ShardAllocation(randomNodeId(), CREATING))),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 creating primary shard.",
                    Map.of("creating_primaries", 1),
                    emptyList(),
                    emptyList()
                )
            )
        );
    }

    public void testShouldBeYellowWhenThereAreUnassignedReplicas() {
        var availableReplicas = randomList(0, 5, () -> new ShardAllocation(randomNodeId(), AVAILABLE));
        var unavailableReplicas = randomList(1, 5, () -> new ShardAllocation(randomNodeId(), UNAVAILABLE));

        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "yellow-index",
                    new ShardAllocation(randomNodeId(), AVAILABLE),
                    concatLists(availableReplicas, unavailableReplicas).toArray(ShardAllocation[]::new)
                )
            ),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    unavailableReplicas.size() > 1
                        ? "This cluster has " + unavailableReplicas.size() + " unavailable replica shards."
                        : "This cluster has 1 unavailable replica shard.",
                    Map.of(
                        "started_primaries",
                        1,
                        "unassigned_replicas",
                        unavailableReplicas.size(),
                        "started_replicas",
                        availableReplicas.size()
                    ),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID,
                            2,
                            "Searches might be slower than usual. Fewer redundant copies of the data exist on 1 index [yellow-index].",
                            List.of(ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, List.of("yellow-index"))))
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicas() {
        var clusterState = createClusterStateWith(
            List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE))),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary shard, 1 unavailable replica shard.",
                    Map.of("unassigned_primaries", 1, "unassigned_replicas", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to 1 index [red-index]. Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, List.of("red-index"))))
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndNoReplicas() {
        var clusterState = createClusterStateWith(List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE))), List.of());
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary shard.",
                    Map.of("unassigned_primaries", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to 1 index [red-index]. Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, List.of("red-index"))))
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicasOnSameIndex() {
        var clusterState = createClusterStateWith(
            List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE))),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        HealthIndicatorResult result = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(RED, result.status());
        assertEquals("This cluster has 1 unavailable primary shard, 1 unavailable replica shard.", result.symptom());
        assertEquals(1, result.impacts().size());
        assertEquals(
            result.impacts().get(0),
            new HealthIndicatorImpact(
                NAME,
                ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                1,
                "Cannot add data to 1 index [red-index]. Searches might return incomplete results.",
                List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicasOnDifferentIndices() {
        List<IndexMetadata> indexMetadataList = createIndexMetadataForIndexNameToPriorityMap(
            Map.of("red-index", 3, "yellow-index-1", 5, "yellow-index-2", 8)
        );
        var clusterState = createClusterStateWith(
            indexMetadataList,
            List.of(
                index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("yellow-index-1", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("yellow-index-2", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE))
            ),
            List.of(),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        HealthIndicatorResult result = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(RED, result.status());
        assertEquals("This cluster has 1 unavailable primary shard, 2 unavailable replica shards.", result.symptom());
        assertEquals(2, result.impacts().size());
        assertEquals(
            result.impacts().get(0),
            new HealthIndicatorImpact(
                NAME,
                ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                1,
                "Cannot add data to 1 index [red-index]. Searches might return incomplete results.",
                List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
            )
        );
        // yellow-index-2 has the higher priority so it ought to be listed first:
        assertThat(
            result.impacts().get(1),
            equalTo(
                new HealthIndicatorImpact(
                    NAME,
                    ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID,
                    2,
                    "Searches might be slower than usual. Fewer redundant copies of the data exist on 2 indices [yellow-index-2, "
                        + "yellow-index-1].",
                    List.of(ImpactArea.SEARCH)
                )
            )
        );
    }

    public void testSortByIndexPriority() {
        var lowPriority = randomIntBetween(1, 5);
        var highPriority = randomIntBetween(6, 20);
        List<IndexMetadata> indexMetadataList = createIndexMetadataForIndexNameToPriorityMap(
            Map.of("index-3", lowPriority, "index-1", lowPriority, "index-2", highPriority)
        );
        var clusterState = createClusterStateWith(
            indexMetadataList,
            List.of(
                index("index-3", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("index-1", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("index-2", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE))
            ),
            List.of(),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        HealthIndicatorResult result = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        // index-2 has the higher priority so it ought to be listed first, followed by index-1 then index-3 which have the same priority:
        assertThat(
            result.impacts().get(0),
            equalTo(
                new HealthIndicatorImpact(
                    NAME,
                    ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID,
                    2,
                    "Searches might be slower than usual. Fewer redundant copies of the data exist on 3 indices [index-2, "
                        + "index-1, index-3].",
                    List.of(ImpactArea.SEARCH)
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreRestartingReplicas() {
        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "restarting-index",
                    new ShardAllocation(randomNodeId(), AVAILABLE),
                    new ShardAllocation("node-0", RESTARTING, System.nanoTime())
                )
            ),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 restarting replica shard.",
                    Map.of("started_primaries", 1, "restarting_replicas", 1),
                    emptyList(),
                    emptyList()
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreNoReplicasExpected() {
        var clusterState = createClusterStateWith(
            List.of(index("primaries-only-index", new ShardAllocation(randomNodeId(), AVAILABLE))),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has all shards available.",
                    Map.of("started_primaries", 1),
                    emptyList(),
                    emptyList()
                )
            )
        );
    }

    public void testShouldBeYellowWhenRestartingReplicasReachedAllocationDelay() {
        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "restarting-index",
                    new ShardAllocation(randomNodeId(), AVAILABLE),
                    new ShardAllocation("node-0", RESTARTING, System.nanoTime() - timeValueSeconds(between(60, 180)).nanos())
                )
            ),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    "This cluster has 1 unavailable replica shard.",
                    Map.of("started_primaries", 1, "unassigned_replicas", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID,
                            2,
                            "Searches might be slower than usual. Fewer redundant copies of the data exist on 1 index "
                                + "[restarting-index].",
                            List.of(ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(
                            DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS,
                            List.of(new Diagnosis.Resource(INDEX, List.of("restarting-index")))
                        )
                    )
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreInitializingPrimaries() {
        var clusterState = createClusterStateWith(List.of(index("restarting-index", new ShardAllocation("node-0", CREATING))), List.of());
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 creating primary shard.",
                    Map.of("creating_primaries", 1),
                    emptyList(),
                    emptyList()
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreRestartingPrimaries() {
        var clusterState = createClusterStateWith(
            List.of(index("restarting-index", new ShardAllocation("node-0", RESTARTING, System.nanoTime()))),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 restarting primary shard.",
                    Map.of("restarting_primaries", 1),
                    emptyList(),
                    emptyList()
                )
            )
        );
    }

    public void testShouldBeRedWhenRestartingPrimariesReachedAllocationDelayAndNoReplicas() {
        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "restarting-index",
                    new ShardAllocation("node-0", RESTARTING, System.nanoTime() - timeValueSeconds(between(60, 120)).nanos())
                )
            ),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary shard.",
                    Map.of("unassigned_primaries", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to 1 index [restarting-index]. Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    ),
                    List.of(
                        new Diagnosis(
                            DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS,
                            List.of(new Diagnosis.Resource(INDEX, List.of("restarting-index")))
                        )
                    )
                )
            )
        );
    }

    public void testDiagnosisNotGeneratedWhenNotDrillingDown() {
        // Index definition, 1 primary no replicas
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Cluster state with index, but its only shard is unassigned because there is no shard copy
        var clusterState = createClusterStateWith(
            List.of(indexMetadata),
            List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE, noShardCopy()))),
            List.of(),
            List.of()
        );

        var service = createShardsAvailabilityIndicatorService(clusterState);

        assertThat(
            service.calculate(false, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                createExpectedTruncatedResult(
                    RED,
                    "This cluster has 1 unavailable primary shard.",
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                            1,
                            "Cannot add data to 1 index [red-index]. Searches might return incomplete results.",
                            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                        )
                    )
                )
            )
        );
    }

    public void testDiagnoseRestoreIndexAfterDataLoss() {
        // Index definition, 1 primary no replicas
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting = createShardRouting(
            new ShardId(indexMetadata.getIndex(), 0),
            true,
            new ShardAllocation(randomNodeId(), UNAVAILABLE, noShardCopy())
        );

        var service = createShardsAvailabilityIndicatorService();
        List<Diagnosis.Definition> definitions = service.diagnoseUnassignedShardRouting(shardRouting, ClusterState.EMPTY_STATE);

        assertThat(definitions, hasSize(1));
        assertThat(definitions, contains(ACTION_RESTORE_FROM_SNAPSHOT));
    }

    public void testRestoreFromSnapshotReportsFeatureStates() {
        // this test adds a mix of regular and system indices and data streams
        // we'll test the `shards_availability` indicator correctly reports the
        // affected feature states and indices

        IndexMetadata featureIndex = IndexMetadata.builder(".feature-index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexMetadata regularIndex = IndexMetadata.builder("regular-index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        String featureDataStreamName = ".test-ds-feature";
        IndexMetadata backingIndex = createBackingIndex(featureDataStreamName, 1).build();

        ShardRouting featureIndexRouting = createShardRouting(
            new ShardId(featureIndex.getIndex(), 0),
            true,
            new ShardAllocation(randomNodeId(), UNAVAILABLE, noShardCopy())
        );

        ShardRouting regularIndexRouting = createShardRouting(
            new ShardId(regularIndex.getIndex(), 0),
            true,
            new ShardAllocation(randomNodeId(), UNAVAILABLE, noShardCopy())
        );

        ShardRouting backingIndexRouting = createShardRouting(
            new ShardId(backingIndex.getIndex(), 0),
            true,
            new ShardAllocation(randomNodeId(), UNAVAILABLE, noShardCopy())
        );

        var clusterState = createClusterStateWith(
            List.of(featureIndex, regularIndex, backingIndex),
            List.of(
                IndexRoutingTable.builder(featureIndex.getIndex()).addShard(featureIndexRouting).build(),
                IndexRoutingTable.builder(regularIndex.getIndex()).addShard(regularIndexRouting).build(),
                IndexRoutingTable.builder(backingIndex.getIndex()).addShard(backingIndexRouting).build()
            ),
            List.of(),
            List.of()
        );

        // add the data stream to the cluster state
        Metadata.Builder mdBuilder = Metadata.builder(clusterState.metadata())
            .put(newInstance(featureDataStreamName, List.of(backingIndex.getIndex())));
        ClusterState state = ClusterState.builder(clusterState).metadata(mdBuilder).build();

        var service = createAllocationHealthIndicatorService(
            Settings.EMPTY,
            state,
            Map.of(),
            getSystemIndices(featureDataStreamName, ".test-ds-*", ".feature-*")
        );
        HealthIndicatorResult result = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertThat(result.status(), is(HealthStatus.RED));
        assertThat(result.diagnosisList().size(), is(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<Diagnosis.Resource> affectedResources = diagnosis.affectedResources();
        assertThat("expecting we report a resource of type INDEX and one of type FEATURE_STATE", affectedResources.size(), is(2));
        for (Diagnosis.Resource resource : affectedResources) {
            if (resource.getType() == INDEX) {
                assertThat(resource.getValues(), hasItems("regular-index"));
            } else {
                assertThat(resource.getType(), is(FEATURE_STATE));
                assertThat(resource.getValues(), hasItems("feature-with-system-data-stream", "feature-with-system-index"));
            }
        }
    }

    public void testGetRestoreFromSnapshotAffectedResources() {
        String featureDataStreamName = ".test-ds-feature";
        IndexMetadata backingIndex = createBackingIndex(featureDataStreamName, 1).build();

        List<IndexMetadata> indexMetadataList = List.of(
            IndexMetadata.builder(".feature-index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            IndexMetadata.builder("regular-index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            backingIndex
        );

        Metadata.Builder metadataBuilder = Metadata.builder();
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (IndexMetadata indexMetadata : indexMetadataList) {
            indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
        }
        metadataBuilder.indices(indexMetadataMap);
        metadataBuilder.put(newInstance(featureDataStreamName, List.of(backingIndex.getIndex())));
        Metadata metadata = metadataBuilder.build();
        {
            List<Diagnosis.Resource> affectedResources = ShardAllocationStatus.getRestoreFromSnapshotAffectedResources(
                metadata,
                getSystemIndices(featureDataStreamName, ".test-ds-*", ".feature-*"),
                Set.of(backingIndex.getIndex().getName(), ".feature-index", "regular-index"),
                10
            );

            assertThat(affectedResources.size(), is(2));
            for (Diagnosis.Resource resource : affectedResources) {
                if (resource.getType() == INDEX) {
                    assertThat(resource.getValues(), hasItems("regular-index"));
                } else {
                    assertThat(resource.getType(), is(FEATURE_STATE));
                    assertThat(resource.getValues(), hasItems("feature-with-system-data-stream", "feature-with-system-index"));
                }
            }
        }

        {
            List<Diagnosis.Resource> affectedResources = ShardAllocationStatus.getRestoreFromSnapshotAffectedResources(
                metadata,
                getSystemIndices(featureDataStreamName, ".test-ds-*", ".feature-*"),
                Set.of(backingIndex.getIndex().getName(), ".feature-index", "regular-index"),
                0
            );

            assertThat(affectedResources.size(), is(2));
            for (Diagnosis.Resource resource : affectedResources) {
                if (resource.getType() == INDEX) {
                    assertThat(resource.getValues(), emptyCollectionOf(String.class));
                } else {
                    assertThat(resource.getType(), is(FEATURE_STATE));
                    assertThat(resource.getValues(), emptyCollectionOf(String.class));
                }
            }
        }

    }

    public void testDiagnoseUnknownAllocationDeciderIssue() {
        // Index definition, 1 primary no replicas
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Cluster state with index, but its only shard is unassigned (Either deciders said no, or a node left)
        var clusterState = createClusterStateWith(
            List.of(indexMetadata),
            List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE, randomFrom(decidersNo(), nodeLeft())))),
            List.of(),
            List.of()
        );

        // All deciders return yes except for one kind that the indicator does not have advice about
        Map<ShardRoutingKey, ShardAllocationDecision> decisionMap = Map.of(
            new ShardRoutingKey("red-index", 0, true),
            new ShardAllocationDecision(
                AllocateUnassignedDecision.fromDecision(
                    Decision.NO,
                    null,
                    List.of(
                        new NodeAllocationResult(
                            DiscoveryNodeUtils.create(randomNodeId()),
                            new Decision.Multi().add(Decision.single(Decision.Type.YES, EnableAllocationDecider.NAME, null))
                                .add(Decision.single(Decision.Type.YES, "data_tier", null))
                                .add(Decision.single(Decision.Type.YES, ShardsLimitAllocationDecider.NAME, null))
                                .add(Decision.single(Decision.Type.YES, FilterAllocationDecider.NAME, null))
                                .add(Decision.single(Decision.Type.YES, SameShardAllocationDecider.NAME, null))
                                .add(Decision.single(Decision.Type.NO, AwarenessAllocationDecider.NAME, null)), // Unhandled in indicator
                            1
                        )
                    )
                ),
                MoveDecision.NOT_TAKEN
            )
        );
        var service = createShardsAvailabilityIndicatorService(clusterState, decisionMap);

        // Get the list of user actions that are generated for this unassigned index shard
        ShardRouting shardRouting = clusterState.routingTable().index(indexMetadata.getIndex()).shard(0).primaryShard();
        List<Diagnosis.Definition> actions = service.diagnoseUnassignedShardRouting(shardRouting, clusterState);

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_CHECK_ALLOCATION_EXPLAIN_API));
    }

    public void testDiagnoseEnableIndexAllocation() {
        // Index definition, 1 primary no replicas, allocation is not allowed
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkIsAllocationDisabled(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    // Shard allocation is disabled on index
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.NO, EnableAllocationDecider.NAME, null)),
                    1
                )
            )
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION));
    }

    public void testNodeAllocationResultWithNullDecision() {
        // Index definition, 1 primary no replicas, allocation is not allowed
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkIsAllocationDisabled(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    // Shard allocation is disabled on index
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new NodeAllocationResult.ShardStoreInfo(10),
                    null
                )
            )
        );

        assertThat(actions, hasSize(0));
    }

    public void testDiagnoseEnableClusterAllocation() {
        // Index definition, 1 primary no replicas
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Disallow allocations in cluster settings
        var service = createShardsAvailabilityIndicatorService(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none").build(),
            ClusterState.EMPTY_STATE,
            Map.of()
        );

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkIsAllocationDisabled(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    // Shard allocation is disabled on index
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.NO, EnableAllocationDecider.NAME, null)),
                    1
                )
            )
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION));
    }

    public void testDiagnoseEnableRoutingAllocation() {
        // Index definition, 1 primary no replicas, allocation is not allowed
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Disallow allocations in cluster settings
        var service = createShardsAvailabilityIndicatorService(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none").build(),
            ClusterState.EMPTY_STATE,
            Map.of()
        );

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkIsAllocationDisabled(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    // Shard allocation is disabled on index
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.NO, EnableAllocationDecider.NAME, null)),
                    1
                )
            )
        );

        // Fix both settings
        assertThat(actions, hasSize(2));
        assertThat(actions, containsInAnyOrder(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION, ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION));
    }

    public void testDiagnoseEnableDataTiers() {
        // Index definition, 1 primary no replicas, in the hot tier
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                // Shard is not allowed due to data tier filter
                new NodeAllocationResult(
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.NO, "data_tier", null)),
                    1
                )
            ),
            ClusterState.EMPTY_STATE
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_ENABLE_TIERS_LOOKUP.get(DataTier.DATA_HOT)));
    }

    public void testDiagnoseIncreaseShardLimitIndexSettingInTier() {
        // Index definition, 2 primaries no replicas, in the hot tier, and at most 1 shard per node
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                    .build()
            )
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();

        // One node that is in the hot tier
        DiscoveryNode hotNode = DiscoveryNodeUtils.create(
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)
        );

        var clusterState = createClusterStateWith(
            List.of(indexMetadata),
            List.of(
                IndexRoutingTable.builder(index)
                    // Already allocated shard on hot node (places it at limit)
                    .addShard(createShardRouting(new ShardId(index, 0), true, new ShardAllocation(hotNode.getId(), AVAILABLE)))
                    // Unallocated shard
                    .addShard(
                        createShardRouting(new ShardId(index, 1), true, new ShardAllocation(randomNodeId(), UNAVAILABLE, decidersNo()))
                    )
                    .build()
            ),
            List.of(),
            List.of(hotNode)
        );
        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    hotNode,
                    // Shard is allowed on data tier, but disallowed because of shard limits
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, ShardsLimitAllocationDecider.NAME, null)),
                    1
                )
            ),
            clusterState
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP.get(DataTier.DATA_HOT)));
    }

    public void testDiagnoseIncreaseShardLimitClusterSettingInTier() {
        // Index definition, 2 primaries no replicas, in the hot tier
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .build()
            )
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();

        // One node that is in the hot tier
        DiscoveryNode hotNode = DiscoveryNodeUtils.create(
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)
        );

        var clusterState = createClusterStateWith(
            List.of(indexMetadata),
            List.of(
                IndexRoutingTable.builder(index)
                    // Already allocated shard on hot node (places it at limit)
                    .addShard(createShardRouting(new ShardId(index, 0), true, new ShardAllocation(hotNode.getId(), AVAILABLE)))
                    // Unallocated shard
                    .addShard(
                        createShardRouting(new ShardId(index, 1), true, new ShardAllocation(randomNodeId(), UNAVAILABLE, decidersNo()))
                    )
                    .build()
            ),
            List.of(),
            List.of(hotNode)
        );

        // Configure at most 1 shard per node
        var service = createShardsAvailabilityIndicatorService(
            Settings.builder().put(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build(),
            clusterState,
            Map.of()
        );

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    hotNode,
                    // Shard is allowed on data tier, but disallowed because of shard limits
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, ShardsLimitAllocationDecider.NAME, null)),
                    1
                )
            ),
            clusterState
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP.get(DataTier.DATA_HOT)));
    }

    public void testDiagnoseIncreaseShardLimitIndexSettingInGeneral() {
        // Index definition, 2 primaries no replicas, in the hot tier, and at most 1 shard per node
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                    .build()
            )
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();

        // One node that is a generic data node
        DiscoveryNode dataNode = DiscoveryNodeUtils.create(
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE)
        );

        var clusterState = createClusterStateWith(
            List.of(indexMetadata),
            List.of(
                IndexRoutingTable.builder(index)
                    // Already allocated shard on data node (places it at limit)
                    .addShard(createShardRouting(new ShardId(index, 0), true, new ShardAllocation(dataNode.getId(), AVAILABLE)))
                    // Unallocated shard
                    .addShard(
                        createShardRouting(new ShardId(index, 1), true, new ShardAllocation(randomNodeId(), UNAVAILABLE, decidersNo()))
                    )
                    .build()
            ),
            List.of(),
            List.of(dataNode)
        );
        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    dataNode,
                    // Shard is allowed on data tier, but disallowed because of shard limits
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, ShardsLimitAllocationDecider.NAME, null)),
                    1
                )
            ),
            clusterState
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING));
    }

    public void testDiagnoseIncreaseShardLimitClusterSettingInGeneral() {
        // Index definition, 2 primaries no replicas, in the hot tier
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .build()
            )
            .numberOfShards(2)
            .numberOfReplicas(0)
            .build();
        Index index = indexMetadata.getIndex();

        // One node that is a generic data node
        DiscoveryNode dataNode = DiscoveryNodeUtils.create(
            randomNodeId(),
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE)
        );

        var clusterState = createClusterStateWith(
            List.of(indexMetadata),
            List.of(
                IndexRoutingTable.builder(index)
                    // Already allocated shard on data node (places it at limit)
                    .addShard(createShardRouting(new ShardId(index, 0), true, new ShardAllocation(dataNode.getId(), AVAILABLE)))
                    // Unallocated shard
                    .addShard(
                        createShardRouting(new ShardId(index, 1), true, new ShardAllocation(randomNodeId(), UNAVAILABLE, decidersNo()))
                    )
                    .build()
            ),
            List.of(),
            List.of(dataNode)
        );

        // Configure at most 1 shard per node
        var service = createShardsAvailabilityIndicatorService(
            Settings.builder().put(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build(),
            clusterState,
            Map.of()
        );

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                new NodeAllocationResult(
                    dataNode,
                    // Shard is allowed on data tier, but disallowed because of shard limits
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, ShardsLimitAllocationDecider.NAME, null)),
                    1
                )
            ),
            clusterState
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING));
    }

    public void testDiagnoseMigrateDataRequiredToDataTiers() {
        // Index definition, 1 primary no replicas, in the hot tier, with require attribute data:hot
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .put(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".data", "hot")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                // Shard is allowed on data tier, but disallowed because of allocation filters
                new NodeAllocationResult(
                    // Node has no data attributes on it
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, FilterAllocationDecider.NAME, null)),
                    1
                )
            ),
            ClusterState.EMPTY_STATE
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP.get(DataTier.DATA_HOT)));
    }

    public void testDiagnoseMigrateDataIncludedToDataTiers() {
        // Index definition, 1 primary no replicas, in the hot tier, with include attribute data:hot
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .put(INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".data", "hot")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                // Shard is allowed on data tier, but disallowed because of allocation filters
                new NodeAllocationResult(
                    // Node has no data attributes on it
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, FilterAllocationDecider.NAME, null)),
                    1
                )
            ),
            ClusterState.EMPTY_STATE
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP.get(DataTier.DATA_HOT)));
    }

    public void testDiagnoseOtherFilteringIssue() {
        // Index definition, 1 primary no replicas, in the hot tier
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                // Shard is allowed on data tier, but disallowed because of allocation filters
                new NodeAllocationResult(
                    // Node does not have data attribute on it
                    DiscoveryNodeUtils.create(randomNodeId()),
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, FilterAllocationDecider.NAME, null)),
                    1
                )
            ),
            ClusterState.EMPTY_STATE
        );

        // checkDataTierRelatedIssues will leave list empty. Diagnosis methods upstream will add "Check allocation explain" action.
        assertThat(actions, hasSize(0));
    }

    public void testDiagnoseIncreaseTierCapacity() {
        // Index definition, 1 primary no replicas, in the hot tier
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                // Shard is allowed on data tier, but disallowed because node is already hosting a copy of it.
                new NodeAllocationResult(
                    DiscoveryNodeUtils.create(
                        randomNodeId(),
                        buildNewFakeTransportAddress(),
                        Map.of(),
                        Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)
                    ),
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, SameShardAllocationDecider.NAME, null)),
                    1
                )
            ),
            ClusterState.EMPTY_STATE
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(DataTier.DATA_HOT)));
    }

    public void testDiagnoseIncreaseNodeCapacity() {
        // Index definition, 1 primary no replicas, in the hot tier
        IndexMetadata indexMetadata = IndexMetadata.builder("red-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        var service = createShardsAvailabilityIndicatorService();

        // Get the list of user actions that are generated for this unassigned index shard
        List<Diagnosis.Definition> actions = service.checkDataTierRelatedIssues(
            indexMetadata,
            List.of(
                // Shard is allowed on data tier, but disallowed because node is already hosting a copy of it.
                new NodeAllocationResult(
                    DiscoveryNodeUtils.create(
                        randomNodeId(),
                        buildNewFakeTransportAddress(),
                        Map.of(),
                        Set.of(DiscoveryNodeRole.DATA_ROLE)
                    ),
                    new Decision.Multi().add(Decision.single(Decision.Type.YES, "data_tier", null))
                        .add(Decision.single(Decision.Type.NO, SameShardAllocationDecider.NAME, null)),
                    1
                )
            ),
            ClusterState.EMPTY_STATE
        );

        assertThat(actions, hasSize(1));
        assertThat(actions, contains(ACTION_INCREASE_NODE_CAPACITY));
    }

    public void testLimitNumberOfAffectedResources() {
        var clusterState = createClusterStateWith(
            List.of(
                index("red-index1", new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("red-index2", new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("red-index3", new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("red-index4", new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("red-index5", new ShardAllocation(randomNodeId(), UNAVAILABLE))
            ),
            List.of()
        );
        var service = createShardsAvailabilityIndicatorService(clusterState);

        {
            // assert the full result to check that details, impacts, and symptoms use the correct count of affected indices (5)
            assertThat(
                service.calculate(true, 2, HealthInfo.EMPTY_HEALTH_INFO),
                equalTo(
                    createExpectedResult(
                        RED,
                        "This cluster has 5 unavailable primary shards.",
                        Map.of("unassigned_primaries", 5),
                        List.of(
                            new HealthIndicatorImpact(
                                NAME,
                                ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID,
                                1,
                                "Cannot add data to 5 indices [red-index1, red-index2, red-index3, red-index4, red-index5]. Searches might "
                                    + "return incomplete results.",
                                List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                            )
                        ),
                        List.of(
                            new Diagnosis(
                                ACTION_CHECK_ALLOCATION_EXPLAIN_API,
                                List.of(new Diagnosis.Resource(INDEX, List.of("red-index1", "red-index2")))
                            )
                        )
                    )
                )
            );
        }

        {
            // larger number of affected resources
            assertThat(
                service.calculate(true, 2_000, HealthInfo.EMPTY_HEALTH_INFO).diagnosisList(),
                equalTo(
                    List.of(
                        new Diagnosis(
                            ACTION_CHECK_ALLOCATION_EXPLAIN_API,
                            List.of(
                                new Diagnosis.Resource(INDEX, List.of("red-index1", "red-index2", "red-index3", "red-index4", "red-index5"))
                            )
                        )
                    )
                )
            );

        }

        {
            // 0 affected resources
            assertThat(
                service.calculate(true, 0, HealthInfo.EMPTY_HEALTH_INFO).diagnosisList(),
                equalTo(List.of(new Diagnosis(ACTION_CHECK_ALLOCATION_EXPLAIN_API, List.of(new Diagnosis.Resource(INDEX, List.of())))))
            );
        }
    }

    /**
     * Creates the {@link SystemIndices} with one standalone system index and a system data stream
     */
    private SystemIndices getSystemIndices(
        String featureDataStreamName,
        String systemDataStreamPattern,
        String standaloneSystemIndexPattern
    ) {
        return new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "feature-with-system-index",
                    "testing",
                    List.of(SystemIndexDescriptorUtils.createUnmanaged(standaloneSystemIndexPattern, "feature with index"))
                ),
                new SystemIndices.Feature(
                    "feature-with-system-data-stream",
                    "feature with data stream",
                    List.of(),
                    List.of(
                        new SystemDataStreamDescriptor(
                            featureDataStreamName,
                            "description",
                            SystemDataStreamDescriptor.Type.EXTERNAL,
                            new ComposableIndexTemplate(
                                List.of(systemDataStreamPattern),
                                null,
                                null,
                                null,
                                null,
                                null,
                                new ComposableIndexTemplate.DataStreamTemplate()
                            ),
                            Map.of(),
                            List.of("test"),
                            new ExecutorNames(
                                ThreadPool.Names.SYSTEM_CRITICAL_READ,
                                ThreadPool.Names.SYSTEM_READ,
                                ThreadPool.Names.SYSTEM_WRITE
                            )
                        )
                    )
                )
            )
        );
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(ShardsAvailabilityHealthIndicatorService.NAME, equalTo("shards_availability"));
        assertThat(
            ACTION_RESTORE_FROM_SNAPSHOT.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:restore_from_snapshot")
        );
        assertThat(
            ACTION_CHECK_ALLOCATION_EXPLAIN_API.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:explain_allocations")
        );
        assertThat(
            DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:delayed_shard_allocations")
        );
        assertThat(
            ACTION_ENABLE_INDEX_ROUTING_ALLOCATION.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:enable_index_allocations")
        );
        assertThat(
            ACTION_ENABLE_CLUSTER_ROUTING_ALLOCATION.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:enable_cluster_allocations")
        );
        assertThat(
            ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:increase_shard_limit_cluster_setting")
        );
        assertThat(
            ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:increase_shard_limit_index_setting")
        );
        assertThat(
            ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:migrate_data_tiers_require_data")
        );
        assertThat(
            ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:migrate_data_tiers_include_data")
        );
        assertThat(
            ACTION_INCREASE_NODE_CAPACITY.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:increase_node_capacity_for_allocations")
        );
        assertThat(
            DIAGNOSIS_WAIT_FOR_INITIALIZATION.getUniqueId(),
            equalTo("elasticsearch:health:shards_availability:diagnosis:initializing_shards")
        );
        for (String tier : List.of("data_content", "data_hot", "data_warm", "data_cold", "data_frozen")) {
            assertThat(
                ACTION_ENABLE_TIERS_LOOKUP.get(tier).getUniqueId(),
                equalTo("elasticsearch:health:shards_availability:diagnosis:enable_data_tiers:tier:" + tier)
            );
            assertThat(
                ACTION_INCREASE_SHARD_LIMIT_INDEX_SETTING_LOOKUP.get(tier).getUniqueId(),
                equalTo("elasticsearch:health:shards_availability:diagnosis:increase_shard_limit_index_setting:tier:" + tier)
            );
            assertThat(
                ACTION_INCREASE_SHARD_LIMIT_CLUSTER_SETTING_LOOKUP.get(tier).getUniqueId(),
                equalTo("elasticsearch:health:shards_availability:diagnosis:increase_shard_limit_cluster_setting:tier:" + tier)
            );
            assertThat(
                ACTION_MIGRATE_TIERS_AWAY_FROM_REQUIRE_DATA_LOOKUP.get(tier).getUniqueId(),
                equalTo("elasticsearch:health:shards_availability:diagnosis:migrate_data_tiers_require_data:tier:" + tier)
            );
            assertThat(
                ACTION_MIGRATE_TIERS_AWAY_FROM_INCLUDE_DATA_LOOKUP.get(tier).getUniqueId(),
                equalTo("elasticsearch:health:shards_availability:diagnosis:migrate_data_tiers_include_data:tier:" + tier)
            );
            assertThat(
                ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(tier).getUniqueId(),
                equalTo("elasticsearch:health:shards_availability:diagnosis:increase_tier_capacity_for_allocations:tier:" + tier)
            );
        }
    }

    private HealthIndicatorResult createExpectedResult(
        HealthStatus status,
        String symptom,
        Map<String, Object> details,
        List<HealthIndicatorImpact> impacts,
        List<Diagnosis> diagnosisList
    ) {
        return new HealthIndicatorResult(
            NAME,
            status,
            symptom,
            new SimpleHealthIndicatorDetails(addDefaults(details)),
            impacts,
            diagnosisList
        );
    }

    private HealthIndicatorResult createExpectedTruncatedResult(HealthStatus status, String symptom, List<HealthIndicatorImpact> impacts) {
        return new HealthIndicatorResult(NAME, status, symptom, HealthIndicatorDetails.EMPTY, impacts, emptyList());
    }

    private static ClusterState createClusterStateWith(List<IndexRoutingTable> indexRoutes, List<NodeShutdown> nodeShutdowns) {
        List<IndexMetadata> indices = indexRoutes.stream()
            .map(
                table -> IndexMetadata.builder(table.getIndex().getName())
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
                    .numberOfShards(1)
                    .numberOfReplicas(table.size() - 1)
                    .build()
            )
            .collect(Collectors.toList());
        return createClusterStateWith(indices, indexRoutes, nodeShutdowns, List.of());
    }

    private static List<IndexMetadata> createIndexMetadataForIndexNameToPriorityMap(Map<String, Integer> indexNameToPriorityMap) {
        List<IndexMetadata> indexMetadataList = new ArrayList<>();
        if (indexNameToPriorityMap != null) {
            for (Map.Entry<String, Integer> indexNameToPriority : indexNameToPriorityMap.entrySet()) {
                String indexName = indexNameToPriority.getKey();
                IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
                Settings settings = Settings.builder()
                    .put(IndexMetadata.SETTING_PRIORITY, indexNameToPriority.getValue())
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                    .build();
                indexMetadataBuilder.settings(settings);
                indexMetadataList.add(indexMetadataBuilder.build());

            }
        }
        return indexMetadataList;
    }

    private static ClusterState createClusterStateWith(
        List<IndexMetadata> indexMetadataList,
        List<IndexRoutingTable> indexRoutingTables,
        List<NodeShutdown> nodeShutdowns,
        List<DiscoveryNode> nodes
    ) {
        var routingTableBuilder = RoutingTable.builder();
        for (IndexRoutingTable indexRoutingTable : indexRoutingTables) {
            routingTableBuilder.add(indexRoutingTable);
        }

        var nodesShutdownMetadata = new NodesShutdownMetadata(
            nodeShutdowns.stream()
                .collect(
                    toMap(
                        it -> it.nodeId,
                        it -> SingleNodeShutdownMetadata.builder()
                            .setNodeId(it.nodeId)
                            .setType(it.type)
                            .setReason("test")
                            .setNodeSeen(true)
                            .setStartedAtMillis(System.currentTimeMillis())
                            .setAllocationDelay(it.allocationDelaySeconds != null ? timeValueSeconds(it.allocationDelaySeconds) : null)
                            .build()
                    )
                )
        );
        Metadata.Builder metadataBuilder = Metadata.builder();
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        for (IndexMetadata indexMetadata : indexMetadataList) {
            indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
        }
        metadataBuilder.indices(indexMetadataMap);
        metadataBuilder.putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata);

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        nodes.forEach(discoveryNodesBuilder::add);

        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTableBuilder.build())
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder.build())
            .build();
    }

    private static Map<String, Object> addDefaults(Map<String, Object> override) {
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
            "restarting_replicas",
            override.getOrDefault("restarting_replicas", 0),
            "started_replicas",
            override.getOrDefault("started_replicas", 0)
        );
    }

    private static IndexRoutingTable index(String name, ShardAllocation primaryState, ShardAllocation... replicaStates) {
        return index(
            IndexMetadata.builder(name)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
                .numberOfShards(1)
                .numberOfReplicas(replicaStates.length)
                .build(),
            primaryState,
            replicaStates
        );
    }

    private static IndexRoutingTable index(IndexMetadata indexMetadata, ShardAllocation primaryState, ShardAllocation... replicaStates) {
        var index = indexMetadata.getIndex();
        var shardId = new ShardId(index, 0);

        var builder = IndexRoutingTable.builder(index);
        builder.addShard(createShardRouting(shardId, true, primaryState));
        for (var replicaState : replicaStates) {
            builder.addShard(createShardRouting(shardId, false, replicaState));
        }
        return builder.build();
    }

    private static ShardRouting createShardRouting(ShardId shardId, boolean primary, ShardAllocation allocation) {
        var routing = newUnassigned(
            shardId,
            primary,
            getSource(primary, allocation.state),
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        );
        if (allocation.state == CREATING) {
            return routing;
        }
        routing = routing.initialize(allocation.nodeId, null, 0);
        if (allocation.state == INITIALIZING) {
            return routing;
        }
        routing = routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        if (allocation.state == AVAILABLE) {
            return routing;
        }
        if (allocation.state == UNAVAILABLE) {
            return routing.moveToUnassigned(Optional.ofNullable(allocation.unassignedInfo).orElse(randomFrom(nodeLeft(), decidersNo())));
        }
        if (allocation.state == RESTARTING) {
            return routing.moveToUnassigned(
                new UnassignedInfo(
                    UnassignedInfo.Reason.NODE_RESTARTING,
                    null,
                    null,
                    -1,
                    allocation.unassignedTimeNanos != null ? allocation.unassignedTimeNanos : 0,
                    0,
                    false,
                    UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                    Set.of(),
                    allocation.nodeId
                )
            );
        }

        throw new AssertionError("Unexpected state [" + allocation.state + "]");
    }

    private static RecoverySource getSource(boolean primary, ShardState state) {
        if (primary) {
            return state == CREATING
                ? RecoverySource.EmptyStoreRecoverySource.INSTANCE
                : RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }
    }

    public enum ShardState {
        UNAVAILABLE,
        CREATING,
        AVAILABLE,
        RESTARTING,
        INITIALIZING,
    }

    private record ShardAllocation(String nodeId, ShardState state, Long unassignedTimeNanos, @Nullable UnassignedInfo unassignedInfo) {

        ShardAllocation(String nodeId, ShardState state) {
            this(nodeId, state, null, null);
        }

        ShardAllocation(String nodeId, ShardState state, Long unassignedTimeNanos) {
            this(nodeId, state, unassignedTimeNanos, null);
        }

        ShardAllocation(String nodeId, ShardState state, UnassignedInfo unassignedInfo) {
            this(nodeId, state, null, unassignedInfo);
        }
    }

    private record NodeShutdown(String nodeId, SingleNodeShutdownMetadata.Type type, Integer allocationDelaySeconds) {}

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private static UnassignedInfo noShardCopy() {
        return new UnassignedInfo(
            randomBoolean() ? UnassignedInfo.Reason.NODE_LEFT : UnassignedInfo.Reason.CLUSTER_RECOVERED,
            null,
            null,
            0,
            0,
            0,
            false,
            UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY,
            Collections.emptySet(),
            null
        );
    }

    private static UnassignedInfo nodeLeft() {
        return new UnassignedInfo(
            UnassignedInfo.Reason.NODE_LEFT,
            null,
            null,
            0,
            0,
            0,
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            Collections.emptySet(),
            null
        );
    }

    private static UnassignedInfo decidersNo() {
        return new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            null,
            null,
            1,
            0,
            0,
            false,
            UnassignedInfo.AllocationStatus.DECIDERS_NO,
            Collections.emptySet(),
            null
        );
    }

    private record ShardRoutingKey(String index, int shard, boolean primary) {}

    private static ShardsAvailabilityHealthIndicatorService createShardsAvailabilityIndicatorService() {
        return createShardsAvailabilityIndicatorService(ClusterState.EMPTY_STATE, Collections.emptyMap());
    }

    private static ShardsAvailabilityHealthIndicatorService createShardsAvailabilityIndicatorService(ClusterState clusterState) {
        return createShardsAvailabilityIndicatorService(clusterState, Collections.emptyMap());
    }

    private static ShardsAvailabilityHealthIndicatorService createShardsAvailabilityIndicatorService(
        ClusterState clusterState,
        final Map<ShardRoutingKey, ShardAllocationDecision> decisions
    ) {
        return createAllocationHealthIndicatorService(Settings.EMPTY, clusterState, decisions, new SystemIndices(List.of()));
    }

    private static ShardsAvailabilityHealthIndicatorService createShardsAvailabilityIndicatorService(
        Settings nodeSettings,
        ClusterState clusterState,
        final Map<ShardRoutingKey, ShardAllocationDecision> decisions
    ) {
        return createAllocationHealthIndicatorService(nodeSettings, clusterState, decisions, new SystemIndices(List.of()));
    }

    private static ShardsAvailabilityHealthIndicatorService createAllocationHealthIndicatorService(
        Settings nodeSettings,
        ClusterState clusterState,
        final Map<ShardRoutingKey, ShardAllocationDecision> decisions,
        SystemIndices systemIndices
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        var allocationService = mock(AllocationService.class);
        when(allocationService.explainShardAllocation(any(), any())).thenAnswer((Answer<ShardAllocationDecision>) invocation -> {
            ShardRouting shardRouting = invocation.getArgument(0);
            var key = new ShardRoutingKey(shardRouting.getIndexName(), shardRouting.getId(), shardRouting.primary());
            return decisions.getOrDefault(key, ShardAllocationDecision.NOT_TAKEN);
        });
        return new ShardsAvailabilityHealthIndicatorService(clusterService, allocationService, systemIndices);
    }
}
