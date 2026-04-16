/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.health;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessShardsAvailabilityHealthIndicatorServiceTests extends ESTestCase {

    private static final String INDEX_NAME = "test-index";

    private static final List<UnassignedInfo.Reason> TRANSIENT_UNASSIGNED_REASONS = Arrays.stream(UnassignedInfo.Reason.values())
        .filter(UnassignedInfo.Reason::isExpectedTransient)
        .toList();

    public void testHealthWhileReplicaShardsUnavailableWithReplicaBuffer() {
        final var projectId = randomProjectIdOrDefault();
        final int totalReplicas = randomIntBetween(1, 5);
        final int unassignedReplicaCount = randomIntBetween(0, totalReplicas - 1);
        final int assignedReplicaCount = totalReplicas - unassignedReplicaCount;

        int expectedProvisionallyUnassignedReplicas = 0;
        int expectedUnassignedReplicas = 0;

        final long nowMillis = System.currentTimeMillis();
        final var withinReplicaBuffer = new TimeValue(nowMillis + TimeValue.timeValueSeconds(30).millis(), TimeUnit.MILLISECONDS);
        final var beyondReplicaBuffer = new TimeValue(nowMillis - TimeValue.timeValueSeconds(30).millis(), TimeUnit.MILLISECONDS);

        final var assignedReplicas = new HashSet<String>();
        while (assignedReplicas.size() < assignedReplicaCount) {
            assignedReplicas.add(randomNodeId());
        }
        final var unassignedReplicas = new LinkedList<UnassignedInfo>();
        for (int i = 0; i < unassignedReplicaCount; i++) {
            final boolean withinBufferForThisReplica = randomBoolean();
            if (withinBufferForThisReplica) {
                expectedProvisionallyUnassignedReplicas++;
            } else {
                expectedUnassignedReplicas++;
            }
            final var reason = randomFrom(TRANSIENT_UNASSIGNED_REASONS);
            final var unassignedAt = withinBufferForThisReplica ? withinReplicaBuffer : beyondReplicaBuffer;
            unassignedReplicas.add(unassignedInfo(reason, unassignedAt));
        }

        final var state = clusterState(projectId, routingTableForIndex(randomNodeId(), assignedReplicas, unassignedReplicas));
        final var service = createStatelessIndicator(
            projectId,
            Settings.builder().put(ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_BUFFER_TIME.getKey(), "20s").build(),
            state
        );

        final var result = service.calculate(true, 10, HealthInfo.EMPTY_HEALTH_INFO);
        final var details = ((SimpleHealthIndicatorDetails) result.details()).details();

        assertThat(details.get("indices_with_unavailable_primaries"), nullValue());
        assertThat(details.get("indices_with_provisionally_unavailable_primaries"), nullValue());

        assertThat(details.get("creating_replicas"), equalTo(expectedProvisionallyUnassignedReplicas));
        assertThat(details.get("unassigned_replicas"), equalTo(expectedUnassignedReplicas));
        assertThat(details.get("started_replicas"), equalTo(assignedReplicaCount));

        if (unassignedReplicaCount == 0) {
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
            assertThat(details.get("indices_with_unavailable_replicas"), nullValue());
            assertThat(details.get("indices_with_provisionally_unavailable_replicas"), nullValue());
        } else if (expectedUnassignedReplicas == 0) {
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
            assertThat(details.get("indices_with_unavailable_replicas"), nullValue());
            assertThat(details.get("indices_with_provisionally_unavailable_replicas"), equalTo(INDEX_NAME));
        } else {
            assertThat(result.status(), equalTo(HealthStatus.YELLOW));
            assertThat(details.get("indices_with_unavailable_replicas"), equalTo(INDEX_NAME));
            if (expectedProvisionallyUnassignedReplicas > 0) {
                assertThat(details.get("indices_with_provisionally_unavailable_replicas"), equalTo(INDEX_NAME));
            } else {
                assertThat(details.get("indices_with_provisionally_unavailable_replicas"), nullValue());
            }
        }
    }

    public void testHealthWhilePrimaryUnassignedWithPrimaryBuffer() {
        final var projectId = randomProjectIdOrDefault();
        final long nowMillis = System.currentTimeMillis();
        final var withinPrimaryBuffer = new TimeValue(nowMillis - TimeValue.timeValueSeconds(5).millis(), TimeUnit.MILLISECONDS);
        final var beyondPrimaryBuffer = new TimeValue(nowMillis - TimeValue.timeValueSeconds(60).millis(), TimeUnit.MILLISECONDS);

        final var primaryUnassignedInfo = randomUnassignedInfo(withinPrimaryBuffer, beyondPrimaryBuffer);

        final var state = clusterState(
            projectId,
            routingTableWithUnassigned(primaryUnassignedInfo, recoverySourceFrom(primaryUnassignedInfo), Set.of())
        );
        final var service = createStatelessIndicator(
            projectId,
            Settings.builder()
                .put(ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_BUFFER_TIME.getKey(), "20s")
                .put(ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_BUFFER_TIME.getKey(), "0s")
                .build(),
            state
        );

        final var result = service.calculate(true, 10, HealthInfo.EMPTY_HEALTH_INFO);
        final var details = ((SimpleHealthIndicatorDetails) result.details()).details();

        if (expectProvisionallyGreen(primaryUnassignedInfo, withinPrimaryBuffer, true)) {
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
            assertThat(details.get("creating_primaries"), equalTo(1));
            assertThat(details.get("unassigned_primaries"), equalTo(0));
            assertThat(details.get("indices_with_provisionally_unavailable_primaries"), equalTo(INDEX_NAME));
            assertThat(details.get("indices_with_unavailable_primaries"), nullValue());
        } else {
            assertThat(result.status(), equalTo(HealthStatus.RED));
            assertThat(details.get("creating_primaries"), equalTo(0));
            assertThat(details.get("unassigned_primaries"), equalTo(1));
            assertThat(details.get("indices_with_unavailable_primaries"), equalTo(INDEX_NAME));
            assertThat(details.get("indices_with_provisionally_unavailable_primaries"), nullValue());
        }
    }

    public void testHealthWhilePrimaryAndReplicaUnassignedWithBuffer() {
        final var projectId = randomProjectIdOrDefault();
        final long nowMillis = System.currentTimeMillis();
        final var withinBuffer = new TimeValue(nowMillis + TimeValue.timeValueSeconds(5).millis(), TimeUnit.MILLISECONDS);
        final var beyondBuffer = new TimeValue(nowMillis - TimeValue.timeValueSeconds(60).millis(), TimeUnit.MILLISECONDS);

        final var primaryInfo = randomUnassignedInfo(withinBuffer, beyondBuffer);
        final var replicaInfo = randomUnassignedInfo(withinBuffer, beyondBuffer);

        final var state = clusterState(
            projectId,
            routingTableWithUnassigned(primaryInfo, recoverySourceFrom(primaryInfo), Set.of(replicaInfo))
        );
        final var service = createStatelessIndicator(
            projectId,
            Settings.builder()
                .put(ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_BUFFER_TIME.getKey(), "20s")
                .put(ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_BUFFER_TIME.getKey(), "20s")
                .build(),
            state
        );

        final var result = service.calculate(true, 10, HealthInfo.EMPTY_HEALTH_INFO);
        final var details = ((SimpleHealthIndicatorDetails) result.details()).details();

        final boolean primaryProvisional = expectProvisionallyGreen(primaryInfo, withinBuffer, true);
        final boolean replicaNew = primaryInfo.reason() == UnassignedInfo.Reason.INDEX_CREATED && primaryProvisional;
        final boolean replicaProvisional = expectProvisionallyGreen(replicaInfo, withinBuffer, false) || replicaNew;

        assertThat(result.status(), equalTo(primaryProvisional && replicaProvisional ? HealthStatus.GREEN : HealthStatus.RED));

        if (primaryProvisional) {
            assertThat(details.get("creating_primaries"), equalTo(1));
            assertThat(details.get("unassigned_primaries"), equalTo(0));
            assertThat(details.get("indices_with_provisionally_unavailable_primaries"), equalTo(INDEX_NAME));
            assertThat(details.get("indices_with_unavailable_primaries"), nullValue());
        } else {
            assertThat(details.get("creating_primaries"), equalTo(0));
            assertThat(details.get("unassigned_primaries"), equalTo(1));
            assertThat(details.get("indices_with_unavailable_primaries"), equalTo(INDEX_NAME));
            assertThat(details.get("indices_with_provisionally_unavailable_primaries"), nullValue());
        }
        if (replicaProvisional) {
            assertThat(details.get("started_replicas"), equalTo(0));
            assertThat(details.get("creating_replicas"), equalTo(1));
            assertThat(details.get("unassigned_replicas"), equalTo(0));
            assertThat(details.get("indices_with_provisionally_unavailable_replicas"), equalTo(INDEX_NAME));
            assertThat(details.get("indices_with_unavailable_replicas"), nullValue());
        } else {
            assertThat(details.get("started_replicas"), equalTo(0));
            assertThat(details.get("creating_replicas"), equalTo(0));
            assertThat(details.get("unassigned_replicas"), equalTo(1));
            assertThat(details.get("indices_with_provisionally_unavailable_replicas"), nullValue());
            assertThat(details.get("indices_with_unavailable_replicas"), equalTo(INDEX_NAME));
        }
    }

    private static UnassignedInfo randomUnassignedInfo(TimeValue withinBuffer, TimeValue beyondBuffer) {
        if (randomBoolean()) {
            return unassignedInfo(
                UnassignedInfo.Reason.INDEX_CREATED,
                withinBuffer,
                randomBoolean() ? UnassignedInfo.AllocationStatus.DECIDERS_NO : UnassignedInfo.AllocationStatus.NO_ATTEMPT
            );
        }
        return unassignedInfo(
            randomFrom(UnassignedInfo.Reason.values()),
            randomBoolean() ? withinBuffer : beyondBuffer,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT
        );
    }

    private static RecoverySource recoverySourceFrom(UnassignedInfo unassignedInfo) {
        return unassignedInfo.reason() == UnassignedInfo.Reason.INDEX_CREATED
            ? RecoverySource.EmptyStoreRecoverySource.INSTANCE
            : RecoverySource.ExistingStoreRecoverySource.INSTANCE;
    }

    private static boolean expectProvisionallyGreen(UnassignedInfo unassignedInfo, TimeValue refWithinBuffer, boolean primary) {
        if (unassignedInfo.lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
            return false;
        }
        if (unassignedInfo.reason() == UnassignedInfo.Reason.INDEX_CREATED && primary) {
            return true;
        }
        if (unassignedInfo.reason().isExpectedTransient() == false) {
            return false;
        }
        return unassignedInfo.unassignedTimeMillis() == refWithinBuffer.getMillis();
    }

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private static UnassignedInfo unassignedInfo(UnassignedInfo.Reason reason, TimeValue unassignedTime) {
        return unassignedInfo(reason, unassignedTime, UnassignedInfo.AllocationStatus.NO_ATTEMPT);
    }

    private static UnassignedInfo unassignedInfo(
        UnassignedInfo.Reason reason,
        TimeValue unassignedTime,
        UnassignedInfo.AllocationStatus lastAllocationStatus
    ) {
        // ALLOCATION_FAILED requires failedAllocations > 0
        final int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        // NODE_RESTARTING requires a non-null lastAllocatedNodeId
        final var lastAllocatedNodeId = reason == UnassignedInfo.Reason.NODE_RESTARTING ? randomNodeId() : null;
        return new UnassignedInfo(
            reason,
            null,
            null,
            failedAllocations,
            unassignedTime.nanos(),
            unassignedTime.millis(),
            false,
            lastAllocationStatus,
            Collections.emptySet(),
            lastAllocatedNodeId
        );
    }

    private static ClusterState clusterState(ProjectId projectId, IndexRoutingTable indexRouting) {
        final var indexMetadata = IndexMetadata.builder(indexRouting.getIndex().getName())
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(indexRouting.size() - 1)
            .build();

        final var projectMetadata = ProjectMetadata.builder(projectId).put(indexMetadata, false).build();
        final var metadata = Metadata.builder().put(projectMetadata).build();
        final var routingTable = RoutingTable.builder().add(indexRouting).build();
        final var globalRouting = GlobalRoutingTable.builder().put(projectId, routingTable).build();

        return ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(metadata)
            .routingTable(globalRouting)
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    /// Builds an `IndexRoutingTable` with an unassigned primary and zero or more unassigned replicas.
    private static IndexRoutingTable routingTableWithUnassigned(
        UnassignedInfo primaryUnassignedInfo,
        RecoverySource recoverySource,
        Set<UnassignedInfo> replicasUnassignedInfo
    ) {
        final var indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(replicasUnassignedInfo.size())
            .build();
        final var idx = indexMetadata.getIndex();
        final var shardId = new ShardId(idx, 0);
        final var primary = newUnassigned(shardId, true, recoverySource, primaryUnassignedInfo, ShardRouting.Role.DEFAULT);
        final var indexRoutingTable = IndexRoutingTable.builder(idx).addShard(primary);
        replicasUnassignedInfo.forEach(
            info -> indexRoutingTable.addShard(
                newUnassigned(shardId, false, RecoverySource.PeerRecoverySource.INSTANCE, info, ShardRouting.Role.DEFAULT)
            )
        );
        return indexRoutingTable.build();
    }

    private static IndexRoutingTable routingTableForIndex(
        String primaryNodeId,
        Set<String> assignedReplicasNodes,
        List<UnassignedInfo> unassignedReplicaInfos
    ) {
        final var indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(assignedReplicasNodes.size() + unassignedReplicaInfos.size())
            .build();
        final var idx = indexMetadata.getIndex();
        final var shardId = new ShardId(idx, 0);
        final var builder = IndexRoutingTable.builder(idx);
        builder.addShard(shardRouting(shardId, true, primaryNodeId, null));
        for (var replicaNode : assignedReplicasNodes) {
            builder.addShard(shardRouting(shardId, false, replicaNode, null));
        }
        for (var unassignedInfo : unassignedReplicaInfos) {
            builder.addShard(shardRouting(shardId, false, randomNodeId(), unassignedInfo));
        }
        return builder.build();
    }

    /// If unassignedInfo is `null`, the shard is started on `nodeId`. Otherwise, it is unassigned with the provided info
    private static ShardRouting shardRouting(ShardId shardId, boolean primary, String nodeId, @Nullable UnassignedInfo unassignedInfo) {
        final var initialInfo = unassignedInfo != null ? unassignedInfo : new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
        final var recoverySource = primary
            ? RecoverySource.ExistingStoreRecoverySource.INSTANCE
            : RecoverySource.PeerRecoverySource.INSTANCE;

        var routing = newUnassigned(shardId, primary, recoverySource, initialInfo, ShardRouting.Role.DEFAULT);
        if (unassignedInfo != null
            && (unassignedInfo.reason() == UnassignedInfo.Reason.INDEX_CREATED
                || unassignedInfo.reason() == UnassignedInfo.Reason.REPLICA_ADDED)) {
            return routing;
        }
        routing = routing.initialize(nodeId, null, 0);
        routing = routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        if (unassignedInfo == null) {
            return routing;
        }
        return routing.moveToUnassigned(unassignedInfo);
    }

    private static StatelessShardsAvailabilityHealthIndicatorService createStatelessIndicator(
        ProjectId projectId,
        Settings nodeSettings,
        ClusterState clusterState
    ) {
        final var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        final var allocationService = mock(AllocationService.class);
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
            TestProjectResolvers.singleProjectOnly(projectId)
        );
    }
}
