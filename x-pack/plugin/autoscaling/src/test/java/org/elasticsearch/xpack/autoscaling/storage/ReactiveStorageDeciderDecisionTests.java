/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySortedSet;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.cluster.routing.ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE;
import static org.elasticsearch.common.util.set.Sets.haveNonEmptyIntersection;
import static org.elasticsearch.xpack.autoscaling.storage.ReactiveStorageDeciderService.AllocationState.MAX_AMOUNT_OF_SHARD_DECISIONS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Test the higher level parts of {@link ReactiveStorageDeciderService} that all require a similar setup.
 */
public class ReactiveStorageDeciderDecisionTests extends AutoscalingTestCase {
    private static final Logger logger = LogManager.getLogger(ReactiveStorageDeciderDecisionTests.class);

    private static final AllocationDecider CAN_ALLOCATE_NO_DECIDER = new AllocationDecider() {
        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NO;
        }
    };
    private static final AllocationDecider CAN_REMAIN_NO_DECIDER = new AllocationDecider() {
        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return Decision.NO;
        }
    };
    private static final BalancedShardsAllocator SHARDS_ALLOCATOR = new BalancedShardsAllocator(Settings.EMPTY);
    private static final DiskThresholdSettings DISK_THRESHOLD_SETTINGS = new DiskThresholdSettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );

    private ClusterState state;
    private final int hotNodes = randomIntBetween(1, 8);
    private final int warmNodes = randomIntBetween(1, 3);
    // these are the shards that the decider tests work on
    private Set<ShardId> subjectShards;
    // say NO with disk label for subject shards
    private final AllocationDecider mockCanAllocateDiskDecider = new AllocationDecider() {
        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) {
                return allocation.decision(Decision.NO, DiskThresholdDecider.NAME, "test");
            }
            return super.canAllocate(shardRouting, node, allocation);
        }
    };
    // say NO with disk label for subject shards
    private final AllocationDecider mockCanRemainDiskDecider = new AllocationDecider() {
        @Override
        public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) return allocation.decision(
                Decision.NO,
                DiskThresholdDecider.NAME,
                "test"
            );
            return super.canRemain(indexMetadata, shardRouting, node, allocation);
        }
    };

    @Before
    public void setup() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        clusterState = addRandomIndices(hotNodes, hotNodes, clusterState);
        clusterState = addDataNodes(DATA_HOT_NODE_ROLE, "hot", clusterState, hotNodes);
        clusterState = addDataNodes(DATA_WARM_NODE_ROLE, "warm", clusterState, warmNodes);
        this.state = clusterState;

        Set<ShardId> shardIds = shardIds(clusterState.getRoutingNodes().unassigned());
        this.subjectShards = new HashSet<>(randomSubsetOf(randomIntBetween(1, shardIds.size()), shardIds));
    }

    public void testStoragePreventsAllocation() {
        ClusterState lastState = null;
        int maxRounds = state.getRoutingNodes().unassigned().size() + 3; // (allocated + start + detect-same)
        int round = 0;
        while (lastState != state && round < maxRounds) {
            var allocatableShards = allocatableShards();
            long numPrevents = allocatableShards.numOfShards();
            assert round != 0 || numPrevents > 0 : "must have shards that can be allocated on first round";

            SortedSet<ShardId> expectedShardIds = allocatableShards.shardIds();
            verify(
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                numPrevents,
                expectedShardIds,
                shardNodeDecisions -> {
                    if (numPrevents > 0) {
                        assertEquals(cappedShardIds(expectedShardIds), shardNodeDecisions.keySet());
                        assertEquals(
                            Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                            firstNoDecision(shardNodeDecisions.get(expectedShardIds.first()).canAllocateDecisions().get(0))
                        );
                        assertNull(shardNodeDecisions.get(expectedShardIds.first()).canRemainDecision());
                    } else {
                        assertTrue(shardNodeDecisions.isEmpty());
                    }
                    return true;
                },
                mockCanAllocateDiskDecider
            );
            verify(
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                0,
                emptySortedSet(),
                Map::isEmpty,
                mockCanAllocateDiskDecider,
                CAN_ALLOCATE_NO_DECIDER
            );
            verify(ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation, 0, emptySortedSet(), Map::isEmpty);
            // verify empty tier (no cold nodes) are always assumed a storage reason.
            SortedSet<ShardId> unassignedShardIds = state.getRoutingNodes()
                .unassigned()
                .stream()
                .map(ShardRouting::shardId)
                .collect(Collectors.toCollection(TreeSet::new));
            verify(
                moveToCold(allIndices()),
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                state.getRoutingNodes().unassigned().size(),
                unassignedShardIds,
                Map::isEmpty,
                DiscoveryNodeRole.DATA_COLD_NODE_ROLE
            );
            verify(
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                0,
                emptySortedSet(),
                Map::isEmpty,
                DiscoveryNodeRole.DATA_COLD_NODE_ROLE
            );
            if (numPrevents > 0) {
                verifyScale(numPrevents, "not enough storage available, needs " + numPrevents + "b", Map::isEmpty, shardIdNodeDecisions -> {
                    assertEquals(cappedShardIds(expectedShardIds), shardIdNodeDecisions.keySet());
                    assertEquals(
                        Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                        firstNoDecision(shardIdNodeDecisions.get(expectedShardIds.first()).canAllocateDecisions().get(0))
                    );
                    assertNull(shardIdNodeDecisions.get(expectedShardIds.first()).canRemainDecision());
                    return true;
                }, mockCanAllocateDiskDecider);
            } else {
                verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty, mockCanAllocateDiskDecider);
            }
            verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty, mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
            verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty);
            verifyScale(
                addDataNodes(DATA_HOT_NODE_ROLE, "additional", state, hotNodes),
                0,
                "storage ok",
                Map::isEmpty,
                Map::isEmpty,
                mockCanAllocateDiskDecider
            );
            lastState = state;
            startRandomShards();
            ++round;
        }
        assert round > 0;
        assertThat(state, sameInstance(lastState));
    }

    public void testStoragePreventsMove() {
        // this test moves shards to warm nodes and then checks that the reactive decider can calculate the storage necessary to move them
        // back to hot nodes.

        // allocate all primary shards
        allocate();

        // set of shards to force on to warm nodes.
        Set<ShardId> warmShards = Sets.union(new HashSet<>(randomSubsetOf(shardIds(state.getRoutingNodes().unassigned()))), subjectShards);

        // start (to be) warm shards. Only use primary shards for simplicity.
        withRoutingAllocation(
            allocation -> RoutingNodesHelper.shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING)
                .stream()
                .filter(ShardRouting::primary)
                .filter(s -> warmShards.contains(s.shardId()))
                .forEach(
                    shard -> allocation.routingNodes().startShard(logger, shard, allocation.changes(), UNAVAILABLE_EXPECTED_SHARD_SIZE)
                )
        );

        do {
            startRandomShards();
            // all of the relevant replicas are assigned too.
        } while (haveNonEmptyIntersection(shardIds(state.getRoutingNodes().unassigned()), warmShards)
            || haveNonEmptyIntersection(
                shardIds(RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.INITIALIZING)),
                warmShards
            ));

        // relocate warm shards to warm nodes and start them
        withRoutingAllocation(
            allocation -> RoutingNodesHelper.shardsWithState(allocation.routingNodes(), ShardRoutingState.STARTED)
                .stream()
                .filter(ShardRouting::primary)
                .filter(s -> warmShards.contains(s.shardId()))
                .forEach(
                    shard -> allocation.routingNodes()
                        .startShard(
                            logger,
                            allocation.routingNodes()
                                .relocateShard(
                                    shard,
                                    randomNodeId(allocation.routingNodes(), DATA_WARM_NODE_ROLE),
                                    0L,
                                    allocation.changes()
                                )
                                .v2(),
                            allocation.changes(),
                            UNAVAILABLE_EXPECTED_SHARD_SIZE
                        )
                )
        );

        SortedSet<ShardId> expectedShardIds = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)
            .stream()
            .map(ShardRouting::shardId)
            .filter(subjectShards::contains)
            .collect(Collectors.toCollection(TreeSet::new));
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            subjectShards.size(),
            expectedShardIds,
            shardIdNodeDecisions -> {
                assertEquals(cappedShardIds(expectedShardIds), shardIdNodeDecisions.keySet());
                assertEquals(
                    Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                    firstNoDecision(shardIdNodeDecisions.get(expectedShardIds.first()).canAllocateDecisions().get(0))
                );
                assertDebugNoDecision(shardIdNodeDecisions.get(expectedShardIds.first()).canRemainDecision().decision());
                return true;
            },
            mockCanAllocateDiskDecider
        );
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            emptySortedSet(),
            Map::isEmpty,
            mockCanAllocateDiskDecider,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0, emptySortedSet(), Map::isEmpty);

        verifyScale(subjectShards.size(), "not enough storage available, needs " + subjectShards.size() + "b", shardIdNodeDecisions -> {
            assertEquals(cappedShardIds(expectedShardIds), shardIdNodeDecisions.keySet());
            assertDebugNoDecision(shardIdNodeDecisions.get(expectedShardIds.first()).canRemainDecision().decision());
            assertEquals(
                Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                firstNoDecision(shardIdNodeDecisions.values().iterator().next().canAllocateDecisions().get(0))
            );
            return true;
        }, Map::isEmpty, mockCanAllocateDiskDecider);
        verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty, mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty);
        verifyScale(
            addDataNodes(DATA_HOT_NODE_ROLE, "additional", state, hotNodes),
            0,
            "storage ok",
            Map::isEmpty,
            Map::isEmpty,
            mockCanAllocateDiskDecider
        );
    }

    public void testMoveToEmpty() {
        // allocate all primary shards
        allocate();

        // start shards.
        withRoutingAllocation(
            allocation -> RoutingNodesHelper.shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING)
                .stream()
                .filter(ShardRouting::primary)
                .forEach(
                    shard -> allocation.routingNodes().startShard(logger, shard, allocation.changes(), UNAVAILABLE_EXPECTED_SHARD_SIZE)
                )
        );

        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            emptySortedSet(),
            Map::isEmpty,
            DiscoveryNodeRole.DATA_COLD_NODE_ROLE
        );

        Set<IndexMetadata> candidates = new HashSet<>(randomSubsetOf(allIndices()));
        int allocatedCandidateShards = candidates.stream().mapToInt(IndexMetadata::getNumberOfShards).sum();
        SortedSet<ShardId> allocatedShardIds = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)
            .stream()
            .filter(sr -> candidates.stream().anyMatch(c -> c.getIndex().equals(sr.index())))
            .map(ShardRouting::shardId)
            .collect(Collectors.toCollection(TreeSet::new));

        verify(
            moveToCold(candidates),
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            allocatedCandidateShards,
            allocatedShardIds,
            shardNodeDecisions -> {
                assertEquals(cappedShardIds(allocatedShardIds), shardNodeDecisions.keySet());
                if (allocatedShardIds.size() > 0) {
                    NodeDecisions nodeDecisions = shardNodeDecisions.get(allocatedShardIds.first());
                    assertTrue(nodeDecisions.canAllocateDecisions().isEmpty());
                    assertNotNull(nodeDecisions.canRemainDecision());
                }
                return true;
            },
            DiscoveryNodeRole.DATA_COLD_NODE_ROLE
        );
    }

    private Set<IndexMetadata> allIndices() {
        return new HashSet<>(state.metadata().getIndices().values());
    }

    private ClusterState moveToCold(Set<IndexMetadata> candidates) {
        ClusterState.Builder stateBuilder = ClusterState.builder(state);
        Metadata.Builder builder = Metadata.builder(state.metadata());
        candidates.forEach(imd -> builder.put(moveToCold(imd), true));
        stateBuilder.metadata(builder);
        return stateBuilder.build();
    }

    private IndexMetadata moveToCold(IndexMetadata imd) {
        Settings.Builder builder = Settings.builder().put(imd.getSettings());
        overrideSetting(
            imd,
            builder,
            DataTier.TIER_PREFERENCE_SETTING,
            randomFrom(DataTier.DATA_COLD, DataTier.DATA_COLD + "," + DataTier.DATA_HOT)
        );
        return IndexMetadata.builder(imd).settings(builder).build();
    }

    private void overrideSetting(IndexMetadata imd, Settings.Builder builder, Setting<String> indexRoutingRequireSetting, String value) {
        if (Strings.hasText(indexRoutingRequireSetting.get(imd.getSettings()))) {
            builder.put(indexRoutingRequireSetting.getKey(), value);
        }
    }

    public void testStoragePreventsRemain() {
        allocate();
        // we can only decide on a move for started shards (due to for instance ThrottlingAllocationDecider assertion).
        for (int i = 0; i < randomIntBetween(1, 4) || hasStartedSubjectShard() == false; ++i) {
            startRandomShards();
        }

        // the remain check only assumes the smallest shard need to move off. More detailed testing of AllocationState.unmovableSize in
        // {@link ReactiveStorageDeciderServiceTests#testUnmovableSize}
        long nodes = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)
            .stream()
            .filter(s -> subjectShards.contains(s.shardId()))
            .map(ShardRouting::currentNodeId)
            .distinct()
            .count();
        SortedSet<ShardId> shardIds = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)
            .stream()
            .map(ShardRouting::shardId)
            .filter(o -> subjectShards.contains(o))
            .collect(Collectors.toCollection(TreeSet::new));

        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, nodes, shardIds, shardNodeDecisions -> {
            assertEquals(cappedShardIds(shardIds), shardNodeDecisions.keySet());
            List<NodeDecision> canAllocateDecisions = shardNodeDecisions.get(shardIds.first()).canAllocateDecisions();
            assertEquals(hotNodes - 1, canAllocateDecisions.size());
            if (canAllocateDecisions.size() > 0) {
                assertDebugNoDecision(canAllocateDecisions.get(0).decision());
            }

            assertEquals(
                Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                firstNoDecision(shardNodeDecisions.get(shardIds.first()).canRemainDecision())
            );
            return true;
        }, mockCanRemainDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            emptySortedSet(),
            Map::isEmpty,
            mockCanRemainDiskDecider,
            CAN_REMAIN_NO_DECIDER,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0, emptySortedSet(), Map::isEmpty);

        // only consider it once (move case) if both cannot remain and cannot allocate.
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, nodes, shardIds, shardNodeDecisions -> {
            assertEquals(cappedShardIds(shardIds), shardNodeDecisions.keySet());
            List<NodeDecision> canAllocateDecisions = shardNodeDecisions.get(shardIds.first()).canAllocateDecisions();
            assertEquals(hotNodes - 1, canAllocateDecisions.size());
            if (canAllocateDecisions.size() > 0) {
                assertDebugNoDecision(canAllocateDecisions.get(0).decision());
            }

            assertEquals(
                Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                firstNoDecision(shardNodeDecisions.get(shardIds.first()).canRemainDecision())
            );
            return true;
        }, mockCanAllocateDiskDecider, mockCanRemainDiskDecider);

        verifyScale(nodes, "not enough storage available, needs " + nodes + "b", shardNodeDecisions -> {
            assertEquals(cappedShardIds(shardIds), shardNodeDecisions.keySet());
            List<NodeDecision> canAllocateDecisions = shardNodeDecisions.get(shardIds.first()).canAllocateDecisions();
            assertEquals(hotNodes - 1, canAllocateDecisions.size());
            if (canAllocateDecisions.size() > 0) {
                assertDebugNoDecision(canAllocateDecisions.get(0).decision());
            }

            assertEquals(
                Decision.single(Decision.Type.NO, "disk_threshold", "test"),
                firstNoDecision(shardNodeDecisions.get(shardIds.first()).canRemainDecision())
            );
            return true;
        }, Map::isEmpty, mockCanRemainDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty, mockCanRemainDiskDecider, CAN_REMAIN_NO_DECIDER, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok", Map::isEmpty, Map::isEmpty);
    }

    private static void assertDebugNoDecision(Decision canAllocateDecision) {
        assertEquals(Decision.Type.NO, canAllocateDecision.type());
        assertTrue(canAllocateDecision.getDecisions().stream().anyMatch(d -> d.type() == Decision.Type.NO));
        assertTrue(canAllocateDecision.getDecisions().stream().anyMatch(d -> d.type() == Decision.Type.YES));
    }

    private static SortedSet<ShardId> cappedShardIds(SortedSet<ShardId> shardIds) {
        return shardIds.stream().limit(MAX_AMOUNT_OF_SHARD_DECISIONS).collect(Collectors.toCollection(TreeSet::new));
    }

    private static Decision firstNoDecision(NodeDecision nodeAllocationResult) {
        List<Decision> noDecisions = nodeAllocationResult.decision()
            .getDecisions()
            .stream()
            .filter(d -> d.type() == Decision.Type.NO)
            .toList();
        if (noDecisions.isEmpty()) {
            throw new IllegalStateException("Unable to find NO can_remain decision");
        }
        return noDecisions.get(0);
    }

    private interface VerificationSubject {
        ShardsAllocationResults invoke(ReactiveStorageDeciderService.AllocationState state);
    }

    private void verify(
        VerificationSubject subject,
        long expectedSizeInBytes,
        SortedSet<ShardId> expectedShardIds,
        Predicate<Map<ShardId, NodeDecisions>> nodeDecisionsCheck,
        AllocationDecider... allocationDeciders
    ) {
        verify(subject, expectedSizeInBytes, expectedShardIds, nodeDecisionsCheck, DATA_HOT_NODE_ROLE, allocationDeciders);
    }

    private void verify(
        VerificationSubject subject,
        long expectedSizeInBytes,
        SortedSet<ShardId> expectedShardIds,
        Predicate<Map<ShardId, NodeDecisions>> nodeDecisionsCheck,
        DiscoveryNodeRole role,
        AllocationDecider... allocationDeciders
    ) {
        verify(this.state, subject, expectedSizeInBytes, expectedShardIds, nodeDecisionsCheck, role, allocationDeciders);
    }

    private static void verify(
        ClusterState state,
        VerificationSubject subject,
        long expectedSizeInBytes,
        SortedSet<ShardId> expectedShardIds,
        Predicate<Map<ShardId, NodeDecisions>> nodeDecisionsCheck,
        DiscoveryNodeRole role,
        AllocationDecider... allocationDeciders
    ) {
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            createContext(state, Set.of(role)),
            DISK_THRESHOLD_SETTINGS,
            createAllocationDeciders(allocationDeciders),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        ShardsAllocationResults shardsAllocationResults = subject.invoke(allocationState);
        assertThat(shardsAllocationResults.sizeInBytes(), equalTo(expectedSizeInBytes));
        assertThat(shardsAllocationResults.shardIds(), equalTo(expectedShardIds));
        assertTrue("failed canAllocate decisions check", nodeDecisionsCheck.test(shardsAllocationResults.shardNodeDecisions()));
    }

    private void verifyScale(
        long expectedDifference,
        String reason,
        Predicate<Map<ShardId, NodeDecisions>> assignedNodeDecisions,
        Predicate<Map<ShardId, NodeDecisions>> unassignedNodeDecisions,
        AllocationDecider... allocationDeciders
    ) {
        verifyScale(state, expectedDifference, reason, assignedNodeDecisions, unassignedNodeDecisions, allocationDeciders);
    }

    private static void verifyScale(
        ClusterState state,
        long expectedDifference,
        String reason,
        Predicate<Map<ShardId, NodeDecisions>> assignedNodeDecisions,
        Predicate<Map<ShardId, NodeDecisions>> unassignedNodeDecisions,
        AllocationDecider... allocationDeciders
    ) {
        ReactiveStorageDeciderService decider = new ReactiveStorageDeciderService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            createAllocationDeciders(allocationDeciders),
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        TestAutoscalingDeciderContext context = createContext(state, Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        AutoscalingDeciderResult result = decider.scale(Settings.EMPTY, context);
        ReactiveStorageDeciderService.ReactiveReason resultReason = (ReactiveStorageDeciderService.ReactiveReason) result.reason();

        if (context.currentCapacity != null) {
            assertThat(
                result.requiredCapacity().total().storage().getBytes() - context.currentCapacity.total().storage().getBytes(),
                equalTo(expectedDifference)
            );
            assertThat(resultReason.summary(), equalTo(reason));
            assertThat(resultReason.unassignedShardIds(), equalTo(decider.allocationState(context).storagePreventsAllocation().shardIds()));
            assertThat(resultReason.assignedShardIds(), equalTo(decider.allocationState(context).storagePreventsRemainOrMove().shardIds()));
            assertTrue("failed assigned decisions check", assignedNodeDecisions.test(resultReason.assignedNodeDecisions()));
            assertTrue("failed unassigned decisions check", unassignedNodeDecisions.test(resultReason.unassignedNodeDecisions()));
        } else {
            assertThat(result.requiredCapacity(), is(nullValue()));
            assertThat(resultReason.summary(), equalTo("current capacity not available"));
            assertThat(resultReason.unassignedShardIds(), equalTo(Set.of()));
            assertThat(resultReason.assignedShardIds(), equalTo(Set.of()));
            assertThat(resultReason.unassignedNodeDecisions(), equalTo(Map.of()));
            assertThat(resultReason.assignedNodeDecisions(), equalTo(Map.of()));
        }
    }

    record AllocatableShards(long numOfShards, SortedSet<ShardId> shardIds) {}

    private AllocatableShards allocatableShards() {
        AllocationDeciders deciders = createAllocationDeciders();
        RoutingAllocation allocation = createRoutingAllocation(state, deciders);
        // There could be duplicated of shard ids, and numOfShards is calculated based on them,
        // so we can't just collect to the shard ids to `TreeSet`
        List<ShardRouting> allocatableShards = state.getRoutingNodes()
            .unassigned()
            .stream()
            .filter(shard -> subjectShards.contains(shard.shardId()))
            .filter(
                shard -> allocation.routingNodes().stream().anyMatch(node -> deciders.canAllocate(shard, node, allocation) != Decision.NO)
            )
            .toList();
        return new AllocatableShards(
            allocatableShards.size(),
            allocatableShards.stream().map(ShardRouting::shardId).collect(Collectors.toCollection(TreeSet::new))
        );
    }

    private boolean hasStartedSubjectShard() {
        return RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)
            .stream()
            .filter(ShardRouting::primary)
            .map(ShardRouting::shardId)
            .anyMatch(subjectShards::contains);
    }

    private static AllocationDeciders createAllocationDeciders(AllocationDecider... extraDeciders) {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder()
                .put(
                    ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                    Integer.MAX_VALUE
                )
                .build()
        );
        Collection<AllocationDecider> systemAllocationDeciders = ClusterModule.createAllocationDeciders(
            Settings.EMPTY,
            clusterSettings,
            Collections.emptyList()
        );
        return new AllocationDeciders(
            Stream.of(Stream.of(extraDeciders), Stream.of(DataTierAllocationDecider.INSTANCE), systemAllocationDeciders.stream())
                .flatMap(s -> s)
                .collect(Collectors.toList())
        );
    }

    private static RoutingAllocation createRoutingAllocation(ClusterState state, AllocationDeciders allocationDeciders) {
        return new RoutingAllocation(
            allocationDeciders,
            state.mutableRoutingNodes(),
            state,
            createClusterInfo(state),
            null,
            System.nanoTime()
        );
    }

    private void withRoutingAllocation(Consumer<RoutingAllocation> block) {
        RoutingAllocation allocation = createRoutingAllocation(state, createAllocationDeciders());
        block.accept(allocation);
        state = ReactiveStorageDeciderServiceTests.updateClusterState(state, allocation);
    }

    private void allocate() {
        withRoutingAllocation(SHARDS_ALLOCATOR::allocate);
    }

    private void startRandomShards() {
        withRoutingAllocation(allocation -> {
            List<ShardRouting> initializingShards = RoutingNodesHelper.shardsWithState(
                allocation.routingNodes(),
                ShardRoutingState.INITIALIZING
            );
            List<ShardRouting> shards = randomSubsetOf(Math.min(randomIntBetween(1, 100), initializingShards.size()), initializingShards);

            // replicas before primaries, since replicas can be reinit'ed, resulting in a new ShardRouting instance.
            shards.stream()
                .filter(not(ShardRouting::primary))
                .forEach(s -> allocation.routingNodes().startShard(logger, s, allocation.changes(), UNAVAILABLE_EXPECTED_SHARD_SIZE));
            shards.stream()
                .filter(ShardRouting::primary)
                .forEach(s -> allocation.routingNodes().startShard(logger, s, allocation.changes(), UNAVAILABLE_EXPECTED_SHARD_SIZE));
            SHARDS_ALLOCATOR.allocate(allocation);

            // ensure progress by only relocating a shard if we started more than one shard.
            if (shards.size() > 1 && randomBoolean()) {
                List<ShardRouting> started = RoutingNodesHelper.shardsWithState(allocation.routingNodes(), ShardRoutingState.STARTED);
                if (started.isEmpty() == false) {
                    ShardRouting toMove = randomFrom(started);
                    Set<RoutingNode> candidates = allocation.routingNodes()
                        .stream()
                        .filter(n -> allocation.deciders().canAllocate(toMove, n, allocation) == Decision.YES)
                        .collect(toSet());
                    if (candidates.isEmpty() == false) {
                        allocation.routingNodes().relocateShard(toMove, randomFrom(candidates).nodeId(), 0L, allocation.changes());
                    }
                }
            }
        });
    }

    private static TestAutoscalingDeciderContext createContext(ClusterState state, Set<DiscoveryNodeRole> roles) {
        return new TestAutoscalingDeciderContext(state, roles, randomCurrentCapacity());
    }

    static AutoscalingCapacity randomCurrentCapacity() {
        if (randomInt(4) > 0) {
            // we only rely on storage.
            boolean includeMemory = randomBoolean();
            boolean includeProcessors = randomBoolean();
            return AutoscalingCapacity.builder()
                .total(randomByteSizeValue(), includeMemory ? randomByteSizeValue() : null, includeProcessors ? randomProcessors() : null)
                .node(randomByteSizeValue(), includeMemory ? randomByteSizeValue() : null, includeProcessors ? randomProcessors() : null)
                .build();
        } else {
            return null;
        }
    }

    private static class TestAutoscalingDeciderContext implements AutoscalingDeciderContext {
        private final ClusterState state;
        private final AutoscalingCapacity currentCapacity;
        private final Set<DiscoveryNode> nodes;
        private final ClusterInfo info;
        private final Set<DiscoveryNodeRole> roles;

        private TestAutoscalingDeciderContext(ClusterState state, Set<DiscoveryNodeRole> roles, AutoscalingCapacity currentCapacity) {
            this.state = state;
            this.currentCapacity = currentCapacity;
            this.nodes = state.nodes().stream().filter(n -> roles.stream().anyMatch(n.getRoles()::contains)).collect(toSet());
            this.roles = roles;
            this.info = createClusterInfo(state);
        }

        @Override
        public ClusterState state() {
            return state;
        }

        @Override
        public AutoscalingCapacity currentCapacity() {
            return currentCapacity;
        }

        @Override
        public Set<DiscoveryNode> nodes() {
            return nodes;
        }

        @Override
        public Set<DiscoveryNodeRole> roles() {
            return roles;
        }

        @Override
        public ClusterInfo info() {
            return info;
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizeInfo() {
            return null;
        }

        @Override
        public void ensureNotCancelled() {

        }
    }

    private static ClusterInfo createClusterInfo(ClusterState state) {
        // we make a simple setup to detect the right decisions are made. The unmovable calculation is tested in more detail elsewhere.
        // the diskusage is set such that the disk threshold decider never rejects an allocation.
        Map<String, DiskUsage> diskUsages = state.nodes()
            .stream()
            .collect(toUnmodifiableMap(DiscoveryNode::getId, node -> new DiskUsage(node.getId(), null, "the_path", 1000, 1000)));

        return new ClusterInfo() {
            @Override
            public Map<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
                return diskUsages;
            }

            @Override
            public Map<String, DiskUsage> getNodeMostAvailableDiskUsages() {
                return diskUsages;
            }

            @Override
            public String getDataPath(ShardRouting shardRouting) {
                return "the_path";
            }

            @Override
            public long getShardSize(ShardRouting shardRouting, long defaultValue) {
                return 1L;
            }

            @Override
            public Long getShardSize(ShardRouting shardRouting) {
                return 1L;
            }
        };
    }

    private static ClusterState addRandomIndices(int minShards, int maxShardCopies, ClusterState state) {
        String[] tierSettingNames = new String[] { DataTier.TIER_PREFERENCE };
        int shards = randomIntBetween(minShards, 20);
        Metadata.Builder builder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        while (shards > 0) {
            IndexMetadata indexMetadata = IndexMetadata.builder("test" + "-" + shards)
                .settings(settings(Version.CURRENT).put(randomFrom(tierSettingNames), "data_hot"))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, maxShardCopies - 1))
                .build();

            builder.put(indexMetadata, false);
            routingTableBuilder.addAsNew(indexMetadata);
            shards -= indexMetadata.getNumberOfShards() * (indexMetadata.getNumberOfReplicas() + 1);
        }

        return ClusterState.builder(state).metadata(builder).routingTable(routingTableBuilder.build()).build();
    }

    static ClusterState addDataNodes(DiscoveryNodeRole role, String prefix, ClusterState state, int nodes) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(state.nodes());
        IntStream.range(0, nodes).mapToObj(i -> newDataNode(role, prefix + "_" + i)).forEach(builder::add);
        return ClusterState.builder(state).nodes(builder).build();
    }

    static DiscoveryNode newDataNode(DiscoveryNodeRole role, String nodeName) {
        return DiscoveryNodeUtils.builder(nodeName).name(nodeName).externalId(UUIDs.randomBase64UUID()).roles(Set.of(role)).build();
    }

    private static String randomNodeId(RoutingNodes routingNodes, DiscoveryNodeRole role) {
        return randomFrom(routingNodes.stream().map(RoutingNode::node).filter(n -> n.getRoles().contains(role)).collect(toSet())).getId();
    }

    private static Set<ShardId> shardIds(Iterable<ShardRouting> candidateShards) {
        return StreamSupport.stream(candidateShards.spliterator(), false).map(ShardRouting::shardId).collect(toSet());
    }
}
