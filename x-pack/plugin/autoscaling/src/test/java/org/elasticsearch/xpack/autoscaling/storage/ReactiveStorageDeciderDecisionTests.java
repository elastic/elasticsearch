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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.common.collect.ImmutableOpenMap;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
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
        public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
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
        public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (subjectShards.contains(shardRouting.shardId()) && node.node().getName().startsWith("hot")) return allocation.decision(
                Decision.NO,
                DiskThresholdDecider.NAME,
                "test"
            );
            return super.canRemain(shardRouting, node, allocation);
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
            long numPrevents = numAllocatableSubjectShards();
            assert round != 0 || numPrevents > 0 : "must have shards that can be allocated on first round";

            verify(ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation, numPrevents, mockCanAllocateDiskDecider);
            verify(
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                0,
                mockCanAllocateDiskDecider,
                CAN_ALLOCATE_NO_DECIDER
            );
            verify(ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation, 0);
            // verify empty tier (no cold nodes) are always assumed a storage reason.
            verify(
                moveToCold(allIndices()),
                ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation,
                state.getRoutingNodes().unassigned().size(),
                DiscoveryNodeRole.DATA_COLD_NODE_ROLE
            );
            verify(ReactiveStorageDeciderService.AllocationState::storagePreventsAllocation, 0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
            if (numPrevents > 0) {
                verifyScale(numPrevents, "not enough storage available, needs " + numPrevents + "b", mockCanAllocateDiskDecider);
            } else {
                verifyScale(0, "storage ok", mockCanAllocateDiskDecider);
            }
            verifyScale(0, "storage ok", mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
            verifyScale(0, "storage ok");
            verifyScale(addDataNodes(DATA_HOT_NODE_ROLE, "additional", state, hotNodes), 0, "storage ok", mockCanAllocateDiskDecider);
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
                .forEach(shard -> allocation.routingNodes().startShard(logger, shard, allocation.changes()))
        );

        do {
            startRandomShards();
            // all of the relevant replicas are assigned too.
        } while (StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .map(ShardRouting::shardId)
            .anyMatch(warmShards::contains));

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
                            allocation.changes()
                        )
                )
        );

        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            subjectShards.size(),
            mockCanAllocateDiskDecider
        );
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            mockCanAllocateDiskDecider,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0);

        verifyScale(subjectShards.size(), "not enough storage available, needs " + subjectShards.size() + "b", mockCanAllocateDiskDecider);
        verifyScale(0, "storage ok", mockCanAllocateDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok");
        verifyScale(addDataNodes(DATA_HOT_NODE_ROLE, "additional", state, hotNodes), 0, "storage ok", mockCanAllocateDiskDecider);
    }

    public void testMoveToEmpty() {
        // allocate all primary shards
        allocate();

        // start shards.
        withRoutingAllocation(
            allocation -> RoutingNodesHelper.shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING)
                .stream()
                .filter(ShardRouting::primary)
                .forEach(shard -> allocation.routingNodes().startShard(logger, shard, allocation.changes()))
        );

        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);

        Set<IndexMetadata> candidates = new HashSet<>(randomSubsetOf(allIndices()));
        int allocatedCandidateShards = candidates.stream().mapToInt(IndexMetadata::getNumberOfShards).sum();

        verify(
            moveToCold(candidates),
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            allocatedCandidateShards,
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

        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            nodes,
            mockCanRemainDiskDecider,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            0,
            mockCanRemainDiskDecider,
            CAN_REMAIN_NO_DECIDER,
            CAN_ALLOCATE_NO_DECIDER
        );
        verify(ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove, 0);

        // only consider it once (move case) if both cannot remain and cannot allocate.
        verify(
            ReactiveStorageDeciderService.AllocationState::storagePreventsRemainOrMove,
            nodes,
            mockCanAllocateDiskDecider,
            mockCanRemainDiskDecider
        );

        verifyScale(nodes, "not enough storage available, needs " + nodes + "b", mockCanRemainDiskDecider, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok", mockCanRemainDiskDecider, CAN_REMAIN_NO_DECIDER, CAN_ALLOCATE_NO_DECIDER);
        verifyScale(0, "storage ok");
    }

    private interface VerificationSubject {
        long invoke(ReactiveStorageDeciderService.AllocationState state);
    }

    private void verify(VerificationSubject subject, long expected, AllocationDecider... allocationDeciders) {
        verify(subject, expected, DATA_HOT_NODE_ROLE, allocationDeciders);
    }

    private void verify(VerificationSubject subject, long expected, DiscoveryNodeRole role, AllocationDecider... allocationDeciders) {
        verify(this.state, subject, expected, role, allocationDeciders);
    }

    private static void verify(
        ClusterState state,
        VerificationSubject subject,
        long expected,
        DiscoveryNodeRole role,
        AllocationDecider... allocationDeciders
    ) {
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            createContext(state, Set.of(role)),
            DISK_THRESHOLD_SETTINGS,
            createAllocationDeciders(allocationDeciders)
        );
        assertThat(subject.invoke(allocationState), equalTo(expected));
    }

    private void verifyScale(long expectedDifference, String reason, AllocationDecider... allocationDeciders) {
        verifyScale(state, expectedDifference, reason, allocationDeciders);
    }

    private static void verifyScale(ClusterState state, long expectedDifference, String reason, AllocationDecider... allocationDeciders) {
        ReactiveStorageDeciderService decider = new ReactiveStorageDeciderService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            createAllocationDeciders(allocationDeciders)
        );
        TestAutoscalingDeciderContext context = createContext(state, Set.of(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        AutoscalingDeciderResult result = decider.scale(Settings.EMPTY, context);
        if (context.currentCapacity != null) {
            assertThat(
                result.requiredCapacity().total().storage().getBytes() - context.currentCapacity.total().storage().getBytes(),
                equalTo(expectedDifference)
            );
            assertThat(result.reason().summary(), equalTo(reason));
        } else {
            assertThat(result.requiredCapacity(), is(nullValue()));
            assertThat(result.reason().summary(), equalTo("current capacity not available"));
        }
    }

    private long numAllocatableSubjectShards() {
        AllocationDeciders deciders = createAllocationDeciders();
        RoutingAllocation allocation = createRoutingAllocation(state, deciders);
        return StreamSupport.stream(state.getRoutingNodes().unassigned().spliterator(), false)
            .filter(shard -> subjectShards.contains(shard.shardId()))
            .filter(
                shard -> StreamSupport.stream(allocation.routingNodes().spliterator(), false)
                    .anyMatch(node -> deciders.canAllocate(shard, node, allocation) != Decision.NO)
            )
            .count();
    }

    private boolean hasStartedSubjectShard() {
        return RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)
            .stream()
            .filter(ShardRouting::primary)
            .map(ShardRouting::shardId)
            .anyMatch(subjectShards::contains);
    }

    private static AllocationDeciders createAllocationDeciders(AllocationDecider... extraDeciders) {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Collection<AllocationDecider> systemAllocationDeciders = ClusterModule.createAllocationDeciders(
            Settings.builder()
                .put(
                    ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                    Integer.MAX_VALUE
                )
                .build(),
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
            initializingShards.sort(Comparator.comparing(ShardRouting::shardId).thenComparing(ShardRouting::primary, Boolean::compare));
            List<ShardRouting> shards = randomSubsetOf(Math.min(randomIntBetween(1, 100), initializingShards.size()), initializingShards);

            // replicas before primaries, since replicas can be reinit'ed, resulting in a new ShardRouting instance.
            shards.stream()
                .filter(Predicate.not(ShardRouting::primary))
                .forEach(s -> allocation.routingNodes().startShard(logger, s, allocation.changes()));
            shards.stream()
                .filter(ShardRouting::primary)
                .forEach(s -> allocation.routingNodes().startShard(logger, s, allocation.changes()));
            SHARDS_ALLOCATOR.allocate(allocation);

            // ensure progress by only relocating a shard if we started more than one shard.
            if (shards.size() > 1 && randomBoolean()) {
                List<ShardRouting> started = RoutingNodesHelper.shardsWithState(allocation.routingNodes(), ShardRoutingState.STARTED);
                if (started.isEmpty() == false) {
                    ShardRouting toMove = randomFrom(started);
                    Set<RoutingNode> candidates = StreamSupport.stream(allocation.routingNodes().spliterator(), false)
                        .filter(n -> allocation.deciders().canAllocate(toMove, n, allocation) == Decision.YES)
                        .collect(Collectors.toSet());
                    if (candidates.isEmpty() == false) {
                        allocation.routingNodes().relocateShard(toMove, randomFrom(candidates).nodeId(), 0L, allocation.changes());
                    }
                }
            }
        });
    }

    private TestAutoscalingDeciderContext createContext(DiscoveryNodeRole role) {
        return createContext(state, Set.of(role));
    }

    private static TestAutoscalingDeciderContext createContext(ClusterState state, Set<DiscoveryNodeRole> roles) {
        return new TestAutoscalingDeciderContext(state, roles, randomCurrentCapacity());
    }

    static AutoscalingCapacity randomCurrentCapacity() {
        if (randomInt(4) > 0) {
            // we only rely on storage.
            boolean includeMemory = randomBoolean();
            return AutoscalingCapacity.builder()
                .total(randomByteSizeValue(), includeMemory ? randomByteSizeValue() : null)
                .node(randomByteSizeValue(), includeMemory ? randomByteSizeValue() : null)
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
            this.nodes = state.nodes().stream().filter(n -> roles.stream().anyMatch(n.getRoles()::contains)).collect(Collectors.toSet());
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
            .collect(Collectors.toMap(DiscoveryNode::getId, node -> new DiskUsage(node.getId(), null, "the_path", 1000, 1000)));
        ImmutableOpenMap<String, DiskUsage> immutableDiskUsages = ImmutableOpenMap.<String, DiskUsage>builder()
            .putAllFromMap(diskUsages)
            .build();

        return new ClusterInfo() {
            @Override
            public ImmutableOpenMap<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
                return immutableDiskUsages;
            }

            @Override
            public ImmutableOpenMap<String, DiskUsage> getNodeMostAvailableDiskUsages() {
                return immutableDiskUsages;
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
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
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
        return new DiscoveryNode(
            nodeName,
            UUIDs.randomBase64UUID(),
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(role),
            Version.CURRENT
        );
    }

    private static String randomNodeId(RoutingNodes routingNodes, DiscoveryNodeRole role) {
        return randomFrom(
            StreamSupport.stream(routingNodes.spliterator(), false)
                .map(RoutingNode::node)
                .filter(n -> n.getRoles().contains(role))
                .collect(Collectors.toSet())
        ).getId();
    }

    private static Set<ShardId> shardIds(Iterable<ShardRouting> candidateShards) {
        return StreamSupport.stream(candidateShards.spliterator(), false).map(ShardRouting::shardId).collect(Collectors.toSet());
    }
}
