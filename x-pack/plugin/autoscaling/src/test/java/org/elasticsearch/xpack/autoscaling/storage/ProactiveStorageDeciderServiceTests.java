/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ProactiveStorageDeciderServiceTests extends AutoscalingTestCase {
    public void testScale() {
        @FixForMultiProject(description = "Use non-default project ID and remove last boolean parameter")
        ProjectMetadata originalProject = DataStreamTestHelper.getProjectWithDataStreams(
            Metadata.DEFAULT_PROJECT_ID,
            List.of(Tuple.tuple("test", between(1, 10))),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            0,
            randomBoolean(),
            false
        );
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        IntStream.range(0, between(1, 10)).forEach(i -> ReactiveStorageDeciderServiceTests.addNode(stateBuilder));
        stateBuilder.putRoutingTable(
            originalProject.id(),
            addRouting(originalProject, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)).build()
        );
        stateBuilder.putProjectMetadata(
            applyCreatedDates(originalProject, (DataStream) originalProject.getIndicesLookup().get("test"), System.currentTimeMillis(), 1)
        );
        ClusterState interimState = stateBuilder.build();
        final ClusterState state = startAll(interimState);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Collection<AllocationDecider> allocationDecidersList = new ArrayList<>(
            ClusterModule.createAllocationDeciders(Settings.EMPTY, clusterSettings, Collections.emptyList())
        );
        allocationDecidersList.add(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return allocation.decision(Decision.NO, DiskThresholdDecider.NAME, "test");
            }
        });
        AllocationDeciders allocationDeciders = new AllocationDeciders(allocationDecidersList);
        ProactiveStorageDeciderService service = new ProactiveStorageDeciderService(
            Settings.EMPTY,
            clusterSettings,
            allocationDeciders,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        AutoscalingCapacity currentCapacity = ReactiveStorageDeciderDecisionTests.randomCurrentCapacity();
        ClusterInfo info = randomClusterInfo(state.projectState(originalProject.id()));
        AutoscalingDeciderContext context = new AutoscalingDeciderContext() {
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
                return Sets.newHashSet(state.nodes());
            }

            @Override
            public Set<DiscoveryNodeRole> roles() {
                return Set.of(DiscoveryNodeRole.DATA_ROLE);
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
            public void ensureNotCancelled() {}
        };
        AutoscalingDeciderResult deciderResult = service.scale(Settings.EMPTY, context);

        if (currentCapacity != null) {
            assertThat(deciderResult.requiredCapacity().total().storage(), Matchers.greaterThan(currentCapacity.total().storage()));
            assertThat(deciderResult.reason().summary(), startsWith("not enough storage available, needs "));
            ProactiveStorageDeciderService.ProactiveReason reason = (ProactiveStorageDeciderService.ProactiveReason) deciderResult.reason();
            assertThat(
                reason.forecasted(),
                equalTo(deciderResult.requiredCapacity().total().storage().getBytes() - currentCapacity.total().storage().getBytes())
            );
            assertThat(
                reason.forecasted(),
                lessThanOrEqualTo(
                    totalSize(
                        state.metadata().getProject(originalProject.id()).dataStreams().get("test").getIndices(),
                        state.routingTable(originalProject.id()),
                        info
                    )
                )
            );

            deciderResult = service.scale(
                Settings.builder().put(ProactiveStorageDeciderService.FORECAST_WINDOW.getKey(), TimeValue.ZERO).build(),
                context
            );
            assertThat(deciderResult.requiredCapacity().total().storage(), Matchers.equalTo(currentCapacity.total().storage()));
            assertThat(deciderResult.reason().summary(), equalTo("storage ok"));
            reason = (ProactiveStorageDeciderService.ProactiveReason) deciderResult.reason();
            assertThat(reason.forecasted(), equalTo(0L));
        } else {
            assertThat(deciderResult.requiredCapacity(), is(nullValue()));
            assertThat(deciderResult.reason().summary(), equalTo("current capacity not available"));
        }
    }

    public void testForecastNoDates() {
        @FixForMultiProject(description = "Use non-default project ID and remove last boolean parameter")
        ProjectMetadata originalProject = DataStreamTestHelper.getProjectWithDataStreams(
            Metadata.DEFAULT_PROJECT_ID,
            List.of(Tuple.tuple("test", between(1, 10))),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            between(0, 4),
            randomBoolean(),
            false
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(originalProject)
            .putRoutingTable(
                originalProject.id(),
                addRouting(originalProject, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)).build()
            )
            .build();
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null,
            null,
            null,
            Set.of(),
            Set.of()
        );

        assertThat(allocationState.forecast(Long.MAX_VALUE, System.currentTimeMillis()), Matchers.sameInstance(allocationState));
    }

    public void testForecastZero() {
        @FixForMultiProject(description = "Use non-default project ID and remove last two boolean parameters")
        ProjectMetadata originalProject = DataStreamTestHelper.getProjectWithDataStreams(
            Metadata.DEFAULT_PROJECT_ID,
            List.of(Tuple.tuple("test", between(1, 10))),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            between(0, 4),
            false,
            false
        );
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        IntStream.range(0, between(1, 10)).forEach(i -> ReactiveStorageDeciderServiceTests.addNode(stateBuilder));
        stateBuilder.putRoutingTable(
            originalProject.id(),
            addRouting(originalProject, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)).build()
        );
        long lastCreated = randomNonNegativeLong();
        stateBuilder.putProjectMetadata(
            applyCreatedDates(originalProject, (DataStream) originalProject.getIndicesLookup().get("test"), lastCreated, 1)
        );
        ClusterState state = stateBuilder.build();
        state = randomAllocate(state);
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null,
            randomClusterInfo(state.projectState(originalProject.id())),
            null,
            Sets.newHashSet(state.nodes()),
            Set.of()
        );

        assertThat(allocationState.forecast(0, lastCreated + between(-3, 1)), Matchers.sameInstance(allocationState));
        assertThat(allocationState.forecast(10, lastCreated + 1), Matchers.not(Matchers.sameInstance(allocationState)));
    }

    public void testForecast() {
        int indices = between(1, 10);
        int shardCopies = between(1, 2);
        @FixForMultiProject(description = "Use non-default project ID and remove last boolean parameter")
        final var projectId = Metadata.DEFAULT_PROJECT_ID;
        ProjectMetadata originalProject = DataStreamTestHelper.getProjectWithDataStreams(
            projectId,
            List.of(Tuple.tuple("test", indices)),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            shardCopies - 1,
            randomBoolean(),
            false
        );
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
        stateBuilder.putRoutingTable(
            projectId,
            addRouting(originalProject, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)).build()
        );
        IntStream.range(0, between(1, 10)).forEach(i -> ReactiveStorageDeciderServiceTests.addNode(stateBuilder));
        long lastCreated = randomNonNegativeLong();
        stateBuilder.putProjectMetadata(
            applyCreatedDates(originalProject, (DataStream) originalProject.getIndicesLookup().get("test"), lastCreated, 1)
        );
        ClusterState state = stateBuilder.build();

        state = randomAllocate(state);

        DataStream dataStream = state.metadata().getProject(projectId).dataStreams().get("test");

        ClusterInfo info = randomClusterInfo(state.projectState(projectId));

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null,
            info,
            null,
            Sets.newHashSet(state.nodes()),
            Set.of()
        );

        for (int window = 0; window < between(1, 20); ++window) {
            ReactiveStorageDeciderService.AllocationState forecast = allocationState.forecast(window, lastCreated + 1);
            int actualWindow = Math.min(window, indices);
            int expectedIndices = actualWindow + indices;
            assertThat(forecast.state().metadata().getProject(projectId).indices().size(), Matchers.equalTo(expectedIndices));
            DataStream forecastDataStream = forecast.state().metadata().getProject(projectId).dataStreams().get("test");
            assertThat(forecastDataStream.getIndices().size(), Matchers.equalTo(expectedIndices));
            assertThat(forecastDataStream.getIndices().subList(0, indices), Matchers.equalTo(dataStream.getIndices()));

            RoutingTable forecastRoutingTable = forecast.state().routingTable(projectId);
            assertThat(forecastRoutingTable.allShards().count(), Matchers.equalTo((long) (expectedIndices) * shardCopies));

            forecastDataStream.getIndices()
                .forEach(index -> assertThat(forecastRoutingTable.allShards(index.getName()).size(), Matchers.equalTo(shardCopies)));

            forecastRoutingTable.allShards().forEach(s -> assertThat(forecast.info().getShardSize(s), Matchers.notNullValue()));

            long expectedTotal = totalSize(
                dataStream.getIndices().subList(indices - actualWindow, indices),
                state.routingTable(projectId),
                info
            );
            List<Index> addedIndices = forecastDataStream.getIndices().subList(indices, forecastDataStream.getIndices().size());
            long actualTotal = totalSize(addedIndices, forecastRoutingTable, forecast.info());

            // three round downs -> max 3 bytes lower and never above.
            assertThat(actualTotal, Matchers.lessThanOrEqualTo(expectedTotal));
            assertThat(actualTotal, Matchers.greaterThanOrEqualTo(actualTotal - 3));
            // omit last index, since it is reduced a bit for rounding. Total validated above so it all adds up.
            for (int i = 0; i < addedIndices.size() - 1; ++i) {
                forecastRoutingTable.allShards(addedIndices.get(i).getName())
                    .forEach(
                        shard -> assertThat(
                            forecast.info().getShardSize(shard),
                            Matchers.equalTo(((expectedTotal - 1) / addedIndices.size() + 1) / shardCopies)
                        )
                    );
            }
        }
    }

    private long totalSize(List<Index> indices, RoutingTable routingTable, ClusterInfo info) {
        return indices.stream().flatMap(i -> routingTable.allShards(i.getName()).stream()).mapToLong(info::getShardSize).sum();
    }

    private ClusterState randomAllocate(ClusterState state) {
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            state.mutableRoutingNodes(),
            state,
            null,
            null,
            System.nanoTime()
        );
        randomAllocate(allocation);
        return ReactiveStorageDeciderServiceTests.updateClusterState(state, allocation);
    }

    private void randomAllocate(RoutingAllocation allocation) {
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        Set<ShardRouting> primaries = StreamSupport.stream(unassigned.spliterator(), false)
            .filter(ShardRouting::primary)
            .collect(Collectors.toSet());
        List<ShardRouting> primariesToAllocate = randomSubsetOf(between(1, primaries.size()), primaries);
        for (RoutingNodes.UnassignedShards.UnassignedIterator iterator = unassigned.iterator(); iterator.hasNext();) {
            if (primariesToAllocate.contains(iterator.next())) {
                iterator.initialize(
                    randomFrom(Sets.newHashSet(allocation.routingNodes())).nodeId(),
                    null,
                    ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                    allocation.changes()
                );
            }
        }
    }

    private ClusterState startAll(ClusterState state) {
        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            state.mutableRoutingNodes(),
            state,
            null,
            null,
            System.nanoTime()
        );
        startAll(allocation);
        return ReactiveStorageDeciderServiceTests.updateClusterState(state, allocation);
    }

    private void startAll(RoutingAllocation allocation) {
        for (RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator(); iterator
            .hasNext();) {
            ShardRouting unassignedShard = iterator.next();
            assert unassignedShard.primary();
            ShardRouting shardRouting = iterator.initialize(
                randomFrom(Sets.newHashSet(allocation.routingNodes())).nodeId(),
                null,
                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                allocation.changes()
            );
            allocation.routingNodes().startShard(shardRouting, allocation.changes(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
    }

    private RoutingTable.Builder addRouting(Iterable<IndexMetadata> indices, RoutingTable.Builder builder) {
        indices.forEach(indexMetadata -> builder.addAsNew(indexMetadata));
        return builder;
    }

    private ClusterInfo randomClusterInfo(ProjectState projectState) {
        Map<String, Long> shardSizes = projectState.routingTable()
            .allShards()
            .map(ClusterInfo::shardIdentifierFromRouting)
            .collect(Collectors.toMap(Function.identity(), id -> randomLongBetween(1, 1000), (v1, v2) -> v1));
        Map<String, DiskUsage> diskUsage = new HashMap<>();
        for (var id : projectState.cluster().nodes().getDataNodes().keySet()) {
            diskUsage.put(id, new DiskUsage(id, id, "/test", Long.MAX_VALUE, Long.MAX_VALUE));
        }
        return new ClusterInfo(diskUsage, diskUsage, shardSizes, Map.of(), Map.of(), Map.of(), Map.of());
    }

    private ProjectMetadata applyCreatedDates(ProjectMetadata project, DataStream ds, long last, long decrement) {
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(project);
        List<Index> indices = ds.getIndices();
        long start = last - (decrement * (indices.size() - 1));
        for (int i = 0; i < indices.size(); ++i) {
            IndexMetadata previousInstance = project.index(indices.get(i));
            projectBuilder.put(IndexMetadata.builder(previousInstance).creationDate(start + (i * decrement)).build(), false);
        }
        return projectBuilder.build();
    }
}
