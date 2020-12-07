/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ProactiveStorageDeciderServiceTests extends AutoscalingTestCase {
    public void testScale() {

    }

    public void testForecastNoDates() {
        ClusterState originalState = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(Tuple.tuple("test", between(1, 10))),
            List.of()
        );
        ClusterState.Builder stateBuilder = ClusterState.builder(originalState);
        stateBuilder.routingTable(addRouting(originalState.metadata(), RoutingTable.builder()).build());
        ClusterState state = stateBuilder.build();
        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            null,
            null,
            null,
            Set.of()
        );

        assertThat(allocationState.forecast(Long.MAX_VALUE, System.currentTimeMillis()), Matchers.sameInstance(allocationState));
    }

    public void testForecast() {
        int indices = between(1, 10);
        ClusterState originalState = DataStreamTestHelper.getClusterStateWithDataStreams(List.of(Tuple.tuple("test", indices)), List.of());
        ClusterState.Builder stateBuilder = ClusterState.builder(originalState);
        stateBuilder.routingTable(addRouting(originalState.metadata(), RoutingTable.builder()).build());
        IntStream.range(0, between(1, 10)).forEach(i -> ReactiveStorageDeciderServiceTests.addNode(stateBuilder));
        long lastCreated = randomNonNegativeLong();
        applyCreatedDates(
            originalState,
            stateBuilder,
            (IndexAbstraction.DataStream) originalState.metadata().getIndicesLookup().get("test"),
            lastCreated,
            1
        );
        ClusterState state = stateBuilder.build();

        state = randomAllocate(state);

        DataStream dataStream = state.metadata().dataStreams().get("test");

        ClusterInfo info = randomClusterInfo(state);

        ReactiveStorageDeciderService.AllocationState allocationState = new ReactiveStorageDeciderService.AllocationState(
            state,
            null,
            null,
            info,
            null,
            Sets.newHashSet(state.nodes())
        );

        for (int window = 0; window < between(1, 20); ++window) {
            ReactiveStorageDeciderService.AllocationState forecast = allocationState.forecast(window, lastCreated + 1);
            int actualWindow = Math.min(window, indices);
            int expectedIndices = actualWindow + indices;
            assertThat(forecast.state().metadata().indices().size(), Matchers.equalTo(expectedIndices));
            DataStream forecastDataStream = forecast.state().metadata().dataStreams().get("test");
            assertThat(forecastDataStream.getIndices().size(), Matchers.equalTo(expectedIndices));
            assertThat(forecastDataStream.getIndices().subList(0, indices), Matchers.equalTo(dataStream.getIndices()));

            RoutingTable forecastRoutingTable = forecast.state().routingTable();
            assertThat(forecastRoutingTable.allShards().size(), Matchers.equalTo((expectedIndices) * 2)); // * 2 for 1 replica

            forecastDataStream.getIndices().forEach(index -> {
                assertThat(forecastRoutingTable.allShards(index.getName()).size(), Matchers.equalTo(2)); // 1 primary + 1 replica
            });

            forecastRoutingTable.allShards().forEach(s -> assertThat(forecast.info().getShardSize(s), Matchers.notNullValue()));

            long expectedTotal = totalSize(dataStream.getIndices().subList(indices - actualWindow, indices), state.routingTable(), info);
            List<Index> addedIndices = forecastDataStream.getIndices().subList(indices, forecastDataStream.getIndices().size());
            long actualTotal = totalSize(addedIndices, forecastRoutingTable, forecast.info());

            // three round downs -> max 3 bytes lower and never above.
            assertThat(actualTotal, Matchers.lessThanOrEqualTo(expectedTotal));
            assertThat(actualTotal, Matchers.greaterThanOrEqualTo(actualTotal - 3));
            // omit last index, since it is reduced a bit for rounding. Total validated above so it all adds up.
            for (int i = 0; i < addedIndices.size() - 1; ++i) {
                forecastRoutingTable.allShards(addedIndices.get(i).getName())
                    .forEach(
                        shard -> {
                            assertThat(
                                forecast.info().getShardSize(shard),
                                Matchers.equalTo(((expectedTotal - 1) / addedIndices.size() + 1) / 2)
                            );
                        }
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
            new RoutingNodes(state, false),
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

    private RoutingTable.Builder addRouting(Iterable<IndexMetadata> indices, RoutingTable.Builder builder) {
        indices.forEach(builder::addAsNew);
        return builder;
    }

    private ClusterInfo randomClusterInfo(ClusterState state) {
        Map<String, Long> collect = state.routingTable()
            .allShards()
            .stream()
            .map(ClusterInfo::shardIdentifierFromRouting)
            .collect(Collectors.toMap(Function.identity(), id -> randomLongBetween(1, 1000)));
        return new ClusterInfo(null, null, ImmutableOpenMap.<String, Long>builder().putAll(collect).build(), null, null);
    }

    private ClusterState.Builder applyCreatedDates(
        ClusterState state,
        ClusterState.Builder builder,
        IndexAbstraction.DataStream ds,
        long last,
        long decrement
    ) {
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata());
        List<IndexMetadata> indices = ds.getIndices();
        long start = last - (decrement * (indices.size() - 1));
        for (int i = 0; i < indices.size(); ++i) {
            metadataBuilder.put(IndexMetadata.builder(indices.get(i)).creationDate(start + (i * decrement)).build(), false);
        }
        return builder.metadata(metadataBuilder);
    }
}
