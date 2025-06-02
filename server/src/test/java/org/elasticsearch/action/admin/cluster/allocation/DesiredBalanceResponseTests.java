/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoTests;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.ClusterBalanceStats;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStatsTests;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStatsTests.randomDesiredBalanceStats;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class DesiredBalanceResponseTests extends AbstractWireSerializingTestCase<DesiredBalanceResponse> {

    @Override
    protected Writeable.Reader<DesiredBalanceResponse> instanceReader() {
        return DesiredBalanceResponse::from;
    }

    @Override
    protected DesiredBalanceResponse createTestInstance() {
        return new DesiredBalanceResponse(
            randomDesiredBalanceStats(),
            randomClusterBalanceStats(),
            randomRoutingTable(),
            randomClusterInfo()
        );
    }

    private ClusterBalanceStats randomClusterBalanceStats() {
        return new ClusterBalanceStats(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomBoolean()
                ? Map.of(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE.roleName(), randomTierBalanceStats())
                : randomSubsetOf(
                    List.of(
                        DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                        DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                        DiscoveryNodeRole.DATA_COLD_NODE_ROLE,
                        DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE
                    )
                ).stream().map(DiscoveryNodeRole::roleName).collect(toMap(identity(), ignore -> randomTierBalanceStats())),
            randomList(10, () -> randomAlphaOfLength(10)).stream().collect(toMap(identity(), ignore -> randomNodeBalanceStats()))
        );
    }

    private ClusterBalanceStats.TierBalanceStats randomTierBalanceStats() {
        return new ClusterBalanceStats.TierBalanceStats(
            randomMetricStats(),
            randomMetricStats(),
            randomMetricStats(),
            randomMetricStats(),
            randomMetricStats()
        );
    }

    private ClusterBalanceStats.MetricStats randomMetricStats() {
        return new ClusterBalanceStats.MetricStats(randomDouble(), randomDouble(), randomDouble(), randomDouble(), randomDouble());
    }

    private ClusterBalanceStats.NodeBalanceStats randomNodeBalanceStats() {
        return new ClusterBalanceStats.NodeBalanceStats(
            randomAlphaOfLength(10),
            List.of(randomFrom("data_content", "data_hot", "data_warm", "data_cold")),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomDouble(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    private ClusterInfo randomClusterInfo() {
        return ClusterInfoTests.randomClusterInfo();
    }

    private Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> randomRoutingTable() {
        Map<String, Map<Integer, DesiredBalanceResponse.DesiredShards>> routingTable = new HashMap<>();
        for (int i = 0; i < randomInt(8); i++) {
            String indexName = randomAlphaOfLength(8);
            Map<Integer, DesiredBalanceResponse.DesiredShards> desiredShards = new HashMap<>();
            for (int j = 0; j < randomInt(8); j++) {
                int shardId = randomInt(1024);
                desiredShards.put(
                    shardId,
                    new DesiredBalanceResponse.DesiredShards(
                        IntStream.range(0, randomIntBetween(1, 4))
                            .mapToObj(
                                k -> new DesiredBalanceResponse.ShardView(
                                    randomFrom(ShardRoutingState.STARTED, ShardRoutingState.UNASSIGNED, ShardRoutingState.INITIALIZING),
                                    randomBoolean(),
                                    randomAlphaOfLength(8),
                                    randomBoolean(),
                                    randomAlphaOfLength(8),
                                    randomBoolean(),
                                    shardId,
                                    indexName,
                                    randomBoolean() ? randomDouble() : null,
                                    randomBoolean() ? randomLong() : null,
                                    randomList(0, 1, () -> randomFrom("hot", "warm", "cold", "frozen"))
                                )
                            )
                            .toList(),
                        new DesiredBalanceResponse.ShardAssignmentView(
                            randomUnique(() -> randomAlphaOfLength(8), randomIntBetween(1, 8)),
                            randomInt(8),
                            randomInt(8),
                            randomInt(8)
                        )
                    )
                );
            }
            routingTable.put(indexName, Collections.unmodifiableMap(desiredShards));
        }
        return Collections.unmodifiableMap(routingTable);
    }

    @Override
    protected DesiredBalanceResponse mutateInstance(DesiredBalanceResponse instance) {
        return switch (randomInt(4)) {
            case 0 -> new DesiredBalanceResponse(
                randomValueOtherThan(instance.getStats(), DesiredBalanceStatsTests::randomDesiredBalanceStats),
                instance.getClusterBalanceStats(),
                instance.getRoutingTable(),
                instance.getClusterInfo()
            );
            case 1 -> new DesiredBalanceResponse(
                instance.getStats(),
                randomValueOtherThan(instance.getClusterBalanceStats(), this::randomClusterBalanceStats),
                instance.getRoutingTable(),
                instance.getClusterInfo()
            );
            case 2 -> new DesiredBalanceResponse(
                instance.getStats(),
                instance.getClusterBalanceStats(),
                randomValueOtherThan(instance.getRoutingTable(), this::randomRoutingTable),
                instance.getClusterInfo()
            );
            case 3 -> new DesiredBalanceResponse(
                instance.getStats(),
                instance.getClusterBalanceStats(),
                instance.getRoutingTable(),
                randomValueOtherThan(instance.getClusterInfo(), this::randomClusterInfo)
            );
            default -> randomValueOtherThan(instance, this::createTestInstance);
        };
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        DesiredBalanceResponse response = new DesiredBalanceResponse(
            randomDesiredBalanceStats(),
            randomClusterBalanceStats(),
            randomRoutingTable(),
            randomClusterInfo()
        );

        Map<String, Object> json;
        try (
            var parser = createParser(
                ChunkedToXContent.wrapAsToXContent(response).toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            )
        ) {
            json = parser.map();
        }
        assertThat(json.keySet(), containsInAnyOrder("stats", "cluster_balance_stats", "routing_table", "cluster_info"));

        // stats
        Map<String, Object> stats = (Map<String, Object>) json.get("stats");
        assertEquals(stats.get("computation_converged_index"), response.getStats().lastConvergedIndex());
        assertEquals(stats.get("computation_active"), response.getStats().computationActive());
        assertEquals(stats.get("computation_submitted"), response.getStats().computationSubmitted());
        assertEquals(stats.get("computation_executed"), response.getStats().computationExecuted());
        assertEquals(stats.get("computation_converged"), response.getStats().computationConverged());
        assertEquals(stats.get("computation_iterations"), response.getStats().computationIterations());
        assertEquals(stats.get("computed_shard_movements"), response.getStats().computedShardMovements());
        assertEquals(stats.get("computation_time_in_millis"), response.getStats().cumulativeComputationTime());
        assertEquals(stats.get("reconciliation_time_in_millis"), response.getStats().cumulativeReconciliationTime());

        // cluster balance stats
        Map<String, Object> clusterBalanceStats = (Map<String, Object>) json.get("cluster_balance_stats");
        assertThat(clusterBalanceStats.keySet(), containsInAnyOrder("shard_count", "undesired_shard_allocation_count", "tiers", "nodes"));

        assertEquals(clusterBalanceStats.get("shard_count"), response.getClusterBalanceStats().shards());
        assertEquals(
            clusterBalanceStats.get("undesired_shard_allocation_count"),
            response.getClusterBalanceStats().undesiredShardAllocations()
        );
        // tier balance stats
        Map<String, Object> tiers = (Map<String, Object>) clusterBalanceStats.get("tiers");
        assertEquals(tiers.keySet(), response.getClusterBalanceStats().tiers().keySet());
        for (var entry : response.getClusterBalanceStats().tiers().entrySet()) {
            Map<String, Object> tierStats = (Map<String, Object>) tiers.get(entry.getKey());
            assertThat(
                tierStats.keySet(),
                containsInAnyOrder(
                    "shard_count",
                    "undesired_shard_allocation_count",
                    "forecast_write_load",
                    "forecast_disk_usage",
                    "actual_disk_usage"
                )
            );

            Map<String, Object> shardCountStats = (Map<String, Object>) tierStats.get("shard_count");
            assertThat(shardCountStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
            assertEquals(shardCountStats.get("total"), entry.getValue().shardCount().total());
            assertEquals(shardCountStats.get("average"), entry.getValue().shardCount().average());
            assertEquals(shardCountStats.get("min"), entry.getValue().shardCount().min());
            assertEquals(shardCountStats.get("max"), entry.getValue().shardCount().max());
            assertEquals(shardCountStats.get("std_dev"), entry.getValue().shardCount().stdDev());

            Map<String, Object> undesiredShardAllocationCountStats = (Map<String, Object>) tierStats.get(
                "undesired_shard_allocation_count"
            );
            assertThat(undesiredShardAllocationCountStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
            assertEquals(undesiredShardAllocationCountStats.get("total"), entry.getValue().undesiredShardAllocations().total());
            assertEquals(undesiredShardAllocationCountStats.get("average"), entry.getValue().undesiredShardAllocations().average());
            assertEquals(undesiredShardAllocationCountStats.get("min"), entry.getValue().undesiredShardAllocations().min());
            assertEquals(undesiredShardAllocationCountStats.get("max"), entry.getValue().undesiredShardAllocations().max());
            assertEquals(undesiredShardAllocationCountStats.get("std_dev"), entry.getValue().undesiredShardAllocations().stdDev());

            Map<String, Object> forecastWriteLoadStats = (Map<String, Object>) tierStats.get("forecast_write_load");
            assertThat(forecastWriteLoadStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
            assertEquals(forecastWriteLoadStats.get("total"), entry.getValue().forecastWriteLoad().total());
            assertEquals(forecastWriteLoadStats.get("average"), entry.getValue().forecastWriteLoad().average());
            assertEquals(forecastWriteLoadStats.get("min"), entry.getValue().forecastWriteLoad().min());
            assertEquals(forecastWriteLoadStats.get("max"), entry.getValue().forecastWriteLoad().max());
            assertEquals(forecastWriteLoadStats.get("std_dev"), entry.getValue().forecastWriteLoad().stdDev());

            Map<String, Object> forecastDiskUsageStats = (Map<String, Object>) tierStats.get("forecast_disk_usage");
            assertThat(forecastDiskUsageStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
            assertEquals(forecastDiskUsageStats.get("total"), entry.getValue().forecastShardSize().total());
            assertEquals(forecastDiskUsageStats.get("average"), entry.getValue().forecastShardSize().average());
            assertEquals(forecastDiskUsageStats.get("min"), entry.getValue().forecastShardSize().min());
            assertEquals(forecastDiskUsageStats.get("max"), entry.getValue().forecastShardSize().max());
            assertEquals(forecastDiskUsageStats.get("std_dev"), entry.getValue().forecastShardSize().stdDev());

            Map<String, Object> actualDiskUsageStats = (Map<String, Object>) tierStats.get("actual_disk_usage");
            assertThat(actualDiskUsageStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
            assertEquals(actualDiskUsageStats.get("total"), entry.getValue().actualShardSize().total());
            assertEquals(actualDiskUsageStats.get("average"), entry.getValue().actualShardSize().average());
            assertEquals(actualDiskUsageStats.get("min"), entry.getValue().actualShardSize().min());
            assertEquals(actualDiskUsageStats.get("max"), entry.getValue().actualShardSize().max());
            assertEquals(actualDiskUsageStats.get("std_dev"), entry.getValue().actualShardSize().stdDev());
        }
        // node balance stats
        Map<String, Object> nodes = (Map<String, Object>) clusterBalanceStats.get("nodes");
        assertEquals(nodes.keySet(), response.getClusterBalanceStats().nodes().keySet());
        for (var entry : response.getClusterBalanceStats().nodes().entrySet()) {
            Map<String, Object> nodesStats = (Map<String, Object>) nodes.get(entry.getKey());
            assertThat(
                nodesStats.keySet(),
                containsInAnyOrder(
                    "node_id",
                    "roles",
                    "shard_count",
                    "undesired_shard_allocation_count",
                    "forecast_write_load",
                    "forecast_disk_usage_bytes",
                    "actual_disk_usage_bytes"
                )
            );
            assertEquals(nodesStats.get("node_id"), entry.getValue().nodeId());
            assertEquals(nodesStats.get("roles"), entry.getValue().roles());
            assertEquals(nodesStats.get("shard_count"), entry.getValue().shards());
            assertEquals(nodesStats.get("undesired_shard_allocation_count"), entry.getValue().undesiredShardAllocations());
            assertEquals(nodesStats.get("forecast_write_load"), entry.getValue().forecastWriteLoad());
            assertEquals(nodesStats.get("forecast_disk_usage_bytes"), entry.getValue().forecastShardSize());
            assertEquals(nodesStats.get("actual_disk_usage_bytes"), entry.getValue().actualShardSize());
        }

        // routing table
        Map<String, Object> jsonRoutingTable = (Map<String, Object>) json.get("routing_table");
        assertEquals(jsonRoutingTable.keySet(), response.getRoutingTable().keySet());
        for (var indexEntry : response.getRoutingTable().entrySet()) {
            Map<String, Object> jsonIndexShards = (Map<String, Object>) jsonRoutingTable.get(indexEntry.getKey());
            assertEquals(
                jsonIndexShards.keySet(),
                indexEntry.getValue().keySet().stream().map(String::valueOf).collect(Collectors.toSet())
            );
            for (var shardEntry : indexEntry.getValue().entrySet()) {
                DesiredBalanceResponse.DesiredShards desiredShards = shardEntry.getValue();
                Map<String, Object> jsonDesiredShard = (Map<String, Object>) jsonIndexShards.get(String.valueOf(shardEntry.getKey()));
                assertThat(jsonDesiredShard.keySet(), containsInAnyOrder("current", "desired"));
                List<Map<String, Object>> jsonCurrent = (List<Map<String, Object>>) jsonDesiredShard.get("current");
                for (int i = 0; i < jsonCurrent.size(); i++) {
                    Map<String, Object> jsonShard = jsonCurrent.get(i);
                    DesiredBalanceResponse.ShardView shardView = desiredShards.current().get(i);
                    assertEquals(jsonShard.get("state"), shardView.state().toString());
                    assertEquals(jsonShard.get("primary"), shardView.primary());
                    assertEquals(jsonShard.get("node"), shardView.node());
                    assertEquals(jsonShard.get("node_is_desired"), shardView.nodeIsDesired());
                    assertEquals(jsonShard.get("relocating_node"), shardView.relocatingNode());
                    assertEquals(jsonShard.get("relocating_node_is_desired"), shardView.relocatingNodeIsDesired());
                    assertEquals(jsonShard.get("shard_id"), shardView.shardId());
                    assertEquals(jsonShard.get("index"), shardView.index());
                    assertEquals(jsonShard.get("forecast_write_load"), shardView.forecastWriteLoad());
                    assertEquals(jsonShard.get("forecast_shard_size_in_bytes"), shardView.forecastShardSizeInBytes());
                    assertEquals(jsonShard.get("tier_preference"), shardView.tierPreference());
                }

                Map<String, Object> jsonDesired = (Map<String, Object>) jsonDesiredShard.get("desired");
                List<String> nodeIds = (List<String>) jsonDesired.get("node_ids");
                assertEquals(nodeIds, List.copyOf(desiredShards.desired().nodeIds()));
                assertEquals(jsonDesired.get("total"), desiredShards.desired().total());
                assertEquals(jsonDesired.get("unassigned"), desiredShards.desired().unassigned());
                assertEquals(jsonDesired.get("ignored"), desiredShards.desired().ignored());
            }
        }

        Map<String, Object> clusterInfo = (Map<String, Object>) json.get("cluster_info");
        assertThat(
            clusterInfo.keySet(),
            containsInAnyOrder("nodes", "shard_paths", "shard_sizes", "shard_data_set_sizes", "reserved_sizes", "heap_usage")
        );
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new DesiredBalanceResponse(randomDesiredBalanceStats(), randomClusterBalanceStats(), randomRoutingTable(), randomClusterInfo()),
            response -> 3 + ClusterInfoTests.getChunkCount(response.getClusterInfo()) + response.getRoutingTable()
                .values()
                .stream()
                .mapToInt(indexEntry -> 2 + indexEntry.values().stream().mapToInt(shardEntry -> 3 + shardEntry.current().size()).sum())
                .sum()
        );
    }
}
