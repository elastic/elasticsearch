/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DesiredBalanceSimulationTests extends ESAllocationTestCase {

    public void testDesiredBalanceFromDesiredBalanceOutput() throws IOException {

        var data = parse();

        logger.info("Initial stats {}", stats(data));

        Settings settings = Settings.builder()
            // .put(
            // ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(),
            // ClusterModule.DESIRED_BALANCE_ALLOCATOR
            // )
            .put(
                ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 50)
            .build();

        // var allocator = createShardsAllocator(settings);
        // var allocation = new RoutingAllocation(
        // randomAllocationDeciders(settings, ClusterSettings.createBuiltInClusterSettings(settings)),
        // data.clusterState.mutableRoutingNodes(),
        // data.clusterState,
        // data.clusterInfo,
        // SnapshotShardSizeInfo.EMPTY,
        // 0L
        // );
        // allocator.allocate(allocation, ActionListener.noop());
        //
        // logger.info("Same: {}, initializing: {}",
        // data.clusterState.getRoutingNodes().equals(allocation.routingNodes()),
        // RoutingNodesHelper.shardsWithState(allocation.routingNodes(), INITIALIZING).size()
        // );

        var service = createAllocationService(settings, () -> data.clusterInfo);
        var newClusterState = applyStartedShardsUntilNoChange(data.clusterState, service);

        logger.info("Routing nodes updated: {}", newClusterState.getRoutingNodes().equals(data.clusterState.getRoutingNodes()) == false);
        logger.info("Updated stats {}", stats(newClusterState, data.clusterInfo));
    }

    private record Data(ClusterState clusterState, ClusterInfo clusterInfo) {}

    private Data parse() throws IOException {

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();
        var discoveryNodesBuilder = DiscoveryNodes.builder();

        final Map<String, DiskUsage> leastAvailableSpaceUsage = new HashMap<>();
        final Map<String, DiskUsage> mostAvailableSpaceUsage = new HashMap<>();
        final Map<String, Long> shardSizes = new HashMap<>();

        try (
            var parser = createParser(
                JsonXContent.jsonXContent,
                DesiredBalanceSimulationTests.class.getResourceAsStream("allocated_index.json")
            )
        ) {
            var data = parser.map();
            var clusterInfo = (Map<String, Object>) data.get("cluster_info");
            var nodes = (Map<String, Object>) clusterInfo.get("nodes");

            for (var nodeEntry : nodes.entrySet()) {
                var nodeId = nodeEntry.getKey();
                var nodeNode = (Map<String, Object>) nodeEntry.getValue();

                var nodeName = (String) nodeNode.get("node_name");
                var leastAvailable = (Map<String, Object>) nodeNode.get("least_available");
                var mostAvailable = (Map<String, Object>) nodeNode.get("most_available");

                leastAvailableSpaceUsage.put(
                    nodeId,
                    new DiskUsage(
                        nodeId,
                        nodeName,
                        (String) leastAvailable.get("path"),
                        (long) leastAvailable.get("total_bytes"),
                        (long) leastAvailable.get("free_bytes")
                    )
                );

                mostAvailableSpaceUsage.put(
                    nodeId,
                    new DiskUsage(
                        nodeId,
                        nodeName,
                        (String) mostAvailable.get("path"),
                        (long) mostAvailable.get("total_bytes"),
                        (long) mostAvailable.get("free_bytes")
                    )
                );
            }

            for (var shardEntry : ((Map<String, Number>) clusterInfo.get("shard_sizes")).entrySet()) {
                String shardId = shardEntry.getKey();
                shardId = shardId.substring(0, shardId.length() - "_bytes".length());
                shardSizes.put(shardId, shardEntry.getValue().longValue());
            }
        }

        var writeLoadForecast = new HashMap<String, Double>();
        var shardSizeForecast = new HashMap<String, Long>();
        var tierPrefference = new HashMap<String, String>();
        try (
            var parser = createParser(JsonXContent.jsonXContent, DesiredBalanceSimulationTests.class.getResourceAsStream("routing.json"))
        ) {
            var data = parser.map();
            var metadata = (Map<String, Object>) data.get("metadata");
            var indices = (Map<String, Object>) metadata.get("indices");

            for (var indexEntry : indices.entrySet()) {
                var indexName = indexEntry.getKey();
                var indexNode = (Map<String, Object>) indexEntry.getValue();

                if (indexNode.containsKey("write_load_forecast")) {
                    writeLoadForecast.put(indexName, ((Number) indexNode.get("write_load_forecast")).doubleValue());
                }
                if (indexNode.containsKey("shard_size_forecast")) {
                    shardSizeForecast.put(indexName, ((Number) indexNode.get("shard_size_forecast")).longValue());
                }
                var tier = (String) get(indexNode, "settings", "index", "routing", "allocation", "include", "_tier_preference");
                if (tier != null) {
                    tierPrefference.put(indexName, tier);
                }
            }
        }

        try (
            var parser = createParser(
                JsonXContent.jsonXContent,
                DesiredBalanceSimulationTests.class.getResourceAsStream("desired_balance.json")
            )
        ) {
            var data = parser.map();
            var routingTable = (Map<String, Object>) data.get("routing_table");

            for (var indexEntry : routingTable.entrySet()) {
                var index = new Index(indexEntry.getKey(), ClusterState.UNKNOWN_UUID);
                var indexNode = (Map<String, Object>) indexEntry.getValue();

                var indexRoutingTableBuilder = IndexRoutingTable.builder(index);
                int primaries = indexNode.size();
                int replicas = 0;
                Double forecastWriteLoad = null;
                Long forecastShardSizeInBytes = null;
                String tier = null;

                var indexMetadataBuilder = IndexMetadata.builder(index.getName())
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT).build());

                for (var shardEntry : indexNode.entrySet()) {
                    var shardId = new ShardId(index, Integer.parseInt(shardEntry.getKey()));
                    var shardNode = (Map<String, Object>) shardEntry.getValue();

                    var shardCopies = (List<Map<String, Object>>) shardNode.get("current");
                    var desiredNode = (Map<String, Object>) shardNode.get("desired");

                    replicas = shardCopies.size() - 1;
                    if (shardNode.containsKey("forecast_write_load")) {
                        forecastWriteLoad = (Double) shardNode.get("forecast_write_load");
                    }
                    if (shardNode.containsKey("forecast_shard_size_in_bytes")) {
                        forecastShardSizeInBytes = (Long) shardNode.get("forecast_shard_size_in_bytes");
                    }
                    if (shardNode.containsKey("tier")) {
                        tier = (String) shardNode.get("tier");
                    }

                    var inSyncIds = new HashSet<String>();
                    for (Map<String, Object> shardCopy : shardCopies) {

                        String node = (String) shardCopy.get("node");
                        if (discoveryNodesBuilder.get(node) == null) {
                            discoveryNodesBuilder.add(newNode(node, node, MASTER_DATA_ROLES));
                        }
                        String relocatingNode = (String) shardCopy.get("relocating_node");
                        if (relocatingNode != null && discoveryNodesBuilder.get(relocatingNode) == null) {
                            discoveryNodesBuilder.add(newNode(relocatingNode, relocatingNode, MASTER_DATA_ROLES));
                        }

                        ShardRouting shardRouting = TestShardRouting.newShardRouting(
                            shardId,
                            node,
                            relocatingNode,
                            (boolean) shardCopy.get("primary"),
                            ShardRoutingState.valueOf((String) shardCopy.get("state"))
                        );
                        indexRoutingTableBuilder.addShard(shardRouting);
                        inSyncIds.add(shardRouting.allocationId().getId());
                    }
                    indexMetadataBuilder.putInSyncAllocationIds(shardId.id(), inSyncIds);
                }

                indexMetadataBuilder.numberOfShards(primaries).numberOfReplicas(replicas);
                if (forecastWriteLoad != null) {
                    indexMetadataBuilder.indexWriteLoadForecast(forecastWriteLoad);
                } else if (writeLoadForecast.containsKey(index.getName())) {
                    indexMetadataBuilder.indexWriteLoadForecast(writeLoadForecast.get(index.getName()));
                }
                if (forecastShardSizeInBytes != null) {
                    indexMetadataBuilder.shardSizeInBytesForecast(forecastShardSizeInBytes);
                } else if (shardSizeForecast.containsKey(index.getName())) {
                    indexMetadataBuilder.shardSizeInBytesForecast(shardSizeForecast.get(index.getName()));
                }
                if (tier != null) {
                    // indexMetadataBuilder.applySettingUpdate(builder -> builder.put(DataTier.TIER_PREFERENCE_SETTING.getKey(), tier));
                } else if (tierPrefference.containsKey(index.getName())) {
                    indexMetadataBuilder.applySettingUpdate(
                        builder -> builder.put(DataTier.TIER_PREFERENCE_SETTING.getKey(), tierPrefference.get(index.getName()))
                    );
                }

                metadataBuilder.put(indexMetadataBuilder.build(), false);
                routingTableBuilder.add(indexRoutingTableBuilder);
            }

            var clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadataBuilder)
                .routingTable(routingTableBuilder)
                .nodes(discoveryNodesBuilder)
                .build();

            return new Data(
                clusterState,
                new ClusterInfo(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of(), Map.of(), Map.of())
            );
        }
    }

    private static Object get(Object json, String... path) {
        for (String key : path) {
            json = ((Map<String, Object>) json).get(key);
            if (json == null) {
                return null;
            }
        }
        return json;
    }

    private ClusterBalanceStats stats(Data data) {
        return stats(data.clusterState, data.clusterInfo);
    }

    private ClusterBalanceStats stats(ClusterState clusterState, ClusterInfo clusterInfo) {
        return ClusterBalanceStats.createFrom(clusterState, clusterInfo, WriteLoadForecaster.DEFAULT);
    }
}
