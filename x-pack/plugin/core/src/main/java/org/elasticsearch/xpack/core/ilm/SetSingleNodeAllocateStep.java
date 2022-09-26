/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeReplacementAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeShutdownAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Allocates all shards in a single index to one node.
 * For example, as preparation for shrinking that index.
 */
public class SetSingleNodeAllocateStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(SetSingleNodeAllocateStep.class);
    public static final String NAME = "set-single-node-allocation";

    public SetSingleNodeAllocateStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState clusterState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        // These allocation deciders were chosen because these are the conditions that can prevent
        // allocation long-term, and that we can inspect in advance. Most other allocation deciders
        // will either only delay relocation (e.g. ThrottlingAllocationDecider), or don't work very
        // well when reallocating potentially many shards at once (e.g. DiskThresholdDecider)
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            List.of(
                new FilterAllocationDecider(
                    clusterState.getMetadata().settings(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                ),
                DataTierAllocationDecider.INSTANCE,
                new NodeVersionAllocationDecider(),
                new NodeShutdownAllocationDecider(),
                new NodeReplacementAllocationDecider()
            )
        );

        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, null, null, System.nanoTime());
        Set<String> validNodeIds = new HashSet<>();
        String indexName = indexMetadata.getIndex().getName();
        final Map<ShardId, List<ShardRouting>> routingsByShardId = clusterState.getRoutingTable()
            .allShards(indexName)
            .stream()
            .collect(Collectors.groupingBy(ShardRouting::shardId));

        if (routingsByShardId.isEmpty() == false) {
            List<String> dataNodeIds = new ArrayList<>();
            for (RoutingNode node : allocation.routingNodes()) {
                boolean canAllocateOneCopyOfEachShard = routingsByShardId.values()
                    .stream()
                    .allMatch(
                        shardRoutings -> shardRoutings.stream()
                            .map(shardRouting -> allocationDeciders.canAllocate(shardRouting, node, allocation).type())
                            .anyMatch(Decision.Type.YES::equals)
                    );
                if (canAllocateOneCopyOfEachShard) {
                    dataNodeIds.add(node.node().getId());
                }
            }

            if (dataNodeIds.size() == 0) {
                logger.debug("could not find any nodes to allocate each shard's copy of the index [{}] onto prior to shrink", indexName);
                listener.onFailure(
                    new NoNodeAvailableException(
                        "could not find any nodes to allocate each shard's copy of the index [" + indexName + "] onto prior to shrink"
                    )
                );
                return;
            }

            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(dataNodeIds.toArray(new String[0])).clear()
                .addMetric(NodesStatsRequest.Metric.FS.metricName())
                .indices(new CommonStatsFlags(CommonStatsFlags.Flag.Store));
            getClient().admin().cluster().nodesStats(nodesStatsRequest, ActionListener.wrap((nodesStatsResponse) -> {
                final Map<String, Long> nodeShardsStorageBytes = new HashMap<>();

                Map<String, NodeStats> nodeStatsMap = nodesStatsResponse.getNodesMap();
                for (String nodeId : dataNodeIds) {
                    if (nodeStatsMap.get(nodeId) != null) {
                        List<IndexShardStats> indexShardStatsList = nodeStatsMap.get(nodeId)
                            .getIndices()
                            .getShardStats(indexMetadata.getIndex());
                        long shardsOnCurrentNodeStorageBytes = indexShardStatsList.stream()
                            .mapToLong(
                                indexShardStats -> Arrays.stream(indexShardStats.getShards())
                                    .mapToLong(
                                        shardStats -> shardStats.getStats().getStore() == null
                                            ? 0
                                            : shardStats.getStats().getStore().getSizeInBytes()
                                    )
                                    .sum()
                            )
                            .sum();
                        if (shardsOnCurrentNodeStorageBytes != 0) {
                            nodeShardsStorageBytes.put(nodeId, shardsOnCurrentNodeStorageBytes);
                        }
                    }
                }
                long indexPrimaryShardsStorageBytes = nodeShardsStorageBytes.values().stream().mapToLong(Long::longValue).sum();
                if (indexMetadata.getNumberOfReplicas() != 0) {
                    indexPrimaryShardsStorageBytes /= indexMetadata.getNumberOfReplicas();
                }

                DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(
                    clusterState.getMetadata().settings(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                );
                for (String nodeId : dataNodeIds) {
                    if (nodeStatsMap.get(nodeId) != null) {
                        FsInfo.Path totalFsInfo = nodeStatsMap.get(nodeId).getFs().getTotal();
                        long nodeAvailableBytes = totalFsInfo.getAvailable().getBytes();
                        long freeBytesThresholdLow = diskThresholdSettings.getFreeBytesThresholdLowStage(totalFsInfo.getTotal()).getBytes();
                        // we should make sure that allocating one copy of the index's primary shards to that node doesn't put the node
                        // above the low watermark, if not, the new shrunken index may not be initialized successfully
                        long shardsOnCurrentNodeStorageBytes = 0;
                        if (nodeShardsStorageBytes.containsKey(nodeId)) {
                            shardsOnCurrentNodeStorageBytes = nodeShardsStorageBytes.get(nodeId);
                        }
                        if (nodeAvailableBytes > freeBytesThresholdLow + indexPrimaryShardsStorageBytes - shardsOnCurrentNodeStorageBytes) {
                            validNodeIds.add(nodeId);
                        }
                    }
                }

                if (validNodeIds.size() == 0) {
                    logger.debug("no nodes have enough disk space to hold one copy of the index [{}] onto prior to shrink ", indexName);
                    listener.onFailure(
                        new NoNodeAvailableException(
                            "no nodes have enough disk space to hold one copy of the index [" + indexName + "] onto prior to shrink"
                        )
                    );
                    return;
                }

                Optional<String> nodeId = selectSingleNode(indexName, validNodeIds, nodeShardsStorageBytes);

                if (nodeId.isPresent()) {
                    Settings settings = Settings.builder()
                        .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", nodeId.get())
                        .putNull(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey())
                        .build();
                    UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName).masterNodeTimeout(
                        TimeValue.MAX_VALUE
                    ).settings(settings);
                    getClient().admin()
                        .indices()
                        .updateSettings(
                            updateSettingsRequest,
                            ActionListener.wrap(response -> listener.onResponse(null), listener::onFailure)
                        );
                } else {
                    // No nodes currently match the allocation rules or have no enough disk bytes,
                    // so report this as an error and we'll retry
                    logger.debug("could not find any nodes to allocate index [{}] onto prior to shrink", indexName);
                    listener.onFailure(
                        new NoNodeAvailableException("could not find any nodes to allocate index [" + indexName + "] onto prior to shrink")
                    );
                }
            },
                (Exception e) -> listener.onFailure(
                    new NoNodeAvailableException(
                        "failed to retrieve disk information" + " to select a single node for shard copy allocation"
                    )
                )
            ));
        } else {
            // There are no shards for the index, the index might be gone. Even though this is a retryable step ILM will not retry in
            // this case as we're using the periodic loop to trigger the retries and that is run over *existing* indices.
            listener.onFailure(new IndexNotFoundException(indexMetadata.getIndex()));
        }
    }

    Optional<String> selectSingleNode(String indexName, Set<String> validNodeIds, Map<String, Long> nodeShardsStorageBytes) {
        List<Map.Entry<String, Long>> nodeShardsStorageList = new ArrayList<>(nodeShardsStorageBytes.entrySet());
        nodeShardsStorageList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        Optional<String> nodeId = Optional.empty();
        for (Map.Entry<String, Long> entry : nodeShardsStorageList) {
            // we prefer to select the node which contains the maximum shards storage bytes of the index from the valid node list
            if (validNodeIds.contains(entry.getKey())) {
                nodeId = Optional.of(entry.getKey());
                break;
            }
        }

        // if we cannot find a node which contains any shard of the index,
        // shuffle the valid node list and select randomly
        if (nodeId.isEmpty()) {
            logger.debug(
                "could not find any valid nodes which contain any shard of the index [{}],"
                    + " shuffle the valid node list and select randomly",
                indexName
            );
            List<String> list = new ArrayList<>(validNodeIds);
            Randomness.shuffle(list);
            nodeId = list.stream().findFirst();
        }
        return nodeId;
    }
}
