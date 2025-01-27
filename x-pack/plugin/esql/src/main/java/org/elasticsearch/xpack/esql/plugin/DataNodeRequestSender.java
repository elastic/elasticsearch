/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Handles computes within a single cluster by dispatching {@link DataNodeRequest} to data nodes
 * and executing these computes on the data nodes.
 */
final class DataNodeRequestSender {
    private final TransportService transportService;
    private final Executor esqlExecutor;
    private final Sender sender;

    DataNodeRequestSender(TransportService transportService, Executor esqlExecutor, Sender sender) {
        this.transportService = transportService;
        this.esqlExecutor = esqlExecutor;
        this.sender = sender;
    }

    void startComputeOnDataNodes(
        String clusterAlias,
        CancellableTask parentTask,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        QueryBuilder requestFilter,
        Runnable runOnTaskFailure,
        ActionListener<ComputeResponse> listener
    ) {
        final long startTimeInNanos = System.nanoTime();
        lookupDataNodes(parentTask, clusterAlias, requestFilter, concreteIndices, originalIndices, ActionListener.wrap(dataNodeResult -> {
            try (var computeListener = new ComputeListener(transportService.getThreadPool(), runOnTaskFailure, listener.map(profiles -> {
                TimeValue took = TimeValue.timeValueNanos(System.nanoTime() - startTimeInNanos);
                return new ComputeResponse(
                    profiles,
                    took,
                    dataNodeResult.totalShards(),
                    dataNodeResult.totalShards(),
                    dataNodeResult.skippedShards(),
                    0
                );
            }))) {
                for (DataNode node : dataNodeResult.dataNodes()) {
                    sender.sendRequestToOneNode(
                        node.connection,
                        node.shardIds,
                        node.aliasFilters,
                        computeListener.acquireCompute().map(DataNodeComputeResponse::profiles)
                    );

                }
            }
        }, listener::onFailure));
    }

    interface Sender {
        void sendRequestToOneNode(
            Transport.Connection connection,
            List<ShardId> shardIds,
            Map<Index, AliasFilter> aliasFilters,
            ActionListener<DataNodeComputeResponse> listener
        );
    }

    record DataNode(Transport.Connection connection, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters) {

    }

    /**
     * Result from lookupDataNodes where can_match is performed to determine what shards can be skipped
     * and which target nodes are needed for running the ES|QL query
     *
     * @param dataNodes     list of DataNode to perform the ES|QL query on
     * @param totalShards   Total number of shards (from can_match phase), including skipped shards
     * @param skippedShards Number of skipped shards (from can_match phase)
     */
    record DataNodeResult(List<DataNode> dataNodes, int totalShards, int skippedShards) {}

    /**
     * Performs can_match and find the target nodes for the given target indices and filter.
     * <p>
     * Ideally, the search_shards API should be called before the field-caps API; however, this can lead
     * to a situation where the column structure (i.e., matched data types) differs depending on the query.
     */
    private void lookupDataNodes(
        Task parentTask,
        String clusterAlias,
        QueryBuilder filter,
        Set<String> concreteIndices,
        OriginalIndices originalIndices,
        ActionListener<DataNodeResult> listener
    ) {
        ActionListener<SearchShardsResponse> searchShardsListener = listener.map(resp -> {
            Map<String, DiscoveryNode> nodes = new HashMap<>();
            for (DiscoveryNode node : resp.getNodes()) {
                nodes.put(node.getId(), node);
            }
            Map<String, List<ShardId>> nodeToShards = new HashMap<>();
            Map<String, Map<Index, AliasFilter>> nodeToAliasFilters = new HashMap<>();
            int totalShards = 0;
            int skippedShards = 0;
            for (SearchShardsGroup group : resp.getGroups()) {
                var shardId = group.shardId();
                if (group.allocatedNodes().isEmpty()) {
                    throw new ShardNotFoundException(group.shardId(), "no shard copies found {}", group.shardId());
                }
                if (concreteIndices.contains(shardId.getIndexName()) == false) {
                    continue;
                }
                totalShards++;
                if (group.skipped()) {
                    skippedShards++;
                    continue;
                }
                String targetNode = group.allocatedNodes().get(0);
                nodeToShards.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(shardId);
                AliasFilter aliasFilter = resp.getAliasFilters().get(shardId.getIndex().getUUID());
                if (aliasFilter != null) {
                    nodeToAliasFilters.computeIfAbsent(targetNode, k -> new HashMap<>()).put(shardId.getIndex(), aliasFilter);
                }
            }
            List<DataNode> dataNodes = new ArrayList<>(nodeToShards.size());
            for (Map.Entry<String, List<ShardId>> e : nodeToShards.entrySet()) {
                DiscoveryNode node = nodes.get(e.getKey());
                Map<Index, AliasFilter> aliasFilters = nodeToAliasFilters.getOrDefault(e.getKey(), Map.of());
                dataNodes.add(new DataNode(transportService.getConnection(node), e.getValue(), aliasFilters));
            }
            return new DataNodeResult(dataNodes, totalShards, skippedShards);
        });
        SearchShardsRequest searchShardsRequest = new SearchShardsRequest(
            originalIndices.indices(),
            originalIndices.indicesOptions(),
            filter,
            null,
            null,
            false,
            clusterAlias
        );
        transportService.sendChildRequest(
            transportService.getLocalNode(),
            EsqlSearchShardsAction.TYPE.name(),
            searchShardsRequest,
            parentTask,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(searchShardsListener, SearchShardsResponse::new, esqlExecutor)
        );
    }
}
