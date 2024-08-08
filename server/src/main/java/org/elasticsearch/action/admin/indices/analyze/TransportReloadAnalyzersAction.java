/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.PlainShardsIterator;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Indices clear cache action.
 */
public class TransportReloadAnalyzersAction extends TransportBroadcastByNodeAction<
    ReloadAnalyzersRequest,
    ReloadAnalyzersResponse,
    TransportReloadAnalyzersAction.ReloadResult> {

    public static final ActionType<ReloadAnalyzersResponse> TYPE = new ActionType<>("indices:admin/reload_analyzers");
    private static final Logger logger = LogManager.getLogger(TransportReloadAnalyzersAction.class);
    private final IndicesService indicesService;

    @Inject
    public TransportReloadAnalyzersAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ReloadAnalyzersRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT),
            false
        );
        this.indicesService = indicesService;
    }

    @Override
    protected ReloadResult readShardResult(StreamInput in) throws IOException {
        return new ReloadResult(in);
    }

    @Override
    protected ResponseFactory<ReloadAnalyzersResponse, ReloadResult> getResponseFactory(
        ReloadAnalyzersRequest request,
        ClusterState clusterState
    ) {
        return (totalShards, successfulShards, failedShards, responses, shardFailures) -> {
            Map<String, ReloadAnalyzersResponse.ReloadDetails> reloadedIndicesDetails = new HashMap<>();
            // if the request was to reload for a specific resource, we only want to return the details for that resource
            boolean includeEmptyReloadDetails = request.resource() == null;
            for (ReloadResult result : responses) {
                if (reloadedIndicesDetails.containsKey(result.index)) {
                    reloadedIndicesDetails.get(result.index).merge(result);
                } else {
                    if (result.reloadedSearchAnalyzers.isEmpty() && includeEmptyReloadDetails == false) {
                        continue;
                    }
                    HashSet<String> nodeIds = new HashSet<>();
                    nodeIds.add(result.nodeId);
                    ReloadAnalyzersResponse.ReloadDetails details = new ReloadAnalyzersResponse.ReloadDetails(
                        result.index,
                        nodeIds,
                        new HashSet<>(result.reloadedSearchAnalyzers)
                    );
                    reloadedIndicesDetails.put(result.index, details);
                }
            }
            return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, shardFailures, reloadedIndicesDetails);
        };
    }

    @Override
    protected ReloadAnalyzersRequest readRequestFrom(StreamInput in) throws IOException {
        return new ReloadAnalyzersRequest(in);
    }

    @Override
    protected void shardOperation(
        ReloadAnalyzersRequest request,
        ShardRouting shardRouting,
        Task task,
        ActionListener<ReloadResult> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            logger.info("reloading analyzers for index shard " + shardRouting);
            IndexService indexService = indicesService.indexService(shardRouting.index());
            List<String> reloadedSearchAnalyzers = indexService.mapperService()
                .reloadSearchAnalyzers(indicesService.getAnalysis(), request.resource(), request.preview());
            return new ReloadResult(shardRouting.index().getName(), shardRouting.currentNodeId(), reloadedSearchAnalyzers);
        });
    }

    static final class ReloadResult implements Writeable {
        String index;
        String nodeId;
        List<String> reloadedSearchAnalyzers;

        private ReloadResult(String index, String nodeId, List<String> reloadedSearchAnalyzers) {
            this.index = index;
            this.nodeId = nodeId;
            this.reloadedSearchAnalyzers = reloadedSearchAnalyzers;
        }

        private ReloadResult(StreamInput in) throws IOException {
            this.index = in.readString();
            this.nodeId = in.readString();
            this.reloadedSearchAnalyzers = in.readStringCollectionAsList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(nodeId);
            out.writeStringCollection(this.reloadedSearchAnalyzers);
        }
    }

    /**
     * The reload request should go to only one shard per node the index lives on
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ReloadAnalyzersRequest request, String[] concreteIndices) {
        RoutingTable routingTable = clusterState.routingTable();
        List<ShardRouting> shards = new ArrayList<>();
        for (String index : concreteIndices) {
            Set<String> nodesCovered = new HashSet<>();
            IndexRoutingTable indexRoutingTable = routingTable.index(index);
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                    if (nodesCovered.contains(shardRouting.currentNodeId()) == false) {
                        shards.add(shardRouting);
                        nodesCovered.add(shardRouting.currentNodeId());
                    }
                }
            }
        }
        return new PlainShardsIterator(shards);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ReloadAnalyzersRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ReloadAnalyzersRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
