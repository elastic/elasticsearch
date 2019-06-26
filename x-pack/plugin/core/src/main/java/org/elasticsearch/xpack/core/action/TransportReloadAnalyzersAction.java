/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.TransportReloadAnalyzersAction.ReloadResult;

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
public class TransportReloadAnalyzersAction
        extends TransportBroadcastByNodeAction<ReloadAnalyzersRequest, ReloadAnalyzersResponse, ReloadResult> {

    private static final Logger logger = LogManager.getLogger(TransportReloadAnalyzersAction.class);
    private final IndicesService indicesService;

    @Inject
    public TransportReloadAnalyzersAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ReloadAnalyzerAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                ReloadAnalyzersRequest::new, ThreadPool.Names.MANAGEMENT, false);
        this.indicesService = indicesService;
    }

    @Override
    protected ReloadResult readShardResult(StreamInput in) throws IOException {
        ReloadResult reloadResult = new ReloadResult();
        reloadResult.readFrom(in);
        return reloadResult;
    }

    @Override
    protected ReloadAnalyzersResponse newResponse(ReloadAnalyzersRequest request, int totalShards, int successfulShards, int failedShards,
            List<ReloadResult> responses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        Map<String, List<String>> reloadedIndicesNodes = new HashMap<String, List<String>>();
        for (ReloadResult result : responses) {
            if (reloadedIndicesNodes.containsKey(result.index)) {
                List<String> nodes = reloadedIndicesNodes.get(result.index);
                nodes.add(result.nodeId);
            } else {
                List<String> nodes = new ArrayList<>();
                nodes.add(result.nodeId);
                reloadedIndicesNodes.put(result.index, nodes);
            }
        }
        return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, shardFailures, reloadedIndicesNodes);
    }

    @Override
    protected ReloadAnalyzersRequest readRequestFrom(StreamInput in) throws IOException {
        final ReloadAnalyzersRequest request = new ReloadAnalyzersRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected ReloadResult shardOperation(ReloadAnalyzersRequest request, ShardRouting shardRouting) throws IOException {
        logger.info("reloading analyzers for index shard " + shardRouting);
        IndexService indexService = indicesService.indexService(shardRouting.index());
        indexService.mapperService().reloadSearchAnalyzers(indicesService.getAnalysis());
        return new ReloadResult(shardRouting.index().getName(), shardRouting.currentNodeId());
    }

    public static final class ReloadResult implements Streamable {
        String index;
        String nodeId;

        private ReloadResult(String index, String nodeId) {
            this.index = index;
            this.nodeId = nodeId;
        }

        private ReloadResult() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.index = in.readString();
            this.nodeId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(nodeId);
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
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
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
