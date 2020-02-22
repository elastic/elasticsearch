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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.ReloadAnalyzersResponse.ReloadDetails;
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
        return new ReloadResult(in);
    }

    @Override
    protected ReloadAnalyzersResponse newResponse(ReloadAnalyzersRequest request, int totalShards, int successfulShards, int failedShards,
            List<ReloadResult> responses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        Map<String, ReloadDetails> reloadedIndicesDetails = new HashMap<String, ReloadDetails>();
        for (ReloadResult result : responses) {
            if (reloadedIndicesDetails.containsKey(result.index)) {
                reloadedIndicesDetails.get(result.index).merge(result);;
            } else {
                HashSet<String> nodeIds = new HashSet<String>();
                nodeIds.add(result.nodeId);
                ReloadDetails details = new ReloadDetails(result.index, nodeIds, new HashSet<String>(result.reloadedSearchAnalyzers));
                reloadedIndicesDetails.put(result.index, details);
            }
        }
        return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, shardFailures, reloadedIndicesDetails);
    }

    @Override
    protected ReloadAnalyzersRequest readRequestFrom(StreamInput in) throws IOException {
        return new ReloadAnalyzersRequest(in);
    }

    @Override
    protected ReloadResult shardOperation(ReloadAnalyzersRequest request, ShardRouting shardRouting) throws IOException {
        logger.info("reloading analyzers for index shard " + shardRouting);
        IndexService indexService = indicesService.indexService(shardRouting.index());
        List<String> reloadedSearchAnalyzers = indexService.mapperService().reloadSearchAnalyzers(indicesService.getAnalysis());
        return new ReloadResult(shardRouting.index().getName(), shardRouting.currentNodeId(), reloadedSearchAnalyzers);
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
            this.reloadedSearchAnalyzers = in.readStringList();
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
