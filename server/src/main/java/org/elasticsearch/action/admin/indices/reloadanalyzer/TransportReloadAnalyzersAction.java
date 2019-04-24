/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.reloadanalyzer;

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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Indices clear cache action.
 */
public class TransportReloadAnalyzersAction extends TransportBroadcastByNodeAction<ReloadAnalyzersRequest, ReloadAnalyzersResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportReloadAnalyzersAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ReloadAnalyzerAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                ReloadAnalyzersRequest::new, ThreadPool.Names.MANAGEMENT, false);
        this.indicesService = indicesService;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected ReloadAnalyzersResponse newResponse(ReloadAnalyzersRequest request, int totalShards, int successfulShards, int failedShards,
            List<EmptyResult> responses, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new ReloadAnalyzersResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ReloadAnalyzersRequest readRequestFrom(StreamInput in) throws IOException {
        final ReloadAnalyzersRequest request = new ReloadAnalyzersRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected EmptyResult shardOperation(ReloadAnalyzersRequest request, ShardRouting shardRouting) throws IOException {
        logger.info("reloading analyzers for index shard " + shardRouting);
        IndexService indexService = indicesService.indexService(shardRouting.index());
        indexService.mapperService().reloadSearchAnalyzers(indicesService.getAnalysis());
        return EmptyResult.INSTANCE;
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
        logger.info("Determined shards for reloading: " + shards);
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
