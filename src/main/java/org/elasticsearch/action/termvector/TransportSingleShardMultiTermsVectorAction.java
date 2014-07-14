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

package org.elasticsearch.action.termvector;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportSingleShardMultiTermsVectorAction extends TransportShardSingleOperationAction<MultiTermVectorsShardRequest, MultiTermVectorsShardResponse> {

    private final IndicesService indicesService;

    private static final String ACTION_NAME = MultiTermVectorsAction.NAME + "/shard";

    @Inject
    public TransportSingleShardMultiTermsVectorAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                                      IndicesService indicesService, ThreadPool threadPool) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GET;
    }

    @Override
    protected MultiTermVectorsShardRequest newRequest() {
        return new MultiTermVectorsShardRequest();
    }

    @Override
    protected MultiTermVectorsShardResponse newResponse() {
        return new MultiTermVectorsShardResponse();
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, MultiTermVectorsShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, MultiTermVectorsShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState state, MultiTermVectorsShardRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.shardId(), request.preference());
    }

    @Override
    protected void resolveRequest(ClusterState state, MultiTermVectorsShardRequest request) {
        // no need to set concrete index and routing here, it has already been set by the multi term vectors action on the item
        // request.index(state.metaData().concreteIndex(request.index()));
    }

    @Override
    protected MultiTermVectorsShardResponse shardOperation(MultiTermVectorsShardRequest request, int shardId) throws ElasticsearchException {

        MultiTermVectorsShardResponse response = new MultiTermVectorsShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            TermVectorRequest termVectorRequest = request.requests.get(i);

            try {
                IndexService indexService = indicesService.indexServiceSafe(request.index());
                IndexShard indexShard = indexService.shardSafe(shardId);
                TermVectorResponse termVectorResponse = indexShard.termVectorService().getTermVector(termVectorRequest);
                response.add(request.locations.get(i), termVectorResponse);
            } catch (Throwable t) {
                if (TransportActions.isShardNotAvailableException(t)) {
                    throw (ElasticsearchException) t;
                } else {
                    logger.debug("[{}][{}] failed to execute multi term vectors for [{}]/[{}]", t, request.index(), shardId, termVectorRequest.type(), termVectorRequest.id());
                    response.add(request.locations.get(i),
                            new MultiTermVectorsResponse.Failure(request.index(), termVectorRequest.type(), termVectorRequest.id(), ExceptionsHelper.detailedMessage(t)));
                }
            }
        }

        return response;
    }

}
