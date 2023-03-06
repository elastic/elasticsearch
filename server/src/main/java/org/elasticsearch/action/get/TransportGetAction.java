/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the get operation.
 */
public class TransportGetAction extends TransportSingleShardAction<GetRequest, GetResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetAction.class);

    private final IndicesService indicesService;
    private final ExecutorSelector executorSelector;

    @Inject
    public TransportGetAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ExecutorSelector executorSelector
    ) {
        super(
            GetAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            GetRequest::new,
            ThreadPool.Names.GET
        );
        this.indicesService = indicesService;
        this.executorSelector = executorSelector;
        // register the internal TransportGetFromTranslogAction
        new TransportGetFromTranslogAction(transportService, indicesService, actionFilters);
    }

    @Override
    protected boolean resolveIndex(GetRequest request) {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        final ShardIterator iterator = clusterService.operationRouting()
            .getShards(
                clusterService.state(),
                request.concreteIndex(),
                request.request().id(),
                request.request().routing(),
                request.request().preference()
            );
        if (iterator == null) {
            return null;
        }
        return new PlainShardIterator(iterator.shardId(), iterator.getShardRoutings().stream().filter(ShardRouting::isSearchable).toList());
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        // update the routing (request#index here is possibly an alias)
        request.request().routing(state.metadata().resolveIndexRouting(request.request().routing(), request.request().index()));
    }

    @Override
    protected void asyncShardOperation(GetRequest request, ShardId shardId, ActionListener<GetResponse> listener) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        var shardRouting = clusterService.state().getRoutingNodes().node(clusterService.localNode().getId()).getByShardId(shardId);
        if (shardRouting == null) {
            listener.onFailure(new ShardNotFoundException(shardId, "shard is no longer assigned to current node"));
            return;
        }
        // A TransportGetAction always goes to a search shard in Stateless.
        if (shardRouting.isPromotableToPrimary() == false) {
            handleGetOnUnpromotableShard(request, indexShard, listener);
        } else {
            assert DiscoveryNode.isStateless(clusterService.getSettings()) == false;
            if (request.realtime()) { // we are not tied to a refresh cycle here anyway
                super.asyncShardOperation(request, shardId, listener);
            } else {
                indexShard.awaitShardSearchActive(b -> {
                    try {
                        super.asyncShardOperation(request, shardId, listener);
                    } catch (Exception ex) {
                        listener.onFailure(ex);
                    }
                });
            }
        }
    }

    private void handleGetOnUnpromotableShard(GetRequest request, IndexShard indexShard, ActionListener<GetResponse> listener)
        throws IOException {
        ShardId shardId = indexShard.shardId();
        var node = clusterService.state()
            .nodes()
            .get(clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard().currentNodeId());
        if (request.refresh()) {
            logger.trace("send refresh action for shard {} to node {}", shardId, node.getId());
            var refreshRequest = new BasicReplicationRequest(shardId);
            refreshRequest.setParentTask(request.getParentTask());
            transportService.sendRequest(
                node,
                TransportShardRefreshAction.NAME,
                refreshRequest,
                new ActionListenerResponseHandler<>(listener.map(t -> shardOperation(request, shardId)), ReplicationResponse::new)
            );
        } else if (request.realtime()) {
            transportService.sendRequest(
                node,
                TransportGetFromTranslogAction.NAME,
                request,
                new TransportResponseHandler<TransportGetFromTranslogAction.Response>() {
                    @Override
                    public void handleResponse(TransportGetFromTranslogAction.Response response) {
                        if (response.getResult() != null) {
                            logger.trace("received result for real-time get for id '{}' from promotable shard", request.id());
                            listener.onResponse(new GetResponse(response.getResult()));
                        } else {
                            assert response.segmentGeneration() > -1L;
                            logger.trace(
                                "no result for real-time get for id '{}' from promotable shard (segment generation to wait for: {})",
                                request.id(),
                                response.segmentGeneration()
                            );
                            indexShard.waitForSegmentGeneration(
                                response.segmentGeneration(),
                                listener.map(aLong -> shardOperation(request, shardId))
                            );
                        }
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GET;
                    }

                    @Override
                    public void handleException(TransportException e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public TransportGetFromTranslogAction.Response read(StreamInput in) throws IOException {
                        return new TransportGetFromTranslogAction.Response(in);
                    }
                }
            );
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    @Override
    protected GetResponse shardOperation(GetRequest request, ShardId shardId) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());

        if (request.refresh() && request.realtime() == false) {
            indexShard.refresh("refresh_flag_get");
        }

        GetResult result = indexShard.getService()
            .get(
                request.id(),
                request.storedFields(),
                request.realtime(),
                request.version(),
                request.versionType(),
                request.fetchSourceContext(),
                request.isForceSyntheticSource()
            );
        return new GetResponse(result);
    }

    @Override
    protected Writeable.Reader<GetResponse> getResponseReader() {
        return GetResponse::new;
    }

    @Override
    protected String getExecutor(GetRequest request, ShardId shardId) {
        final ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().getIndexSafe(shardId.getIndex()).isSystem()) {
            return executorSelector.executorForGet(shardId.getIndexName());
        } else if (indicesService.indexServiceSafe(shardId.getIndex()).getIndexSettings().isSearchThrottled()) {
            return ThreadPool.Names.SEARCH_THROTTLED;
        } else {
            return super.getExecutor(request, shardId);
        }
    }
}
