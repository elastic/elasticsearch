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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
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
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

public class TransportShardMultiGetAction extends TransportSingleShardAction<MultiGetShardRequest, MultiGetShardResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardMultiGetAction.class);

    private static final String ACTION_NAME = MultiGetAction.NAME + "[shard]";
    public static final ActionType<MultiGetShardResponse> TYPE = new ActionType<>(ACTION_NAME, MultiGetShardResponse::new);

    private final IndicesService indicesService;
    private final ExecutorSelector executorSelector;

    @Inject
    public TransportShardMultiGetAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ExecutorSelector executorSelector
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            MultiGetShardRequest::new,
            ThreadPool.Names.GET
        );
        this.indicesService = indicesService;
        this.executorSelector = executorSelector;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<MultiGetShardResponse> getResponseReader() {
        return MultiGetShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(MultiGetShardRequest request) {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        ShardIterator iterator = clusterService.operationRouting()
            .getShards(state, request.request().index(), request.request().shardId(), request.request().preference());
        if (iterator == null) {
            return null;
        }
        return new PlainShardIterator(iterator.shardId(), iterator.getShardRoutings().stream().filter(ShardRouting::isSearchable).toList());
    }

    @Override
    protected void asyncShardOperation(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        var shardRouting = clusterService.state().getRoutingNodes().node(clusterService.localNode().getId()).getByShardId(shardId);
        if (shardRouting == null) {
            listener.onFailure(new ShardNotFoundException(shardId, "shard is no longer assigned to current node"));
            return;
        }
        // A TransportShardMultiGetAction always goes to a search shard in Stateless.
        if (shardRouting.isPromotableToPrimary() == false) {
            handleMultiGetOnUnpromotableShard(request, indexShard, listener);
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

    private void handleMultiGetOnUnpromotableShard(
        MultiGetShardRequest request,
        IndexShard indexShard,
        ActionListener<MultiGetShardResponse> listener
    ) throws IOException {
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
                new ActionListenerResponseHandler<>(
                    listener.map(t -> shardOperation(request, shardId)),
                    ReplicationResponse::new,
                    ThreadPool.Names.GET
                )
            );
        } else if (request.realtime()) {
            transportService.sendRequest(
                node,
                TransportGetFromTranslogAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener.delegateFailure((l, r) -> {
                    if (r.segmentGeneration() > -1L) {
                        // wait for the segment generation and fill in the gets that were not in the translog
                        logger.trace(
                            "couldn't get all IDs from the promotable shard (segment generation to wait for: {})",
                            r.segmentGeneration()
                        );
                        assert r.segmentGeneration() > -1L;
                        indexShard.waitForSegmentGeneration(
                            r.segmentGeneration(),
                            l.map(aLong -> handleLocalGets(request, r.multiGetShardResponse(), shardId))
                        );
                    } else {
                        logger.trace("received result for real-time mget from promotable shard");
                        l.onResponse(r.multiGetShardResponse());
                    }
                }), TransportGetFromTranslogAction.Response::new, ThreadPool.Names.GET)
            );
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    private MultiGetShardResponse handleLocalGets(MultiGetShardRequest request, MultiGetShardResponse response, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        for (int i = 0; i < request.locations.size(); i++) {
            var r = response.responses.get(i);
            var f = response.failures.get(i);
            if (r == null && f == null) {
                getItemIntoResponse(request, response, i, indexShard, shardId);
            }
        }
        return response;
    }

    @Override
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());

        if (request.refresh() && request.realtime() == false) {
            indexShard.refresh("refresh_flag_mget");
        }

        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            getItemIntoResponse(request, response, i, indexShard, shardId);
        }

        return response;
    }

    private void getItemIntoResponse(
        MultiGetShardRequest request,
        MultiGetShardResponse response,
        int i,
        IndexShard indexShard,
        ShardId shardId
    ) {
        MultiGetRequest.Item item = request.items.get(i);
        try {
            GetResult getResult = indexShard.getService()
                .get(
                    item.id(),
                    item.storedFields(),
                    request.realtime(),
                    item.version(),
                    item.versionType(),
                    item.fetchSourceContext(),
                    request.isForceSyntheticSource()
                );
            response.add(request.locations.get(i), new GetResponse(getResult));
        } catch (RuntimeException e) {
            if (TransportActions.isShardNotAvailableException(e)) {
                throw e;
            } else {
                logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
                response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.id(), e));
            }
        } catch (IOException e) {
            logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
            response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.id(), e));
        }
    }

    @Override
    protected String getExecutor(MultiGetShardRequest request, ShardId shardId) {
        final ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().index(shardId.getIndex()).isSystem()) {
            return executorSelector.executorForGet(shardId.getIndexName());
        } else if (indicesService.indexServiceSafe(shardId.getIndex()).getIndexSettings().isSearchThrottled()) {
            return ThreadPool.Names.SEARCH_THROTTLED;
        } else {
            return super.getExecutor(request, shardId);
        }
    }
}
