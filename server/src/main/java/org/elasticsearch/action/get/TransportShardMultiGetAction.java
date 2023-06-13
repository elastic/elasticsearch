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
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.internal.node.NodeClient;
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
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

public class TransportShardMultiGetAction extends TransportSingleShardAction<MultiGetShardRequest, MultiGetShardResponse> {

    private static final String ACTION_NAME = MultiGetAction.NAME + "[shard]";
    public static final ActionType<MultiGetShardResponse> TYPE = new ActionType<>(ACTION_NAME, MultiGetShardResponse::new);
    private static final Logger logger = LogManager.getLogger(TransportShardMultiGetAction.class);

    private final IndicesService indicesService;
    private final ExecutorSelector executorSelector;
    private final NodeClient client;

    @Inject
    public TransportShardMultiGetAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ExecutorSelector executorSelector,
        NodeClient client
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
        this.client = client;
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
        if (indexShard.routingEntry().isPromotableToPrimary() == false) {
            handleMultiGetOnUnpromotableShard(request, indexShard, listener);
            return;
        }
        assert DiscoveryNode.isStateless(clusterService.getSettings()) == false
            : "A TransportShardMultiGetAction should always be handled by a search shard in Stateless";
        if (request.realtime()) { // we are not tied to a refresh cycle here anyway
            asyncShardMultiGet(request, shardId, listener);
        } else {
            indexShard.awaitShardSearchActive(b -> {
                try {
                    asyncShardMultiGet(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    @Override
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, ShardId shardId) {
        var indexShard = getIndexShard(shardId);
        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
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

        return response;
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

    private void handleMultiGetOnUnpromotableShard(
        MultiGetShardRequest request,
        IndexShard indexShard,
        ActionListener<MultiGetShardResponse> listener
    ) throws IOException {
        ShardId shardId = indexShard.shardId();
        var node = getCurrentNodeOfPrimary(shardId);
        if (request.refresh()) {
            logger.trace("send refresh action for shard {} to node {}", shardId, node.getId());
            var refreshRequest = new BasicReplicationRequest(shardId);
            refreshRequest.setParentTask(request.getParentTask());
            client.executeLocally(
                TransportShardRefreshAction.TYPE,
                refreshRequest,
                listener.delegateFailureAndWrap((l, replicationResponse) -> super.asyncShardOperation(request, shardId, l))
            );
        } else if (request.realtime()) {
            TransportShardMultiGetFomTranslogAction.Request getFromTranslogRequest = new TransportShardMultiGetFomTranslogAction.Request(
                request,
                shardId
            );
            getFromTranslogRequest.setParentTask(request.getParentTask());
            transportService.sendRequest(
                node,
                TransportShardMultiGetFomTranslogAction.NAME,
                getFromTranslogRequest,
                new ActionListenerResponseHandler<>(listener.delegateFailure((l, r) -> {
                    var missingLocations = locationsWithMissingResults(r);
                    if (missingLocations.isEmpty()) {
                        logger.debug("received result of all ids in real-time mget[shard] from the promotable shard.");
                        l.onResponse(r.multiGetShardResponse());
                    } else {
                        logger.debug(
                            () -> format(
                                "no result for ids '%s' from the promotable shard (segment generation to wait for: %s)",
                                missingLocations.stream().map(i -> request.items.get(i).id()).toList(),
                                r.segmentGeneration()
                            )
                        );
                        if (r.segmentGeneration() == -1) {
                            // Nothing to wait for (no previous unsafe generation), just handle the rest locally.
                            ActionRunnable.supply(l, () -> handleLocalGets(missingLocations, request, r.multiGetShardResponse(), shardId))
                                .run();
                        } else {
                            assert r.segmentGeneration() > -1L;
                            indexShard.waitForSegmentGeneration(
                                r.segmentGeneration(),
                                listener.delegateFailureAndWrap(
                                    (ll, aLong) -> threadPool.executor(getExecutor(request, shardId))
                                        .execute(
                                            ActionRunnable.supply(
                                                ll,
                                                () -> handleLocalGets(missingLocations, request, r.multiGetShardResponse(), shardId)
                                            )
                                        )
                                )
                            );
                        }
                    }
                }), TransportShardMultiGetFomTranslogAction.Response::new, getExecutor(request, shardId))
            );
        } else {
            // A non-real-time mget with no explicit refresh requested.
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    // Returns the index of entries in response.locations that have a missing result with no failure on the promotable shard.
    private static List<Integer> locationsWithMissingResults(TransportShardMultiGetFomTranslogAction.Response response) {
        List<Integer> locations = new ArrayList<>();
        for (int i = 0; i < response.multiGetShardResponse().locations.size(); i++) {
            if (response.multiGetShardResponse().responses.get(i) == null && response.multiGetShardResponse().failures.get(i) == null) {
                locations.add(i);
            }
        }
        return locations;
    }

    private MultiGetShardResponse handleLocalGets(
        List<Integer> missingLocations,
        MultiGetShardRequest request,
        MultiGetShardResponse response,
        ShardId shardId
    ) {
        logger.trace("handling local gets for locations: {}", missingLocations);
        var indexShard = getIndexShard(shardId);
        for (var l : missingLocations) {
            MultiGetRequest.Item item = request.items.get(l);
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
                response.add(request.locations.get(l), new GetResponse(getResult));
            } catch (RuntimeException e) {
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
                    response.add(request.locations.get(l), new MultiGetResponse.Failure(request.index(), item.id(), e));
                }
            } catch (IOException e) {
                logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
                response.add(request.locations.get(l), new MultiGetResponse.Failure(request.index(), item.id(), e));
            }
        }
        return response;
    }

    private void asyncShardMultiGet(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        if (request.refresh() && request.realtime() == false) {
            threadPool.executor(getExecutor(request, shardId)).execute(ActionRunnable.wrap(listener, l -> {
                var indexShard = getIndexShard(shardId);
                indexShard.externalRefresh("refresh_flag_mget", l.map(r -> shardOperation(request, shardId)));
            }));
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    private DiscoveryNode getCurrentNodeOfPrimary(ShardId shardId) {
        var clusterState = clusterService.state();
        var shardRoutingTable = clusterState.routingTable().shardRoutingTable(shardId);
        if (shardRoutingTable.primaryShard() == null || shardRoutingTable.primaryShard().active() == false) {
            throw new NoShardAvailableActionException(shardId, "primary shard is not active");
        }
        DiscoveryNode node = clusterState.nodes().get(shardRoutingTable.primaryShard().currentNodeId());
        assert node != null;
        return node;
    }

    private IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
    }
}
