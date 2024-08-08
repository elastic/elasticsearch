/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.MultiEngineGet;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.elasticsearch.action.get.TransportGetAction.getCurrentNodeOfPrimary;
import static org.elasticsearch.core.Strings.format;

public class TransportShardMultiGetAction extends TransportSingleShardAction<MultiGetShardRequest, MultiGetShardResponse> {

    private static final String ACTION_NAME = TransportMultiGetAction.NAME + "[shard]";
    public static final ActionType<MultiGetShardResponse> TYPE = new ActionType<>(ACTION_NAME);
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
            threadPool.executor(ThreadPool.Names.GET)
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
        return new PlainShardIterator(
            iterator.shardId(),
            iterator.getShardRoutings().stream().filter(shardRouting -> OperationRouting.canSearchShard(shardRouting, state)).toList()
        );
    }

    @Override
    protected void asyncShardOperation(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        if (indexShard.routingEntry().isPromotableToPrimary() == false) {
            assert indexShard.indexSettings().isFastRefresh() == false
                : "a search shard should not receive a TransportShardMultiGetAction for an index with fast refresh";
            handleMultiGetOnUnpromotableShard(request, indexShard, listener);
            return;
        }
        assert DiscoveryNode.isStateless(clusterService.getSettings()) == false || indexShard.indexSettings().isFastRefresh()
            : "in Stateless a promotable to primary shard can receive a TransportShardMultiGetAction only if an index has "
                + "the fast refresh setting";
        if (request.realtime()) { // we are not tied to a refresh cycle here anyway
            asyncShardMultiGet(request, shardId, listener);
        } else {
            indexShard.ensureShardSearchActive(b -> {
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
        MultiGetShardResponse response = new MultiGetShardResponse();
        getIndexShard(shardId).mget(mget -> {
            for (int i = 0; i < request.locations.size(); i++) {
                getAndAddToResponse(shardId, mget, i, request, response);
            }
        });
        return response;
    }

    @Override
    protected Executor getExecutor(MultiGetShardRequest request, ShardId shardId) {
        final ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().index(shardId.getIndex()).isSystem()) {
            return threadPool.executor(executorSelector.executorForGet(shardId.getIndexName()));
        } else if (indicesService.indexServiceSafe(shardId.getIndex()).getIndexSettings().isSearchThrottled()) {
            return threadPool.executor(ThreadPool.Names.SEARCH_THROTTLED);
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
        if (request.refresh()) {
            logger.trace("send refresh action for shard {}", shardId);
            var refreshRequest = new BasicReplicationRequest(shardId);
            refreshRequest.setParentTask(request.getParentTask());
            client.executeLocally(
                TransportShardRefreshAction.TYPE,
                refreshRequest,
                listener.delegateFailureAndWrap((l, replicationResponse) -> super.asyncShardOperation(request, shardId, l))
            );
            return;
        }
        if (request.realtime()) {
            final var state = clusterService.state();
            final var observer = new ClusterStateObserver(
                state,
                clusterService,
                TimeValue.timeValueSeconds(60),
                logger,
                threadPool.getThreadContext()
            );
            shardMultiGetFromTranslog(request, indexShard, state, observer, listener);
        } else {
            // A non-real-time mget with no explicit refresh requested.
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    private void shardMultiGetFromTranslog(
        MultiGetShardRequest request,
        IndexShard indexShard,
        ClusterState state,
        ClusterStateObserver observer,
        ActionListener<MultiGetShardResponse> listener
    ) {
        DiscoveryNode node;
        try {
            node = getCurrentNodeOfPrimary(state, indexShard.shardId());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final var retryingListener = listener.delegateResponse((l, e) -> {
            final var cause = ExceptionsHelper.unwrapCause(e);
            logger.debug("mget_from_translog[shard] failed", cause);
            if (cause instanceof ShardNotFoundException || cause instanceof IndexNotFoundException) {
                logger.debug("retrying mget_from_translog[shard]");
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        shardMultiGetFromTranslog(request, indexShard, state, observer, l);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        l.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        l.onFailure(new ElasticsearchException("Timed out retrying mget_from_translog[shard]", cause));
                    }
                });
            } else {
                l.onFailure(e);
            }
        });
        tryShardMultiGetFromTranslog(request, indexShard, node, retryingListener);
    }

    private void tryShardMultiGetFromTranslog(
        MultiGetShardRequest request,
        IndexShard indexShard,
        DiscoveryNode node,
        ActionListener<MultiGetShardResponse> listener
    ) {
        final var shardId = indexShard.shardId();
        TransportShardMultiGetFomTranslogAction.Request mgetFromTranslogRequest = new TransportShardMultiGetFomTranslogAction.Request(
            request,
            shardId
        );
        mgetFromTranslogRequest.setParentTask(request.getParentTask());
        transportService.sendRequest(
            node,
            TransportShardMultiGetFomTranslogAction.NAME,
            mgetFromTranslogRequest,
            new ActionListenerResponseHandler<>(listener.delegateFailure((l, r) -> {
                var responseHasMissingLocations = false;
                for (int i = 0; i < r.multiGetShardResponse().locations.size(); i++) {
                    if (r.multiGetShardResponse().responses.get(i) == null && r.multiGetShardResponse().failures.get(i) == null) {
                        responseHasMissingLocations = true;
                        break;
                    }
                }
                if (responseHasMissingLocations == false) {
                    logger.debug("received result of all ids in real-time mget[shard] from the promotable shard.");
                    l.onResponse(r.multiGetShardResponse());
                } else {
                    logger.debug(
                        "no result for some ids from the promotable shard (segment generation to wait for: {})",
                        r.segmentGeneration()
                    );
                    if (r.segmentGeneration() == -1) {
                        // Nothing to wait for (no previous unsafe generation), just handle the rest locally.
                        ActionRunnable.supply(l, () -> handleLocalGets(request, r.multiGetShardResponse(), shardId)).run();
                    } else {
                        assert r.segmentGeneration() > -1L;
                        assert r.primaryTerm() > Engine.UNKNOWN_PRIMARY_TERM;
                        indexShard.waitForPrimaryTermAndGeneration(
                            r.primaryTerm(),
                            r.segmentGeneration(),
                            listener.delegateFailureAndWrap(
                                (ll, aLong) -> getExecutor(request, shardId).execute(
                                    ActionRunnable.supply(ll, () -> handleLocalGets(request, r.multiGetShardResponse(), shardId))
                                )
                            )
                        );
                    }
                }
            }), TransportShardMultiGetFomTranslogAction.Response::new, getExecutor(request, shardId))
        );
    }

    private MultiGetShardResponse handleLocalGets(MultiGetShardRequest request, MultiGetShardResponse response, ShardId shardId) {
        logger.trace("handling local gets for missing locations");
        getIndexShard(shardId).mget(mget -> {
            for (int i = 0; i < response.locations.size(); i++) {
                if (response.responses.get(i) == null && response.failures.get(i) == null) {
                    getAndAddToResponse(shardId, mget, i, request, response);
                }
            }
        });
        return response;
    }

    private void getAndAddToResponse(
        ShardId shardId,
        MultiEngineGet mget,
        int location,
        MultiGetShardRequest request,
        MultiGetShardResponse response
    ) {
        var indexShard = getIndexShard(shardId);
        MultiGetRequest.Item item = request.items.get(location);
        try {
            GetResult getResult = indexShard.getService()
                .get(
                    item.id(),
                    item.storedFields(),
                    request.realtime(),
                    item.version(),
                    item.versionType(),
                    item.fetchSourceContext(),
                    request.isForceSyntheticSource(),
                    mget
                );
            response.add(request.locations.get(location), new GetResponse(getResult));
        } catch (RuntimeException e) {
            if (TransportActions.isShardNotAvailableException(e)) {
                throw e;
            } else {
                logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
                response.add(request.locations.get(location), new MultiGetResponse.Failure(request.index(), item.id(), e));
            }
        } catch (IOException e) {
            logger.debug(() -> format("%s failed to execute multi_get for [%s]", shardId, item.id()), e);
            response.add(request.locations.get(location), new MultiGetResponse.Failure(request.index(), item.id(), e));
        }
    }

    private void asyncShardMultiGet(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        if (request.refresh() && request.realtime() == false) {
            getExecutor(request, shardId).execute(ActionRunnable.wrap(listener, l -> {
                var indexShard = getIndexShard(shardId);
                indexShard.externalRefresh("refresh_flag_mget", l.map(r -> shardOperation(request, shardId)));
            }));
        } else {
            super.asyncShardOperation(request, shardId, listener);
        }
    }

    private IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
    }
}
