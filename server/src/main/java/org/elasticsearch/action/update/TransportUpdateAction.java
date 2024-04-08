/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.action.bulk.TransportBulkAction.unwrappingSingleItemBulkResponse;
import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;

public class TransportUpdateAction extends TransportInstanceSingleOperationAction<UpdateRequest, UpdateResponse> {

    public static final String NAME = "indices:data/write/update";
    public static final ActionType<UpdateResponse> TYPE = new ActionType<>(NAME);
    private final AutoCreateIndex autoCreateIndex;
    private final UpdateHelper updateHelper;
    private final IndicesService indicesService;
    private final NodeClient client;
    private final ClusterService clusterService;

    @Inject
    public TransportUpdateAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        AutoCreateIndex autoCreateIndex,
        NodeClient client
    ) {
        super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, UpdateRequest::new);
        this.updateHelper = updateHelper;
        this.indicesService = indicesService;
        this.autoCreateIndex = autoCreateIndex;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected Executor executor(ShardId shardId) {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return threadPool.executor(indexService.getIndexSettings().getIndexMetadata().isSystem() ? Names.SYSTEM_WRITE : Names.WRITE);
    }

    @Override
    protected UpdateResponse newResponse(StreamInput in) throws IOException {
        return new UpdateResponse(in);
    }

    @Override
    protected boolean retryOnFailure(Exception e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    @Override
    protected void resolveRequest(ClusterState state, UpdateRequest docWriteRequest) {
        docWriteRequest.routing(state.metadata().resolveWriteIndexRouting(docWriteRequest.routing(), docWriteRequest.index()));
    }

    @Override
    protected void doExecute(Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        if (request.isRequireAlias() && (clusterService.state().getMetadata().hasAlias(request.index()) == false)) {
            throw new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + request.index() + "] is not an alias",
                request.index()
            );
        }
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest().index(request.index()).cause("auto(update api)").masterNodeTimeout(request.timeout()),
                    new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            innerExecute(task, request, listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                                // we have the index, do it
                                try {
                                    innerExecute(task, request, listener);
                                } catch (Exception inner) {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    }
                );
        } else {
            innerExecute(task, request, listener);
        }
    }

    private void innerExecute(final Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, UpdateRequest request) {
        if (request.getShardId() != null) {
            return clusterState.routingTable().index(request.concreteIndex()).shard(request.getShardId().getId()).primaryShardIt();
        }
        IndexMetadata indexMetadata = clusterState.metadata().index(request.concreteIndex());
        if (indexMetadata == null) {
            throw new IndexNotFoundException(request.concreteIndex());
        }
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        int shardId = indexRouting.updateShard(request.id(), request.routing());
        return RoutingTable.shardRoutingTable(clusterState.routingTable().index(request.concreteIndex()), shardId).primaryShardIt();
    }

    @Override
    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        try {
            shardOperation(request, listener, 0);
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount)
        throws IOException {
        final ShardId shardId = request.getShardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        final UpdateHelper.Result result = updateHelper.prepare(request, indexShard, threadPool::absoluteTimeInMillis);
        switch (result.getResponseResult()) {
            case CREATED -> {
                IndexRequest upsertRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference upsertSourceBytes = upsertRequest.source();
                client.bulk(
                    toSingleItemBulkRequest(upsertRequest),
                    unwrappingSingleItemBulkResponse(ActionListener.<DocWriteResponse>wrap(response -> {
                        UpdateResponse update = new UpdateResponse(
                            response.getShardInfo(),
                            response.getShardId(),
                            response.getId(),
                            response.getSeqNo(),
                            response.getPrimaryTerm(),
                            response.getVersion(),
                            response.getResult()
                        );
                        if (request.fetchSource() != null && request.fetchSource().fetchSource()) {
                            Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(
                                upsertSourceBytes,
                                true,
                                upsertRequest.getContentType()
                            );
                            update.setGetResult(
                                UpdateHelper.extractGetResult(
                                    request,
                                    request.concreteIndex(),
                                    response.getSeqNo(),
                                    response.getPrimaryTerm(),
                                    response.getVersion(),
                                    sourceAndContent.v2(),
                                    sourceAndContent.v1(),
                                    upsertSourceBytes
                                )
                            );
                        } else {
                            update.setGetResult(null);
                        }
                        update.setForcedRefresh(response.forcedRefresh());
                        listener.onResponse(update);
                    }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount)))
                );
            }
            case UPDATED -> {
                IndexRequest indexRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference indexSourceBytes = indexRequest.source();
                client.bulk(
                    toSingleItemBulkRequest(indexRequest),
                    unwrappingSingleItemBulkResponse(ActionListener.<DocWriteResponse>wrap(response -> {
                        UpdateResponse update = new UpdateResponse(
                            response.getShardInfo(),
                            response.getShardId(),
                            response.getId(),
                            response.getSeqNo(),
                            response.getPrimaryTerm(),
                            response.getVersion(),
                            response.getResult()
                        );
                        update.setGetResult(
                            UpdateHelper.extractGetResult(
                                request,
                                request.concreteIndex(),
                                response.getSeqNo(),
                                response.getPrimaryTerm(),
                                response.getVersion(),
                                result.updatedSourceAsMap(),
                                result.updateSourceContentType(),
                                indexSourceBytes
                            )
                        );
                        update.setForcedRefresh(response.forcedRefresh());
                        listener.onResponse(update);
                    }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount)))
                );
            }
            case DELETED -> {
                DeleteRequest deleteRequest = result.action();
                client.bulk(
                    toSingleItemBulkRequest(deleteRequest),
                    unwrappingSingleItemBulkResponse(ActionListener.<DeleteResponse>wrap(response -> {
                        UpdateResponse update = new UpdateResponse(
                            response.getShardInfo(),
                            response.getShardId(),
                            response.getId(),
                            response.getSeqNo(),
                            response.getPrimaryTerm(),
                            response.getVersion(),
                            response.getResult()
                        );
                        update.setGetResult(
                            UpdateHelper.extractGetResult(
                                request,
                                request.concreteIndex(),
                                response.getSeqNo(),
                                response.getPrimaryTerm(),
                                response.getVersion(),
                                result.updatedSourceAsMap(),
                                result.updateSourceContentType(),
                                null
                            )
                        );
                        update.setForcedRefresh(response.forcedRefresh());
                        listener.onResponse(update);
                    }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount)))
                );
            }
            case NOOP -> {
                UpdateResponse update = result.action();
                IndexService indexServiceOrNull = indicesService.indexService(shardId.getIndex());
                if (indexServiceOrNull != null) {
                    IndexShard shard = indexService.getShardOrNull(shardId.getId());
                    if (shard != null) {
                        shard.noopUpdate();
                    }
                }
                listener.onResponse(update);
            }
            default -> throw new IllegalStateException("Illegal result " + result.getResponseResult());
        }
    }

    private void handleUpdateFailureWithRetry(
        final ActionListener<UpdateResponse> listener,
        final UpdateRequest request,
        final Exception failure,
        int retryCount
    ) {
        final Throwable cause = unwrapCause(failure);
        if (cause instanceof VersionConflictEngineException versionConflictEngineException && retryCount < request.retryOnConflict()) {
            logger.trace(
                "Retry attempt [{}] of [{}] on version conflict on [{}][{}][{}]",
                retryCount + 1,
                request.retryOnConflict(),
                request.index(),
                request.getShardId(),
                request.id()
            );

            final Executor executor;
            try {
                executor = executor(request.getShardId());
            } catch (Exception e) {
                // might fail if shard no longer exists locally, in which case we cannot retry
                e.addSuppressed(versionConflictEngineException);
                listener.onFailure(e);
                return;
            }
            executor.execute(ActionRunnable.wrap(listener, l -> shardOperation(request, l, retryCount + 1)));
            return;
        }
        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
    }
}
