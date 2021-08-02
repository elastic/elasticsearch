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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;
import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.wrapBulkResponse;

public class TransportUpdateAction extends TransportInstanceSingleOperationAction<UpdateRequest, UpdateResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final UpdateHelper updateHelper;
    private final IndicesService indicesService;
    private final NodeClient client;
    private final ClusterService clusterService;

    @Inject
    public TransportUpdateAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                 UpdateHelper updateHelper, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService,
                                 AutoCreateIndex autoCreateIndex, NodeClient client) {
        super(UpdateAction.NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, UpdateRequest::new);
        this.updateHelper = updateHelper;
        this.indicesService = indicesService;
        this.autoCreateIndex = autoCreateIndex;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected String executor(ShardId shardId) {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().getIndexMetadata().isSystem() ? Names.SYSTEM_WRITE : Names.WRITE;
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
    protected void resolveRequest(ClusterState state, UpdateRequest request) {
        resolveAndValidateRouting(state.metadata(), request.concreteIndex(), request);
    }

    public static void resolveAndValidateRouting(Metadata metadata, String concreteIndex, UpdateRequest request) {
        request.routing((metadata.resolveWriteIndexRouting(request.routing(), request.index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && metadata.routingRequired(concreteIndex)) {
            throw new RoutingMissingException(concreteIndex, request.id());
        }
    }

    @Override
    protected void doExecute(Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        if (request.isRequireAlias() && (clusterService.state().getMetadata().hasAlias(request.index()) == false)) {
            throw new IndexNotFoundException("["
                + DocWriteRequest.REQUIRE_ALIAS
                + "] request flag is [true] and ["
                + request.index()
                + "] is not an alias", request.index());
        }
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            client.admin().indices().create(new CreateIndexRequest()
                .index(request.index())
                .cause("auto(update api)")
                .masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
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
            });
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
        ShardIterator shardIterator = clusterService.operationRouting()
                .indexShards(clusterState, request.concreteIndex(), request.id(), request.routing());
        ShardRouting shard;
        while ((shard = shardIterator.nextOrNull()) != null) {
            if (shard.primary()) {
                return new PlainShardIterator(shardIterator.shardId(), Collections.singletonList(shard));
            }
        }
        return new PlainShardIterator(shardIterator.shardId(), Collections.emptyList());
    }

    @Override
    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        shardOperation(request, listener, 0);
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount) {
        final ShardId shardId = request.getShardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        indexService.getMetadata().mode().checkDocWriteRequest(OpType.UPDATE, shardId.getIndexName());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        final UpdateHelper.Result result = updateHelper.prepare(request, indexShard, threadPool::absoluteTimeInMillis);
        switch (result.getResponseResult()) {
            case CREATED:
                IndexRequest upsertRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference upsertSourceBytes = upsertRequest.source();
                client.bulk(toSingleItemBulkRequest(upsertRequest), wrapBulkResponse(
                        ActionListener.<IndexResponse>wrap(response -> {
                            UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(),
                                response.getId(), response.getSeqNo(), response.getPrimaryTerm(),
                                response.getVersion(), response.getResult());
                            if (request.fetchSource() != null && request.fetchSource().fetchSource()) {
                                Tuple<XContentType, Map<String, Object>> sourceAndContent =
                                        XContentHelper.convertToMap(upsertSourceBytes, true, upsertRequest.getContentType());
                                update.setGetResult(UpdateHelper.extractGetResult(request, request.concreteIndex(),
                                    response.getSeqNo(), response.getPrimaryTerm(), response.getVersion(), sourceAndContent.v2(),
                                    sourceAndContent.v1(), upsertSourceBytes));
                            } else {
                                update.setGetResult(null);
                            }
                            update.setForcedRefresh(response.forcedRefresh());
                            listener.onResponse(update);
                        }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount)))
                );

                break;
            case UPDATED:
                IndexRequest indexRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference indexSourceBytes = indexRequest.source();
                client.bulk(toSingleItemBulkRequest(indexRequest), wrapBulkResponse(
                        ActionListener.<IndexResponse>wrap(response -> {
                            UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(),
                                response.getId(), response.getSeqNo(), response.getPrimaryTerm(),
                                response.getVersion(), response.getResult());
                            update.setGetResult(UpdateHelper.extractGetResult(request, request.concreteIndex(),
                                response.getSeqNo(), response.getPrimaryTerm(), response.getVersion(),
                                result.updatedSourceAsMap(), result.updateSourceContentType(), indexSourceBytes));
                            update.setForcedRefresh(response.forcedRefresh());
                            listener.onResponse(update);
                        }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount)))
                );
                break;
            case DELETED:
                DeleteRequest deleteRequest = result.action();
                client.bulk(toSingleItemBulkRequest(deleteRequest), wrapBulkResponse(
                        ActionListener.<DeleteResponse>wrap(response -> {
                            UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(),
                                response.getId(), response.getSeqNo(), response.getPrimaryTerm(), response.getVersion(),
                                response.getResult());
                            update.setGetResult(UpdateHelper.extractGetResult(request, request.concreteIndex(),
                                response.getSeqNo(), response.getPrimaryTerm(), response.getVersion(),
                                result.updatedSourceAsMap(), result.updateSourceContentType(), null));
                            update.setForcedRefresh(response.forcedRefresh());
                            listener.onResponse(update);
                        }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount)))
                );
                break;
            case NOOP:
                UpdateResponse update = result.action();
                IndexService indexServiceOrNull = indicesService.indexService(shardId.getIndex());
                if (indexServiceOrNull !=  null) {
                    IndexShard shard = indexService.getShardOrNull(shardId.getId());
                    if (shard != null) {
                        shard.noopUpdate();
                    }
                }
                listener.onResponse(update);
                break;
            default:
                throw new IllegalStateException("Illegal result " + result.getResponseResult());
        }
    }

    private void handleUpdateFailureWithRetry(final ActionListener<UpdateResponse> listener, final UpdateRequest request,
                                              final Exception failure, int retryCount) {
        final Throwable cause = unwrapCause(failure);
        if (cause instanceof VersionConflictEngineException) {
            if (retryCount < request.retryOnConflict()) {
                logger.trace("Retry attempt [{}] of [{}] on version conflict on [{}][{}][{}]",
                        retryCount + 1, request.retryOnConflict(), request.index(), request.getShardId(), request.id());
                threadPool.executor(executor(request.getShardId()))
                    .execute(ActionRunnable.wrap(listener, l -> shardOperation(request, l, retryCount + 1)));
                return;
            }
        }
        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
    }
}
