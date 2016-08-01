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

package org.elasticsearch.action.update;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;

/**
 */
public class TransportUpdateAction extends TransportInstanceSingleOperationAction<UpdateRequest, UpdateResponse> {

    private final TransportDeleteAction deleteAction;
    private final TransportIndexAction indexAction;
    private final AutoCreateIndex autoCreateIndex;
    private final TransportCreateIndexAction createIndexAction;
    private final UpdateHelper updateHelper;
    private final IndicesService indicesService;

    @Inject
    public TransportUpdateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                 TransportIndexAction indexAction, TransportDeleteAction deleteAction, TransportCreateIndexAction createIndexAction,
                                 UpdateHelper updateHelper, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                 IndicesService indicesService, AutoCreateIndex autoCreateIndex) {
        super(settings, UpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, UpdateRequest::new);
        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.createIndexAction = createIndexAction;
        this.updateHelper = updateHelper;
        this.indicesService = indicesService;
        this.autoCreateIndex = autoCreateIndex;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected UpdateResponse newResponse() {
        return new UpdateResponse();
    }

    @Override
    protected boolean retryOnFailure(Exception e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    @Override
    protected void resolveRequest(ClusterState state, UpdateRequest request) {
        resolveAndValidateRouting(state.metaData(), request.concreteIndex(), request);
    }

    public static void resolveAndValidateRouting(MetaData metaData, String concreteIndex, UpdateRequest request) {
        request.routing((metaData.resolveIndexRouting(request.parent(), request.routing(), request.index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && metaData.routingRequired(concreteIndex, request.type())) {
            throw new RoutingMissingException(concreteIndex, request.type(), request.id());
        }
    }

    @Override
    protected void doExecute(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            createIndexAction.execute(new CreateIndexRequest().index(request.index()).cause("auto(update api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(request, listener);
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
            innerExecute(request, listener);
        }
    }

    private void innerExecute(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        super.doExecute(request, listener);
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
        return new PlainShardIterator(shardIterator.shardId(), Collections.<ShardRouting>emptyList());
    }

    @Override
    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        shardOperation(request, listener, 0);
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount) {
        final ShardId shardId = request.getShardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        final UpdateHelper.Result result = updateHelper.prepare(request, indexShard);
        switch (result.getResponseResult()) {
            case CREATED:
                IndexRequest upsertRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference upsertSourceBytes = upsertRequest.source();
                indexAction.execute(upsertRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                        if (request.fields() != null && request.fields().length > 0) {
                            Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(upsertSourceBytes, true);
                            update.setGetResult(updateHelper.extractGetResult(request, request.concreteIndex(), response.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), upsertSourceBytes));
                        } else {
                            update.setGetResult(null);
                        }
                        update.setForcedRefresh(response.forcedRefresh());
                        listener.onResponse(update);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (cause instanceof VersionConflictEngineException) {
                            if (retryCount < request.retryOnConflict()) {
                                logger.trace("Retry attempt [{}] of [{}] on version conflict on [{}][{}][{}]",
                                        retryCount + 1, request.retryOnConflict(), request.index(), request.getShardId(), request.id());
                                threadPool.executor(executor()).execute(new ActionRunnable<UpdateResponse>(listener) {
                                    @Override
                                    protected void doRun() {
                                        shardOperation(request, listener, retryCount + 1);
                                    }
                                });
                                return;
                            }
                        }
                        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
                    }
                });
                break;
            case UPDATED:
                IndexRequest indexRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference indexSourceBytes = indexRequest.source();
                indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                        update.setGetResult(updateHelper.extractGetResult(request, request.concreteIndex(), response.getVersion(), result.updatedSourceAsMap(), result.updateSourceContentType(), indexSourceBytes));
                        update.setForcedRefresh(response.forcedRefresh());
                        listener.onResponse(update);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final Throwable cause = unwrapCause(e);
                        if (cause instanceof VersionConflictEngineException) {
                            if (retryCount < request.retryOnConflict()) {
                                threadPool.executor(executor()).execute(new ActionRunnable<UpdateResponse>(listener) {
                                    @Override
                                    protected void doRun() {
                                        shardOperation(request, listener, retryCount + 1);
                                    }
                                });
                                return;
                            }
                        }
                        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
                    }
                });
                break;
            case DELETED:
                DeleteRequest deleteRequest = result.action();
                deleteAction.execute(deleteRequest, new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse response) {
                        UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                        update.setGetResult(updateHelper.extractGetResult(request, request.concreteIndex(), response.getVersion(), result.updatedSourceAsMap(), result.updateSourceContentType(), null));
                        update.setForcedRefresh(response.forcedRefresh());
                        listener.onResponse(update);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final Throwable cause = unwrapCause(e);
                        if (cause instanceof VersionConflictEngineException) {
                            if (retryCount < request.retryOnConflict()) {
                                threadPool.executor(executor()).execute(new ActionRunnable<UpdateResponse>(listener) {
                                    @Override
                                    protected void doRun() {
                                        shardOperation(request, listener, retryCount + 1);
                                    }
                                });
                                return;
                            }
                        }
                        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
                    }
                });
                break;
            case NOOP:
                UpdateResponse update = result.action();
                IndexService indexServiceOrNull = indicesService.indexService(shardId.getIndex());
                if (indexServiceOrNull !=  null) {
                    IndexShard shard = indexService.getShardOrNull(shardId.getId());
                    if (shard != null) {
                        shard.noopUpdate(request.type());
                    }
                }
                listener.onResponse(update);
                break;
            default:
                throw new IllegalStateException("Illegal result " + result.getResponseResult());
        }
    }
}
