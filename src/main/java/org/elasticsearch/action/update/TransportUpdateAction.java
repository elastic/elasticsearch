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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 */
public class TransportUpdateAction extends TransportInstanceSingleOperationAction<UpdateRequest, UpdateResponse> {

    private final TransportDeleteAction deleteAction;
    private final TransportIndexAction indexAction;
    private final AutoCreateIndex autoCreateIndex;
    private final TransportCreateIndexAction createIndexAction;
    private final UpdateHelper updateHelper;

    @Inject
    public TransportUpdateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                 TransportIndexAction indexAction, TransportDeleteAction deleteAction, TransportCreateIndexAction createIndexAction,
                                 UpdateHelper updateHelper) {
        super(settings, threadPool, clusterService, transportService);
        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.createIndexAction = createIndexAction;
        this.updateHelper = updateHelper;
        this.autoCreateIndex = new AutoCreateIndex(settings);
    }

    @Override
    protected String transportAction() {
        return UpdateAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected UpdateRequest newRequest() {
        return new UpdateRequest();
    }

    @Override
    protected UpdateResponse newResponse() {
        return new UpdateResponse();
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpdateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpdateRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected boolean retryOnFailure(Throwable e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    @Override
    protected boolean resolveRequest(ClusterState state, UpdateRequest request, ActionListener<UpdateResponse> listener) {
        MetaData metaData = clusterService.state().metaData();
        String aliasOrIndex = request.index();
        request.routing((metaData.resolveIndexRouting(request.routing(), aliasOrIndex)));
        request.index(metaData.concreteSingleIndex(request.index()));

        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && state.getMetaData().routingRequired(request.index(), request.type())) {
            throw new RoutingMissingException(request.index(), request.type(), request.id());
        }
        return true;
    }

    @Override
    protected void doExecute(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            request.beforeLocalFork(); // we fork on another thread...
            createIndexAction.execute(new CreateIndexRequest(request.index()).cause("auto(update api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(request, listener);
                        } catch (Throwable e1) {
                            listener.onFailure(e1);
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
    protected ShardIterator shards(ClusterState clusterState, UpdateRequest request) throws ElasticsearchException {
        if (request.shardId() != -1) {
            return clusterState.routingTable().index(request.index()).shard(request.shardId()).primaryShardIt();
        }
        ShardIterator shardIterator = clusterService.operationRouting()
                .indexShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing());
        ShardRouting shard;
        while ((shard = shardIterator.nextOrNull()) != null) {
            if (shard.primary()) {
                return new PlainShardIterator(shardIterator.shardId(), ImmutableList.of(shard));
            }
        }
        return new PlainShardIterator(shardIterator.shardId(), ImmutableList.<ShardRouting>of());
    }

    @Override
    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) throws ElasticsearchException {
        shardOperation(request, listener, 0);
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount) throws ElasticsearchException {
        final UpdateHelper.Result result = updateHelper.prepare(request);
        switch (result.operation()) {
            case UPSERT:
                IndexRequest upsertRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference upsertSourceBytes = upsertRequest.source();
                indexAction.execute(upsertRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        UpdateResponse update = new UpdateResponse(response.getIndex(), response.getType(), response.getId(), response.getVersion(), response.isCreated());
                        if (request.fields() != null && request.fields().length > 0) {
                            Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(upsertSourceBytes, true);
                            update.setGetResult(updateHelper.extractGetResult(request, response.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), upsertSourceBytes));
                        } else {
                            update.setGetResult(null);
                        }
                        listener.onResponse(update);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        e = ExceptionsHelper.unwrapCause(e);
                        if (e instanceof VersionConflictEngineException || e instanceof DocumentAlreadyExistsException) {
                            if (retryCount < request.retryOnConflict()) {
                                threadPool.executor(executor()).execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        shardOperation(request, listener, retryCount + 1);
                                    }
                                });
                                return;
                            }
                        }
                        listener.onFailure(e);
                    }
                });
                break;
            case INDEX:
                IndexRequest indexRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference indexSourceBytes = indexRequest.source();
                indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        UpdateResponse update = new UpdateResponse(response.getIndex(), response.getType(), response.getId(), response.getVersion(), response.isCreated());
                        update.setGetResult(updateHelper.extractGetResult(request, response.getVersion(), result.updatedSourceAsMap(), result.updateSourceContentType(), indexSourceBytes));
                        listener.onResponse(update);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        e = ExceptionsHelper.unwrapCause(e);
                        if (e instanceof VersionConflictEngineException) {
                            if (retryCount < request.retryOnConflict()) {
                                threadPool.executor(executor()).execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        shardOperation(request, listener, retryCount + 1);
                                    }
                                });
                                return;
                            }
                        }
                        listener.onFailure(e);
                    }
                });
                break;
            case DELETE:
                DeleteRequest deleteRequest = result.action();
                deleteAction.execute(deleteRequest, new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse response) {
                        UpdateResponse update = new UpdateResponse(response.getIndex(), response.getType(), response.getId(), response.getVersion(), false);
                        update.setGetResult(updateHelper.extractGetResult(request, response.getVersion(), result.updatedSourceAsMap(), result.updateSourceContentType(), null));
                        listener.onResponse(update);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        e = ExceptionsHelper.unwrapCause(e);
                        if (e instanceof VersionConflictEngineException) {
                            if (retryCount < request.retryOnConflict()) {
                                threadPool.executor(executor()).execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        shardOperation(request, listener, retryCount + 1);
                                    }
                                });
                                return;
                            }
                        }
                        listener.onFailure(e);
                    }
                });
                break;
            case NONE:
                UpdateResponse update = result.action();
                listener.onResponse(update);
                break;
            default:
                throw new ElasticsearchIllegalStateException("Illegal operation " + result.operation());
        }
    }
}
