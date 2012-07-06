/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

/**
 */
public class TransportUpdateAction extends TransportInstanceSingleOperationAction<UpdateRequest, UpdateResponse> {

    private final IndicesService indicesService;

    private final TransportDeleteAction deleteAction;

    private final TransportIndexAction indexAction;

    private final ScriptService scriptService;

    @Inject
    public TransportUpdateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                 IndicesService indicesService, TransportIndexAction indexAction, TransportDeleteAction deleteAction, ScriptService scriptService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.scriptService = scriptService;
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
        e = ExceptionsHelper.unwrapCause(e);
        if (e instanceof IllegalIndexShardStateException) {
            return true;
        }
        return false;
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, UpdateRequest request) throws ElasticSearchException {
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
    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) throws ElasticSearchException {
        shardOperation(request, listener, 0);
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());

        long getDate = System.currentTimeMillis();
        final GetResult getResult = indexShard.getService().get(request.type(), request.id(),
                new String[]{SourceFieldMapper.NAME, RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME}, true);

        // no doc, what to do, what to do...
        if (!getResult.exists()) {
            if (request.upsertRequest() == null) {
                listener.onFailure(new DocumentMissingException(new ShardId(request.index(), request.shardId()), request.type(), request.id()));
                return;
            }
            IndexRequest indexRequest = request.upsertRequest();
            indexRequest.index(request.index()).type(request.type()).id(request.id())
                    // it has to be a "create!"
                    .create(true)
                    .routing(request.routing())
                    .percolate(request.percolate())
                    .refresh(request.refresh())
                    .replicationType(request.replicationType()).consistencyLevel(request.consistencyLevel());
            indexRequest.operationThreaded(false);
            // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
            final BytesReference updateSourceBytes = indexRequest.source();
            indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse response) {
                    UpdateResponse update = new UpdateResponse(response.index(), response.type(), response.id(), response.version());
                    update.matches(response.matches());
                    // TODO: we can parse the index _source and extractGetResult if applicable
                    update.getResult(null);
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
            return;
        }

        if (getResult.internalSourceRef() == null) {
            // no source, we can't do nothing, through a failure...
            listener.onFailure(new DocumentSourceMissingException(new ShardId(request.index(), request.shardId()), request.type(), request.id()));
            return;
        }

        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
        String operation = null;
        String timestamp = null;
        Long ttl = null;
        Object fetchedTTL = null;
        final Map<String, Object> updatedSourceAsMap;
        final XContentType updateSourceContentType = sourceAndContent.v1();
        String routing = getResult.fields().containsKey(RoutingFieldMapper.NAME) ? getResult.field(RoutingFieldMapper.NAME).value().toString() : null;
        String parent = getResult.fields().containsKey(ParentFieldMapper.NAME) ? getResult.field(ParentFieldMapper.NAME).value().toString() : null;

        if (request.script() == null && request.doc() != null) {
            IndexRequest indexRequest = request.doc();
            updatedSourceAsMap = sourceAndContent.v2();
            if (indexRequest.ttl() > 0) {
                ttl = indexRequest.ttl();
            }
            timestamp = indexRequest.timestamp();
            if (indexRequest.routing() != null) {
                routing = indexRequest.routing();
            }
            if (indexRequest.parent() != null) {
                parent = indexRequest.parent();
            }
            XContentHelper.update(updatedSourceAsMap, indexRequest.sourceAsMap());
        } else {
            Map<String, Object> ctx = new HashMap<String, Object>(2);
            ctx.put("_source", sourceAndContent.v2());

            try {
                ExecutableScript script = scriptService.executable(request.scriptLang, request.script, request.scriptParams);
                script.setNextVar("ctx", ctx);
                script.run();
                // we need to unwrap the ctx...
                ctx = (Map<String, Object>) script.unwrap(ctx);
            } catch (Exception e) {
                throw new ElasticSearchIllegalArgumentException("failed to execute script", e);
            }

            operation = (String) ctx.get("op");
            timestamp = (String) ctx.get("_timestamp");
            fetchedTTL = ctx.get("_ttl");
            if (fetchedTTL != null) {
                if (fetchedTTL instanceof Number) {
                    ttl = ((Number) fetchedTTL).longValue();
                } else {
                    ttl = TimeValue.parseTimeValue((String) fetchedTTL, null).millis();
                }
            }

            updatedSourceAsMap = (Map<String, Object>) ctx.get("_source");
        }

        // apply script to update the source
        // No TTL has been given in the update script so we keep previous TTL value if there is one
        if (ttl == null) {
            ttl = getResult.fields().containsKey(TTLFieldMapper.NAME) ? (Long) getResult.field(TTLFieldMapper.NAME).value() : null;
            if (ttl != null) {
                ttl = ttl - (System.currentTimeMillis() - getDate); // It is an approximation of exact TTL value, could be improved
            }
        }

        // TODO: external version type, does it make sense here? does not seem like it...

        if (operation == null || "index".equals(operation)) {
            final IndexRequest indexRequest = Requests.indexRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .source(updatedSourceAsMap, updateSourceContentType)
                    .version(getResult.version()).replicationType(request.replicationType()).consistencyLevel(request.consistencyLevel())
                    .timestamp(timestamp).ttl(ttl)
                    .percolate(request.percolate())
                    .refresh(request.refresh());
            indexRequest.operationThreaded(false);
            // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
            final BytesReference updateSourceBytes = indexRequest.source();
            indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse response) {
                    UpdateResponse update = new UpdateResponse(response.index(), response.type(), response.id(), response.version());
                    update.matches(response.matches());
                    update.getResult(extractGetResult(request, response.version(), updatedSourceAsMap, updateSourceContentType, updateSourceBytes));
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
        } else if ("delete".equals(operation)) {
            DeleteRequest deleteRequest = Requests.deleteRequest(request.index()).type(request.type()).id(request.id()).routing(routing).parent(parent)
                    .version(getResult.version()).replicationType(request.replicationType()).consistencyLevel(request.consistencyLevel());
            deleteRequest.operationThreaded(false);
            deleteAction.execute(deleteRequest, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse response) {
                    UpdateResponse update = new UpdateResponse(response.index(), response.type(), response.id(), response.version());
                    update.getResult(extractGetResult(request, response.version(), updatedSourceAsMap, updateSourceContentType, null));
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
        } else if ("none".equals(operation)) {
            UpdateResponse update = new UpdateResponse(getResult.index(), getResult.type(), getResult.id(), getResult.version());
            update.getResult(extractGetResult(request, getResult.version(), updatedSourceAsMap, updateSourceContentType, null));
            listener.onResponse(update);
        } else {
            logger.warn("Used update operation [{}] for script [{}], doing nothing...", operation, request.script);
            listener.onResponse(new UpdateResponse(getResult.index(), getResult.type(), getResult.id(), getResult.version()));
        }
    }

    @Nullable
    protected GetResult extractGetResult(final UpdateRequest request, long version, final Map<String, Object> source, XContentType sourceContentType, @Nullable final BytesReference sourceAsBytes) {
        if (request.fields() == null || request.fields().length == 0) {
            return null;
        }
        boolean sourceRequested = false;
        Map<String, GetField> fields = null;
        if (request.fields() != null && request.fields().length > 0) {
            SourceLookup sourceLookup = new SourceLookup();
            sourceLookup.setNextSource(source);
            for (String field : request.fields()) {
                if (field.equals("_source")) {
                    sourceRequested = true;
                    continue;
                }
                Object value = sourceLookup.extractValue(field);
                if (value != null) {
                    if (fields == null) {
                        fields = newHashMapWithExpectedSize(2);
                    }
                    GetField getField = fields.get(field);
                    if (getField == null) {
                        getField = new GetField(field, new ArrayList<Object>(2));
                        fields.put(field, getField);
                    }
                    getField.values().add(value);
                }
            }
        }

        // TODO when using delete/none, we can still return the source as bytes by generating it (using the sourceContentType)

        return new GetResult(request.index(), request.type(), request.id(), version, true, sourceRequested ? sourceAsBytes : null, fields);
    }
}
