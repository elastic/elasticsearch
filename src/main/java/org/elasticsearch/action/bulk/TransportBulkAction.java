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

package org.elasticsearch.action.bulk;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportBulkAction extends TransportAction<BulkRequest, BulkResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final ClusterService clusterService;

    private final TransportShardBulkAction shardBulkAction;

    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportBulkAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                               TransportShardBulkAction shardBulkAction, TransportCreateIndexAction createIndexAction) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;
        this.createIndexAction = createIndexAction;

        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.allowIdGeneration = componentSettings.getAsBoolean("action.allow_id_generation", true);

        transportService.registerHandler(BulkAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
        final long startTime = System.currentTimeMillis();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<BulkItemResponse>(bulkRequest.requests.size());

        if (autoCreateIndex.needToCheck()) {
            final Set<String> indices = Sets.newHashSet();
            for (ActionRequest request : bulkRequest.requests) {
                if (request instanceof IndexRequest) {
                    IndexRequest indexRequest = (IndexRequest) request;
                    if (!indices.contains(indexRequest.index())) {
                        indices.add(indexRequest.index());
                    }
                } else if (request instanceof DeleteRequest) {
                    DeleteRequest deleteRequest = (DeleteRequest) request;
                    if (!indices.contains(deleteRequest.index())) {
                        indices.add(deleteRequest.index());
                    }
                } else if (request instanceof UpdateRequest) {
                    UpdateRequest updateRequest = (UpdateRequest) request;
                    if (!indices.contains(updateRequest.index())) {
                        indices.add(updateRequest.index());
                    }
                } else {
                    throw new ElasticsearchException("Parsed unknown request in bulk actions: " + request.getClass().getSimpleName());
                }
            }

            final AtomicInteger counter = new AtomicInteger(indices.size());
            ClusterState state = clusterService.state();
            for (final String index : indices) {
                if (autoCreateIndex.shouldAutoCreate(index, state)) {
                    createIndexAction.execute(new CreateIndexRequest(index).cause("auto(bulk api)"), new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(bulkRequest, startTime, listener, responses);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            if (!(ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException)) {
                                // fail all requests involving this index, if create didnt work
                                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                    ActionRequest request = bulkRequest.requests.get(i);
                                    if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                        bulkRequest.requests.set(i, null);
                                    }
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(bulkRequest, startTime, listener, responses);
                            }
                        }
                    });
                } else {
                    if (counter.decrementAndGet() == 0) {
                        executeBulk(bulkRequest, startTime, listener, responses);
                    }
                }
            }
        } else {
            executeBulk(bulkRequest, startTime, listener, responses);
        }
    }

    private boolean setResponseFailureIfIndexMatches(AtomicArray<BulkItemResponse> responses, int idx, ActionRequest request, String index, Throwable e) {
        if (request instanceof IndexRequest) {
            IndexRequest indexRequest = (IndexRequest) request;
            if (index.equals(indexRequest.index())) {
                responses.set(idx, new BulkItemResponse(idx, "index", new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), e)));
                return true;
            }
        } else if (request instanceof DeleteRequest) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            if (index.equals(deleteRequest.index())) {
                responses.set(idx, new BulkItemResponse(idx, "index", new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), e)));
                return true;
            }
        } else if (request instanceof UpdateRequest) {
            UpdateRequest updateRequest = (UpdateRequest) request;
            if (index.equals(updateRequest.index())) {
                responses.set(idx, new BulkItemResponse(idx, "index", new BulkItemResponse.Failure(updateRequest.index(), updateRequest.type(), updateRequest.id(), e)));
                return true;
            }
        } else {
            throw new ElasticsearchException("Parsed unknown request in bulk actions: " + request.getClass().getSimpleName());
        }
        return false;
    }

    /**
     * This method executes the {@link BulkRequest} and calls the given listener once the request returns.
     * This method will not create any indices even if auto-create indices is enabled.
     *
     * @see #doExecute(BulkRequest, org.elasticsearch.action.ActionListener)
     */
    public void executeBulk(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
        final long startTime = System.currentTimeMillis();
        executeBulk(bulkRequest, startTime, listener, new AtomicArray<BulkItemResponse>(bulkRequest.requests.size()));
    }

    private void executeBulk(final BulkRequest bulkRequest, final long startTime, final ActionListener<BulkResponse> listener, final AtomicArray<BulkItemResponse> responses ) {
        ClusterState clusterState = clusterService.state();
        // TODO use timeout to wait here if its blocked...
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        MetaData metaData = clusterState.metaData();
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            ActionRequest request = bulkRequest.requests.get(i);
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                String aliasOrIndex = indexRequest.index();
                indexRequest.index(clusterState.metaData().concreteSingleIndex(indexRequest.index()));

                MappingMetaData mappingMd = null;
                if (metaData.hasIndex(indexRequest.index())) {
                    mappingMd = metaData.index(indexRequest.index()).mappingOrDefault(indexRequest.type());
                }
                try {
                    indexRequest.process(metaData, aliasOrIndex, mappingMd, allowIdGeneration);
                } catch (ElasticsearchParseException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, "index", failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    bulkRequest.requests.set(i, null);
                }
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                deleteRequest.routing(clusterState.metaData().resolveIndexRouting(deleteRequest.routing(), deleteRequest.index()));
                deleteRequest.index(clusterState.metaData().concreteSingleIndex(deleteRequest.index()));
            } else if (request instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                updateRequest.routing(clusterState.metaData().resolveIndexRouting(updateRequest.routing(), updateRequest.index()));
                updateRequest.index(clusterState.metaData().concreteSingleIndex(updateRequest.index()));
            }
        }

        // first, go over all the requests and create a ShardId -> Operations mapping
        Map<ShardId, List<BulkItemRequest>> requestsByShard = Maps.newHashMap();

        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            ActionRequest request = bulkRequest.requests.get(i);
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, indexRequest.index(), indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = Lists.newArrayList();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                MappingMetaData mappingMd = clusterState.metaData().index(deleteRequest.index()).mappingOrDefault(deleteRequest.type());
                if (mappingMd != null && mappingMd.routing().required() && deleteRequest.routing() == null) {
                    // if routing is required, and no routing on the delete request, we need to broadcast it....
                    GroupShardsIterator groupShards = clusterService.operationRouting().broadcastDeleteShards(clusterState, deleteRequest.index());
                    for (ShardIterator shardIt : groupShards) {
                        List<BulkItemRequest> list = requestsByShard.get(shardIt.shardId());
                        if (list == null) {
                            list = Lists.newArrayList();
                            requestsByShard.put(shardIt.shardId(), list);
                        }
                        list.add(new BulkItemRequest(i, new DeleteRequest(deleteRequest)));
                    }
                } else {
                    ShardId shardId = clusterService.operationRouting().deleteShards(clusterState, deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                    List<BulkItemRequest> list = requestsByShard.get(shardId);
                    if (list == null) {
                        list = Lists.newArrayList();
                        requestsByShard.put(shardId, list);
                    }
                    list.add(new BulkItemRequest(i, request));
                }
            } else if (request instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                MappingMetaData mappingMd = clusterState.metaData().index(updateRequest.index()).mappingOrDefault(updateRequest.type());
                if (mappingMd != null && mappingMd.routing().required() && updateRequest.routing() == null) {
                    continue; // What to do?
                }
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, updateRequest.index(), updateRequest.type(), updateRequest.id(), updateRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = Lists.newArrayList();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            }
        }

        if (requestsByShard.isEmpty()) {
            listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), System.currentTimeMillis() - startTime));
            return;
        }

        final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<BulkItemRequest> requests = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId.index().name(), shardId.id(), bulkRequest.refresh(), requests.toArray(new BulkItemRequest[requests.size()]));
            bulkShardRequest.replicationType(bulkRequest.replicationType());
            bulkShardRequest.consistencyLevel(bulkRequest.consistencyLevel());
            bulkShardRequest.timeout(bulkRequest.timeout());
            shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
                @Override
                public void onResponse(BulkShardResponse bulkShardResponse) {
                    for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                        responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    // create failures for all relevant requests
                    String message = ExceptionsHelper.detailedMessage(e);
                    RestStatus status = ExceptionsHelper.status(e);
                    for (BulkItemRequest request : requests) {
                        if (request.request() instanceof IndexRequest) {
                            IndexRequest indexRequest = (IndexRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), indexRequest.opType().toString().toLowerCase(Locale.ENGLISH),
                                    new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), message, status)));
                        } else if (request.request() instanceof DeleteRequest) {
                            DeleteRequest deleteRequest = (DeleteRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), "delete",
                                    new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), message, status)));
                        } else if (request.request() instanceof UpdateRequest) {
                            UpdateRequest updateRequest = (UpdateRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), "update",
                                    new BulkItemResponse.Failure(updateRequest.index(), updateRequest.type(), updateRequest.id(), message, status)));
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), System.currentTimeMillis() - startTime));
                }
            });
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<BulkRequest> {

        @Override
        public BulkRequest newInstance() {
            return new BulkRequest();
        }

        @Override
        public void messageReceived(final BulkRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [" + BulkAction.NAME + "] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
