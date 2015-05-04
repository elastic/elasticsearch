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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final boolean allowIdGeneration;
    private final ClusterService clusterService;
    private final TransportShardBulkAction shardBulkAction;
    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportBulkAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                               TransportShardBulkAction shardBulkAction, TransportCreateIndexAction createIndexAction, ActionFilters actionFilters) {
        super(settings, BulkAction.NAME, threadPool, transportService, actionFilters, BulkRequest.class);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;
        this.createIndexAction = createIndexAction;

        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.allowIdGeneration = this.settings.getAsBoolean("action.bulk.action.allow_id_generation", true);
    }

    @Override
    protected void doExecute(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
        final long startTime = System.currentTimeMillis();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());

        if (autoCreateIndex.needToCheck()) {
            // Keep track of all unique indices and all unique types per index for the create index requests:
            final Map<String, Set<String>> indicesAndTypes = new HashMap<>();
            for (ActionRequest request : bulkRequest.requests) {
                if (request instanceof DocumentRequest) {
                    DocumentRequest req = (DocumentRequest) request;
                    Set<String> types = indicesAndTypes.get(req.index());
                    if (types == null) {
                        indicesAndTypes.put(req.index(), types = new HashSet<>());
                    }
                    types.add(req.type());
                } else {
                    throw new ElasticsearchException("Parsed unknown request in bulk actions: " + request.getClass().getSimpleName());
                }
            }
            final AtomicInteger counter = new AtomicInteger(indicesAndTypes.size());
            ClusterState state = clusterService.state();
            for (Map.Entry<String, Set<String>> entry : indicesAndTypes.entrySet()) {
                final String index = entry.getKey();
                if (autoCreateIndex.shouldAutoCreate(index, state)) {
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest(bulkRequest);
                    createIndexRequest.index(index);
                    for (String type : entry.getValue()) {
                        createIndexRequest.mapping(type);
                    }
                    createIndexRequest.cause("auto(bulk api)");
                    createIndexRequest.masterNodeTimeout(bulkRequest.timeout());
                    createIndexAction.execute(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                try {
                                    executeBulk(bulkRequest, startTime, listener, responses);
                                } catch (Throwable t) {
                                    listener.onFailure(t);
                                }
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
                                try {
                                    executeBulk(bulkRequest, startTime, listener, responses);
                                } catch (Throwable t) {
                                    listener.onFailure(t);
                                }
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

    private final long buildTookInMillis(long startTime) {
        // protect ourselves against time going backwards
        return Math.max(1, System.currentTimeMillis() - startTime);
    }

    private void executeBulk(final BulkRequest bulkRequest, final long startTime, final ActionListener<BulkResponse> listener, final AtomicArray<BulkItemResponse> responses ) {
        final ClusterState clusterState = clusterService.state();
        // TODO use timeout to wait here if its blocked...
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.WRITE);

        final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState.metaData());
        MetaData metaData = clusterState.metaData();
        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            ActionRequest request = bulkRequest.requests.get(i);
            if (request instanceof DocumentRequest) {
                DocumentRequest req = (DocumentRequest) request;

                if (addFailureIfIndexIsUnavailable(req, bulkRequest, responses, i, concreteIndices, metaData)) {
                    continue;
                }

                String concreteIndex = concreteIndices.resolveIfAbsent(req.index(), req.indicesOptions());
                if (request instanceof IndexRequest) {
                    IndexRequest indexRequest = (IndexRequest) request;
                    MappingMetaData mappingMd = null;
                    if (metaData.hasIndex(concreteIndex)) {
                        mappingMd = metaData.index(concreteIndex).mappingOrDefault(indexRequest.type());
                    }
                    try {
                        indexRequest.process(metaData, mappingMd, allowIdGeneration, concreteIndex);
                    } catch (ElasticsearchParseException | RoutingMissingException e) {
                        BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex, indexRequest.type(), indexRequest.id(), e);
                        BulkItemResponse bulkItemResponse = new BulkItemResponse(i, "index", failure);
                        responses.set(i, bulkItemResponse);
                        // make sure the request gets never processed again
                        bulkRequest.requests.set(i, null);
                    }
                } else {
                    concreteIndices.resolveIfAbsent(req.index(), req.indicesOptions());
                    req.routing(clusterState.metaData().resolveIndexRouting(req.routing(), req.index()));
                }
            }
        }

        // first, go over all the requests and create a ShardId -> Operations mapping
        Map<ShardId, List<BulkItemRequest>> requestsByShard = Maps.newHashMap();

        for (int i = 0; i < bulkRequest.requests.size(); i++) {
            ActionRequest request = bulkRequest.requests.get(i);
            if (request instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(indexRequest.index());
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, indexRequest.type(), indexRequest.id(), indexRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = Lists.newArrayList();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            } else if (request instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(deleteRequest.index());
                MappingMetaData mappingMd = clusterState.metaData().index(concreteIndex).mappingOrDefault(deleteRequest.type());
                if (mappingMd != null && mappingMd.routing().required() && deleteRequest.routing() == null) {
                    // if routing is required, and no routing on the delete request, we need to broadcast it....
                    GroupShardsIterator groupShards = clusterService.operationRouting().broadcastDeleteShards(clusterState, concreteIndex);
                    for (ShardIterator shardIt : groupShards) {
                        List<BulkItemRequest> list = requestsByShard.get(shardIt.shardId());
                        if (list == null) {
                            list = Lists.newArrayList();
                            requestsByShard.put(shardIt.shardId(), list);
                        }
                        list.add(new BulkItemRequest(i, new DeleteRequest(deleteRequest)));
                    }
                } else {
                    ShardId shardId = clusterService.operationRouting().deleteShards(clusterState, concreteIndex, deleteRequest.type(), deleteRequest.id(), deleteRequest.routing()).shardId();
                    List<BulkItemRequest> list = requestsByShard.get(shardId);
                    if (list == null) {
                        list = Lists.newArrayList();
                        requestsByShard.put(shardId, list);
                    }
                    list.add(new BulkItemRequest(i, request));
                }
            } else if (request instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) request;
                String concreteIndex = concreteIndices.getConcreteIndex(updateRequest.index());
                MappingMetaData mappingMd = clusterState.metaData().index(concreteIndex).mappingOrDefault(updateRequest.type());
                if (mappingMd != null && mappingMd.routing().required() && updateRequest.routing() == null) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(updateRequest.index(), updateRequest.type(),
                            updateRequest.id(), "routing is required for this item", RestStatus.BAD_REQUEST);
                    responses.set(i, new BulkItemResponse(i, updateRequest.type(), failure));
                    continue;
                }
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, updateRequest.type(), updateRequest.id(), updateRequest.routing()).shardId();
                List<BulkItemRequest> list = requestsByShard.get(shardId);
                if (list == null) {
                    list = Lists.newArrayList();
                    requestsByShard.put(shardId, list);
                }
                list.add(new BulkItemRequest(i, request));
            }
        }

        if (requestsByShard.isEmpty()) {
            listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime)));
            return;
        }

        final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<BulkItemRequest> requests = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(bulkRequest, shardId.index().name(), shardId.id(), bulkRequest.refresh(), requests.toArray(new BulkItemRequest[requests.size()]));
            bulkShardRequest.consistencyLevel(bulkRequest.consistencyLevel());
            bulkShardRequest.timeout(bulkRequest.timeout());
            shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
                @Override
                public void onResponse(BulkShardResponse bulkShardResponse) {
                    for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                        // we may have no response if item failed
                        if (bulkItemResponse.getResponse() != null) {
                            bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                        }
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
                                    new BulkItemResponse.Failure(concreteIndices.getConcreteIndex(indexRequest.index()), indexRequest.type(), indexRequest.id(), message, status)));
                        } else if (request.request() instanceof DeleteRequest) {
                            DeleteRequest deleteRequest = (DeleteRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), "delete",
                                    new BulkItemResponse.Failure(concreteIndices.getConcreteIndex(deleteRequest.index()), deleteRequest.type(), deleteRequest.id(), message, status)));
                        } else if (request.request() instanceof UpdateRequest) {
                            UpdateRequest updateRequest = (UpdateRequest) request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), "update",
                                    new BulkItemResponse.Failure(concreteIndices.getConcreteIndex(updateRequest.index()), updateRequest.type(), updateRequest.id(), message, status)));
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                private void finishHim() {
                    listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTime)));
                }
            });
        }
    }

    private boolean addFailureIfIndexIsUnavailable(DocumentRequest request, BulkRequest bulkRequest, AtomicArray<BulkItemResponse> responses, int idx,
                                              final ConcreteIndices concreteIndices,
                                              final MetaData metaData) {
        String concreteIndex = concreteIndices.getConcreteIndex(request.index());
        Exception unavailableException = null;
        if (concreteIndex == null) {
            try {
                concreteIndex = concreteIndices.resolveIfAbsent(request.index(), request.indicesOptions());
            } catch (IndexClosedException ice) {
                unavailableException = ice;
            } catch (IndexMissingException ime) {
                // Fix for issue where bulk request references an index that
                // cannot be auto-created see issue #8125
                unavailableException = ime;
            }
        }
        if (unavailableException == null) {
            IndexMetaData indexMetaData = metaData.index(concreteIndex);
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                unavailableException = new IndexClosedException(new Index(metaData.index(request.index()).getIndex()));
            }
        }
        if (unavailableException != null) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.type(), request.id(),
                    unavailableException);
            BulkItemResponse bulkItemResponse = new BulkItemResponse(idx, "index", failure);
            responses.set(idx, bulkItemResponse);
            // make sure the request gets never processed again
            bulkRequest.requests.set(idx, null);
            return true;
        }
        return false;
    }


    private static class ConcreteIndices  {
        private final Map<String, String> indices = new HashMap<>();
        private final MetaData metaData;

        ConcreteIndices(MetaData metaData) {
            this.metaData = metaData;
        }

        String getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        String resolveIfAbsent(String indexOrAlias, IndicesOptions indicesOptions) {
            String concreteIndex = indices.get(indexOrAlias);
            if (concreteIndex == null) {
                concreteIndex = metaData.concreteSingleIndex(indexOrAlias, indicesOptions);
                indices.put(indexOrAlias, concreteIndex);
            }
            return concreteIndex;
        }
    }
}
