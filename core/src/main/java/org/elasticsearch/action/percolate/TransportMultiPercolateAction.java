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

package org.elasticsearch.action.percolate;

import com.carrotsearch.hppc.IntArrayList;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public class TransportMultiPercolateAction extends HandledTransportAction<MultiPercolateRequest, MultiPercolateResponse> {

    private final ClusterService clusterService;
    private final PercolatorService percolatorService;

    private final TransportMultiGetAction multiGetAction;
    private final TransportShardMultiPercolateAction shardMultiPercolateAction;

    @Inject
    public TransportMultiPercolateAction(Settings settings, ThreadPool threadPool, TransportShardMultiPercolateAction shardMultiPercolateAction,
                                         ClusterService clusterService, TransportService transportService, PercolatorService percolatorService,
                                         TransportMultiGetAction multiGetAction, ActionFilters actionFilters) {
        super(settings, MultiPercolateAction.NAME, threadPool, transportService, actionFilters, MultiPercolateRequest.class);
        this.shardMultiPercolateAction = shardMultiPercolateAction;
        this.clusterService = clusterService;
        this.percolatorService = percolatorService;
        this.multiGetAction = multiGetAction;
    }

    @Override
    protected void doExecute(final MultiPercolateRequest request, final ActionListener<MultiPercolateResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final List<Object> percolateRequests = new ArrayList<>(request.requests().size());
        // Can have a mixture of percolate requests. (normal percolate requests & percolate existing doc),
        // so we need to keep track for what percolate request we had a get request
        final IntArrayList getRequestSlots = new IntArrayList();
        List<GetRequest> existingDocsRequests = new ArrayList<>();
        for (int slot = 0;  slot < request.requests().size(); slot++) {
            PercolateRequest percolateRequest = request.requests().get(slot);
            percolateRequest.startTime = System.currentTimeMillis();
            percolateRequests.add(percolateRequest);
            if (percolateRequest.getRequest() != null) {
                existingDocsRequests.add(percolateRequest.getRequest());
                getRequestSlots.add(slot);
            }
        }

        if (!existingDocsRequests.isEmpty()) {
            final MultiGetRequest multiGetRequest = new MultiGetRequest(request);
            for (GetRequest getRequest : existingDocsRequests) {
                multiGetRequest.add(
                        new MultiGetRequest.Item(getRequest.index(), getRequest.type(), getRequest.id())
                        .routing(getRequest.routing())
                );
            }

            multiGetAction.execute(multiGetRequest, new ActionListener<MultiGetResponse>() {

                @Override
                public void onResponse(MultiGetResponse multiGetItemResponses) {
                    for (int i = 0; i < multiGetItemResponses.getResponses().length; i++) {
                        MultiGetItemResponse itemResponse = multiGetItemResponses.getResponses()[i];
                        int slot = getRequestSlots.get(i);
                        if (!itemResponse.isFailed()) {
                            GetResponse getResponse = itemResponse.getResponse();
                            if (getResponse.isExists()) {
                                PercolateRequest originalRequest = (PercolateRequest) percolateRequests.get(slot);
                                percolateRequests.set(slot, new PercolateRequest(originalRequest, getResponse.getSourceAsBytesRef()));
                            } else {
                                logger.trace("mpercolate existing doc, item[{}] doesn't exist", slot);
                                percolateRequests.set(slot, new DocumentMissingException(null, getResponse.getType(), getResponse.getId()));
                            }
                        } else {
                            logger.trace("mpercolate existing doc, item[{}] failure {}", slot, itemResponse.getFailure());
                            percolateRequests.set(slot, itemResponse.getFailure());
                        }
                    }
                    new ASyncAction(request, percolateRequests, listener, clusterState).run();
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            new ASyncAction(request, percolateRequests, listener, clusterState).run();
        }

    }

    private final class ASyncAction {

        final ActionListener<MultiPercolateResponse> finalListener;
        final Map<ShardId, TransportShardMultiPercolateAction.Request> requestsByShard;
        final MultiPercolateRequest multiPercolateRequest;
        final List<Object> percolateRequests;

        final Map<ShardId, IntArrayList> shardToSlots;
        final AtomicInteger expectedOperations;
        final AtomicArray<Object> reducedResponses;
        final AtomicReferenceArray<AtomicInteger> expectedOperationsPerItem;
        final AtomicReferenceArray<AtomicReferenceArray> responsesByItemAndShard;

        ASyncAction(MultiPercolateRequest multiPercolateRequest, List<Object> percolateRequests, ActionListener<MultiPercolateResponse> finalListener, ClusterState clusterState) {
            this.finalListener = finalListener;
            this.multiPercolateRequest = multiPercolateRequest;
            this.percolateRequests = percolateRequests;
            responsesByItemAndShard = new AtomicReferenceArray<>(percolateRequests.size());
            expectedOperationsPerItem = new AtomicReferenceArray<>(percolateRequests.size());
            reducedResponses = new AtomicArray<>(percolateRequests.size());

            // Resolving concrete indices and routing and grouping the requests by shard
            requestsByShard = new HashMap<>();
            // Keep track what slots belong to what shard, in case a request to a shard fails on all copies
            shardToSlots = new HashMap<>();
            int expectedResults = 0;
            for (int slot = 0;  slot < percolateRequests.size(); slot++) {
                Object element = percolateRequests.get(slot);
                assert element != null;
                if (element instanceof PercolateRequest) {
                    PercolateRequest percolateRequest = (PercolateRequest) element;
                    String[] concreteIndices;
                    try {
                         concreteIndices = clusterState.metaData().concreteIndices(percolateRequest.indicesOptions(), percolateRequest.indices());
                    } catch (IndexMissingException e) {
                        reducedResponses.set(slot, e);
                        responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                        expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                        continue;
                    }
                    Map<String, Set<String>> routing = clusterState.metaData().resolveSearchRouting(percolateRequest.routing(), percolateRequest.indices());
                    // TODO: I only need shardIds, ShardIterator(ShardRouting) is only needed in TransportShardMultiPercolateAction
                    GroupShardsIterator shards = clusterService.operationRouting().searchShards(
                            clusterState, percolateRequest.indices(), concreteIndices, routing, percolateRequest.preference()
                    );
                    if (shards.size() == 0) {
                        reducedResponses.set(slot, new UnavailableShardsException(null, "No shards available"));
                        responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                        expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                        continue;
                    }

                    // The shard id is used as index in the atomic ref array, so we need to find out how many shards there are regardless of routing:
                    int numShards = clusterService.operationRouting().searchShardsCount(clusterState, percolateRequest.indices(), concreteIndices, null, null);
                    responsesByItemAndShard.set(slot, new AtomicReferenceArray(numShards));
                    expectedOperationsPerItem.set(slot, new AtomicInteger(shards.size()));
                    for (ShardIterator shard : shards) {
                        ShardId shardId = shard.shardId();
                        TransportShardMultiPercolateAction.Request requests = requestsByShard.get(shardId);
                        if (requests == null) {
                            requestsByShard.put(shardId, requests = new TransportShardMultiPercolateAction.Request(multiPercolateRequest, shardId.getIndex(), shardId.getId(), percolateRequest.preference()));
                        }
                        logger.trace("Adding shard[{}] percolate request for item[{}]", shardId, slot);
                        requests.add(new TransportShardMultiPercolateAction.Request.Item(slot, new PercolateShardRequest(shardId, percolateRequest)));

                        IntArrayList items = shardToSlots.get(shardId);
                        if (items == null) {
                            shardToSlots.put(shardId, items = new IntArrayList());
                        }
                        items.add(slot);
                    }
                    expectedResults++;
                } else if (element instanceof Throwable || element instanceof MultiGetResponse.Failure) {
                    logger.trace("item[{}] won't be executed, reason: {}", slot, element);
                    reducedResponses.set(slot, element);
                    responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                    expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                }
            }
            expectedOperations = new AtomicInteger(expectedResults);
        }

        void run() {
            if (expectedOperations.get() == 0) {
                finish();
                return;
            }

            logger.trace("mpercolate executing for shards {}", requestsByShard.keySet());
            for (Map.Entry<ShardId, TransportShardMultiPercolateAction.Request> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                TransportShardMultiPercolateAction.Request shardRequest = entry.getValue();
                shardMultiPercolateAction.execute(shardRequest, new ActionListener<TransportShardMultiPercolateAction.Response>() {

                    @Override
                    public void onResponse(TransportShardMultiPercolateAction.Response response) {
                        onShardResponse(shardId, response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        onShardFailure(shardId, e);
                    }

                });
            }
        }

        @SuppressWarnings("unchecked")
        void onShardResponse(ShardId shardId, TransportShardMultiPercolateAction.Response response) {
            logger.trace("{} Percolate shard response", shardId);
            try {
                for (TransportShardMultiPercolateAction.Response.Item item : response.items()) {
                    AtomicReferenceArray shardResults = responsesByItemAndShard.get(item.slot());
                    if (shardResults == null) {
                        assert false : "shardResults can't be null";
                        continue;
                    }

                    if (item.failed()) {
                        shardResults.set(shardId.id(), new BroadcastShardOperationFailedException(shardId, item.error().string()));
                    } else {
                        shardResults.set(shardId.id(), item.response());
                    }

                    assert expectedOperationsPerItem.get(item.slot()).get() >= 1 : "slot[" + item.slot() + "] can't be lower than one";
                    if (expectedOperationsPerItem.get(item.slot()).decrementAndGet() == 0) {
                        // Failure won't bubble up, since we fail the whole request now via the catch clause below,
                        // so expectedOperationsPerItem will not be decremented twice.
                        reduce(item.slot());
                    }
                }
            } catch (Throwable e) {
                logger.error("{} Percolate original reduce error", e, shardId);
                finalListener.onFailure(e);
            }
        }

        @SuppressWarnings("unchecked")
        void onShardFailure(ShardId shardId, Throwable e) {
            logger.debug("{} Shard multi percolate failure", e, shardId);
            try {
                IntArrayList slots = shardToSlots.get(shardId);
                for (int i = 0; i < slots.size(); i++) {
                    int slot = slots.get(i);
                    AtomicReferenceArray shardResults = responsesByItemAndShard.get(slot);
                    if (shardResults == null) {
                        continue;
                    }

                    shardResults.set(shardId.id(), new BroadcastShardOperationFailedException(shardId, e));
                    assert expectedOperationsPerItem.get(slot).get() >= 1 : "slot[" + slot + "] can't be lower than one. Caused by: " + e.getMessage();
                    if (expectedOperationsPerItem.get(slot).decrementAndGet() == 0) {
                        reduce(slot);
                    }
                }
            } catch (Throwable t) {
                logger.error("{} Percolate original reduce error, original error {}", t, shardId, e);
                finalListener.onFailure(t);
            }
        }

        void reduce(int slot) {
            AtomicReferenceArray shardResponses = responsesByItemAndShard.get(slot);
            PercolateResponse reducedResponse = TransportPercolateAction.reduce((PercolateRequest) percolateRequests.get(slot), shardResponses, percolatorService);
            reducedResponses.set(slot, reducedResponse);
            assert expectedOperations.get() >= 1 : "slot[" + slot + "] expected options should be >= 1 but is " + expectedOperations.get();
            if (expectedOperations.decrementAndGet() == 0) {
                finish();
            }
        }

        void finish() {
            MultiPercolateResponse.Item[] finalResponse = new MultiPercolateResponse.Item[reducedResponses.length()];
            for (int slot = 0; slot < reducedResponses.length(); slot++) {
                Object element = reducedResponses.get(slot);
                assert element != null : "Element[" + slot + "] shouldn't be null";
                if (element instanceof PercolateResponse) {
                    finalResponse[slot] = new MultiPercolateResponse.Item((PercolateResponse) element);
                } else if (element instanceof Throwable) {
                    finalResponse[slot] = new MultiPercolateResponse.Item(ExceptionsHelper.detailedMessage((Throwable) element));
                } else if (element instanceof MultiGetResponse.Failure) {
                    finalResponse[slot] = new MultiPercolateResponse.Item(((MultiGetResponse.Failure)element).getMessage());
                }
            }
            finalListener.onResponse(new MultiPercolateResponse(finalResponse));
        }

    }

}
