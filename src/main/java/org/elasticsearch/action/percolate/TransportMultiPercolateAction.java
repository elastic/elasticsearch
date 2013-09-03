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

package org.elasticsearch.action.percolate;

import gnu.trove.list.array.TIntArrayList;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public class TransportMultiPercolateAction extends TransportAction<MultiPercolateRequest, MultiPercolateResponse> {

    private final ClusterService clusterService;
    private final PercolatorService percolatorService;

    private final TransportMultiGetAction multiGetAction;
    private final TransportShardMultiPercolateAction shardMultiPercolateAction;

    @Inject
    public TransportMultiPercolateAction(Settings settings, ThreadPool threadPool, TransportShardMultiPercolateAction shardMultiPercolateAction,
                                         ClusterService clusterService, TransportService transportService, PercolatorService percolatorService,
                                         TransportMultiGetAction multiGetAction) {
        super(settings, threadPool);
        this.shardMultiPercolateAction = shardMultiPercolateAction;
        this.clusterService = clusterService;
        this.percolatorService = percolatorService;
        this.multiGetAction = multiGetAction;

        transportService.registerHandler(MultiPercolateAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final MultiPercolateRequest request, final ActionListener<MultiPercolateResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);

        final AtomicReferenceArray<Object> percolateRequests = new AtomicReferenceArray<Object>(request.requests().size());
        TIntArrayList getRequestSlots = new TIntArrayList();
        List<GetRequest> existingDocsRequests = new ArrayList<GetRequest>();
        for (int i = 0;  i < request.requests().size(); i++) {
            PercolateRequest percolateRequest = request.requests().get(i);
            percolateRequest.startTime = System.currentTimeMillis();
            percolateRequests.set(i, percolateRequest);
            if (percolateRequest.getRequest() != null) {
                existingDocsRequests.add(percolateRequest.getRequest());
                getRequestSlots.add(i);
            }
        }

        // Can have a mixture of percolate requests. (normal percolate requests & percolate existing doc),
        // so we need to keep track for what percolate request we had a get request
        final AtomicIntegerArray percolateRequestSlotsWithGet = new AtomicIntegerArray(getRequestSlots.toArray());

        if (!existingDocsRequests.isEmpty()) {
            final MultiGetRequest multiGetRequest = new MultiGetRequest();
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
                        int slot = percolateRequestSlotsWithGet.get(i);
                        if (!itemResponse.isFailed()) {
                            GetResponse getResponse = itemResponse.getResponse();
                            if (getResponse.isExists()) {
                                PercolateRequest originalRequest = (PercolateRequest) percolateRequests.get(slot);
                                percolateRequests.set(slot, new PercolateRequest(originalRequest, getResponse.getSourceAsBytesRef()));
                            } else {
                                percolateRequests.set(slot, new DocumentMissingException(null, getResponse.getType(), getResponse.getId()));
                            }
                        } else {
                            percolateRequests.set(slot, itemResponse.getFailure());
                        }
                    }
                    new ASyncAction(percolateRequests, listener, clusterState).run();
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            new ASyncAction(percolateRequests, listener, clusterState).run();
        }

    }


    private class ASyncAction {

        final ActionListener<MultiPercolateResponse> listener;
        final Map<ShardId, TransportShardMultiPercolateAction.Request> requestsByShard;
        final AtomicReferenceArray<Object> percolateRequests;

        final AtomicInteger expectedOperations;
        final AtomicArray<Object> reducedResponses;
        final ConcurrentMap<ShardId, AtomicIntegerArray> shardToSlots;
        final AtomicReferenceArray<AtomicInteger> expectedOperationsPerItem;
        final AtomicReferenceArray<AtomicReferenceArray> responsesByItemAndShard;

        ASyncAction(AtomicReferenceArray<Object> percolateRequests, ActionListener<MultiPercolateResponse> listener, ClusterState clusterState) {
            this.listener = listener;
            this.percolateRequests = percolateRequests;
            responsesByItemAndShard = new AtomicReferenceArray<AtomicReferenceArray>(percolateRequests.length());
            expectedOperationsPerItem = new AtomicReferenceArray<AtomicInteger>(percolateRequests.length());
            reducedResponses = new AtomicArray<Object>(percolateRequests.length());

            // Resolving concrete indices and routing and grouping the requests by shard
            requestsByShard = new HashMap<ShardId, TransportShardMultiPercolateAction.Request>();
            // Keep track what slots belong to what shard, in case a request to a shard fails on all copies
            Map<ShardId, TIntArrayList> shardToSlotsBuilder = new HashMap<ShardId, TIntArrayList>();
            int expectedResults = 0;
            for (int slot = 0;  slot < percolateRequests.length(); slot++) {
                Object element = percolateRequests.get(slot);
                assert element != null;
                if (element instanceof PercolateRequest) {
                    PercolateRequest percolateRequest = (PercolateRequest) element;
                    String[] concreteIndices = clusterState.metaData().concreteIndices(percolateRequest.indices(), percolateRequest.ignoreIndices(), true);
                    Map<String, Set<String>> routing = clusterState.metaData().resolveSearchRouting(percolateRequest.routing(), percolateRequest.indices());
                    // TODO: I only need shardIds, ShardIterator(ShardRouting) is only needed in TransportShardMultiPercolateAction
                    GroupShardsIterator shards = clusterService.operationRouting().searchShards(
                            clusterState, percolateRequest.indices(), concreteIndices, routing, percolateRequest.preference()
                    );

                    responsesByItemAndShard.set(slot, new AtomicReferenceArray(shards.size()));
                    expectedOperationsPerItem.set(slot, new AtomicInteger(shards.size()));
                    for (ShardIterator shard : shards) {
                        ShardId shardId = shard.shardId();
                        TransportShardMultiPercolateAction.Request requests = requestsByShard.get(shardId);
                        if (requests == null) {
                            requestsByShard.put(shardId, requests = new TransportShardMultiPercolateAction.Request(shard.shardId().getIndex(), shardId.id(), percolateRequest.preference()));
                        }
                        requests.add(new TransportShardMultiPercolateAction.Request.Item(slot, new PercolateShardRequest(shardId, percolateRequest)));

                        TIntArrayList items = shardToSlotsBuilder.get(shardId);
                        if (items == null) {
                            shardToSlotsBuilder.put(shardId, items = new TIntArrayList());
                        }
                        items.add(slot);
                    }
                    expectedResults++;
                } else if (element instanceof Throwable || element instanceof MultiGetResponse.Failure) {
                    reducedResponses.set(slot, element);
                    responsesByItemAndShard.set(slot, new AtomicReferenceArray(0));
                    expectedOperationsPerItem.set(slot, new AtomicInteger(0));
                }
            }
            expectedOperations = new AtomicInteger(expectedResults);
            // Move slot to shard tracking from normal map to concurrent save map
            shardToSlots = ConcurrentCollections.newConcurrentMap();
            for (Map.Entry<ShardId, TIntArrayList> entry : shardToSlotsBuilder.entrySet()) {
                shardToSlots.put(entry.getKey(), new AtomicIntegerArray(entry.getValue().toArray()));
            }
        }

        void run() {
            if (expectedOperations.get() == 0) {
                finish();
                return;
            }

            for (Map.Entry<ShardId, TransportShardMultiPercolateAction.Request> entry : requestsByShard.entrySet()) {
                final int shardId = entry.getKey().id();
                final String index =  entry.getKey().index().name();

                TransportShardMultiPercolateAction.Request shardRequest = entry.getValue();
                shardMultiPercolateAction.execute(shardRequest, new ActionListener<TransportShardMultiPercolateAction.Response>() {

                    @Override
                    public void onResponse(TransportShardMultiPercolateAction.Response response) {
                        onShardResponse(new ShardId(index, shardId), response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        onShardFailure(new ShardId(index, shardId), e);
                    }

                });
            }
        }

        @SuppressWarnings("unchecked")
        void onShardResponse(ShardId shardId, TransportShardMultiPercolateAction.Response response) {
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
                listener.onFailure(e);
            }
        }

        @SuppressWarnings("unchecked")
        void onShardFailure(ShardId shardId, Throwable e) {
            logger.debug("Shard multi percolate failure", e);
            try {
                AtomicIntegerArray slots = shardToSlots.get(shardId);
                for (int i = 0; i < slots.length(); i++) {
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
                listener.onFailure(t);
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
            listener.onResponse(new MultiPercolateResponse(finalResponse));
        }

    }


    class TransportHandler extends BaseTransportRequestHandler<MultiPercolateRequest> {

        @Override
        public MultiPercolateRequest newInstance() {
            return new MultiPercolateRequest();
        }

        @Override
        public void messageReceived(final MultiPercolateRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<MultiPercolateResponse>() {
                @Override
                public void onResponse(MultiPercolateResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [mpercolate] and request [" + request + "]", e1);
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
