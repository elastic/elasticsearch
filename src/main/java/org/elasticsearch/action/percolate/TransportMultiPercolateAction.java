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
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
        @SuppressWarnings("unchecked")
        final List<Object> percolateRequests = (List) request.requests();

        final TIntArrayList slots = new TIntArrayList();
        final List<GetRequest> existingDocsRequests = new ArrayList<GetRequest>();
        for (int i = 0;  i < request.requests().size(); i++) {
            PercolateRequest percolateRequest = request.requests().get(i);
            percolateRequest.startTime = System.currentTimeMillis();
            if (percolateRequest.getRequest() != null) {
                existingDocsRequests.add(percolateRequest.getRequest());
                slots.add(i);
            }
        }

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
                        int slot = slots.get(i);
                        if (!itemResponse.isFailed()) {
                            GetResponse getResponse = itemResponse.getResponse();
                            if (getResponse.isExists()) {
                                percolateRequests.set(slot, new PercolateRequest((PercolateRequest) percolateRequests.get(slot), getResponse.getSourceAsBytesRef()));
                            } else {
                                percolateRequests.set(slot, new DocumentMissingException(null, getResponse.getType(), getResponse.getId()));
                            }
                        } else {
                            percolateRequests.set(slot, itemResponse.getFailure());
                        }
                    }
                    multiPercolate(request, percolateRequests, listener, clusterState);
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } else {
            multiPercolate(request, percolateRequests, listener, clusterState);
        }

    }

    private void multiPercolate(MultiPercolateRequest multiPercolateRequest, final List<Object> percolateRequests,
                                final ActionListener<MultiPercolateResponse> listener, ClusterState clusterState) {

        final AtomicInteger[] expectedOperationsPerItem = new AtomicInteger[percolateRequests.size()];
        final AtomicReferenceArray<AtomicReferenceArray> responsesByItemAndShard = new AtomicReferenceArray<AtomicReferenceArray>(multiPercolateRequest.requests().size());
        final AtomicArray<Object> reducedResponses = new AtomicArray<Object>(percolateRequests.size());

        // Resolving concrete indices and routing and grouping the requests by shard
        final Map<ShardId, TransportShardMultiPercolateAction.Request> requestsByShard = new HashMap<ShardId, TransportShardMultiPercolateAction.Request>();
        int expectedResults = 0;
        for (int i = 0;  i < percolateRequests.size(); i++) {
            Object element = percolateRequests.get(i);
            assert element != null;
            if (element instanceof PercolateRequest) {
                PercolateRequest percolateRequest = (PercolateRequest) element;
                String[] concreteIndices = clusterState.metaData().concreteIndices(percolateRequest.indices(), percolateRequest.ignoreIndices(), true);
                Map<String, Set<String>> routing = clusterState.metaData().resolveSearchRouting(percolateRequest.routing(), multiPercolateRequest.indices());
                // TODO: I only need shardIds, ShardIterator(ShardRouting) is only needed in TransportShardMultiPercolateAction
                GroupShardsIterator shards = clusterService.operationRouting().searchShards(
                        clusterState, percolateRequest.indices(), concreteIndices, routing, percolateRequest.preference()
                );

                responsesByItemAndShard.set(i, new AtomicReferenceArray(shards.size()));
                expectedOperationsPerItem[i] = new AtomicInteger(shards.size());
                for (ShardIterator shard : shards) {
                    ShardId shardId = shard.shardId();
                    TransportShardMultiPercolateAction.Request requests = requestsByShard.get(shardId);
                    if (requests == null) {
                        requestsByShard.put(shardId, requests = new TransportShardMultiPercolateAction.Request(shard.shardId().getIndex(), shardId.id(), percolateRequest.preference()));
                    }
                    requests.add(new TransportShardMultiPercolateAction.Request.Item(i, new PercolateShardRequest(shardId, percolateRequest)));
                }
                expectedResults++;
            } else if (element instanceof Throwable) {
                reducedResponses.set(i, element);
                responsesByItemAndShard.set(i, new AtomicReferenceArray(0));
                expectedOperationsPerItem[i] = new AtomicInteger(0);
            }
        }

        if (expectedResults == 0) {
            finish(reducedResponses, listener);
            return;
        }

        final AtomicInteger expectedOperations = new AtomicInteger(expectedResults);
        for (Map.Entry<ShardId, TransportShardMultiPercolateAction.Request> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final TransportShardMultiPercolateAction.Request shardRequest = entry.getValue();
            shardMultiPercolateAction.execute(shardRequest, new ActionListener<TransportShardMultiPercolateAction.Response>() {

                @Override
                @SuppressWarnings("unchecked")
                public void onResponse(TransportShardMultiPercolateAction.Response response) {
                    try {
                        for (TransportShardMultiPercolateAction.Response.Item item : response.items()) {
                            AtomicReferenceArray shardResults = responsesByItemAndShard.get(item.slot());
                            if (shardResults == null) {
                                continue;
                            }

                            if (item.failed()) {
                                shardResults.set(shardId.id(), new BroadcastShardOperationFailedException(shardId, item.error().string()));
                            } else {
                                shardResults.set(shardId.id(), item.response());
                            }

                            assert expectedOperationsPerItem[item.slot()].get() >= 1;
                            if (expectedOperationsPerItem[item.slot()].decrementAndGet() == 0) {
                                reduce(item.slot(), percolateRequests, expectedOperations, reducedResponses, listener, responsesByItemAndShard);
                            }
                        }
                    } catch (Throwable e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                @SuppressWarnings("unchecked")
                public void onFailure(Throwable e) {
                    try {
                        for (TransportShardMultiPercolateAction.Request.Item item : shardRequest.items()) {
                            AtomicReferenceArray shardResults = responsesByItemAndShard.get(item.slot());
                            if (shardResults == null) {
                                continue;
                            }

                            shardResults.set(shardId.id(), new BroadcastShardOperationFailedException(shardId, e));
                            assert expectedOperationsPerItem[item.slot()].get() >= 1;
                            if (expectedOperationsPerItem[item.slot()].decrementAndGet() == 0) {
                                reduce(item.slot(), percolateRequests, expectedOperations, reducedResponses, listener, responsesByItemAndShard);
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("{} Percolate original reduce error", e, shardId);
                        listener.onFailure(t);
                    }
                }

            });
        }
    }

    private void reduce(int slot,
                        List<Object> percolateRequests,
                        AtomicInteger expectedOperations,
                        AtomicArray<Object> reducedResponses,
                        ActionListener<MultiPercolateResponse> listener,
                        AtomicReferenceArray<AtomicReferenceArray> responsesByItemAndShard) {

        AtomicReferenceArray shardResponses = responsesByItemAndShard.get(slot);
        PercolateResponse reducedResponse = TransportPercolateAction.reduce((PercolateRequest) percolateRequests.get(slot), shardResponses, percolatorService);
        reducedResponses.set(slot, reducedResponse);
        assert expectedOperations.get() >= 1;
        if (expectedOperations.decrementAndGet() == 0) {
            finish(reducedResponses, listener);
        }
    }

    private void finish(AtomicArray<Object> reducedResponses, ActionListener<MultiPercolateResponse> listener) {
        MultiPercolateResponse.Item[] finalResponse = new MultiPercolateResponse.Item[reducedResponses.length()];
        for (int i = 0; i < reducedResponses.length(); i++) {
            Object element = reducedResponses.get(i);
            assert element != null;
            if (element instanceof PercolateResponse) {
                finalResponse[i] = new MultiPercolateResponse.Item((PercolateResponse) element);
            } else if (element instanceof Throwable) {
                finalResponse[i] = new MultiPercolateResponse.Item(ExceptionsHelper.detailedMessage((Throwable) element));
            }
        }
        listener.onResponse(new MultiPercolateResponse(finalResponse));
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
