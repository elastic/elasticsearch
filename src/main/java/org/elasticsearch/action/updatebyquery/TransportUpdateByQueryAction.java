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

package org.elasticsearch.action.updatebyquery;

import com.google.common.collect.Lists;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Delegates a {@link IndexUpdateByQueryRequest} to the primary shards of the index this request is targeted to.
 * The requests is transformed into {@link ShardUpdateByQueryRequest} and send to each primary shard. Afterwards
 * the responses from the shards are merged into a final response which is send to the client.
 */
// Perhaps create a base transport update action that sends shard requests to primary shards only
public class TransportUpdateByQueryAction extends TransportAction<UpdateByQueryRequest, UpdateByQueryResponse> {

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final TransportShardUpdateByQueryAction transportShardUpdateByQueryAction;

    @Inject
    public TransportUpdateByQueryAction(Settings settings,
                                        ThreadPool threadPool,
                                        TransportService transportService,
                                        ClusterService clusterService,
                                        TransportShardUpdateByQueryAction transportShardUpdateByQueryAction) {
        super(settings, threadPool);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.transportShardUpdateByQueryAction = transportShardUpdateByQueryAction;
        transportService.registerHandler(UpdateByQueryAction.NAME, new TransportHandler());
    }

    protected void doExecute(UpdateByQueryRequest request, ActionListener<UpdateByQueryResponse> listener) {
        long startTime = System.currentTimeMillis();
        MetaData metaData = clusterService.state().metaData();
        String[] concreteIndices = metaData.concreteIndices(request.indices(), IgnoreIndices.MISSING, true);
        Map<String, Set<String>> routingMap = metaData.resolveSearchRouting(request.routing(), request.indices());
        if (concreteIndices.length == 1) {
            doExecuteIndexRequest(request, metaData, concreteIndices[0], routingMap, new SingleIndexUpdateByQueryActionListener(startTime, listener));
        } else {
            MultipleIndexUpdateByQueryActionListener indexActionListener =
                    new MultipleIndexUpdateByQueryActionListener(startTime, listener, concreteIndices.length);
            for (String concreteIndex : concreteIndices) {
                doExecuteIndexRequest(request, metaData, concreteIndex, routingMap, indexActionListener);
            }
        }

    }

    private static class MultipleIndexUpdateByQueryActionListener implements ActionListener<IndexUpdateByQueryResponse> {

        private final long startTime;
        private final ActionListener<UpdateByQueryResponse> listener;
        private final int expectedNumberOfResponses;
        private final AtomicReferenceArray<IndexUpdateByQueryResponse> successFullIndexResponses;
        private final AtomicReferenceArray<Throwable> failedIndexResponses;
        private final AtomicInteger indexCounter;

        private MultipleIndexUpdateByQueryActionListener(long startTime, ActionListener<UpdateByQueryResponse> listener, int expectedNumberOfResponses) {
            this.startTime = startTime;
            this.listener = listener;
            successFullIndexResponses = new AtomicReferenceArray<IndexUpdateByQueryResponse>(expectedNumberOfResponses);
            failedIndexResponses = new AtomicReferenceArray<Throwable>(expectedNumberOfResponses);
            this.expectedNumberOfResponses = expectedNumberOfResponses;
            indexCounter = new AtomicInteger();
        }

        public void onResponse(IndexUpdateByQueryResponse indexUpdateByQueryResponse) {
            successFullIndexResponses.set(indexCounter.getAndIncrement(), indexUpdateByQueryResponse);
            if (indexCounter.get() == expectedNumberOfResponses) {
                finishHim();

            }
        }

        public void onFailure(Throwable e) {
            failedIndexResponses.set(indexCounter.getAndIncrement(), e);
            if (indexCounter.get() == expectedNumberOfResponses) {
                finishHim();
            }
        }

        private void finishHim() {
            long tookInMillis = System.currentTimeMillis() - startTime;
            UpdateByQueryResponse response = new UpdateByQueryResponse(tookInMillis);
            List<IndexUpdateByQueryResponse> indexResponses = Lists.newArrayList();
            List<String> indexFailures = Lists.newArrayList();
            for (int i = 0; i < expectedNumberOfResponses; i++) {
                IndexUpdateByQueryResponse indexResponse = successFullIndexResponses.get(i);
                if (indexResponse != null) {
                    indexResponses.add(indexResponse);
                } else {
                    indexFailures.add(ExceptionsHelper.detailedMessage(failedIndexResponses.get(i)));
                }
            }
            response.indexResponses(indexResponses.toArray(new IndexUpdateByQueryResponse[indexResponses.size()]));
            response.mainFailures(indexFailures.toArray(new String[indexFailures.size()]));
            listener.onResponse(response);
        }
    }

    private static class SingleIndexUpdateByQueryActionListener implements ActionListener<IndexUpdateByQueryResponse> {

        private final long startTime;
        private final ActionListener<UpdateByQueryResponse> listener;

        private SingleIndexUpdateByQueryActionListener(long startTime, ActionListener<UpdateByQueryResponse> listener) {
            this.listener = listener;
            this.startTime = startTime;
        }

        public void onResponse(IndexUpdateByQueryResponse indexUpdateByQueryResponse) {
            long tookInMillis = System.currentTimeMillis() - startTime;
            UpdateByQueryResponse finalResponse = new UpdateByQueryResponse(tookInMillis, indexUpdateByQueryResponse);
            listener.onResponse(finalResponse);
        }

        public void onFailure(Throwable e) {
            long tookInMillis = System.currentTimeMillis() - startTime;
            UpdateByQueryResponse finalResponse = new UpdateByQueryResponse(tookInMillis);
            finalResponse.mainFailures(new String[]{ExceptionsHelper.detailedMessage(e)});
            listener.onResponse(finalResponse);
        }

    }


    // Index operations happen below here:

    protected void doExecuteIndexRequest(UpdateByQueryRequest request, MetaData metaData, String concreteIndex,
                                         @Nullable Map<String, Set<String>> routingMap, ActionListener<IndexUpdateByQueryResponse> listener) {
        String[] filteringAliases = metaData.filteringAliases(concreteIndex, request.indices());
        Set<String> routing = null;
        if (routingMap != null) {
            routing = routingMap.get(concreteIndex);
        }
        IndexUpdateByQueryRequest indexRequest = new IndexUpdateByQueryRequest(request, concreteIndex, filteringAliases, routing);
        new UpdateByQueryIndexOperationAction(indexRequest, listener).startExecution();
    }

    private class UpdateByQueryIndexOperationAction {

        final IndexUpdateByQueryRequest request;
        final ActionListener<IndexUpdateByQueryResponse> indexActionListener;

        private UpdateByQueryIndexOperationAction(IndexUpdateByQueryRequest request, ActionListener<IndexUpdateByQueryResponse> listener) {
            this.request = request;
            this.indexActionListener = listener;
        }

        void startExecution() {
            startExecution(false);
        }

        boolean startExecution(boolean fromClusterEvent) {
            ClusterState state = clusterService.state();
            ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
            if (blockException != null) {
                logger.trace("[{}] global block exception, retrying...", request.index());
                overallRetry(fromClusterEvent, null, blockException);
                return false;
            }
            blockException = state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
            if (blockException != null) {
                logger.trace("[{}] index block exception, retrying...", request.index());
                overallRetry(fromClusterEvent, null, blockException);
                return false;
            }

            List<ShardRouting> primaryShards = Lists.newArrayList();
            GroupShardsIterator groupShardsIterator =
                    clusterService.operationRouting().deleteByQueryShards(state, request.index(), request.routing());
            for (ShardIterator shardIt : groupShardsIterator) {
                for (ShardRouting shardRouting = shardIt.nextOrNull(); shardRouting != null; shardRouting = shardIt.nextOrNull()) {
                    if (shardRouting.primary()) {
                        if (shardRouting.started()) {
                            primaryShards.add(shardRouting);
                        } else {
                            logger.trace(
                                    "[{}][{}] required primary shard isn't available, retrying...",
                                    request.index(), shardRouting.id()
                            );
                            overallRetry(fromClusterEvent, shardRouting, null);
                            return false;
                        }
                    }
                }
            }

            if (primaryShards.size() != groupShardsIterator.size()) {
                logger.trace(
                        "[{}] not all required primary shards[{}/{}] are available for index[{}], retrying...",
                        request.index(), primaryShards.size(), groupShardsIterator.size()
                );
                overallRetry(fromClusterEvent, null, null);
                return false;
            }

            logger.trace("[{}] executing IndexUpdateByQueryRequest", request.index());
            DiscoveryNodes nodes = state.nodes();
            final ShardResponseListener responseListener = new ShardResponseListener(primaryShards.size(), indexActionListener);
            for (ShardRouting shard : primaryShards) {
                logger.trace("[{}][{}] executing ShardUpdateByQueryRequest", request.index(), shard.id());
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    executeLocally(shard, responseListener);
                } else {
                    executeRemotely(shard, nodes, responseListener);
                }
            }

            return true;
        }

        void overallRetry(boolean fromClusterEvent, final ShardRouting shardRouting, @Nullable final Throwable failure) {
            if (!fromClusterEvent) {
                clusterService.add(request.timeout(), new TimeoutClusterStateListener() {

                    public void postAdded() {
                        logger.trace("[{}] post added, retrying update by query", request.index());
                        if (startExecution(true)) {
                            // if we managed to start and perform the operation on the primary, we can remove this listener
                            clusterService.remove(this);
                        }
                    }

                    public void onClose() {
                        logger.trace("[{}] update by query for, node closed", request.index());
                        clusterService.remove(this);
                        indexActionListener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    public void clusterChanged(ClusterChangedEvent event) {
                        logger.trace("[{}] cluster changed, retrying update by query", request.index());
                        if (startExecution(true)) {
                            // if we managed to start and perform the operation on the primary, we can remove this listener
                            clusterService.remove(this);
                        }
                    }

                    public void onTimeout(TimeValue timeValue) {
                        // just to be on the safe side, see if we can start it now?
                        logger.trace("[{}] timeout, retrying update by query", request.index());
                        if (startExecution(true)) {
                            clusterService.remove(this);
                            return;
                        }
                        clusterService.remove(this);
                        Throwable listenerFailure = failure;
                        if (listenerFailure == null) {
                            if (shardRouting == null) {
                                listenerFailure = new UnavailableShardsException(null, "no available shards: Timeout waiting for [" + timeValue + "], request: " + request.toString());
                            } else {
                                listenerFailure = new UnavailableShardsException(shardRouting.shardId(), "[" + shardRouting.shardId() + "] not started, Timeout waiting for [" + timeValue + "], request: " + request.toString());
                            }
                        }
                        indexActionListener.onFailure(listenerFailure);
                    }
                });
            }
        }

        void executeLocally(final ShardRouting shard, final ShardResponseListener responseListener) {
            final ShardUpdateByQueryRequest localShardRequest = new ShardUpdateByQueryRequest(request, shard.id(), shard.currentNodeId());
            transportShardUpdateByQueryAction.execute(localShardRequest, new ActionListener<ShardUpdateByQueryResponse>() {

                public void onResponse(ShardUpdateByQueryResponse shardUpdateByQueryResponse) {
                    responseListener.handleResponse(shardUpdateByQueryResponse);
                }

                public void onFailure(Throwable e) {
                    responseListener.handleException(e, shard);
                }
            });
        }

        void executeRemotely(final ShardRouting shard, DiscoveryNodes nodes, final ShardResponseListener responseListener) {
            final DiscoveryNode discoveryNode = nodes.get(shard.currentNodeId());
            if (discoveryNode == null) {
                responseListener.handleException(new RuntimeException("No node for shard"), shard);
                return;
            }

            ShardUpdateByQueryRequest localShardRequest = new ShardUpdateByQueryRequest(request, shard.id(), shard.currentNodeId());
            transportService.sendRequest(discoveryNode, TransportShardUpdateByQueryAction.ACTION_NAME,
                    localShardRequest, new BaseTransportResponseHandler<ShardUpdateByQueryResponse>() {

                public ShardUpdateByQueryResponse newInstance() {
                    return new ShardUpdateByQueryResponse();
                }

                public void handleResponse(ShardUpdateByQueryResponse response) {
                    responseListener.handleResponse(response);
                }

                public void handleException(TransportException e) {
                    responseListener.handleException(e, shard);
                }

                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }

        private class ShardResponseListener {

            final AtomicReferenceArray<ShardUpdateByQueryResponse> shardResponses;

            final AtomicInteger indexCounter;

            final ActionListener<IndexUpdateByQueryResponse> finalListener;

            final int numberOfExpectedShardResponses;

            private ShardResponseListener(int numberOfPrimaryShards, ActionListener<IndexUpdateByQueryResponse> finalListener) {
                shardResponses = new AtomicReferenceArray<ShardUpdateByQueryResponse>(numberOfPrimaryShards);
                numberOfExpectedShardResponses = numberOfPrimaryShards;
                indexCounter = new AtomicInteger();
                this.finalListener = finalListener;
            }

            void handleResponse(ShardUpdateByQueryResponse response) {
                shardResponses.set(indexCounter.getAndIncrement(), response);
                if (indexCounter.get() == numberOfExpectedShardResponses) {
                    finalizeAction();
                }
            }

            void handleException(Throwable e, ShardRouting shard) {
                logger.debug("[{}][{}] error while executing update by query shard request", e, request.index(), shard.id());
                String failure = ExceptionsHelper.detailedMessage(e);
                shardResponses.set(indexCounter.getAndIncrement(), new ShardUpdateByQueryResponse(shard.id(), failure));
                if (indexCounter.get() == numberOfExpectedShardResponses) {
                    finalizeAction();
                }
            }

            void finalizeAction() {
                ShardUpdateByQueryResponse[] responses = new ShardUpdateByQueryResponse[shardResponses.length()];
                for (int i = 0; i < shardResponses.length(); i++) {
                    responses[i] = shardResponses.get(i);
                }
                IndexUpdateByQueryResponse finalResponse = new IndexUpdateByQueryResponse(
                        request.index(),
                        responses
                );
                finalListener.onResponse(finalResponse);
            }

        }

    }

    private class TransportHandler extends BaseTransportRequestHandler<UpdateByQueryRequest> {

        public UpdateByQueryRequest newInstance() {
            return new UpdateByQueryRequest();
        }

        public String executor() {
            return ThreadPool.Names.SAME;
        }

        public void messageReceived(UpdateByQueryRequest request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            doExecute(request, new ActionListener<UpdateByQueryResponse>() {

                public void onResponse(UpdateByQueryResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }
    }

}
