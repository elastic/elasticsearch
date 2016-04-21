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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

/**
 *
 */
public abstract class TransportBroadcastAction<Request extends BroadcastRequest<Request>, Response extends BroadcastResponse, ShardRequest extends BroadcastShardRequest, ShardResponse extends BroadcastShardResponse>
        extends HandledTransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final TransportService transportService;

    final String transportShardAction;

    protected TransportBroadcastAction(Settings settings, String actionName, ThreadPool threadPool, ClusterService clusterService,
                                       TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       Supplier<Request> request, Supplier<ShardRequest> shardRequest, String shardExecutor) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, request);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportShardAction = actionName + "[s]";

        transportService.registerRequestHandler(transportShardAction, shardRequest, shardExecutor, new ShardTransportHandler());
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(task, request, listener).start();
    }

    @Override
    protected final void doExecute(Request request, ActionListener<Response> listener) {
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    protected abstract Response newResponse(Request request, AtomicReferenceArray shardsResponses, ClusterState clusterState);

    protected abstract ShardRequest newShardRequest(int numShards, ShardRouting shard, Request request);

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardResponse shardOperation(ShardRequest request);

    protected ShardResponse shardOperation(ShardRequest request, Task task) {
        return shardOperation(request);
    }

    /**
     * Determines the shards this operation will be executed on. The operation is executed once per shard iterator, typically
     * on the first shard in it. If the operation fails, it will be retried on the next shard in the iterator.
     */
    protected abstract GroupShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices);

    protected abstract ClusterBlockException checkGlobalBlock(ClusterState state, Request request);

    protected abstract ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices);

    protected class AsyncBroadcastAction {

        private final Task task;
        private final Request request;
        private final ActionListener<Response> listener;
        private final ClusterState clusterState;
        private final DiscoveryNodes nodes;
        private final GroupShardsIterator shardsIts;
        private final int expectedOps;
        private final AtomicInteger counterOps = new AtomicInteger();
        private final AtomicReferenceArray shardsResponses;

        protected AsyncBroadcastAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();

            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }
            // update to concrete indices
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
            blockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (blockException != null) {
                throw blockException;
            }

            nodes = clusterState.nodes();
            logger.trace("resolving shards based on cluster state version [{}]", clusterState.version());
            shardsIts = shards(clusterState, request, concreteIndices);
            expectedOps = shardsIts.size();

            shardsResponses = new AtomicReferenceArray<>(expectedOps);
        }

        public void start() {
            if (shardsIts.size() == 0) {
                // no shards
                try {
                    listener.onResponse(newResponse(request, new AtomicReferenceArray(0), clusterState));
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
                return;
            }
            // count the local operations, and perform the non local ones
            int shardIndex = -1;
            for (final ShardIterator shardIt : shardsIts) {
                shardIndex++;
                final ShardRouting shard = shardIt.nextOrNull();
                if (shard != null) {
                    performOperation(shardIt, shard, shardIndex);
                } else {
                    // really, no shards active in this group
                    onOperation(null, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
                }
            }
        }

        protected void performOperation(final ShardIterator shardIt, final ShardRouting shard, final int shardIndex) {
            if (shard == null) {
                // no more active shards... (we should not really get here, just safety)
                onOperation(null, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
            } else {
                try {
                    final ShardRequest shardRequest = newShardRequest(shardIt.size(), shard, request);
                    shardRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    DiscoveryNode node = nodes.get(shard.currentNodeId());
                    if (node == null) {
                        // no node connected, act as failure
                        onOperation(shard, shardIt, shardIndex, new NoShardAvailableActionException(shardIt.shardId()));
                    } else {
                        taskManager.registerChildTask(task, node.getId());
                        transportService.sendRequest(node, transportShardAction, shardRequest, new BaseTransportResponseHandler<ShardResponse>() {
                            @Override
                            public ShardResponse newInstance() {
                                return newShardResponse();
                            }

                            @Override
                            public String executor() {
                                return ThreadPool.Names.SAME;
                            }

                            @Override
                            public void handleResponse(ShardResponse response) {
                                onOperation(shard, shardIndex, response);
                            }

                            @Override
                            public void handleException(TransportException e) {
                                onOperation(shard, shardIt, shardIndex, e);
                            }
                        });
                    }
                } catch (Throwable e) {
                    onOperation(shard, shardIt, shardIndex, e);
                }
            }
        }

        @SuppressWarnings({"unchecked"})
        protected void onOperation(ShardRouting shard, int shardIndex, ShardResponse response) {
            logger.trace("received response for {}", shard);
            shardsResponses.set(shardIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim();
            }
        }

        @SuppressWarnings({"unchecked"})
        void onOperation(@Nullable ShardRouting shard, final ShardIterator shardIt, int shardIndex, Throwable t) {
            // we set the shard failure always, even if its the first in the replication group, and the next one
            // will work (it will just override it...)
            setFailure(shardIt, shardIndex, t);
            ShardRouting nextShard = shardIt.nextOrNull();
            if (nextShard != null) {
                if (t != null) {
                    if (logger.isTraceEnabled()) {
                        if (!TransportActions.isShardNotAvailableException(t)) {
                            logger.trace("{}: failed to execute [{}]", t, shard != null ? shard.shortSummary() : shardIt.shardId(), request);
                        }
                    }
                }
                performOperation(shardIt, nextShard, shardIndex);
            } else {
                if (logger.isDebugEnabled()) {
                    if (t != null) {
                        if (!TransportActions.isShardNotAvailableException(t)) {
                            logger.debug("{}: failed to execute [{}]", t, shard != null ? shard.shortSummary() : shardIt.shardId(), request);
                        }
                    }
                }
                if (expectedOps == counterOps.incrementAndGet()) {
                    finishHim();
                }
            }
        }

        protected void finishHim() {
            try {
                listener.onResponse(newResponse(request, shardsResponses, clusterState));
            } catch (Throwable e) {
                listener.onFailure(e);
            }
        }

        void setFailure(ShardIterator shardIt, int shardIndex, Throwable t) {
            // we don't aggregate shard failures on non active shards (but do keep the header counts right)
            if (TransportActions.isShardNotAvailableException(t)) {
                return;
            }

            if (!(t instanceof BroadcastShardOperationFailedException)) {
                t = new BroadcastShardOperationFailedException(shardIt.shardId(), t);
            }

            Object response = shardsResponses.get(shardIndex);
            if (response == null) {
                // just override it and return
                shardsResponses.set(shardIndex, t);
            }

            if (!(response instanceof Throwable)) {
                // we should never really get here...
                return;
            }

            // the failure is already present, try and not override it with an exception that is less meaningless
            // for example, getting illegal shard state
            if (TransportActions.isReadOverrideException(t)) {
                shardsResponses.set(shardIndex, t);
            }
        }
    }

    class ShardTransportHandler implements TransportRequestHandler<ShardRequest> {

        @Override
        public void messageReceived(ShardRequest request, TransportChannel channel, Task task) throws Exception {
            channel.sendResponse(shardOperation(request));
        }

        @Override
        public final void messageReceived(final ShardRequest request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required");
        }
    }
}
