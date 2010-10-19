/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.ThrowableObjectInputStream;
import org.elasticsearch.common.io.ThrowableObjectOutputStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class TransportBroadcastOperationAction<Request extends BroadcastOperationRequest, Response extends BroadcastOperationResponse, ShardRequest extends BroadcastShardOperationRequest, ShardResponse extends BroadcastShardOperationResponse>
        extends BaseAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    protected final ThreadPool threadPool;

    protected TransportBroadcastOperationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;

        transportService.registerHandler(transportAction(), new TransportHandler());
        transportService.registerHandler(transportShardAction(), new ShardTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(request, listener).start();
    }

    protected abstract String transportAction();

    protected abstract String transportShardAction();

    protected abstract Request newRequest();

    protected abstract Response newResponse(Request request, AtomicReferenceArray shardsResponses, ClusterState clusterState);

    protected abstract ShardRequest newShardRequest();

    protected abstract ShardRequest newShardRequest(ShardRouting shard, Request request);

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardResponse shardOperation(ShardRequest request) throws ElasticSearchException;

    protected abstract GroupShardsIterator shards(Request request, ClusterState clusterState);

    /**
     * Allows to override how shard routing is iterated over. Default implementation uses
     * {@link ShardsIterator#nextActiveOrNull()}.
     *
     * <p>Note, if overriding this method, make sure to also override {@link #hasNextShard(org.elasticsearch.cluster.routing.ShardsIterator)}.
     */
    protected ShardRouting nextShardOrNull(ShardsIterator shardIt) {
        return shardIt.nextActiveOrNull();
    }

    /**
     * Allows to override how shard routing is iterated over. Default implementation uses
     * {@link ShardsIterator#hasNextActive()}.
     *
     * <p>Note, if overriding this method, make sure to also override {@link #nextShardOrNull(org.elasticsearch.cluster.routing.ShardsIterator)}.
     */
    protected boolean hasNextShard(ShardsIterator shardIt) {
        return shardIt.hasNextActive();
    }

    protected boolean accumulateExceptions() {
        return true;
    }

    protected boolean ignoreNonActiveExceptions() {
        return false;
    }

    protected void checkBlock(Request request, ClusterState state) {

    }

    class AsyncBroadcastAction {

        private final Request request;

        private final ActionListener<Response> listener;

        private final ClusterState clusterState;

        private final DiscoveryNodes nodes;

        private final GroupShardsIterator shardsIts;

        private final int expectedOps;

        private final AtomicInteger counterOps = new AtomicInteger();

        private final AtomicInteger indexCounter = new AtomicInteger();

        private final AtomicReferenceArray shardsResponses;

        AsyncBroadcastAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            clusterState = clusterService.state();

            // update to concrete indices
            request.indices(clusterState.metaData().concreteIndices(request.indices()));
            checkBlock(request, clusterState);

            nodes = clusterState.nodes();
            shardsIts = shards(request, clusterState);
            expectedOps = shardsIts.size();


            shardsResponses = new AtomicReferenceArray<Object>(expectedOps);
        }

        public void start() {
            if (shardsIts.size() == 0) {
                // no shards
                listener.onResponse(newResponse(request, new AtomicReferenceArray(0), clusterState));
            }
            // count the local operations, and perform the non local ones
            int localOperations = 0;
            for (final ShardsIterator shardIt : shardsIts) {
                final ShardRouting shard = nextShardOrNull(shardIt);
                if (shard != null) {
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // do the remote operation here, the localAsync flag is not relevant
                        performOperation(shardIt.reset(), true);
                    }
                } else {
                    // really, no shards active in this group
                    onOperation(shard, shardIt, null, false);
                }
            }
            // we have local operations, perform them now
            if (localOperations > 0) {
                if (request.operationThreading() == BroadcastOperationThreading.SINGLE_THREAD) {
                    request.beforeLocalFork();
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (final ShardsIterator shardIt : shardsIts) {
                                final ShardRouting shard = nextShardOrNull(shardIt.reset());
                                if (shard != null) {
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performOperation(shardIt.reset(), false);
                                    }
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == BroadcastOperationThreading.THREAD_PER_SHARD;
                    if (localAsync) {
                        request.beforeLocalFork();
                    }
                    for (final ShardsIterator shardIt : shardsIts) {
                        final ShardRouting shard = nextShardOrNull(shardIt.reset());
                        if (shard != null) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                performOperation(shardIt.reset(), localAsync);
                            }
                        }
                    }
                }
            }
        }

        private void performOperation(final ShardsIterator shardIt, boolean localAsync) {
            final ShardRouting shard = nextShardOrNull(shardIt);
            if (shard == null) {
                // no more active shards... (we should not really get here, just safety)
                onOperation(shard, shardIt, null, false);
            } else {
                final ShardRequest shardRequest = newShardRequest(shard, request);
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    if (localAsync) {
                        threadPool.execute(new Runnable() {
                            @Override public void run() {
                                try {
                                    onOperation(shard, shardOperation(shardRequest), true);
                                } catch (Exception e) {
                                    onOperation(shard, shardIt, e, true);
                                }
                            }
                        });
                    } else {
                        try {
                            onOperation(shard, shardOperation(shardRequest), false);
                        } catch (Exception e) {
                            onOperation(shard, shardIt, e, false);
                        }
                    }
                } else {
                    DiscoveryNode node = nodes.get(shard.currentNodeId());
                    if (node == null) {
                        // no node connected, act as failure
                        onOperation(shard, shardIt, null, false);
                    } else {
                        transportService.sendRequest(node, transportShardAction(), shardRequest, new BaseTransportResponseHandler<ShardResponse>() {
                            @Override public ShardResponse newInstance() {
                                return newShardResponse();
                            }

                            @Override public void handleResponse(ShardResponse response) {
                                onOperation(shard, response, false);
                            }

                            @Override public void handleException(RemoteTransportException e) {
                                onOperation(shard, shardIt, e, false);
                            }

                            @Override public boolean spawn() {
                                // we never spawn here, we will span if needed in onOperation
                                return false;
                            }
                        });
                    }
                }
            }
        }

        @SuppressWarnings({"unchecked"})
        private void onOperation(ShardRouting shard, ShardResponse response, boolean alreadyThreaded) {
            shardsResponses.set(indexCounter.getAndIncrement(), response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(alreadyThreaded);
            }
        }

        @SuppressWarnings({"unchecked"})
        private void onOperation(ShardRouting shard, final ShardsIterator shardIt, Throwable t, boolean alreadyThreaded) {
            if (!hasNextShard(shardIt)) {
                // e is null when there is no next active....
                if (logger.isDebugEnabled()) {
                    if (t != null) {
                        if (shard != null) {
                            logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                        } else {
                            logger.debug(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                        }
                    }
                }
                // no more shards in this group
                int index = indexCounter.getAndIncrement();
                if (accumulateExceptions()) {
                    if (t == null) {
                        if (!ignoreNonActiveExceptions()) {
                            t = new BroadcastShardOperationFailedException(shardIt.shardId(), "No active shard(s)");
                        }
                    } else if (!(t instanceof BroadcastShardOperationFailedException)) {
                        t = new BroadcastShardOperationFailedException(shardIt.shardId(), t);
                    }
                    shardsResponses.set(index, t);
                }
                if (expectedOps == counterOps.incrementAndGet()) {
                    finishHim(alreadyThreaded);
                }
                return;
            } else {
                // trace log this exception
                if (logger.isTraceEnabled()) {
                    if (t != null) {
                        if (shard != null) {
                            logger.trace(shard.shortSummary() + ": Failed to execute [" + request + "]", t);
                        } else {
                            logger.trace(shardIt.shardId() + ": Failed to execute [" + request + "]", t);
                        }
                    }
                }
            }
            // we are not threaded here if we got here from the transport
            // or we possibly threaded if we got from a local threaded one,
            // in which case, the next shard in the partition will not be local one
            // so there is no meaning to this flag
            performOperation(shardIt, true);
        }

        private void finishHim(boolean alreadyThreaded) {
            // if we need to execute the listener on a thread, and we are not threaded already
            // then do it
            if (request.listenerThreaded() && !alreadyThreaded) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            listener.onResponse(newResponse(request, shardsResponses, clusterState));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                try {
                    listener.onResponse(newResponse(request, shardsResponses, clusterState));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }
    }

    class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequest();
        }

        @Override public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // we just send back a response, no need to fork a listener
            request.listenerThreaded(false);
            // we don't spawn, so if we get a request with no threading, change it to single threaded
            if (request.operationThreading() == BroadcastOperationThreading.NO_THREADS) {
                request.operationThreading(BroadcastOperationThreading.SINGLE_THREAD);
            }
            execute(request, new ActionListener<Response>() {
                @Override public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    class ShardTransportHandler extends BaseTransportRequestHandler<ShardRequest> {

        @Override public ShardRequest newInstance() {
            return newShardRequest();
        }

        @Override public void messageReceived(ShardRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(shardOperation(request));
        }
    }

    // FROM HERE: When we move to a single remote call with all shard requests to the same node, then
    // the below classes can help

    class ShardsTransportHandler extends BaseTransportRequestHandler<ShardsRequest> {

        @Override public ShardsRequest newInstance() {
            return new ShardsRequest();
        }

        @Override public void messageReceived(final ShardsRequest request, final TransportChannel channel) throws Exception {
            if (request.operationThreading() == BroadcastOperationThreading.THREAD_PER_SHARD) {
                final AtomicInteger counter = new AtomicInteger(request.requests().size());
                final AtomicInteger index = new AtomicInteger();
                final AtomicReferenceArray results = new AtomicReferenceArray(request.requests().size());
                for (final ShardRequest singleRequest : request.requests()) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            int arrIndex = index.getAndIncrement();
                            try {
                                results.set(arrIndex, shardOperation(singleRequest));
                            } catch (Exception e) {
                                results.set(arrIndex, new BroadcastShardOperationFailedException(new ShardId(singleRequest.index(), singleRequest.shardId()), e));
                            }
                            if (counter.decrementAndGet() == 0) {
                                // we are done
                                List<ShardResponse> responses = newArrayListWithCapacity(request.requests().size());
                                List<BroadcastShardOperationFailedException> exceptions = null;
                                for (int i = 0; i < results.length(); i++) {
                                    Object result = results.get(i);
                                    if (result instanceof BroadcastShardOperationFailedException) {
                                        if (exceptions == null) {
                                            exceptions = newArrayList();
                                        }
                                        exceptions.add((BroadcastShardOperationFailedException) result);
                                    } else {
                                        responses.add((ShardResponse) result);
                                    }
                                }
                                try {
                                    channel.sendResponse(new ShardsResponse(responses, exceptions));
                                } catch (IOException e) {
                                    logger.warn("Failed to send broadcast response", e);
                                }
                            }
                        }
                    });
                }
            } else {
                // single thread
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        List<ShardResponse> responses = newArrayListWithCapacity(request.requests().size());
                        List<BroadcastShardOperationFailedException> exceptions = null;
                        for (ShardRequest singleRequest : request.requests()) {
                            try {
                                responses.add(shardOperation(singleRequest));
                            } catch (Exception e) {
                                if (exceptions == null) {
                                    exceptions = newArrayList();
                                }
                                exceptions.add(new BroadcastShardOperationFailedException(new ShardId(singleRequest.index(), singleRequest.shardId()), e));
                            }
                        }
                        try {
                            channel.sendResponse(new ShardsResponse(responses, exceptions));
                        } catch (IOException e) {
                            logger.warn("Failed to send broadcast response", e);
                        }
                    }
                });
            }
        }

        @Override public boolean spawn() {
            // we handle the forking here...
            return false;
        }
    }

    class ShardsResponse implements Streamable {

        private List<ShardResponse> responses;

        private List<BroadcastShardOperationFailedException> exceptions;

        ShardsResponse() {
        }

        ShardsResponse(List<ShardResponse> responses, List<BroadcastShardOperationFailedException> exceptions) {
            this.responses = responses;
            this.exceptions = exceptions;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            responses = newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                ShardResponse response = newShardResponse();
                response.readFrom(in);
                responses.add(response);
            }
            size = in.readVInt();
            if (size > 0) {
                exceptions = newArrayListWithCapacity(size);
                ThrowableObjectInputStream toi = new ThrowableObjectInputStream(in);
                for (int i = 0; i < size; i++) {
                    try {
                        exceptions.add((BroadcastShardOperationFailedException) toi.readObject());
                    } catch (ClassNotFoundException e) {
                        throw new IOException("Failed to load class", e);
                    }
                }
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(responses.size());
            for (BroadcastShardOperationResponse response : responses) {
                response.writeTo(out);
            }

            if (exceptions == null || exceptions.isEmpty()) {
                out.writeInt(0);
            } else {
                out.writeInt(exceptions.size());
                ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(out);
                for (BroadcastShardOperationFailedException ex : exceptions) {
                    too.writeObject(ex);
                }
                too.flush();
            }
        }
    }

    class ShardsRequest implements Streamable {

        private BroadcastOperationThreading operationThreading = BroadcastOperationThreading.SINGLE_THREAD;

        private List<ShardRequest> requests;

        ShardsRequest() {
        }

        public List<ShardRequest> requests() {
            return this.requests;
        }

        public BroadcastOperationThreading operationThreading() {
            return operationThreading;
        }

        ShardsRequest(BroadcastOperationThreading operationThreading, List<ShardRequest> requests) {
            this.operationThreading = operationThreading;
            this.requests = requests;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            operationThreading = BroadcastOperationThreading.fromId(in.readByte());
            int size = in.readVInt();
            if (size == 0) {
                requests = ImmutableList.of();
            } else {
                requests = newArrayListWithCapacity(in.readVInt());
                for (int i = 0; i < size; i++) {
                    ShardRequest request = newShardRequest();
                    request.readFrom(in);
                    requests.add(request);
                }
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(operationThreading.id());
            out.writeVInt(requests.size());
            for (BroadcastShardOperationRequest request : requests) {
                request.writeTo(out);
            }
        }
    }
}
