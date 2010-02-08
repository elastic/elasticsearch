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

package org.elasticsearch.action.support.replication;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class TransportIndexReplicationOperationAction<Request extends IndexReplicationOperationRequest, Response extends ActionResponse, ShardRequest extends ShardReplicationOperationRequest, ShardResponse extends ActionResponse>
        extends BaseAction<Request, Response> {

    protected final ThreadPool threadPool;

    protected final TransportShardReplicationOperationAction<ShardRequest, ShardResponse> shardAction;

    @Inject public TransportIndexReplicationOperationAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                                            TransportShardReplicationOperationAction<ShardRequest, ShardResponse> shardAction) {
        super(settings);
        this.threadPool = threadPool;
        this.shardAction = shardAction;

        transportService.registerHandler(transportAction(), new TransportHandler());
    }

    @Override protected void doExecute(final Request request, final ActionListener<Response> listener) {
        GroupShardsIterator groups;
        try {
            groups = shards(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(groups.size());
        final AtomicReferenceArray<Object> shardsResponses = new AtomicReferenceArray<Object>(groups.size());

        for (final ShardsIterator shards : groups) {
            ShardRequest shardRequest = newShardRequestInstance(request, shards.shardId().id());
            // TODO for now, we fork operations on shards of the index
            shardRequest.operationThreaded(true);
            // no need for threaded listener, we will fork when its done based on the index request
            shardRequest.listenerThreaded(false);
            shardAction.execute(shardRequest, new ActionListener<ShardResponse>() {
                @Override public void onResponse(ShardResponse result) {
                    shardsResponses.set(indexCounter.getAndIncrement(), result);
                    if (completionCounter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    listener.onResponse(newResponseInstance(request, shardsResponses));
                                }
                            });
                        } else {
                            listener.onResponse(newResponseInstance(request, shardsResponses));
                        }
                    }
                }

                @Override public void onFailure(Throwable e) {
                    int index = indexCounter.getAndIncrement();
                    if (accumulateExceptions()) {
                        shardsResponses.set(index, e);
                    }
                    if (completionCounter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    listener.onResponse(newResponseInstance(request, shardsResponses));
                                }
                            });
                        } else {
                            listener.onResponse(newResponseInstance(request, shardsResponses));
                        }
                    }
                }
            });
        }
    }

    protected abstract Request newRequestInstance();

    protected abstract Response newResponseInstance(Request request, AtomicReferenceArray shardsResponses);

    protected abstract String transportAction();

    protected abstract GroupShardsIterator shards(Request request) throws ElasticSearchException;

    protected abstract ShardRequest newShardRequestInstance(Request request, int shardId);

    protected abstract boolean accumulateExceptions();

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequestInstance();
        }

        @Override public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
            execute(request, new ActionListener<Response>() {
                @Override public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action [" + transportAction() + "] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            // no need to spawn, since in the doExecute we always execute with threaded operation set to true
            return false;
        }
    }
}