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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.PrimaryNotStartedActionException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.cluster.node.Nodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.io.VoidStreamable;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class TransportShardReplicationOperationAction<Request extends ShardReplicationOperationRequest, Response extends ActionResponse> extends BaseAction<Request, Response> {

    protected final TransportService transportService;

    protected final ClusterService clusterService;

    protected final IndicesService indicesService;

    protected final ThreadPool threadPool;

    protected final ShardStateAction shardStateAction;

    protected TransportShardReplicationOperationAction(Settings settings, TransportService transportService,
                                                       ClusterService clusterService, IndicesService indicesService,
                                                       ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.shardStateAction = shardStateAction;

        transportService.registerHandler(transportAction(), new OperationTransportHandler());
        transportService.registerHandler(transportBackupAction(), new BackupOperationTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncShardOperationAction(request, listener).start();
    }

    protected abstract Request newRequestInstance();

    protected abstract Response newResponseInstance();

    protected abstract String transportAction();

    protected abstract Response shardOperationOnPrimary(ShardOperationRequest shardRequest);

    protected abstract void shardOperationOnBackup(ShardOperationRequest shardRequest);

    protected abstract ShardsIterator shards(Request request) throws ElasticSearchException;

    /**
     * Should the operations be performed on the backups as well. Defaults to <tt>false</tt> meaning operations
     * will be executed on the backup.
     */
    protected boolean ignoreBackups() {
        return false;
    }

    private String transportBackupAction() {
        return transportAction() + "/backup";
    }

    protected IndexShard indexShard(ShardOperationRequest shardRequest) {
        return indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
    }

    private class OperationTransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequestInstance();
        }

        @Override public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.operationThreaded(true);
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
                        logger.warn("Failed to send response for " + transportAction(), e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class BackupOperationTransportHandler extends BaseTransportRequestHandler<ShardOperationRequest> {

        @Override public ShardOperationRequest newInstance() {
            return new ShardOperationRequest();
        }

        @Override public void messageReceived(ShardOperationRequest request, TransportChannel channel) throws Exception {
            shardOperationOnBackup(request);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    protected class ShardOperationRequest implements Streamable {

        public int shardId;

        public Request request;

        public ShardOperationRequest() {
        }

        public ShardOperationRequest(int shardId, Request request) {
            this.shardId = shardId;
            this.request = request;
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            shardId = in.readInt();
            request = newRequestInstance();
            request.readFrom(in);
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeInt(shardId);
            request.writeTo(out);
        }
    }

    private class AsyncShardOperationAction {

        private final ActionListener<Response> listener;

        private final Request request;

        private Nodes nodes;

        private ShardsIterator shards;

        private final AtomicBoolean primaryOperationStarted = new AtomicBoolean();

        private AsyncShardOperationAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            start(false);
        }

        /**
         * Returns <tt>true</tt> if the action starting to be performed on the primary (or is done).
         */
        public boolean start(final boolean fromClusterEvent) throws ElasticSearchException {
            ClusterState clusterState = clusterService.state();
            nodes = clusterState.nodes();
            try {
                shards = shards(request);
            } catch (Exception e) {
                listener.onFailure(new ShardOperationFailedException(shards.shardId(), e));
                return true;
            }

            boolean foundPrimary = false;
            for (final ShardRouting shard : shards) {
                if (shard.primary()) {
                    if (!shard.active()) {
                        retryPrimary(fromClusterEvent, shard);
                        return false;
                    }

                    if (!primaryOperationStarted.compareAndSet(false, true)) {
                        return false;
                    }

                    foundPrimary = true;
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        if (request.operationThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    performOnPrimary(shard.id(), fromClusterEvent, true, shard);
                                }
                            });
                        } else {
                            performOnPrimary(shard.id(), fromClusterEvent, false, shard);
                        }
                    } else {
                        Node node = nodes.get(shard.currentNodeId());
                        transportService.sendRequest(node, transportAction(), request, new BaseTransportResponseHandler<Response>() {

                            @Override public Response newInstance() {
                                return newResponseInstance();
                            }

                            @Override public void handleResponse(Response response) {
                                listener.onResponse(response);
                            }

                            @Override public void handleException(RemoteTransportException exp) {
                                listener.onFailure(exp);
                            }

                            @Override public boolean spawn() {
                                return request.listenerThreaded();
                            }
                        });
                    }
                    break;
                }
            }
            // we should never get here, but here we go
            if (!foundPrimary) {
                final PrimaryNotStartedActionException failure = new PrimaryNotStartedActionException(shards.shardId(), "Primary not found");
                if (request.listenerThreaded()) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            listener.onFailure(failure);
                        }
                    });
                } else {
                    listener.onFailure(failure);
                }
            }
            return true;
        }

        private void retryPrimary(boolean fromClusterEvent, final ShardRouting shard) {
            if (!fromClusterEvent) {
                // make it threaded operation so we fork on the discovery listener thread
                request.operationThreaded(true);
                clusterService.add(request.timeout(), new TimeoutClusterStateListener() {
                    @Override public void clusterChanged(ClusterChangedEvent event) {
                        if (start(true)) {
                            // if we managed to start and perform the operation on the primary, we can remove this listener
                            clusterService.remove(this);
                        }
                    }

                    @Override public void onTimeout(TimeValue timeValue) {
                        final PrimaryNotStartedActionException failure = new PrimaryNotStartedActionException(shard.shardId(), "Timeout waiting for [" + timeValue + "]");
                        if (request.listenerThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    listener.onFailure(failure);
                                }
                            });
                        } else {
                            listener.onFailure(failure);
                        }
                    }
                });
            }
        }

        private void performOnPrimary(int primaryShardId, boolean fromDiscoveryListener, boolean alreadyThreaded, final ShardRouting shard) {
            try {
                Response response = shardOperationOnPrimary(new ShardOperationRequest(primaryShardId, request));
                performBackups(response, alreadyThreaded);
            } catch (IndexShardNotStartedException e) {
                // still in recovery, retry (we know that its not UNASSIGNED OR INITIALIZING since we are checking it in the calling method)
                retryPrimary(fromDiscoveryListener, shard);
            } catch (Exception e) {
                listener.onFailure(new ShardOperationFailedException(shards.shardId(), e));
            }
        }

        private void performBackups(final Response response, boolean alreadyThreaded) {
            if (ignoreBackups() || shards.size() == 1 /* no backups */) {
                if (alreadyThreaded || !request.listenerThreaded()) {
                    listener.onResponse(response);
                } else {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            listener.onResponse(response);
                        }
                    });
                }
                return;
            }

            // initialize the counter
            int backupCounter = 0;
            for (final ShardRouting shard : shards.reset()) {
                if (shard.primary()) {
                    continue;
                }
                backupCounter++;
                // if we are relocating the backup, we want to perform the index operation on both the relocating
                // shard and the target shard. This means that we won't loose index operations between end of recovery
                // and reassignment of the shard by the master node
                if (shard.relocating()) {
                    backupCounter++;
                }
            }

            AtomicInteger counter = new AtomicInteger(backupCounter);
            for (final ShardRouting shard : shards.reset()) {
                if (shard.primary()) {
                    continue;
                }
                // we index on a backup that is initializing as well since we might not have got the event
                // yet that it was started. We will get an exception IllegalShardState exception if its not started
                // and that's fine, we will ignore it
                if (shard.unassigned()) {
                    if (counter.decrementAndGet() == 0) {
                        if (alreadyThreaded || !request.listenerThreaded()) {
                            listener.onResponse(response);
                        } else {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    listener.onResponse(response);
                                }
                            });
                        }
                        break;
                    }
                    continue;
                }
                performOnBackup(response, counter, shard, shard.currentNodeId());
                if (shard.relocating()) {
                    performOnBackup(response, counter, shard, shard.relocatingNodeId());
                }
            }
        }

        private void performOnBackup(final Response response, final AtomicInteger counter, final ShardRouting shard, String nodeId) {
            final ShardOperationRequest shardRequest = new ShardOperationRequest(shards.shardId().id(), request);
            if (!nodeId.equals(nodes.localNodeId())) {
                Node node = nodes.get(nodeId);
                transportService.sendRequest(node, transportBackupAction(), shardRequest, new VoidTransportResponseHandler() {
                    @Override public void handleResponse(VoidStreamable vResponse) {
                        finishIfPossible();
                    }

                    @Override public void handleException(RemoteTransportException exp) {
                        if (!ignoreBackupException(exp.unwrapCause())) {
                            logger.warn("Failed to perform " + transportAction() + " on backup " + shards.shardId(), exp);
                            shardStateAction.shardFailed(shard);
                        }
                        finishIfPossible();
                    }

                    private void finishIfPossible() {
                        if (counter.decrementAndGet() == 0) {
                            if (request.listenerThreaded()) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        listener.onResponse(response);
                                    }
                                });
                            } else {
                                listener.onResponse(response);
                            }
                        }
                    }

                    @Override public boolean spawn() {
                        // don't spawn, we will call the listener on a thread pool if needed
                        return false;
                    }
                });
            } else {
                if (request.operationThreaded()) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            try {
                                shardOperationOnBackup(shardRequest);
                            } catch (Exception e) {
                                if (!ignoreBackupException(e)) {
                                    logger.warn("Failed to perform " + transportAction() + " on backup " + shards.shardId(), e);
                                    shardStateAction.shardFailed(shard);
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(response);
                            }
                        }
                    });
                } else {
                    try {
                        shardOperationOnBackup(shardRequest);
                    } catch (Exception e) {
                        if (!ignoreBackupException(e)) {
                            logger.warn("Failed to perform " + transportAction() + " on backup " + shards.shardId(), e);
                            shardStateAction.shardFailed(shard);
                        }
                    }
                    if (counter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    listener.onResponse(response);
                                }
                            });
                        } else {
                            listener.onResponse(response);
                        }
                    }
                }
            }
        }

        /**
         * Should an exception be ignored when the operation is performed on the backup. The exception
         * is ignored if it is:
         *
         * <ul>
         * <li><tt>IllegalIndexShardStateException</tt>: The shard has not yet moved to started mode (it is still recovering).
         * <li><tt>IndexMissingException</tt>/<tt>IndexShardMissingException</tt>: The shard has not yet started to initialize on the target node.
         * </ul>
         */
        private boolean ignoreBackupException(Throwable e) {
            if (e instanceof IllegalIndexShardStateException) {
                return true;
            }
            if (e instanceof IndexMissingException) {
                return true;
            }
            if (e instanceof IndexShardMissingException) {
                return true;
            }
            return false;
        }
    }
}
