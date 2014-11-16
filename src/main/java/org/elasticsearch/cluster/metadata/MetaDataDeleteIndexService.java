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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MetaDataDeleteIndexService extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    private final MetaDataService metaDataService;

    @Inject
    public MetaDataDeleteIndexService(Settings settings, ThreadPool threadPool, ClusterService clusterService, AllocationService allocationService,
                                      NodeIndexDeletedAction nodeIndexDeletedAction, MetaDataService metaDataService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.metaDataService = metaDataService;
    }

    public void deleteIndex(final Request request, final Listener userListener) {
        // we lock here, and not within the cluster service callback since we don't want to
        // block the whole cluster state handling
        final Semaphore mdLock = metaDataService.indexMetaDataLock(request.index);

        // quick check to see if we can acquire a lock, otherwise spawn to a thread pool
        if (mdLock.tryAcquire()) {
            deleteIndex(request, userListener, mdLock);
            return;
        }

        threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!mdLock.tryAcquire(request.masterTimeout.nanos(), TimeUnit.NANOSECONDS)) {
                        userListener.onFailure(new ProcessClusterEventTimeoutException(request.masterTimeout, "acquire index lock"));
                        return;
                    }
                } catch (InterruptedException e) {
                    userListener.onFailure(e);
                    return;
                }

                deleteIndex(request, userListener, mdLock);
            }
        });
    }

    private void deleteIndex(final Request request, final Listener userListener, Semaphore mdLock) {
        final DeleteIndexListener listener = new DeleteIndexListener(mdLock, userListener);
        clusterService.submitStateUpdateTask("delete-index [" + request.index + "]", Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                if (!currentState.metaData().hasConcreteIndex(request.index)) {
                    throw new IndexMissingException(new Index(request.index));
                }

                logger.info("[{}] deleting index", request.index);

                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                routingTableBuilder.remove(request.index);

                MetaData newMetaData = MetaData.builder(currentState.metaData())
                        .remove(request.index)
                        .build();

                RoutingAllocation.Result routingResult = allocationService.reroute(
                        ClusterState.builder(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());

                ClusterBlocks blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(request.index).build();

                // wait for events from all nodes that it has been removed from their respective metadata...
                int count = currentState.nodes().size();
                // add the notifications that the store was deleted from *data* nodes
                count += currentState.nodes().dataNodes().size();
                final AtomicInteger counter = new AtomicInteger(count);
                // this listener will be notified once we get back a notification based on the cluster state change below.
                final NodeIndexDeletedAction.Listener nodeIndexDeleteListener = new NodeIndexDeletedAction.Listener() {
                    @Override
                    public void onNodeIndexDeleted(String index, String nodeId) {
                        if (index.equals(request.index)) {
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(new Response(true));
                                nodeIndexDeletedAction.remove(this);
                            }
                        }
                    }

                    @Override
                    public void onNodeIndexStoreDeleted(String index, String nodeId) {
                        if (index.equals(request.index)) {
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(new Response(true));
                                nodeIndexDeletedAction.remove(this);
                            }
                        }
                    }
                };
                nodeIndexDeletedAction.add(nodeIndexDeleteListener);

                listener.future = threadPool.schedule(request.timeout, ThreadPool.Names.SAME, new Runnable() {
                    @Override
                    public void run() {
                        listener.onResponse(new Response(false));
                        nodeIndexDeletedAction.remove(nodeIndexDeleteListener);
                    }
                });

                return ClusterState.builder(currentState).routingResult(routingResult).metaData(newMetaData).blocks(blocks).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            }
        });
    }

    class DeleteIndexListener implements Listener {

        private final AtomicBoolean notified = new AtomicBoolean();
        private final Semaphore mdLock;
        private final Listener listener;
        volatile ScheduledFuture<?> future;

        private DeleteIndexListener(Semaphore mdLock, Listener listener) {
            this.mdLock = mdLock;
            this.listener = listener;
        }

        @Override
        public void onResponse(final Response response) {
            if (notified.compareAndSet(false, true)) {
                mdLock.release();
                FutureUtils.cancel(future);
                listener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (notified.compareAndSet(false, true)) {
                mdLock.release();
                FutureUtils.cancel(future);
                listener.onFailure(t);
            }
        }
    }


    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String index;

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        TimeValue masterTimeout = MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public Request(String index) {
            this.index = index;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Request masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
        }
    }

    public static class Response {
        private final boolean acknowledged;

        public Response(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }
}
