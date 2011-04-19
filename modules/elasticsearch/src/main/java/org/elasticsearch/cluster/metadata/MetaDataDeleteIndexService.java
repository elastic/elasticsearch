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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardsAllocation;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;

/**
 * @author kimchy (shay.banon)
 */
public class MetaDataDeleteIndexService extends AbstractComponent {

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final ShardsAllocation shardsAllocation;

    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    @Inject public MetaDataDeleteIndexService(Settings settings, ThreadPool threadPool, ClusterService clusterService, ShardsAllocation shardsAllocation,
                                              NodeIndexDeletedAction nodeIndexDeletedAction) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.shardsAllocation = shardsAllocation;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
    }

    public void deleteIndex(final Request request, final Listener userListener) {
        clusterService.submitStateUpdateTask("delete-index [" + request.index + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                final DeleteIndexListener listener = new DeleteIndexListener(request, userListener);
                try {
                    if (!currentState.metaData().hasIndex(request.index)) {
                        listener.onFailure(new IndexMissingException(new Index(request.index)));
                        return currentState;
                    }

                    logger.info("[{}] deleting index", request.index);

                    RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                    for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                        if (!indexRoutingTable.index().equals(request.index)) {
                            routingTableBuilder.add(indexRoutingTable);
                        }
                    }
                    MetaData newMetaData = newMetaDataBuilder()
                            .metaData(currentState.metaData())
                            .remove(request.index)
                            .build();

                    RoutingAllocation.Result routingResult = shardsAllocation.reroute(
                            newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());

                    ClusterBlocks blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(request.index).build();

                    // initialize the counter only for nodes the shards are allocated to
                    Set<String> allocatedNodes = Sets.newHashSet();
                    if (currentState.routingTable().hasIndex(request.index)) {
                        for (IndexShardRoutingTable indexShardRoutingTable : currentState.routingTable().index(request.index)) {
                            for (ShardRouting shardRouting : indexShardRoutingTable) {
                                if (shardRouting.currentNodeId() != null) {
                                    allocatedNodes.add(shardRouting.currentNodeId());
                                }
                                if (shardRouting.relocatingNodeId() != null) {
                                    allocatedNodes.add(shardRouting.relocatingNodeId());
                                }
                            }
                        }
                    }

                    if (allocatedNodes.isEmpty()) {
                        // no nodes allocated, don't wait for a response
                        listener.onResponse(new Response(true));
                    } else {
                        final AtomicInteger counter = new AtomicInteger(allocatedNodes.size());

                        final NodeIndexDeletedAction.Listener nodeIndexDeleteListener = new NodeIndexDeletedAction.Listener() {
                            @Override public void onNodeIndexDeleted(String index, String nodeId) {
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
                            @Override public void run() {
                                listener.onResponse(new Response(false));
                                nodeIndexDeletedAction.remove(nodeIndexDeleteListener);
                            }
                        });
                    }

                    return newClusterStateBuilder().state(currentState).routingResult(routingResult).metaData(newMetaData).blocks(blocks).build();
                } catch (Exception e) {
                    listener.onFailure(e);
                    return currentState;
                }
            }
        });
    }

    class DeleteIndexListener implements Listener {

        private AtomicBoolean notified = new AtomicBoolean();

        private final Request request;

        private final Listener listener;

        volatile ScheduledFuture future;

        private DeleteIndexListener(Request request, Listener listener) {
            this.request = request;
            this.listener = listener;
        }

        @Override public void onResponse(final Response response) {
            if (notified.compareAndSet(false, true)) {
                if (future != null) {
                    future.cancel(false);
                }
                listener.onResponse(response);
            }
        }

        @Override public void onFailure(Throwable t) {
            if (notified.compareAndSet(false, true)) {
                if (future != null) {
                    future.cancel(false);
                }
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

        public Request(String index) {
            this.index = index;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
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
