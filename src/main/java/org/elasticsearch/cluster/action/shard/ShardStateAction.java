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

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;

/**
 *
 */
public class ShardStateAction extends AbstractComponent {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final ThreadPool threadPool;

    private final BlockingQueue<ShardRouting> startedShardsQueue = ConcurrentCollections.newBlockingQueue();

    @Inject
    public ShardStateAction(Settings settings, ClusterService clusterService, TransportService transportService,
                            AllocationService allocationService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.allocationService = allocationService;
        this.threadPool = threadPool;

        transportService.registerHandler(ShardStartedTransportHandler.ACTION, new ShardStartedTransportHandler());
        transportService.registerHandler(ShardFailedTransportHandler.ACTION, new ShardFailedTransportHandler());
    }

    public void shardFailed(final ShardRouting shardRouting, final String reason) throws ElasticSearchException {
        logger.warn("sending failed shard for {}, reason [{}]", shardRouting, reason);
        DiscoveryNodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            innerShardFailed(shardRouting, reason);
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    ShardFailedTransportHandler.ACTION, new ShardRoutingEntry(shardRouting, reason), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleException(TransportException exp) {
                    logger.warn("failed to send failed shard to [{}]", exp, clusterService.state().nodes().masterNode());
                }
            });
        }
    }

    public void shardStarted(final ShardRouting shardRouting, final String reason) throws ElasticSearchException {
        if (logger.isDebugEnabled()) {
            logger.debug("sending shard started for {}, reason [{}]", shardRouting, reason);
        }
        DiscoveryNodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            innerShardStarted(shardRouting, reason);
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    ShardStartedTransportHandler.ACTION, new ShardRoutingEntry(shardRouting, reason), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleException(TransportException exp) {
                    logger.warn("failed to send shard started to [{}]", exp, clusterService.state().nodes().masterNode());
                }
            });
        }
    }

    private void innerShardFailed(final ShardRouting shardRouting, final String reason) {
        logger.warn("received shard failed for {}, reason [{}]", shardRouting, reason);
        clusterService.submitStateUpdateTask("shard-failed (" + shardRouting + "), reason [" + reason + "]", Priority.HIGH, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Received failed shard {}, reason [{}]", shardRouting, reason);
                }
                RoutingAllocation.Result routingResult = allocationService.applyFailedShard(currentState, shardRouting);
                if (!routingResult.changed()) {
                    return currentState;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Applying failed shard {}, reason [{}]", shardRouting, reason);
                }
                return newClusterStateBuilder().state(currentState).routingResult(routingResult).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }
        });
    }

    private void innerShardStarted(final ShardRouting shardRouting, final String reason) {
        if (logger.isDebugEnabled()) {
            logger.debug("received shard started for {}, reason [{}]", shardRouting, reason);
        }
        // buffer shard started requests, and the state update tasks will simply drain it
        // this is to optimize the number of "started" events we generate, and batch them
        // possibly, we can do time based batching as well, but usually, we would want to
        // process started events as fast as possible, to make shards available
        startedShardsQueue.add(shardRouting);

        clusterService.submitStateUpdateTask("shard-started (" + shardRouting + "), reason [" + reason + "]", Priority.HIGH, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {

                List<ShardRouting> shards = new ArrayList<ShardRouting>();
                startedShardsQueue.drainTo(shards);

                // nothing to process (a previous event has process it already)
                if (shards.isEmpty()) {
                    return currentState;
                }

                RoutingTable routingTable = currentState.routingTable();

                for (int i = 0; i < shards.size(); i++) {
                    ShardRouting shardRouting = shards.get(i);
                    IndexRoutingTable indexRoutingTable = routingTable.index(shardRouting.index());
                    // if there is no routing table, the index has been deleted while it was being allocated
                    // which is fine, we should just ignore this
                    if (indexRoutingTable == null) {
                        shards.remove(i);
                    } else {
                        // find the one that maps to us, if its already started, no need to do anything...
                        // the shard might already be started since the nodes that is starting the shards might get cluster events
                        // with the shard still initializing, and it will try and start it again (until the verification comes)
                        IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardRouting.id());
                        for (ShardRouting entry : indexShardRoutingTable) {
                            if (shardRouting.currentNodeId().equals(entry.currentNodeId())) {
                                // we found the same shard that exists on the same node id
                                if (entry.started()) {
                                    // already started, do nothing here...
                                    shards.remove(i);
                                }
                            }
                        }
                    }
                }

                if (shards.isEmpty()) {
                    return currentState;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("applying started shards {}, reason [{}]", shards, reason);
                }

                RoutingAllocation.Result routingResult = allocationService.applyStartedShards(currentState, shards, true);
                if (!routingResult.changed()) {
                    return currentState;
                }
                return newClusterStateBuilder().state(currentState).routingResult(routingResult).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }
        });
    }

    private class ShardFailedTransportHandler extends BaseTransportRequestHandler<ShardRoutingEntry> {

        static final String ACTION = "cluster/shardFailure";

        @Override
        public ShardRoutingEntry newInstance() {
            return new ShardRoutingEntry();
        }

        @Override
        public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            innerShardFailed(request.shardRouting, request.reason);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    class ShardStartedTransportHandler extends BaseTransportRequestHandler<ShardRoutingEntry> {

        static final String ACTION = "cluster/shardStarted";

        @Override
        public ShardRoutingEntry newInstance() {
            return new ShardRoutingEntry();
        }

        @Override
        public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            innerShardStarted(request.shardRouting, request.reason);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class ShardRoutingEntry extends TransportRequest {

        private ShardRouting shardRouting;

        private String reason;

        private ShardRoutingEntry() {
        }

        private ShardRoutingEntry(ShardRouting shardRouting, String reason) {
            this.shardRouting = shardRouting;
            this.reason = reason;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardRouting = readShardRoutingEntry(in);
            reason = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardRouting.writeTo(out);
            out.writeString(reason);
        }
    }
}
