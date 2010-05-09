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
import org.elasticsearch.cluster.routing.strategy.ShardsRoutingStrategy;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.VoidTransportResponseHandler;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.io.stream.VoidStreamable;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.routing.ImmutableShardRouting.*;
import static org.elasticsearch.util.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardStateAction extends AbstractComponent {

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ShardsRoutingStrategy shardsRoutingStrategy;

    private final ThreadPool threadPool;

    @Inject public ShardStateAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                    ShardsRoutingStrategy shardsRoutingStrategy, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.shardsRoutingStrategy = shardsRoutingStrategy;
        this.threadPool = threadPool;

        transportService.registerHandler(ShardStartedTransportHandler.ACTION, new ShardStartedTransportHandler());
        transportService.registerHandler(ShardFailedTransportHandler.ACTION, new ShardFailedTransportHandler());
    }

    public void shardFailed(final ShardRouting shardRouting, final String reason) throws ElasticSearchException {
        logger.warn("Sending failed shard for {}, reason [{}]", shardRouting, reason);
        DiscoveryNodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    innerShardFailed(shardRouting, reason);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    ShardFailedTransportHandler.ACTION, new ShardRoutingEntry(shardRouting, reason), VoidTransportResponseHandler.INSTANCE);
        }
    }

    public void shardStarted(final ShardRouting shardRouting, final String reason) throws ElasticSearchException {
        if (logger.isDebugEnabled()) {
            logger.debug("Sending shard started for {}, reason [{}]", shardRouting, reason);
        }
        DiscoveryNodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    innerShardStarted(shardRouting, reason);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    ShardStartedTransportHandler.ACTION, new ShardRoutingEntry(shardRouting, reason), VoidTransportResponseHandler.INSTANCE);
        }
    }

    private void innerShardFailed(final ShardRouting shardRouting, final String reason) {
        logger.warn("Received shard failed for {}, reason [{}]", shardRouting, reason);
        clusterService.submitStateUpdateTask("shard-failed (" + shardRouting + "), reason [" + reason + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                IndexRoutingTable indexRoutingTable = routingTable.index(shardRouting.index());
                // if there is no routing table, the index has been deleted while it was being allocated
                // which is fine, we should just ignore this
                if (indexRoutingTable == null) {
                    return currentState;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Applying failed shard {}, reason [{}]", shardRouting, reason);
                }
                RoutingTable prevRoutingTable = currentState.routingTable();
                RoutingTable newRoutingTable = shardsRoutingStrategy.applyFailedShards(currentState, newArrayList(shardRouting));
                if (prevRoutingTable == newRoutingTable) {
                    return currentState;
                }
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).build();
            }
        });
    }

    private void innerShardStarted(final ShardRouting shardRouting, final String reason) {
        if (logger.isDebugEnabled()) {
            logger.debug("Received shard started for {}, reason [{}]", shardRouting, reason);
        }
        clusterService.submitStateUpdateTask("shard-started (" + shardRouting + "), reason [" + reason + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                IndexRoutingTable indexRoutingTable = routingTable.index(shardRouting.index());
                // if there is no routing table, the index has been deleted while it was being allocated
                // which is fine, we should just ignore this
                if (indexRoutingTable == null) {
                    return currentState;
                }
                // find the one that maps to us, if its already started, no need to do anything...
                // the shard might already be started since the nodes that is starting the shards might get cluster events
                // with the shard still initializing, and it will try and start it again (until the verification comes)
                IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardRouting.id());
                for (ShardRouting entry : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId().equals(entry.currentNodeId())) {
                        // we found the same shard that exists on the same node id
                        if (entry.started()) {
                            // already started, do nothing here...
                            return currentState;
                        }
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Applying started shard {}, reason [{}]", shardRouting, reason);
                }
                RoutingTable newRoutingTable = shardsRoutingStrategy.applyStartedShards(currentState, newArrayList(shardRouting));
                if (routingTable == newRoutingTable) {
                    return currentState;
                }
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).build();
            }
        });
    }

    private class ShardFailedTransportHandler extends BaseTransportRequestHandler<ShardRoutingEntry> {

        static final String ACTION = "cluster/shardFailure";

        @Override public ShardRoutingEntry newInstance() {
            return new ShardRoutingEntry();
        }

        @Override public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            innerShardFailed(request.shardRouting, request.reason);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    private class ShardStartedTransportHandler extends BaseTransportRequestHandler<ShardRoutingEntry> {

        static final String ACTION = "cluster/shardStarted";

        @Override public ShardRoutingEntry newInstance() {
            return new ShardRoutingEntry();
        }

        @Override public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            innerShardStarted(request.shardRouting, request.reason);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    private static class ShardRoutingEntry implements Streamable {

        private ShardRouting shardRouting;

        private String reason;

        private ShardRoutingEntry() {
        }

        private ShardRoutingEntry(ShardRouting shardRouting, String reason) {
            this.shardRouting = shardRouting;
            this.reason = reason;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            shardRouting = readShardRoutingEntry(in);
            reason = in.readUTF();
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            shardRouting.writeTo(out);
            out.writeUTF(reason);
        }
    }
}
