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

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
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

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;

/**
 *
 */
public class ShardStateAction extends AbstractComponent {

    public static final String SHARD_STARTED_ACTION_NAME = "internal:cluster/shard/started";
    public static final String SHARD_FAILED_ACTION_NAME = "internal:cluster/shard/failure";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final RoutingService routingService;

    private final BlockingQueue<ShardRoutingEntry> startedShardsQueue = ConcurrentCollections.newBlockingQueue();
    private final BlockingQueue<ShardRoutingEntry> failedShardQueue = ConcurrentCollections.newBlockingQueue();

    @Inject
    public ShardStateAction(Settings settings, ClusterService clusterService, TransportService transportService,
                            AllocationService allocationService, RoutingService routingService) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.allocationService = allocationService;
        this.routingService = routingService;

        transportService.registerRequestHandler(SHARD_STARTED_ACTION_NAME, ShardRoutingEntry.class, ThreadPool.Names.SAME, new ShardStartedTransportHandler());
        transportService.registerRequestHandler(SHARD_FAILED_ACTION_NAME, ShardRoutingEntry.class, ThreadPool.Names.SAME, new ShardFailedTransportHandler());
    }

    public void shardFailed(final ShardRouting shardRouting, final String indexUUID, final String reason) throws ElasticsearchException {
        DiscoveryNode masterNode = clusterService.state().nodes().masterNode();
        if (masterNode == null) {
            logger.warn("can't send shard failed for {}, no master known.", shardRouting);
            return;
        }
        innerShardFailed(shardRouting, indexUUID, reason, masterNode);
    }

    public void resendShardFailed(final ShardRouting shardRouting, final String indexUUID, final String reason, final DiscoveryNode masterNode) throws ElasticsearchException {
        logger.trace("{} re-sending failed shard for {}, indexUUID [{}], reason [{}]", shardRouting.shardId(), shardRouting, indexUUID, reason);
        innerShardFailed(shardRouting, indexUUID, reason, masterNode);
    }

    private void innerShardFailed(final ShardRouting shardRouting, final String indexUUID, final String reason, final DiscoveryNode masterNode) {
        ShardRoutingEntry shardRoutingEntry = new ShardRoutingEntry(shardRouting, indexUUID, reason);
        transportService.sendRequest(masterNode,
                SHARD_FAILED_ACTION_NAME, shardRoutingEntry, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn("failed to send failed shard to {}", exp, masterNode);
                    }
                });
    }

    public void shardStarted(final ShardRouting shardRouting, String indexUUID, final String reason) throws ElasticsearchException {
        DiscoveryNode masterNode = clusterService.state().nodes().masterNode();
        if (masterNode == null) {
            logger.warn("can't send shard started for {}. no master known.", shardRouting);
            return;
        }
        shardStarted(shardRouting, indexUUID, reason, masterNode);
    }

    public void shardStarted(final ShardRouting shardRouting, String indexUUID, final String reason, final DiscoveryNode masterNode) throws ElasticsearchException {

        ShardRoutingEntry shardRoutingEntry = new ShardRoutingEntry(shardRouting, indexUUID, reason);

        logger.debug("sending shard started for {}", shardRoutingEntry);

        transportService.sendRequest(masterNode,
                SHARD_STARTED_ACTION_NAME, new ShardRoutingEntry(shardRouting, indexUUID, reason), new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn("failed to send shard started to [{}]", exp, masterNode);
                    }

                });
    }

    private void handleShardFailureOnMaster(final ShardRoutingEntry shardRoutingEntry) {
        logger.warn("{} received shard failed for {}", shardRoutingEntry.shardRouting.shardId(), shardRoutingEntry);
        failedShardQueue.add(shardRoutingEntry);
        clusterService.submitStateUpdateTask("shard-failed (" + shardRoutingEntry.shardRouting + "), reason [" + shardRoutingEntry.reason + "]", Priority.HIGH, new ProcessedClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (shardRoutingEntry.processed) {
                    return currentState;
                }

                List<ShardRoutingEntry> shardRoutingEntries = new ArrayList<>();
                failedShardQueue.drainTo(shardRoutingEntries);

                // nothing to process (a previous event has processed it already)
                if (shardRoutingEntries.isEmpty()) {
                    return currentState;
                }

                MetaData metaData = currentState.getMetaData();

                List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(shardRoutingEntries.size());
                for (int i = 0; i < shardRoutingEntries.size(); i++) {
                    ShardRoutingEntry shardRoutingEntry = shardRoutingEntries.get(i);
                    shardRoutingEntry.processed = true;
                    ShardRouting shardRouting = shardRoutingEntry.shardRouting;
                    IndexMetaData indexMetaData = metaData.index(shardRouting.index());
                    // if there is no metadata or the current index is not of the right uuid, the index has been deleted while it was being allocated
                    // which is fine, we should just ignore this
                    if (indexMetaData == null) {
                        continue;
                    }
                    if (!indexMetaData.isSameUUID(shardRoutingEntry.indexUUID)) {
                        logger.debug("{} ignoring shard failed, different index uuid, current {}, got {}", shardRouting.shardId(), indexMetaData.getUUID(), shardRoutingEntry);
                        continue;
                    }

                    logger.debug("{} will apply shard failed {}", shardRouting.shardId(), shardRoutingEntry);
                    shardRoutingsToBeApplied.add(shardRouting);
                }

                RoutingAllocation.Result routingResult = allocationService.applyFailedShards(currentState, shardRoutingsToBeApplied);
                if (!routingResult.changed()) {
                    return currentState;
                }
                return ClusterState.builder(currentState).routingResult(routingResult).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState != newState && newState.getRoutingNodes().hasUnassigned()) {
                    logger.trace("unassigned shards after shard failures. scheduling a reroute.");
                    routingService.scheduleReroute();
                }
            }
        });
    }

    private void shardStartedOnMaster(final ShardRoutingEntry shardRoutingEntry) {
        logger.debug("received shard started for {}", shardRoutingEntry);
        // buffer shard started requests, and the state update tasks will simply drain it
        // this is to optimize the number of "started" events we generate, and batch them
        // possibly, we can do time based batching as well, but usually, we would want to
        // process started events as fast as possible, to make shards available
        startedShardsQueue.add(shardRoutingEntry);

        clusterService.submitStateUpdateTask("shard-started (" + shardRoutingEntry.shardRouting + "), reason [" + shardRoutingEntry.reason + "]", Priority.URGENT,
                new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {

                        if (shardRoutingEntry.processed) {
                            return currentState;
                        }

                        List<ShardRoutingEntry> shardRoutingEntries = new ArrayList<>();
                        startedShardsQueue.drainTo(shardRoutingEntries);

                        // nothing to process (a previous event has processed it already)
                        if (shardRoutingEntries.isEmpty()) {
                            return currentState;
                        }

                        RoutingTable routingTable = currentState.routingTable();
                        MetaData metaData = currentState.getMetaData();

                        List<ShardRouting> shardRoutingToBeApplied = new ArrayList<>(shardRoutingEntries.size());

                        for (int i = 0; i < shardRoutingEntries.size(); i++) {
                            ShardRoutingEntry shardRoutingEntry = shardRoutingEntries.get(i);
                            shardRoutingEntry.processed = true;
                            ShardRouting shardRouting = shardRoutingEntry.shardRouting;
                            try {
                                IndexMetaData indexMetaData = metaData.index(shardRouting.index());
                                IndexRoutingTable indexRoutingTable = routingTable.index(shardRouting.index());
                                // if there is no metadata, no routing table or the current index is not of the right uuid, the index has been deleted while it was being allocated
                                // which is fine, we should just ignore this
                                if (indexMetaData == null) {
                                    continue;
                                }
                                if (indexRoutingTable == null) {
                                    continue;
                                }

                                if (!indexMetaData.isSameUUID(shardRoutingEntry.indexUUID)) {
                                    logger.debug("{} ignoring shard started, different index uuid, current {}, got {}", shardRouting.shardId(), indexMetaData.getUUID(), shardRoutingEntry);
                                    continue;
                                }

                                // find the one that maps to us, if its already started, no need to do anything...
                                // the shard might already be started since the nodes that is starting the shards might get cluster events
                                // with the shard still initializing, and it will try and start it again (until the verification comes)

                                IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardRouting.id());

                                boolean applyShardEvent = true;

                                for (ShardRouting entry : indexShardRoutingTable) {
                                    if (shardRouting.currentNodeId().equals(entry.currentNodeId())) {
                                        // we found the same shard that exists on the same node id
                                        if (!entry.initializing()) {
                                            // shard is in initialized state, skipping event (probable already started)
                                            logger.debug("{} ignoring shard started event for {}, current state: {}", shardRouting.shardId(), shardRoutingEntry, entry.state());
                                            applyShardEvent = false;
                                        }
                                    }
                                }

                                if (applyShardEvent) {
                                    shardRoutingToBeApplied.add(shardRouting);
                                    logger.debug("{} will apply shard started {}", shardRouting.shardId(), shardRoutingEntry);
                                }

                            } catch (Throwable t) {
                                logger.error("{} unexpected failure while processing shard started [{}]", t, shardRouting.shardId(), shardRouting);
                            }
                        }

                        if (shardRoutingToBeApplied.isEmpty()) {
                            return currentState;
                        }

                        RoutingAllocation.Result routingResult = allocationService.applyStartedShards(currentState, shardRoutingToBeApplied, true);
                        if (!routingResult.changed()) {
                            return currentState;
                        }
                        return ClusterState.builder(currentState).routingResult(routingResult).build();
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.error("unexpected failure during [{}]", t, source);
                    }
                });
    }

    private class ShardFailedTransportHandler implements TransportRequestHandler<ShardRoutingEntry> {

        @Override
        public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            handleShardFailureOnMaster(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class ShardStartedTransportHandler implements TransportRequestHandler<ShardRoutingEntry> {

        @Override
        public void messageReceived(ShardRoutingEntry request, TransportChannel channel) throws Exception {
            shardStartedOnMaster(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    static class ShardRoutingEntry extends TransportRequest {

        private ShardRouting shardRouting;

        private String indexUUID = IndexMetaData.INDEX_UUID_NA_VALUE;

        private String reason;

        volatile boolean processed; // state field, no need to serialize

        ShardRoutingEntry() {
        }

        private ShardRoutingEntry(ShardRouting shardRouting, String indexUUID, String reason) {
            this.shardRouting = shardRouting;
            this.reason = reason;
            this.indexUUID = indexUUID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardRouting = readShardRoutingEntry(in);
            reason = in.readString();
            indexUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardRouting.writeTo(out);
            out.writeString(reason);
            out.writeString(indexUUID);
        }

        @Override
        public String toString() {
            return "" + shardRouting + ", indexUUID [" + indexUUID + "], reason [" + reason + "]";
        }
    }
}
